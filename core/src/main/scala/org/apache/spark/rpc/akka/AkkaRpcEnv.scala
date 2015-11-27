/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rpc.akka

import java.io.File
import java.nio.channels.ReadableByteChannel
import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.Future
import scala.language.postfixOps
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import akka.actor.{ActorSystem, ExtendedActorSystem, Actor, ActorRef, Props, Address}
import akka.event.Logging.Error
import akka.pattern.{ask => akkaAsk}
import akka.remote.{AssociationEvent, AssociatedEvent, DisassociatedEvent, AssociationErrorEvent}
import akka.serialization.JavaSerializer

import org.apache.spark.{HttpFileServer, Logging, SecurityManager, SparkConf, SparkException}
import org.apache.spark.rpc._
import org.apache.spark.util.{ActorLogReceive, AkkaUtils, ThreadUtils}

/**
 * A RpcEnv implementation based on Akka.
 *
 * TODO Once we remove all usages of Akka in other place, we can move this file to a new project and
 * remove Akka from the dependencies.
 */
private[spark] class AkkaRpcEnv private[akka] (
    val actorSystem: ActorSystem,
    val securityManager: SecurityManager,
    conf: SparkConf,
    boundPort: Int)
  extends RpcEnv(conf) with Logging {

  private val defaultAddress: RpcAddress = {
    val address = actorSystem.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
    // In some test case, ActorSystem doesn't bind to any address.
    // So just use some default value since they are only some unit tests
    RpcAddress(address.host.getOrElse("localhost"), address.port.getOrElse(boundPort))
  }

  override val address: RpcAddress = defaultAddress

  /**
   * A lookup table to search a [[RpcEndpointRef]] for a [[RpcEndpoint]]. We need it to make
   * [[RpcEndpoint.self]] work.
   */
  private val endpointToRef = new ConcurrentHashMap[RpcEndpoint, RpcEndpointRef]()

  /**
   * Need this map to remove `RpcEndpoint` from `endpointToRef` via a `RpcEndpointRef`
   */
  private val refToEndpoint = new ConcurrentHashMap[RpcEndpointRef, RpcEndpoint]()

  private val _fileServer = new AkkaFileServer(conf, securityManager)

  private def registerEndpoint(endpoint: RpcEndpoint, endpointRef: RpcEndpointRef): Unit = {
    endpointToRef.put(endpoint, endpointRef)
    refToEndpoint.put(endpointRef, endpoint)
  }

  private def unregisterEndpoint(endpointRef: RpcEndpointRef): Unit = {
    val endpoint = refToEndpoint.remove(endpointRef)
    if (endpoint != null) {
      endpointToRef.remove(endpoint)
    }
  }

  /**
   * Retrieve the [[RpcEndpointRef]] of `endpoint`.
   */
  override def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef = endpointToRef.get(endpoint)

  override def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef = {
    @volatile var endpointRef: AkkaRpcEndpointRef = null
    // Use defered function because the Actor needs to use `endpointRef`.
    // So `actorRef` should be created after assigning `endpointRef`.
    val actorRef = () => actorSystem.actorOf(Props(new Actor with ActorLogReceive with Logging {

      assert(endpointRef != null)

      override def preStart(): Unit = {
        // Listen for remote client network events
        context.system.eventStream.subscribe(self, classOf[AssociationEvent])
        safelyCall(endpoint) {
          endpoint.onStart()
        }
      }

      override def receiveWithLogging: Receive = {
        case AssociatedEvent(_, remoteAddress, _) =>
          safelyCall(endpoint) {
            endpoint.onConnected(akkaAddressToRpcAddress(remoteAddress))
          }

        case DisassociatedEvent(_, remoteAddress, _) =>
          safelyCall(endpoint) {
            endpoint.onDisconnected(akkaAddressToRpcAddress(remoteAddress))
          }

        case AssociationErrorEvent(cause, localAddress, remoteAddress, inbound, _) =>
          safelyCall(endpoint) {
            endpoint.onNetworkError(cause, akkaAddressToRpcAddress(remoteAddress))
          }

        case e: AssociationEvent =>
          // TODO ignore?

        case m: AkkaMessage =>
          logDebug(s"Received RPC message: $m")
          safelyCall(endpoint) {
            processMessage(endpoint, m, sender)
          }

        case AkkaFailure(e) =>
          safelyCall(endpoint) {
            throw e
          }

        case message: Any => {
          logWarning(s"Unknown message: $message")
        }

      }

      override def postStop(): Unit = {
        unregisterEndpoint(endpoint.self)
        safelyCall(endpoint) {
          endpoint.onStop()
        }
      }

      }), name = name)
    endpointRef = new AkkaRpcEndpointRef(defaultAddress, actorRef, conf, initInConstructor = false)
    registerEndpoint(endpoint, endpointRef)
    // Now actorRef can be created safely
    endpointRef.init()
    endpointRef
  }

  private def processMessage(endpoint: RpcEndpoint, m: AkkaMessage, _sender: ActorRef): Unit = {
    val message = m.message
    val needReply = m.needReply
    val pf: PartialFunction[Any, Unit] =
      if (needReply) {
        endpoint.receiveAndReply(new RpcCallContext {
          override def sendFailure(e: Throwable): Unit = {
            _sender ! AkkaFailure(e)
          }

          override def reply(response: Any): Unit = {
            _sender ! AkkaMessage(response, false)
          }

          // Use "lazy" because most of RpcEndpoints don't need "senderAddress"
          override lazy val senderAddress: RpcAddress =
            new AkkaRpcEndpointRef(defaultAddress, _sender, conf).address
        })
      } else {
        endpoint.receive
      }
    try {
      pf.applyOrElse[Any, Unit](message, { message =>
        throw new SparkException(s"Unmatched message $message from ${_sender}")
      })
    } catch {
      case NonFatal(e) =>
        _sender ! AkkaFailure(e)
        if (!needReply) {
          // If the sender does not require a reply, it may not handle the exception. So we rethrow
          // "e" to make sure it will be processed.
          throw e
        }
    }
  }

  /**
   * Run `action` safely to avoid to crash the thread. If any non-fatal exception happens, it will
   * call `endpoint.onError`. If `endpoint.onError` throws any non-fatal exception, just log it.
   */
  private def safelyCall(endpoint: RpcEndpoint)(action: => Unit): Unit = {
    try {
      action
    } catch {
      case NonFatal(e) => {
        try {
          endpoint.onError(e)
        } catch {
          case NonFatal(e) => logError(s"Ignore error: ${e.getMessage}", e)
        }
      }
    }
  }

  private def akkaAddressToRpcAddress(address: Address): RpcAddress = {
    RpcAddress(address.host.getOrElse(defaultAddress.host),
      address.port.getOrElse(defaultAddress.port))
  }

  override def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef] = {
    import actorSystem.dispatcher
    actorSystem.actorSelection(uri).resolveOne(defaultLookupTimeout.duration).
      map(new AkkaRpcEndpointRef(defaultAddress, _, conf)).
      // this is just in case there is a timeout from creating the future in resolveOne, we want the
      // exception to indicate the conf that determines the timeout
      recover(defaultLookupTimeout.addMessageIfTimeout)
  }

  override def uriOf(systemName: String, address: RpcAddress, endpointName: String): String = {
    AkkaUtils.address(
      AkkaUtils.protocol(actorSystem), systemName, address.host, address.port, endpointName)
  }

  override def shutdown(): Unit = {
    actorSystem.shutdown()
    _fileServer.shutdown()
  }

  override def stop(endpoint: RpcEndpointRef): Unit = {
    require(endpoint.isInstanceOf[AkkaRpcEndpointRef])
    actorSystem.stop(endpoint.asInstanceOf[AkkaRpcEndpointRef].actorRef)
  }

  override def awaitTermination(): Unit = {
    actorSystem.awaitTermination()
  }

  override def toString: String = s"${getClass.getSimpleName}($actorSystem)"

  override def deserialize[T](deserializationAction: () => T): T = {
    JavaSerializer.currentSystem.withValue(actorSystem.asInstanceOf[ExtendedActorSystem]) {
      deserializationAction()
    }
  }

  override def openChannel(uri: String): ReadableByteChannel = {
    throw new UnsupportedOperationException(
      "AkkaRpcEnv's files should be retrieved using an HTTP client.")
  }

  override def fileServer: RpcEnvFileServer = _fileServer

}

private[akka] class AkkaFileServer(
    conf: SparkConf,
    securityManager: SecurityManager) extends RpcEnvFileServer {

  @volatile private var httpFileServer: HttpFileServer = _

  override def addFile(file: File): String = {
    getFileServer().addFile(file)
  }

  override def addJar(file: File): String = {
    getFileServer().addJar(file)
  }

  def shutdown(): Unit = {
    if (httpFileServer != null) {
      httpFileServer.stop()
    }
  }

  private def getFileServer(): HttpFileServer = {
    if (httpFileServer == null) synchronized {
      if (httpFileServer == null) {
        httpFileServer = startFileServer()
      }
    }
    httpFileServer
  }

  private def startFileServer(): HttpFileServer = {
    val fileServerPort = conf.getInt("spark.fileserver.port", 0)
    val server = new HttpFileServer(conf, securityManager, fileServerPort)
    server.initialize()
    server
  }

}

private[spark] class AkkaRpcEnvFactory extends RpcEnvFactory {

  def create(config: RpcEnvConfig): RpcEnv = {
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(
      config.name, config.host, config.port, config.conf, config.securityManager)
    actorSystem.actorOf(Props(classOf[ErrorMonitor]), "ErrorMonitor")
    new AkkaRpcEnv(actorSystem, config.securityManager, config.conf, boundPort)
  }
}

/**
 * Monitor errors reported by Akka and log them.
 */
private[akka] class ErrorMonitor extends Actor with ActorLogReceive with Logging {

  override def preStart(): Unit = {
    context.system.eventStream.subscribe(self, classOf[Error])
  }

  override def receiveWithLogging: Actor.Receive = {
    case Error(cause: Throwable, _, _, message: String) => logDebug(message, cause)
  }
}

private[akka] class AkkaRpcEndpointRef(
    @transient private val defaultAddress: RpcAddress,
    @transient private val _actorRef: () => ActorRef,
    conf: SparkConf,
    initInConstructor: Boolean)
  extends RpcEndpointRef(conf) with Logging {

  def this(
      defaultAddress: RpcAddress,
      _actorRef: ActorRef,
      conf: SparkConf) = {
    this(defaultAddress, () => _actorRef, conf, true)
  }

  lazy val actorRef = _actorRef()

  override lazy val address: RpcAddress = {
    val akkaAddress = actorRef.path.address
    RpcAddress(akkaAddress.host.getOrElse(defaultAddress.host),
      akkaAddress.port.getOrElse(defaultAddress.port))
  }

  override lazy val name: String = actorRef.path.name

  private[akka] def init(): Unit = {
    // Initialize the lazy vals
    actorRef
    address
    name
  }

  if (initInConstructor) {
    init()
  }

  override def send(message: Any): Unit = {
    actorRef ! AkkaMessage(message, false)
  }

  override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
    actorRef.ask(AkkaMessage(message, true))(timeout.duration).flatMap {
      // The function will run in the calling thread, so it should be short and never block.
      case msg @ AkkaMessage(message, reply) =>
        if (reply) {
          logError(s"Receive $msg but the sender cannot reply")
          Future.failed(new SparkException(s"Receive $msg but the sender cannot reply"))
        } else {
          Future.successful(message)
        }
      case AkkaFailure(e) =>
        Future.failed(e)
    }(ThreadUtils.sameThread).mapTo[T].
    recover(timeout.addMessageIfTimeout)(ThreadUtils.sameThread)
  }

  override def toString: String = s"${getClass.getSimpleName}($actorRef)"

  final override def equals(that: Any): Boolean = that match {
    case other: AkkaRpcEndpointRef => actorRef == other.actorRef
    case _ => false
  }

  final override def hashCode(): Int = if (actorRef == null) 0 else actorRef.hashCode()
}

/**
 * A wrapper to `message` so that the receiver knows if the sender expects a reply.
 * @param message
 * @param needReply if the sender expects a reply message
 */
private[akka] case class AkkaMessage(message: Any, needReply: Boolean)

/**
 * A reply with the failure error from the receiver to the sender
 */
private[akka] case class AkkaFailure(e: Throwable)
