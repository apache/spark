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

import java.net.URI
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.language.postfixOps
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import akka.actor.{ActorSystem, ExtendedActorSystem, Actor, ActorRef, Props, Address}
import akka.pattern.{ask => akkaAsk}
import akka.remote.{AssociatedEvent => AkkaAssociatedEvent}
import akka.remote.{DisassociatedEvent => AkkaDisassociatedEvent}
import akka.remote.{AssociationErrorEvent, RemotingLifecycleEvent}
import org.apache.spark.{SparkException, Logging, SparkConf}
import org.apache.spark.rpc._
import org.apache.spark.util.{ActorLogReceive, AkkaUtils}

/**
 * A RpcEnv implementation based on Akka.
 *
 * TODO Once we remove all usages of Akka in other place, we can move this file to a new project and
 * remove Akka from the dependencies.
 *
 * @param actorSystem
 * @param conf
 * @param boundPort
 */
private[spark] class AkkaRpcEnv private (
    val actorSystem: ActorSystem, conf: SparkConf, boundPort: Int) extends RpcEnv with Logging {

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

  override val scheduler = new ActionSchedulerImpl(conf)

  /**
   * Need this map to remove `RpcEndpoint` from `endpointToRef` via a `RpcEndpointRef`
   */
  private val refToEndpoint = new ConcurrentHashMap[RpcEndpointRef, RpcEndpoint]()

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
  override def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef = {
    val endpointRef = endpointToRef.get(endpoint)
    require(endpointRef != null, s"Cannot find RpcEndpointRef of ${endpoint} in ${this}")
    endpointRef
  }

  override def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef = {
    setupThreadSafeEndpoint(name, endpoint)
  }

  override def setupThreadSafeEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef = {
    val latch = new CountDownLatch(1)
    try {
      @volatile var endpointRef: AkkaRpcEndpointRef = null
      val actorRef = actorSystem.actorOf(Props(new Actor with ActorLogReceive with Logging {

        // Wait until `endpointRef` is set. TODO better solution?
        latch.await()
        require(endpointRef != null)
        registerEndpoint(endpoint, endpointRef)

        override def preStart(): Unit = {
          // Listen for remote client network events
          context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
          safelyCall(endpoint) {
            endpoint.onStart()
          }
        }

        override def receiveWithLogging: Receive = {
          case AkkaAssociatedEvent(_, remoteAddress, _) =>
            safelyCall(endpoint) {
              val event = AssociatedEvent(akkaAddressToRpcAddress(remoteAddress))
              val pf = endpoint.receive
              if (pf.isDefinedAt(event)) {
                pf.apply(event)
              }
            }

          case AkkaDisassociatedEvent(_, remoteAddress, _) =>
            safelyCall(endpoint) {
              val event = DisassociatedEvent(akkaAddressToRpcAddress(remoteAddress))
              val pf = endpoint.receive
              if (pf.isDefinedAt(event)) {
                pf.apply(event)
              }
            }

          case AssociationErrorEvent(cause, localAddress, remoteAddress, inbound, _) =>
            safelyCall(endpoint) {
              val event = NetworkErrorEvent(akkaAddressToRpcAddress(remoteAddress), cause)
              val pf = endpoint.receive
              if (pf.isDefinedAt(event)) {
                pf.apply(event)
              }
            }
          case e: RemotingLifecycleEvent =>
          // TODO ignore?

          case AkkaMessage(message: Any, reply: Boolean)=>
            logDebug("Received RPC message: " + AkkaMessage(message, reply))
            safelyCall(endpoint) {
              val s = sender()
              val pf =
                if (reply) {
                  endpoint.receiveAndReply(new RpcCallContext {
                    override def fail(e: Throwable): Unit = {
                      s ! AkkaFailure(e)
                    }

                    override def reply(response: Any): Unit = {
                      s ! AkkaMessage(response, false)
                    }

                    override def replyWithSender(response: Any, sender: RpcEndpointRef): Unit = {
                      s.!(AkkaMessage(response, true))(
                        sender.asInstanceOf[AkkaRpcEndpointRef].actorRef)
                    }
                  })
                } else {
                  endpoint.receive
                }
              try {
                if (pf.isDefinedAt(message)) {
                  pf.apply(message)
                }
              } catch {
                case NonFatal(e) =>
                  if (reply) {
                    // If the sender asks a reply, we should send the error back to the sender
                    s ! AkkaFailure(e)
                  } else {
                    throw e
                  }
              }
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
      endpointRef = new AkkaRpcEndpointRef(defaultAddress, actorRef, conf)
      endpointRef
    } finally {
      latch.countDown()
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

  override def setupDriverEndpointRef(name: String): RpcEndpointRef = {
    new AkkaRpcEndpointRef(defaultAddress, AkkaUtils.makeDriverRef(name, conf, actorSystem), conf)
  }

  override def setupEndpointRefByUrl(url: String): RpcEndpointRef = {
    val timeout = Duration.create(conf.getLong("spark.akka.lookupTimeout", 30), "seconds")
    val ref = Await.result(actorSystem.actorSelection(url).resolveOne(timeout), timeout)
    // TODO defaultAddress is wrong
    new AkkaRpcEndpointRef(defaultAddress, ref, conf)
  }

  override def setupEndpointRef(
      systemName: String, address: RpcAddress, endpointName: String): RpcEndpointRef = {
    setupEndpointRefByUrl(uriOf(systemName, address, endpointName))
  }

  override def uriOf(systemName: String, address: RpcAddress, endpointName: String): String = {
    AkkaUtils.address(
      AkkaUtils.protocol(actorSystem), systemName, address.host, address.port, endpointName)
  }


  override def shutdown(): Unit = {
    actorSystem.shutdown()
  }

  override def stop(endpoint: RpcEndpointRef): Unit = {
    require(endpoint.isInstanceOf[AkkaRpcEndpointRef])
    actorSystem.stop(endpoint.asInstanceOf[AkkaRpcEndpointRef].actorRef)
  }

  override def awaitTermination(): Unit = {
    actorSystem.awaitTermination()
  }

  override def toString = s"${getClass.getSimpleName}($actorSystem)"
}

private[spark] object AkkaRpcEnv {

  def apply(config: RpcEnvConfig): RpcEnv = {
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(
        config.name, config.host, config.port, config.conf, config.securityManager)
    new AkkaRpcEnv(actorSystem, config.conf, boundPort)
  }

}

private[akka] class AkkaRpcEndpointRef(
    @transient defaultAddress: RpcAddress,
    val actorRef: ActorRef,
    @transient conf: SparkConf) extends RpcEndpointRef with Serializable with Logging {
  // `defaultAddress` and `conf` won't be used after initialization. So it's safe to be transient.

  private[this] val maxRetries = conf.getInt("spark.akka.num.retries", 3)
  private[this] val retryWaitMs = conf.getLong("spark.akka.retry.wait", 3000)
  private[this] val defaultTimeout = conf.getLong("spark.akka.lookupTimeout", 30) seconds

  override val address: RpcAddress = {
    val akkaAddress = actorRef.path.address
    RpcAddress(akkaAddress.host.getOrElse(defaultAddress.host),
      akkaAddress.port.getOrElse(defaultAddress.port))
  }

  override val name: String = actorRef.path.name

  override def askWithReply[T: ClassTag](message: Any): T = askWithReply(message, defaultTimeout)

  override def askWithReply[T: ClassTag](message: Any, timeout: FiniteDuration): T = {
    // TODO: Consider removing multiple attempts
    var attempts = 0
    var lastException: Exception = null
    while (attempts < maxRetries) {
      attempts += 1
      try {
        val future = sendWithReply[T](message, timeout)
        val result = Await.result(future, timeout)
        if (result == null) {
          throw new SparkException("Actor returned null")
        }
        return result
      } catch {
        case ie: InterruptedException => throw ie
        case e: Exception =>
          lastException = e
          logWarning(s"Error sending message [message = $message] in $attempts attempts", e)
      }
      Thread.sleep(retryWaitMs)
    }

    throw new SparkException(
      s"Error sending message [message = $message]", lastException)
  }

  override def send(message: Any): Unit = {
    actorRef ! AkkaMessage(message, false)
  }

  override def sendWithReply(message: Any, sender: RpcEndpointRef): Unit = {
    implicit val actorSender: ActorRef =
      if (sender == null) {
        Actor.noSender
      } else {
        require(sender.isInstanceOf[AkkaRpcEndpointRef])
        sender.asInstanceOf[AkkaRpcEndpointRef].actorRef
      }
    actorRef ! AkkaMessage(message, true)
  }


  override def sendWithReply[T: ClassTag](message: Any): Future[T] = {
    sendWithReply(message, defaultTimeout)
  }

  override def sendWithReply[T: ClassTag](message: Any, timeout: FiniteDuration): Future[T] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    actorRef.ask(AkkaMessage(message, true))(timeout).flatMap {
      case AkkaMessage(message, reply) =>
        if (reply) {
          Future.failed(new SparkException("The sender cannot reply"))
        } else {
          Future.successful(message)
        }
      case AkkaFailure(e) =>
        Future.failed(e)
    }.mapTo[T]
  }

  override def toString: String = s"${getClass.getSimpleName}($actorRef)"

  override def toURI: URI = new URI(actorRef.path.toString)
}

/**
 * A wrapper to `message` so that the receiver knows if the sender expects a reply.
 * @param message
 * @param reply if the sender expects a reply message
 */
private[akka] case class AkkaMessage(message: Any, reply: Boolean)

/**
 * A reply with the failure error from the receiver to the sender
 */
private[akka] case class AkkaFailure(e: Throwable)
