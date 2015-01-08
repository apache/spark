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

import java.util.concurrent.CountDownLatch

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.language.postfixOps
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import _root_.akka.actor._
import akka.pattern.{ask => akkaAsk}
import akka.remote._
import com.google.common.annotations.VisibleForTesting

import org.apache.spark.{Logging, SparkConf}
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
    val actorSystem: ActorSystem, conf: SparkConf, val boundPort: Int) extends RpcEnv {

  private val defaultAddress: RpcAddress = {
    val address = actorSystem.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
    AkkaUtils.akkaAddressToRpcAddress(address)
  }

  override def setupEndpoint(name: String, endpointCreator: => RpcEndpoint): RpcEndpointRef = {
    val latch = new CountDownLatch(1)
    try {
      @volatile var endpointRef: AkkaRpcEndpointRef = null
      val actorRef = actorSystem.actorOf(Props(new Actor with ActorLogReceive with Logging {

        val endpoint = endpointCreator
        // Wait until `endpointRef` is set. TODO better solution?
        latch.await()
        require(endpointRef != null)
        registerEndpoint(endpoint, endpointRef)

        var isNetworkRpcEndpoint = false

        override def preStart(): Unit = {
          if (endpoint.isInstanceOf[NetworkRpcEndpoint]) {
            isNetworkRpcEndpoint = true
            // Listen for remote client network events only when it's `NetworkRpcEndpoint`
            context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
          }
          endpoint.onStart()
        }

        override def receiveWithLogging: Receive = if (isNetworkRpcEndpoint) {
          case AssociatedEvent(_, remoteAddress, _) =>
            try {
              endpoint.asInstanceOf[NetworkRpcEndpoint].
                onConnected(akkaAddressToRpcAddress(remoteAddress))
            } catch {
              case NonFatal(e) => endpoint.onError(e)
            }

          case DisassociatedEvent(_, remoteAddress, _) =>
            try {
              endpoint.asInstanceOf[NetworkRpcEndpoint].
                onDisconnected(akkaAddressToRpcAddress(remoteAddress))
            } catch {
              case NonFatal(e) => endpoint.onError(e)
            }

          case AssociationErrorEvent(cause, localAddress, remoteAddress, inbound, _) =>
            try {
              endpoint.asInstanceOf[NetworkRpcEndpoint].
                onNetworkError(cause, akkaAddressToRpcAddress(remoteAddress))
            } catch {
              case NonFatal(e) => endpoint.onError(e)
            }
          case e: RemotingLifecycleEvent =>
          // TODO ignore?

          case message: Any =>
            logDebug("Received RPC message: " + message)
            try {
              val pf = endpoint.receive(new AkkaRpcEndpointRef(defaultAddress, sender(), conf))
              if (pf.isDefinedAt(message)) {
                pf.apply(message)
              }
            } catch {
              case NonFatal(e) => endpoint.onError(e)
            }
        } else {
          case message: Any =>
            logDebug("Received RPC message: " + message)
            try {
              val pf = endpoint.receive(new AkkaRpcEndpointRef(defaultAddress, sender(), conf))
              if (pf.isDefinedAt(message)) {
                pf.apply(message)
              }
            } catch {
              case NonFatal(e) => endpoint.onError(e)
            }
        }

        override def postStop(): Unit = {
          endpoint.onStop()
        }

        }), name = name)
      endpointRef = new AkkaRpcEndpointRef(defaultAddress, actorRef, conf)
      endpointRef
    } finally {
      latch.countDown()
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
    new AkkaRpcEndpointRef(defaultAddress, ref, conf)
  }

  override def setupEndpointRef(
      systemName: String, address: RpcAddress, endpointName: String): RpcEndpointRef = {
    setupEndpointRefByUrl(
      "akka.tcp://%s@%s:%s/user/%s".format(systemName, address.host, address.port, endpointName))
  }

  override def stopAll(): Unit = {
    actorSystem.shutdown()
  }

  override def stop(endpoint: RpcEndpointRef): Unit = {
    require(endpoint.isInstanceOf[AkkaRpcEndpointRef])
    unregisterEndpoint(endpoint)
    actorSystem.stop(endpoint.asInstanceOf[AkkaRpcEndpointRef].actorRef)
  }

  override def awaitTermination(): Unit = {
    actorSystem.awaitTermination()
  }

  override def toString = s"${getClass.getSimpleName}($actorSystem)"
}

private[rpc] object AkkaRpcEnv {

  def apply(config: RpcEnvConfig): RpcEnv = {
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(
        config.name, config.host, config.port, config.conf, config.securityManager)
    new AkkaRpcEnv(actorSystem, config.conf, boundPort)
  }

  // TODO Remove it
  @VisibleForTesting
  def apply(name: String, conf: SparkConf): AkkaRpcEnv = {
    new AkkaRpcEnv(ActorSystem(name), conf, -1)
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

  override def ask[T: ClassTag](message: Any): Future[T] = ask(message, defaultTimeout)

  override def ask[T: ClassTag](message: Any, timeout: FiniteDuration): Future[T] = {
    actorRef.ask(message)(timeout).mapTo[T]
  }

  override def askWithReply[T](message: Any): T = askWithReply(message, defaultTimeout)

  override def askWithReply[T](message: Any, timeout: FiniteDuration): T = {
    // TODO: Consider removing multiple attempts
    AkkaUtils.askWithReply(message, actorRef, maxRetries, retryWaitMs, timeout)
  }

  override def send(message: Any)(implicit sender: RpcEndpointRef = RpcEndpoint.noSender): Unit = {
    implicit val actorSender: ActorRef =
      if (sender == null) {
        Actor.noSender
      } else {
        require(sender.isInstanceOf[AkkaRpcEndpointRef])
        sender.asInstanceOf[AkkaRpcEndpointRef].actorRef
      }
    actorRef ! message
  }

  override def toString: String = s"${getClass.getSimpleName}($actorRef)"
}
