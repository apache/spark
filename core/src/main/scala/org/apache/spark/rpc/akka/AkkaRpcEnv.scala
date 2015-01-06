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

import akka.actor.{ActorRef, Actor, Props, ActorSystem}
import akka.pattern.{ask => akkaAsk}
import akka.remote._

import org.apache.spark.{Logging, SparkException, SparkConf}
import org.apache.spark.rpc._
import org.apache.spark.util.{ActorLogReceive, AkkaUtils}

import scala.reflect.ClassTag
import scala.util.control.NonFatal

class AkkaRpcEnv(val actorSystem: ActorSystem, conf: SparkConf) extends RpcEnv {
  // TODO Once finishing the new Rpc mechanism, make actorSystem be a private val

  override def setupEndpoint(name: String, endpointCreator: => RpcEndpoint): RpcEndpointRef = {
    val latch = new CountDownLatch(1)
    try {
      @volatile var endpointRef: AkkaRpcEndpointRef = null
      val actorRef = actorSystem.actorOf(Props(new Actor with ActorLogReceive with Logging {

        val endpoint = endpointCreator
        latch.await()
        require(endpointRef != null)
        registerEndpoint(endpoint, endpointRef)

        override def preStart(): Unit = {
          endpoint.onStart()
          if (endpoint.isInstanceOf[NetworkRpcEndpoint]) {
            // Listen for remote client disconnection events,
            // since they don't go through Akka's watch()
            context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
          }
        }

        override def receiveWithLogging: Receive = {
          case AssociatedEvent(_, remoteAddress, _) =>
            try {
              endpoint.asInstanceOf[NetworkRpcEndpoint].
                onConnected(AkkaUtils.akkaAddressToRpcAddress(remoteAddress))
            } catch {
              case NonFatal(e) => endpoint.onError(e)
            }

          case DisassociatedEvent(_, remoteAddress, _) =>
            try {
              endpoint.asInstanceOf[NetworkRpcEndpoint].
                onDisconnected(AkkaUtils.akkaAddressToRpcAddress(remoteAddress))
            } catch {
              case NonFatal(e) => endpoint.onError(e)
            }

          case AssociationErrorEvent(cause, localAddress, remoteAddress, inbound, _) =>
            try {
              endpoint.asInstanceOf[NetworkRpcEndpoint].
                onNetworkError(cause, AkkaUtils.akkaAddressToRpcAddress(remoteAddress))
            } catch {
              case NonFatal(e) => endpoint.onError(e)
            }
          case e: RemotingLifecycleEvent =>
            // ignore?

          case message: Any =>
            try {
              logInfo("Received RPC message: " + message)
              val pf = endpoint.receive(new AkkaRpcEndpointRef(sender(), conf))
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
      endpointRef = new AkkaRpcEndpointRef(actorRef, conf)
      endpointRef
    } finally {
      latch.countDown()
    }
  }

  override def setupDriverEndpointRef(name: String): RpcEndpointRef = {
    new AkkaRpcEndpointRef(AkkaUtils.makeDriverRef(name, conf, actorSystem), conf)
  }

  override def setupEndpointRefByUrl(url: String): RpcEndpointRef = {
    val timeout = Duration.create(conf.getLong("spark.akka.lookupTimeout", 30), "seconds")
    val ref = Await.result(actorSystem.actorSelection(url).resolveOne(timeout), timeout)
    new AkkaRpcEndpointRef(ref, conf)
  }

  override def stopAll(): Unit = {
    // Do nothing since actorSystem was created outside.
  }

  override def stop(endpoint: RpcEndpointRef): Unit = {
    require(endpoint.isInstanceOf[AkkaRpcEndpointRef])
    unregisterEndpoint(endpoint)
    actorSystem.stop(endpoint.asInstanceOf[AkkaRpcEndpointRef].actorRef)
  }

  override def toString = s"${getClass.getSimpleName}($actorSystem)"
}

private[akka] class AkkaRpcEndpointRef(val actorRef: ActorRef, @transient conf: SparkConf)
  extends RpcEndpointRef with Serializable with Logging {

  private[this] val maxRetries = conf.getInt("spark.akka.num.retries", 3)
  private[this] val retryWaitMs = conf.getInt("spark.akka.retry.wait", 3000)
  private[this] val defaultTimeout = conf.getLong("spark.akka.lookupTimeout", 30) seconds

  override val address: RpcAddress = AkkaUtils.akkaAddressToRpcAddress(actorRef.path.address)

  override def ask[T: ClassTag](message: Any): Future[T] = ask(message, defaultTimeout)

  override def ask[T: ClassTag](message: Any, timeout: FiniteDuration): Future[T] = {
    actorRef.ask(message)(timeout).mapTo[T]
  }

  override def askWithReply[T](message: Any): T = askWithReply(message, defaultTimeout)

  override def askWithReply[T](message: Any, timeout: FiniteDuration): T = {
    var attempts = 0
    var lastException: Exception = null
    while (attempts < maxRetries) {
      attempts += 1
      try {
        val future = actorRef.ask(message)(timeout)
        val result = Await.result(future, timeout)
        if (result == null) {
          throw new SparkException("Actor returned null")
        }
        return result.asInstanceOf[T]
      } catch {
        case ie: InterruptedException => throw ie
        case e: Exception =>
          lastException = e
          logWarning("Error sending message in " + attempts + " attempts", e)
      }
      Thread.sleep(retryWaitMs)
    }

    throw new SparkException(
      "Error sending message [message = " + message + "]", lastException)
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
