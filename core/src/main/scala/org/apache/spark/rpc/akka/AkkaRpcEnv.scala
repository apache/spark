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

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import akka.actor.{ActorRef, Actor, Props, ActorSystem}
import akka.pattern.ask
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}

import org.apache.spark.{Logging, SparkException, SparkConf}
import org.apache.spark.rpc._
import org.apache.spark.util.AkkaUtils

class AkkaRpcEnv(actorSystem: ActorSystem, conf: SparkConf) extends RpcEnv {

  override def setupEndPoint(name: String, endPoint: RpcEndPoint): RpcEndPointRef = {
    val actorRef = actorSystem.actorOf(Props(new Actor {
      override def preStart(): Unit = {
        // Listen for remote client disconnection events, since they don't go through Akka's watch()
        context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
      }

      override def receive: Receive = {
        case DisassociatedEvent(_, remoteAddress, _) =>
          endPoint.remoteConnectionTerminated(remoteAddress.toString)

        case message: Any =>
          endPoint.logMessage.trace("Received RPC message: " + message)
          val pf = endPoint.receive(new AkkaRpcEndPointRef(sender(), conf))
          if (pf.isDefinedAt(message)) {
            pf.apply(message)
          }
      }
    }), name = name)
    val endPointRef = new AkkaRpcEndPointRef(actorRef, conf)
    registerEndPoint(endPoint, endPointRef)
    endPointRef
  }

  override def setupDriverEndPointRef(name: String): RpcEndPointRef = {
    new AkkaRpcEndPointRef(AkkaUtils.makeDriverRef(name, conf, actorSystem), conf)
  }

  override def setupEndPointRefByUrl(url: String): RpcEndPointRef = {
    val timeout = Duration.create(conf.getLong("spark.akka.lookupTimeout", 30), "seconds")
    val ref = Await.result(actorSystem.actorSelection(url).resolveOne(timeout), timeout)
    new AkkaRpcEndPointRef(ref, conf)
  }

  override def stopAll(): Unit = {
    // Do nothing since actorSystem was created outside.
  }

  override def stop(endPoint: RpcEndPointRef): Unit = {
    require(endPoint.isInstanceOf[AkkaRpcEndPointRef])
    unregisterEndPoint(endPoint)
    actorSystem.stop(endPoint.asInstanceOf[AkkaRpcEndPointRef].actorRef)
  }
}

private[akka] class AkkaRpcEndPointRef(val actorRef: ActorRef, conf: SparkConf)
  extends RpcEndPointRef with Serializable with Logging {

  private[this] val maxRetries = conf.getInt("spark.akka.num.retries", 3)
  private[this] val retryWaitMs = conf.getInt("spark.akka.retry.wait", 3000)
  private[this] val timeout =
    Duration.create(conf.getLong("spark.akka.lookupTimeout", 30), "seconds")

  override val address: String = actorRef.path.address.toString

  override def askWithReply[T](message: Any): T = {
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

  override def send(message: Any)(implicit sender: RpcEndPointRef = RpcEndPoint.noSender): Unit = {
    implicit val actorSender: ActorRef =
      if (sender == null) {
        Actor.noSender
      } else {
        sender.asInstanceOf[AkkaRpcEndPointRef].actorRef
      }
    actorRef ! message
  }

  override def toString: String = s"${getClass.getSimpleName}($actorRef)"
}
