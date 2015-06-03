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

import java.util.concurrent.TimeoutException

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Success, Failure}
import scala.language.postfixOps

import akka.actor.{ActorSystem, Actor, ActorRef, Props, Address}
import akka.pattern.ask

import org.apache.spark.rpc._
import org.apache.spark.{SecurityManager, SparkConf}


class AkkaRpcEnvSuite extends RpcEnvSuite {

  override def createRpcEnv(conf: SparkConf, name: String, port: Int): RpcEnv = {
    new AkkaRpcEnvFactory().create(
      RpcEnvConfig(conf, name, "localhost", port, new SecurityManager(conf)))
  }

  test("setupEndpointRef: systemName, address, endpointName") {
    val ref = env.setupEndpoint("test_endpoint", new RpcEndpoint {
      override val rpcEnv = env

      override def receive = {
        case _ =>
      }
    })
    val conf = new SparkConf()
    val newRpcEnv = new AkkaRpcEnvFactory().create(
      RpcEnvConfig(conf, "test", "localhost", 12346, new SecurityManager(conf)))
    try {
      val newRef = newRpcEnv.setupEndpointRef("local", ref.address, "test_endpoint")
      assert(s"akka.tcp://local@${env.address}/user/test_endpoint" ===
        newRef.asInstanceOf[AkkaRpcEndpointRef].actorRef.path.toString)
    } finally {
      newRpcEnv.shutdown()
    }
  }

  test("Future failure with RpcTimeout") {

    class EchoActor extends Actor {
      def receive: Receive = {
        case msg =>
          Thread.sleep(500)
          sender() ! msg
      }
    }

    val system = ActorSystem("EchoSystem")
    val echoActor = system.actorOf(Props(new EchoActor), name = "echoA")

    val timeout = new RpcTimeout(50 millis, "spark.rpc.short.timeout")

    val fut = echoActor.ask("hello")(1000 millis).mapTo[String].recover {
      case te: TimeoutException => throw timeout.amend(te)
    }

    fut.onFailure {
      case te: TimeoutException => println("failed with timeout exception")
    }

    fut.onComplete {
      case Success(str) => println("future success")
      case Failure(ex) => println("future failure")
    }

    println("sleeping")
    Thread.sleep(50)
    println("Future complete: " + fut.isCompleted.toString() + ", " + fut.value.toString())

    println("Caught TimeoutException: " +
      intercept[TimeoutException] {
        //timeout.awaitResult(fut)  // prints RpcTimeout description twice
        Await.result(fut, 10 millis)
      }.getMessage()
    )

    /*
    val ref = env.setupEndpoint("test_future", new RpcEndpoint {
      override val rpcEnv = env

      override def receive = {
        case _ =>
      }
    })
    val conf = new SparkConf()
    val newRpcEnv = new AkkaRpcEnvFactory().create(
      RpcEnvConfig(conf, "test", "localhost", 12346, new SecurityManager(conf)))
    try {
      val newRef = newRpcEnv.setupEndpointRef("local", ref.address, "test_future")
      val akkaActorRef = newRef.asInstanceOf[AkkaRpcEndpointRef].actorRef

      val timeout = new RpcTimeout(1 millis, "spark.rpc.short.timeout")
      val fut = akkaActorRef.ask("hello")(timeout.duration).mapTo[String]

      Thread.sleep(500)
      println("Future complete: " + fut.isCompleted.toString() + ", " + fut.value.toString())

    } finally {
      newRpcEnv.shutdown()
    }
    */


  }

}
