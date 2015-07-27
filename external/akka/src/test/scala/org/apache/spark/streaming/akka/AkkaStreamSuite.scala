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

package org.apache.spark.streaming.akka

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.duration._

import akka.actor._
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

class AkkaStreamSuite extends SparkFunSuite with Eventually with BeforeAndAfter {

  private var ssc: StreamingContext = _

  private var actorSystem: ActorSystem = _

  after {
    if (ssc != null) {
      ssc.stop()
      ssc = null
    }
    if (actorSystem != null) {
      actorSystem.shutdown()
      actorSystem.awaitTermination(30.seconds)
      actorSystem = null
    }
  }

  test("actor input stream") {
    // `Props` contains a reference to com.typesafe.config.Config and will be serialized. However,
    // because of https://github.com/typesafehub/config/issues/176, this unit test cannot run with
    // "-Dsun.io.serialization.extendedDebugInfo=true". Therefore,
    // "sun.io.serialization.extendedDebugInfo" is disabled in SparkBuild.scala for streaming-akka
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName(this.getClass.getSimpleName)
    ssc = new StreamingContext(sparkConf, Milliseconds(500))

    val akkaConf = ConfigFactory.parseMap(
      Map("akka.actor.provider" -> "akka.remote.RemoteActorRefProvider",
        "akka.remote.netty.tcp.transport-class" -> "akka.remote.transport.netty.NettyTransport"))
    actorSystem = ActorSystem("test", akkaConf)
    CachedActorSystem.set(actorSystem)
    actorSystem.actorOf(Props(classOf[FeederActor]), "FeederActor")
    val feederUri =
      actorSystem.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress + "/user/FeederActor"
    val actorStream = AkkaUtils.createStream[String](ssc, () => CachedActorSystem.get,
      Props(classOf[TestActorReceiver], feederUri), "TestActorReceiver")

    val result = new mutable.ArrayBuffer[String] with mutable.SynchronizedBuffer[String]
    actorStream.foreachRDD { rdd =>
      result ++= rdd.collect()
    }
    ssc.start()

    eventually(timeout(10.seconds), interval(10.milliseconds)) {
      assert((1 to 10).map(_.toString) === result)
    }
  }
}

/**
 * Provide a global class to reuse the ActorSystem in the unit test
 */
object CachedActorSystem {

  private var actorSystem: ActorSystem = null

  def set(actorSystem: ActorSystem): Unit = synchronized {
    this.actorSystem = actorSystem
  }

  def get: ActorSystem = synchronized {
    actorSystem
  }
}

case class SubscribeReceiver(receiverActor: ActorRef)

class FeederActor extends Actor {

  def receive: Receive = {
    case SubscribeReceiver(receiverActor: ActorRef) =>
      (1 to 10).foreach(i => receiverActor ! i.toString())
  }
}

class TestActorReceiver(uriOfPublisher: String) extends Actor with ActorHelper {

  lazy private val remotePublisher = context.actorSelection(uriOfPublisher)

  override def preStart(): Unit = {
    remotePublisher ! SubscribeReceiver(self)
  }

  def receive: PartialFunction[Any, Unit] = {
    case msg: String => store(msg)
  }

}
