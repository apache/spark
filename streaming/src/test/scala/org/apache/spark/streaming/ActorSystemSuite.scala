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

package org.apache.spark.streaming

import akka.actor.{Actor, Props}
import org.apache.spark._
import org.apache.spark.util.{AkkaUtils, ConfiguredTimeout}
import org.scalatest.concurrent.Timeouts
import org.scalatest.{BeforeAndAfter, FunSuite}


class EchoActor extends Actor {
  def receive: Receive = {
    case msg =>
      Thread.sleep(1200)
      sender() ! msg
  }
}


class ActorSystemSuite extends FunSuite with BeforeAndAfter with Timeouts with Logging {

  val master = "local[2]"
  val appName = this.getClass.getSimpleName
  val batchDuration = Milliseconds(500)
  val sparkHome = "someDir"
  val envPair = "key" -> "value"

  var ssc: StreamingContext = null

  test("actor askWithReply using ConfiguredTimeout") {
    val conf = new SparkConf()

    val shortProp = "spark.ask.short.timeout"

    conf.set(shortProp, "1s")

    ssc = new StreamingContext(master, appName, batchDuration)

    val actorSystem = AkkaUtils.createActorSystem("EchoActors", "127.0.0.1", 9999, conf = conf,
      securityManager = new SecurityManager(conf))._1

    val askingActor = actorSystem.actorOf(Props[EchoActor], "AskingActor")

    AkkaUtils.askWithReply[String]("this should echo", askingActor, 1, 0,
      ConfiguredTimeout.createAskTimeout(conf))

    try {
      AkkaUtils.askWithReply[String]("this should timeout", askingActor, 1, 0,
        ConfiguredTimeout(conf, shortProp))
      throw new TestException("required exception not raised during AkkaUtils.askWithReply")
    } catch {
      case se: SparkException =>
        assert(se.getCause().getMessage().contains(shortProp))
    }
  }

}
