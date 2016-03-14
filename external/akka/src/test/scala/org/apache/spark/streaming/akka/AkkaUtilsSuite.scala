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

import scala.concurrent.duration._

import akka.actor.{Props, SupervisorStrategy}

import org.apache.spark.SparkFunSuite
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

class AkkaUtilsSuite extends SparkFunSuite {

  test("createStream") {
    val ssc: StreamingContext = new StreamingContext("local[2]", "test", Seconds(1000))
    try {
      // tests the API, does not actually test data receiving
      val test1: ReceiverInputDStream[String] = AkkaUtils.createStream(
        ssc, Props[TestActor](), "test")
      val test2: ReceiverInputDStream[String] = AkkaUtils.createStream(
        ssc, Props[TestActor](), "test", StorageLevel.MEMORY_AND_DISK_SER_2)
      val test3: ReceiverInputDStream[String] = AkkaUtils.createStream(
        ssc,
        Props[TestActor](),
        "test",
        StorageLevel.MEMORY_AND_DISK_SER_2,
        supervisorStrategy = SupervisorStrategy.defaultStrategy)
      val test4: ReceiverInputDStream[String] = AkkaUtils.createStream(
        ssc, Props[TestActor](), "test", StorageLevel.MEMORY_AND_DISK_SER_2, () => null)
      val test5: ReceiverInputDStream[String] = AkkaUtils.createStream(
        ssc, Props[TestActor](), "test", StorageLevel.MEMORY_AND_DISK_SER_2, () => null)
      val test6: ReceiverInputDStream[String] = AkkaUtils.createStream(
        ssc,
        Props[TestActor](),
        "test",
        StorageLevel.MEMORY_AND_DISK_SER_2,
        () => null,
        SupervisorStrategy.defaultStrategy)
    } finally {
      ssc.stop()
    }
  }
}

class TestActor extends ActorReceiver {
  override def receive: Receive = {
    case m: String => store(m)
    case m => store(m, 10.seconds)
  }
}
