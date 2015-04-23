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

package org.apache.spark.streaming.zeromq

import akka.actor.SupervisorStrategy
import akka.util.ByteString
import akka.zeromq.Subscribe
import org.scalatest.FunSuite

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

class ZeroMQStreamSuite extends FunSuite {

  val batchDuration = Seconds(1)

  private val master: String = "local[2]"

  private val framework: String = this.getClass.getSimpleName

  test("zeromq input stream") {
    val ssc = new StreamingContext(master, framework, batchDuration)
    val publishUrl = "abc"
    val subscribe = new Subscribe(null.asInstanceOf[ByteString])
    val bytesToObjects = (bytes: Seq[ByteString]) => null.asInstanceOf[Iterator[String]]

    // tests the API, does not actually test data receiving
    val test1: ReceiverInputDStream[String] =
      ZeroMQUtils.createStream(ssc, publishUrl, subscribe, bytesToObjects)
    val test2: ReceiverInputDStream[String] = ZeroMQUtils.createStream(
      ssc, publishUrl, subscribe, bytesToObjects, StorageLevel.MEMORY_AND_DISK_SER_2)
    val test3: ReceiverInputDStream[String] = ZeroMQUtils.createStream(
      ssc, publishUrl, subscribe, bytesToObjects,
      StorageLevel.MEMORY_AND_DISK_SER_2, SupervisorStrategy.defaultStrategy)

    // TODO: Actually test data receiving
    ssc.stop()
  }
}
