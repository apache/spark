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

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, FunSuite}
import org.apache.spark.streaming.dstream.InputDStream
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import scala.util.Random

class UISuite extends FunSuite with BeforeAndAfterAll {

  test("Testing") {
    runStreaming(1000000)
  }

  def runStreaming(duration: Long) {
    val ssc = new StreamingContext("local[10]", "test", Seconds(1))
    val servers = (1 to 5).map { i => new TestServer(10000 + i) }

    val inputStream = ssc.union(servers.map(server => ssc.socketTextStream("localhost", server.port)))
    inputStream.count.print

    ssc.start()
    servers.foreach(_.start())
    val startTime = System.currentTimeMillis()
    while (System.currentTimeMillis() - startTime < duration) {
      servers.map(_.send(Random.nextString(10) + "\n"))
      //Thread.sleep(1)
    }
    ssc.stop()
    servers.foreach(_.stop())
  }
}

class FunctionBasedInputDStream[T: ClassTag](
    ssc_ : StreamingContext,
    function: (StreamingContext, Time) => Option[RDD[T]]
  ) extends InputDStream[T](ssc_) {

  def start(): Unit = {}

  def stop(): Unit = {}

  def compute(validTime: Time): Option[RDD[T]] = function(ssc, validTime)
}