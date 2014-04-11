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

import scala.reflect.ClassTag
import scala.util.Random
import scala.io.Source

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._
import org.scalatest.matchers.ShouldMatchers

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream

class UISuite extends FunSuite with ShouldMatchers with BeforeAndAfterAll with BeforeAndAfter {
  var sc: SparkContext = null
  var ssc: StreamingContext = null

  override def beforeAll() {
    val conf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)
    conf.set("spark.cleaner.ttl", "1800")
    sc = new SparkContext(conf)
  }

  override def afterAll() {
    if (sc != null) sc.stop()
  }

  before {
    ssc = new StreamingContext(sc, Seconds(1))
  }

  after {
    if (ssc != null) {
      ssc.stop()
      ssc = null
    }
  }

  test("streaming tab in spark UI") {
    val ssc = new StreamingContext(sc, Seconds(1))
    eventually(timeout(10 seconds), interval(50 milliseconds)) {
      val uiData = Source.fromURL(
        ssc.sparkContext.ui.appUIAddress.stripSuffix("/") + "/streaming").mkString
      assert(uiData.contains("streaming"))
    }
  }

  test("multiple streaming tabs") {
    val ssc1 = new StreamingContext(sc, Seconds(1))
    val ssc2 = new StreamingContext(sc, Seconds(2))
    ssc1.uiTab.prefix should not be ssc2.uiTab.prefix
  }

  ignore("Testing") {
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