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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, NetworkReceiver}
import org.apache.spark.util.{MetadataCleaner, Utils}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.concurrent.Timeouts
import org.scalatest.exceptions.TestFailedDueToTimeoutException
import org.scalatest.time.SpanSugar._

class StreamingContextSuite extends FunSuite with BeforeAndAfter with Timeouts with Logging {

  val master = "local[2]"
  val appName = this.getClass.getSimpleName
  val batchDuration = Milliseconds(500)
  val sparkHome = "someDir"
  val envPair = "key" -> "value"
  val ttl = StreamingContext.DEFAULT_CLEANER_TTL + 100

  var sc: SparkContext = null
  var ssc: StreamingContext = null

  before {
    System.clearProperty("spark.cleaner.ttl")
  }

  after {
    if (ssc != null) {
      ssc.stop()
      ssc = null
    }
    if (sc != null) {
      sc.stop()
      sc = null
    }
  }

  test("from no conf constructor") {
    ssc = new StreamingContext(master, appName, batchDuration)
    assert(ssc.sparkContext.conf.get("spark.master") === master)
    assert(ssc.sparkContext.conf.get("spark.app.name") === appName)
    assert(MetadataCleaner.getDelaySeconds(ssc.sparkContext.conf) ===
      StreamingContext.DEFAULT_CLEANER_TTL)
  }

  test("from no conf + spark home") {
    ssc = new StreamingContext(master, appName, batchDuration, sparkHome, Nil)
    assert(ssc.conf.get("spark.home") === sparkHome)
    assert(MetadataCleaner.getDelaySeconds(ssc.sparkContext.conf) ===
      StreamingContext.DEFAULT_CLEANER_TTL)
  }

  test("from no conf + spark home + env") {
    ssc = new StreamingContext(master, appName, batchDuration,
      sparkHome, Nil, Map(envPair))
    assert(ssc.conf.getExecutorEnv.exists(_ == envPair))
    assert(MetadataCleaner.getDelaySeconds(ssc.sparkContext.conf) ===
      StreamingContext.DEFAULT_CLEANER_TTL)
  }

  test("from conf without ttl set") {
    val myConf = SparkContext.updatedConf(new SparkConf(false), master, appName)
    ssc = new StreamingContext(myConf, batchDuration)
    assert(MetadataCleaner.getDelaySeconds(ssc.conf) ===
      StreamingContext.DEFAULT_CLEANER_TTL)
  }

  test("from conf with ttl set") {
    val myConf = SparkContext.updatedConf(new SparkConf(false), master, appName)
    myConf.set("spark.cleaner.ttl", ttl.toString)
    ssc = new StreamingContext(myConf, batchDuration)
    assert(ssc.conf.getInt("spark.cleaner.ttl", -1) === ttl)
  }

  test("from existing SparkContext without ttl set") {
    sc = new SparkContext(master, appName)
    val exception = intercept[SparkException] {
      ssc = new StreamingContext(sc, batchDuration)
    }
    assert(exception.getMessage.contains("ttl"))
  }

  test("from existing SparkContext with ttl set") {
    val myConf = SparkContext.updatedConf(new SparkConf(false), master, appName)
    myConf.set("spark.cleaner.ttl", ttl.toString)
    ssc = new StreamingContext(myConf, batchDuration)
    assert(ssc.conf.getInt("spark.cleaner.ttl", -1) === ttl)
  }

  test("from checkpoint") {
    val myConf = SparkContext.updatedConf(new SparkConf(false), master, appName)
    myConf.set("spark.cleaner.ttl", ttl.toString)
    val ssc1 = new StreamingContext(myConf, batchDuration)
    addInputStream(ssc1).register
    ssc1.start()
    val cp = new Checkpoint(ssc1, Time(1000))
    assert(MetadataCleaner.getDelaySeconds(cp.sparkConf) === ttl)
    ssc1.stop()
    val newCp = Utils.deserialize[Checkpoint](Utils.serialize(cp))
    assert(MetadataCleaner.getDelaySeconds(newCp.sparkConf) === ttl)
    ssc = new StreamingContext(null, newCp, null)
    assert(MetadataCleaner.getDelaySeconds(ssc.conf) === ttl)
  }

  test("start and stop state check") {
    ssc = new StreamingContext(master, appName, batchDuration)
    addInputStream(ssc).register

    assert(ssc.state === Service.State.Uninitialized)
    ssc.start()
    assert(ssc.state === Service.State.Started)
    ssc.stop()
    assert(ssc.state === Service.State.Stopped)
  }

  test("start multiple times") {
    ssc = new StreamingContext(master, appName, batchDuration)
    addInputStream(ssc).register
    ssc.start()
    intercept[IllegalArgumentException] {
      ssc.start()
    }
  }

  test("stop multiple times") {
    ssc = new StreamingContext(master, appName, batchDuration)
    addInputStream(ssc).register
    ssc.start()
    ssc.stop()
    ssc.stop()
  }

  test("stop before start and start after stop") {
    ssc = new StreamingContext(master, appName, batchDuration)
    addInputStream(ssc).register
    ssc.stop()  // stop before start should not throw exception
    ssc.start()
    ssc.stop()
    intercept[SparkException] {
      ssc.start() // start after stop should throw exception
    }
  }

  test("stop only streaming context") {
    ssc = new StreamingContext(master, appName, batchDuration)
    sc = ssc.sparkContext
    addInputStream(ssc).register
    ssc.start()
    ssc.stop(false)
    assert(sc.makeRDD(1 to 100).collect().size === 100)
    ssc = new StreamingContext(sc, batchDuration)
    addInputStream(ssc).register
    ssc.start()
    ssc.stop()
  }

  test("stop gracefully") {
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    conf.set("spark.cleaner.ttl", "3600")
    sc = new SparkContext(conf)
    for (i <- 1 to 4) {
      logInfo("==================================")
      ssc = new StreamingContext(sc, batchDuration)
      var runningCount = 0
      TestReceiver.counter.set(1)
      val input = ssc.networkStream(new TestReceiver)
      input.count.foreachRDD(rdd => {
        val count = rdd.first()
        logInfo("Count = " + count)
        runningCount += count.toInt
      })
      ssc.start()
      ssc.awaitTermination(500)
      ssc.stop(stopSparkContext = false, stopGracefully = true)
      logInfo("Running count = " + runningCount)
      logInfo("TestReceiver.counter = " + TestReceiver.counter.get())
      assert(runningCount > 0)
      assert(
        (TestReceiver.counter.get() == runningCount + 1) ||
          (TestReceiver.counter.get() == runningCount + 2),
        "Received records = " + TestReceiver.counter.get() + ", " +
          "processed records = " + runningCount
      )
    }
  }

  test("awaitTermination") {
    ssc = new StreamingContext(master, appName, batchDuration)
    val inputStream = addInputStream(ssc)
    inputStream.map(x => x).register

    // test whether start() blocks indefinitely or not
    failAfter(2000 millis) {
      ssc.start()
    }

    // test whether waitForStop() exits after give amount of time
    failAfter(1000 millis) {
      ssc.awaitTermination(500)
    }

    // test whether waitForStop() does not exit if not time is given
    val exception = intercept[Exception] {
      failAfter(1000 millis) {
        ssc.awaitTermination()
        throw new Exception("Did not wait for stop")
      }
    }
    assert(exception.isInstanceOf[TestFailedDueToTimeoutException], "Did not wait for stop")

    // test whether wait exits if context is stopped
    failAfter(10000 millis) { // 10 seconds because spark takes a long time to shutdown
      new Thread() {
        override def run {
          Thread.sleep(500)
          ssc.stop()
        }
      }.start()
      ssc.awaitTermination()
    }
  }

  test("awaitTermination with error in task") {
    ssc = new StreamingContext(master, appName, batchDuration)
    val inputStream = addInputStream(ssc)
    inputStream.map(x => { throw new TestException("error in map task"); x})
               .foreachRDD(_.count)

    val exception = intercept[Exception] {
      ssc.start()
      ssc.awaitTermination(5000)
    }
    assert(exception.getMessage.contains("map task"), "Expected exception not thrown")
  }

  test("awaitTermination with error in job generation") {
    ssc = new StreamingContext(master, appName, batchDuration)
    val inputStream = addInputStream(ssc)
    inputStream.transform(rdd => { throw new TestException("error in transform"); rdd }).register
    val exception = intercept[TestException] {
      ssc.start()
      ssc.awaitTermination(5000)
    }
    assert(exception.getMessage.contains("transform"), "Expected exception not thrown")
  }

  def addInputStream(s: StreamingContext): DStream[Int] = {
    val input = (1 to 100).map(i => (1 to i))
    val inputStream = new TestInputStream(s, input, 1)
    inputStream
  }
}

class TestException(msg: String) extends Exception(msg)

/** Custom receiver for testing whether all data received by a receiver gets processed or not */
class TestReceiver extends NetworkReceiver[Int] {
  protected lazy val blockGenerator = new BlockGenerator(StorageLevel.MEMORY_ONLY)
  protected def onStart() {
    blockGenerator.start()
    logInfo("BlockGenerator started on thread " + receivingThread)
    try {
      while(true) {
        blockGenerator += TestReceiver.counter.getAndIncrement
        Thread.sleep(0)
      }
    } finally {
      logInfo("Receiving stopped at count value of " + TestReceiver.counter.get())
    }
  }

  protected def onStop() {
    blockGenerator.stop()
  }
}

object TestReceiver {
  val counter = new AtomicInteger(1)
}
