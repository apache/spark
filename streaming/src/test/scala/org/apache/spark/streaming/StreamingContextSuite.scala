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

import java.io.{File, NotSerializableException}
import java.util.Locale
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue

import org.apache.commons.io.FileUtils
import org.scalatest.{Assertions, BeforeAndAfter, PrivateMethodTester}
import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimits}
import org.scalatest.concurrent.Eventually._
import org.scalatest.exceptions.TestFailedDueToTimeoutException
import org.scalatest.time.SpanSugar._

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.metrics.source.Source
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.util.Utils


class StreamingContextSuite extends SparkFunSuite with BeforeAndAfter with TimeLimits with Logging {

  // Necessary to make ScalaTest 3.x interrupt a thread on the JVM like ScalaTest 2.2.x
  implicit val signaler: Signaler = ThreadSignaler

  val master = "local[2]"
  val appName = this.getClass.getSimpleName
  val batchDuration = Milliseconds(500)
  val sparkHome = "someDir"
  val envPair = "key" -> "value"
  val conf = new SparkConf().setMaster(master).setAppName(appName)

  var sc: SparkContext = null
  var ssc: StreamingContext = null

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
  }

  test("from no conf + spark home") {
    ssc = new StreamingContext(master, appName, batchDuration, sparkHome, Nil)
    assert(ssc.conf.get("spark.home") === sparkHome)
  }

  test("from no conf + spark home + env") {
    ssc = new StreamingContext(master, appName, batchDuration,
      sparkHome, Nil, Map(envPair))
    assert(ssc.conf.getExecutorEnv.contains(envPair))
  }

  test("from conf with settings") {
    val myConf = SparkContext.updatedConf(new SparkConf(false), master, appName)
    myConf.set("spark.dummyTimeConfig", "10s")
    ssc = new StreamingContext(myConf, batchDuration)
    assert(ssc.conf.getTimeAsSeconds("spark.dummyTimeConfig", "-1") === 10)
  }

  test("from existing SparkContext") {
    sc = new SparkContext(master, appName)
    ssc = new StreamingContext(sc, batchDuration)
  }

  test("from existing SparkContext with settings") {
    val myConf = SparkContext.updatedConf(new SparkConf(false), master, appName)
    myConf.set("spark.dummyTimeConfig", "10s")
    ssc = new StreamingContext(myConf, batchDuration)
    assert(ssc.conf.getTimeAsSeconds("spark.dummyTimeConfig", "-1") === 10)
  }

  test("from checkpoint") {
    val myConf = SparkContext.updatedConf(new SparkConf(false), master, appName)
    myConf.set("spark.dummyTimeConfig", "10s")
    val ssc1 = new StreamingContext(myConf, batchDuration)
    addInputStream(ssc1).register()
    ssc1.start()
    val cp = new Checkpoint(ssc1, Time(1000))
    assert(
      Utils.timeStringAsSeconds(cp.sparkConfPairs
          .toMap.getOrElse("spark.dummyTimeConfig", "-1")) === 10)
    ssc1.stop()
    val newCp = Utils.deserialize[Checkpoint](Utils.serialize(cp))
    assert(
      newCp.createSparkConf().getTimeAsSeconds("spark.dummyTimeConfig", "-1") === 10)
    ssc = new StreamingContext(null, newCp, null)
    assert(ssc.conf.getTimeAsSeconds("spark.dummyTimeConfig", "-1") === 10)
  }

  test("checkPoint from conf") {
    val checkpointDirectory = Utils.createTempDir().getAbsolutePath()

    val myConf = SparkContext.updatedConf(new SparkConf(false), master, appName)
    myConf.set("spark.streaming.checkpoint.directory", checkpointDirectory)
    ssc = new StreamingContext(myConf, batchDuration)
    assert(ssc.checkpointDir != null)
  }

  test("state matching") {
    import StreamingContextState._
    assert(INITIALIZED === INITIALIZED)
    assert(INITIALIZED != ACTIVE)
  }

  test("start and stop state check") {
    ssc = new StreamingContext(master, appName, batchDuration)
    addInputStream(ssc).register()

    assert(ssc.getState() === StreamingContextState.INITIALIZED)
    ssc.start()
    assert(ssc.getState() === StreamingContextState.ACTIVE)
    ssc.stop()
    assert(ssc.getState() === StreamingContextState.STOPPED)

    // Make sure that the SparkContext is also stopped by default
    intercept[Exception] {
      ssc.sparkContext.makeRDD(1 to 10)
    }
  }

  test("start with non-serializable DStream checkpoints") {
    val checkpointDir = Utils.createTempDir()
    ssc = new StreamingContext(conf, batchDuration)
    ssc.checkpoint(checkpointDir.getAbsolutePath)
    addInputStream(ssc).foreachRDD { rdd =>
      // Refer to this.appName from inside closure so that this closure refers to
      // the instance of StreamingContextSuite, and is therefore not serializable
      rdd.count() + appName
    }

    // Test whether start() fails early when checkpointing is enabled
    val exception = intercept[NotSerializableException] {
      ssc.start()
    }
    assert(exception.getMessage().contains("DStreams with their functions are not serializable"))
    assert(ssc.getState() !== StreamingContextState.ACTIVE)
    assert(StreamingContext.getActive().isEmpty)
  }

  test("start failure should stop internal components") {
    ssc = new StreamingContext(conf, batchDuration)
    val inputStream = addInputStream(ssc)
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      Some(values.sum + state.getOrElse(0))
    }
    inputStream.map(x => (x, 1)).updateStateByKey[Int](updateFunc)
    // Require that the start fails because checkpoint directory was not set
    intercept[Exception] {
      ssc.start()
    }
    assert(ssc.getState() === StreamingContextState.STOPPED)
    assert(ssc.scheduler.isStarted === false)
  }

  test("start should set local properties of streaming jobs correctly") {
    ssc = new StreamingContext(conf, batchDuration)
    ssc.sc.setJobGroup("non-streaming", "non-streaming", true)
    val sc = ssc.sc

    @volatile var jobGroupFound: String = ""
    @volatile var jobDescFound: String = ""
    @volatile var jobInterruptFound: String = ""
    @volatile var customPropFound: String = ""
    @volatile var allFound: Boolean = false

    addInputStream(ssc).foreachRDD { rdd =>
      jobGroupFound = sc.getLocalProperty(SparkContext.SPARK_JOB_GROUP_ID)
      jobDescFound = sc.getLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION)
      jobInterruptFound = sc.getLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL)
      customPropFound = sc.getLocalProperty("customPropKey")
      allFound = true
    }
    ssc.sc.setLocalProperty("customPropKey", "value1")
    ssc.start()

    // Local props set after start should be ignored
    ssc.sc.setLocalProperty("customPropKey", "value2")

    eventually(timeout(10.seconds), interval(10.milliseconds)) {
      assert(allFound)
    }

    // Verify streaming jobs have expected thread-local properties
    assert(jobGroupFound === null)
    assert(jobDescFound.contains("Streaming job from"))
    assert(jobInterruptFound === "false")
    assert(customPropFound === "value1")

    // Verify current thread's thread-local properties have not changed
    assert(sc.getLocalProperty(SparkContext.SPARK_JOB_GROUP_ID) === "non-streaming")
    assert(sc.getLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION) === "non-streaming")
    assert(sc.getLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL) === "true")
    assert(sc.getLocalProperty("customPropKey") === "value2")
  }

  test("start multiple times") {
    ssc = new StreamingContext(master, appName, batchDuration)
    addInputStream(ssc).register()
    ssc.start()
    assert(ssc.getState() === StreamingContextState.ACTIVE)
    ssc.start()
    assert(ssc.getState() === StreamingContextState.ACTIVE)
  }

  test("stop multiple times") {
    ssc = new StreamingContext(master, appName, batchDuration)
    addInputStream(ssc).register()
    ssc.start()
    ssc.stop()
    assert(ssc.getState() === StreamingContextState.STOPPED)
    ssc.stop()
    assert(ssc.getState() === StreamingContextState.STOPPED)
  }

  test("stop before start") {
    ssc = new StreamingContext(master, appName, batchDuration)
    addInputStream(ssc).register()
    ssc.stop()  // stop before start should not throw exception
    assert(ssc.getState() === StreamingContextState.STOPPED)
  }

  test("start after stop") {
    // Regression test for SPARK-4301
    ssc = new StreamingContext(master, appName, batchDuration)
    addInputStream(ssc).register()
    ssc.stop()
    intercept[IllegalStateException] {
      ssc.start() // start after stop should throw exception
    }
    assert(ssc.getState() === StreamingContextState.STOPPED)
  }

  test("stop only streaming context") {
    val conf = new SparkConf().setMaster(master).setAppName(appName)

    // Explicitly do not stop SparkContext
    ssc = new StreamingContext(conf, batchDuration)
    sc = ssc.sparkContext
    addInputStream(ssc).register()
    ssc.start()
    ssc.stop(stopSparkContext = false)
    assert(ssc.getState() === StreamingContextState.STOPPED)
    assert(sc.makeRDD(1 to 100).collect().size === 100)
    sc.stop()

    // Implicitly do not stop SparkContext
    conf.set("spark.streaming.stopSparkContextByDefault", "false")
    ssc = new StreamingContext(conf, batchDuration)
    sc = ssc.sparkContext
    addInputStream(ssc).register()
    ssc.start()
    ssc.stop()
    assert(sc.makeRDD(1 to 100).collect().size === 100)
    sc.stop()
  }

  test("stop(stopSparkContext=true) after stop(stopSparkContext=false)") {
    ssc = new StreamingContext(master, appName, batchDuration)
    addInputStream(ssc).register()
    ssc.stop(stopSparkContext = false)
    assert(ssc.sc.makeRDD(1 to 100).collect().size === 100)
    ssc.stop(stopSparkContext = true)
    // Check that the SparkContext is actually stopped:
    intercept[Exception] {
      ssc.sc.makeRDD(1 to 100).collect()
    }
  }

  test("stop gracefully") {
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    conf.set("spark.dummyTimeConfig", "3600s")
    sc = new SparkContext(conf)
    for (i <- 1 to 4) {
      logInfo("==================================\n\n\n")
      ssc = new StreamingContext(sc, Milliseconds(100))
      @volatile var runningCount = 0
      TestReceiver.counter.set(1)
      val input = ssc.receiverStream(new TestReceiver)
      input.count().foreachRDD { rdd =>
        val count = rdd.first()
        runningCount += count.toInt
        logInfo("Count = " + count + ", Running count = " + runningCount)
      }
      ssc.start()
      eventually(timeout(10.seconds), interval(10.millis)) {
        assert(runningCount > 0)
      }
      ssc.stop(stopSparkContext = false, stopGracefully = true)
      logInfo("Running count = " + runningCount)
      logInfo("TestReceiver.counter = " + TestReceiver.counter.get())
      assert(
        TestReceiver.counter.get() == runningCount + 1,
        "Received records = " + TestReceiver.counter.get() + ", " +
          "processed records = " + runningCount
      )
      Thread.sleep(100)
    }
  }

  test("stop gracefully even if a receiver misses StopReceiver") {
    // This is not a deterministic unit. But if this unit test is flaky, then there is definitely
    // something wrong. See SPARK-5681
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    sc = new SparkContext(conf)
    ssc = new StreamingContext(sc, Milliseconds(100))
    val input = ssc.receiverStream(new TestReceiver)
    input.foreachRDD(_ => {})
    ssc.start()
    // Call `ssc.stop` at once so that it's possible that the receiver will miss "StopReceiver"
    failAfter(30.seconds) {
      ssc.stop(stopSparkContext = true, stopGracefully = true)
    }
  }

  test("stop slow receiver gracefully") {
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    conf.set("spark.streaming.gracefulStopTimeout", "20000s")
    sc = new SparkContext(conf)
    logInfo("==================================\n\n\n")
    ssc = new StreamingContext(sc, Milliseconds(100))
    var runningCount = 0
    SlowTestReceiver.receivedAllRecords = false
    // Create test receiver that sleeps in onStop()
    val totalNumRecords = 15
    val recordsPerSecond = 1
    val input = ssc.receiverStream(new SlowTestReceiver(totalNumRecords, recordsPerSecond))
    input.count().foreachRDD { rdd =>
      val count = rdd.first()
      runningCount += count.toInt
      logInfo("Count = " + count + ", Running count = " + runningCount)
    }
    ssc.start()
    ssc.awaitTerminationOrTimeout(500)
    ssc.stop(stopSparkContext = false, stopGracefully = true)
    logInfo("Running count = " + runningCount)
    assert(runningCount > 0)
    assert(runningCount == totalNumRecords)
    Thread.sleep(100)
  }

  test ("registering and de-registering of streamingSource") {
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    ssc = new StreamingContext(conf, batchDuration)
    assert(ssc.getState() === StreamingContextState.INITIALIZED)
    addInputStream(ssc).register()
    ssc.start()

    val sources = StreamingContextSuite.getSources(ssc.env.metricsSystem)
    val streamingSource = StreamingContextSuite.getStreamingSource(ssc)
    assert(sources.contains(streamingSource))
    assert(ssc.getState() === StreamingContextState.ACTIVE)

    ssc.stop()
    val sourcesAfterStop = StreamingContextSuite.getSources(ssc.env.metricsSystem)
    val streamingSourceAfterStop = StreamingContextSuite.getStreamingSource(ssc)
    assert(ssc.getState() === StreamingContextState.STOPPED)
    assert(!sourcesAfterStop.contains(streamingSourceAfterStop))
  }

  test("awaitTermination") {
    ssc = new StreamingContext(master, appName, batchDuration)
    val inputStream = addInputStream(ssc)
    inputStream.map(x => x).register()

    // test whether start() blocks indefinitely or not
    failAfter(2.seconds) {
      ssc.start()
    }

    // test whether awaitTermination() exits after give amount of time
    failAfter(1.second) {
      ssc.awaitTerminationOrTimeout(500)
    }

    // test whether awaitTermination() does not exit if not time is given
    val exception = intercept[Exception] {
      failAfter(1.second) {
        ssc.awaitTermination()
        throw new Exception("Did not wait for stop")
      }
    }
    assert(exception.isInstanceOf[TestFailedDueToTimeoutException], "Did not wait for stop")

    var t: Thread = null
    // test whether wait exits if context is stopped
    failAfter(10.seconds) { // 10 seconds because spark takes a long time to shutdown
      t = new Thread() {
        override def run() {
          Thread.sleep(500)
          ssc.stop()
        }
      }
      t.start()
      ssc.awaitTermination()
    }
    // SparkContext.stop will set SparkEnv.env to null. We need to make sure SparkContext is stopped
    // before running the next test. Otherwise, it's possible that we set SparkEnv.env to null after
    // the next test creates the new SparkContext and fail the test.
    t.join()
  }

  test("awaitTermination after stop") {
    ssc = new StreamingContext(master, appName, batchDuration)
    val inputStream = addInputStream(ssc)
    inputStream.map(x => x).register()

    failAfter(10.seconds) {
      ssc.start()
      ssc.stop()
      ssc.awaitTermination()
    }
  }

  test("awaitTermination with error in task") {
    ssc = new StreamingContext(master, appName, batchDuration)
    val inputStream = addInputStream(ssc)
    inputStream
      .map { x => throw new TestException("error in map task"); x }
      .foreachRDD(_.count())

    val exception = intercept[Exception] {
      ssc.start()
      ssc.awaitTerminationOrTimeout(5000)
    }
    assert(exception.getMessage.contains("map task"), "Expected exception not thrown")
  }

  test("awaitTermination with error in job generation") {
    ssc = new StreamingContext(master, appName, batchDuration)
    val inputStream = addInputStream(ssc)
    inputStream.transform { rdd => throw new TestException("error in transform"); rdd }.register()
    val exception = intercept[TestException] {
      ssc.start()
      ssc.awaitTerminationOrTimeout(5000)
    }
    assert(exception.getMessage.contains("transform"), "Expected exception not thrown")
  }

  test("awaitTerminationOrTimeout") {
    ssc = new StreamingContext(master, appName, batchDuration)
    val inputStream = addInputStream(ssc)
    inputStream.map(x => x).register()

    ssc.start()

    // test whether awaitTerminationOrTimeout() return false after give amount of time
    failAfter(1.second) {
      assert(ssc.awaitTerminationOrTimeout(500) === false)
    }

    var t: Thread = null
    // test whether awaitTerminationOrTimeout() return true if context is stopped
    failAfter(10.seconds) { // 10 seconds because spark takes a long time to shutdown
      t = new Thread() {
        override def run() {
          Thread.sleep(500)
          ssc.stop()
        }
      }
      t.start()
      assert(ssc.awaitTerminationOrTimeout(10000))
    }
    // SparkContext.stop will set SparkEnv.env to null. We need to make sure SparkContext is stopped
    // before running the next test. Otherwise, it's possible that we set SparkEnv.env to null after
    // the next test creates the new SparkContext and fail the test.
    t.join()
  }

  test("getOrCreate") {
    val conf = new SparkConf().setMaster(master).setAppName(appName)

    // Function to create StreamingContext that has a config to identify it to be new context
    var newContextCreated = false
    def creatingFunction(): StreamingContext = {
      newContextCreated = true
      new StreamingContext(conf, batchDuration)
    }

    // Call ssc.stop after a body of code
    def testGetOrCreate(body: => Unit): Unit = {
      newContextCreated = false
      try {
        body
      } finally {
        if (ssc != null) {
          ssc.stop()
        }
        ssc = null
      }
    }

    val emptyPath = Utils.createTempDir().getAbsolutePath()

    // getOrCreate should create new context with empty path
    testGetOrCreate {
      ssc = StreamingContext.getOrCreate(emptyPath, () => creatingFunction())
      assert(ssc != null, "no context created")
      assert(newContextCreated, "new context not created")
    }

    val corruptedCheckpointPath = createCorruptedCheckpoint()

    // getOrCreate should throw exception with fake checkpoint file and createOnError = false
    intercept[Exception] {
      ssc = StreamingContext.getOrCreate(corruptedCheckpointPath, () => creatingFunction())
    }

    // getOrCreate should throw exception with fake checkpoint file
    intercept[Exception] {
      ssc = StreamingContext.getOrCreate(
        corruptedCheckpointPath, () => creatingFunction(), createOnError = false)
    }

    // getOrCreate should create new context with fake checkpoint file and createOnError = true
    testGetOrCreate {
      ssc = StreamingContext.getOrCreate(
        corruptedCheckpointPath, () => creatingFunction(), createOnError = true)
      assert(ssc != null, "no context created")
      assert(newContextCreated, "new context not created")
    }

    val checkpointPath = createValidCheckpoint()

    // getOrCreate should recover context with checkpoint path, and recover old configuration
    testGetOrCreate {
      ssc = StreamingContext.getOrCreate(checkpointPath, () => creatingFunction())
      assert(ssc != null, "no context created")
      assert(!newContextCreated, "old context not recovered")
      assert(ssc.conf.get("someKey") === "someValue", "checkpointed config not recovered")
    }

    // getOrCreate should recover StreamingContext with existing SparkContext
    testGetOrCreate {
      sc = new SparkContext(conf)
      ssc = StreamingContext.getOrCreate(checkpointPath, () => creatingFunction())
      assert(ssc != null, "no context created")
      assert(!newContextCreated, "old context not recovered")
      assert(!ssc.conf.contains("someKey"), "checkpointed config unexpectedly recovered")
    }
  }

  test("getActive and getActiveOrCreate") {
    require(StreamingContext.getActive().isEmpty, "context exists from before")
    var newContextCreated = false

    def creatingFunc(): StreamingContext = {
      newContextCreated = true
      val newSsc = new StreamingContext(sc, batchDuration)
      val input = addInputStream(newSsc)
      input.foreachRDD { rdd => rdd.count }
      newSsc
    }

    def testGetActiveOrCreate(body: => Unit): Unit = {
      newContextCreated = false
      try {
        body
      } finally {

        if (ssc != null) {
          ssc.stop(stopSparkContext = false)
        }
        ssc = null
      }
    }

    // getActiveOrCreate should create new context and getActive should return it only
    // after starting the context
    testGetActiveOrCreate {
      sc = new SparkContext(conf)
      ssc = StreamingContext.getActiveOrCreate(creatingFunc _)
      assert(ssc != null, "no context created")
      assert(newContextCreated, "new context not created")
      assert(StreamingContext.getActive().isEmpty,
        "new initialized context returned before starting")
      ssc.start()
      assert(StreamingContext.getActive() === Some(ssc),
        "active context not returned")
      assert(StreamingContext.getActiveOrCreate(creatingFunc _) === ssc,
        "active context not returned")
      ssc.stop()
      assert(StreamingContext.getActive().isEmpty,
        "inactive context returned")
      assert(StreamingContext.getActiveOrCreate(creatingFunc _) !== ssc,
        "inactive context returned")
    }

    // getActiveOrCreate and getActive should return independently created context after activating
    testGetActiveOrCreate {
      sc = new SparkContext(conf)
      ssc = creatingFunc()  // Create
      assert(StreamingContext.getActive().isEmpty,
        "new initialized context returned before starting")
      ssc.start()
      assert(StreamingContext.getActive() === Some(ssc),
        "active context not returned")
      assert(StreamingContext.getActiveOrCreate(creatingFunc _) === ssc,
        "active context not returned")
      ssc.stop()
      assert(StreamingContext.getActive().isEmpty,
        "inactive context returned")
    }
  }

  test("getActiveOrCreate with checkpoint") {
    // Function to create StreamingContext that has a config to identify it to be new context
    var newContextCreated = false
    def creatingFunction(): StreamingContext = {
      newContextCreated = true
      new StreamingContext(conf, batchDuration)
    }

    // Call ssc.stop after a body of code
    def testGetActiveOrCreate(body: => Unit): Unit = {
      require(StreamingContext.getActive().isEmpty) // no active context
      newContextCreated = false
      try {
        body
      } finally {
        if (ssc != null) {
          ssc.stop()
        }
        ssc = null
      }
    }

    val emptyPath = Utils.createTempDir().getAbsolutePath()
    val corruptedCheckpointPath = createCorruptedCheckpoint()
    val checkpointPath = createValidCheckpoint()

    // getActiveOrCreate should return the current active context if there is one
    testGetActiveOrCreate {
      ssc = new StreamingContext(
        conf.clone.set("spark.streaming.clock", "org.apache.spark.util.ManualClock"), batchDuration)
      addInputStream(ssc).register()
      ssc.start()
      val returnedSsc = StreamingContext.getActiveOrCreate(checkpointPath, () => creatingFunction())
      assert(!newContextCreated, "new context created instead of returning")
      assert(returnedSsc.eq(ssc), "returned context is not the activated context")
    }

    // getActiveOrCreate should create new context with empty path
    testGetActiveOrCreate {
      ssc = StreamingContext.getActiveOrCreate(emptyPath, () => creatingFunction())
      assert(ssc != null, "no context created")
      assert(newContextCreated, "new context not created")
    }

    // getActiveOrCreate should throw exception with fake checkpoint file and createOnError = false
    intercept[Exception] {
      ssc = StreamingContext.getOrCreate(corruptedCheckpointPath, () => creatingFunction())
    }

    // getActiveOrCreate should throw exception with fake checkpoint file
    intercept[Exception] {
      ssc = StreamingContext.getActiveOrCreate(
        corruptedCheckpointPath, () => creatingFunction(), createOnError = false)
    }

    // getActiveOrCreate should create new context with fake
    // checkpoint file and createOnError = true
    testGetActiveOrCreate {
      ssc = StreamingContext.getActiveOrCreate(
        corruptedCheckpointPath, () => creatingFunction(), createOnError = true)
      assert(ssc != null, "no context created")
      assert(newContextCreated, "new context not created")
    }

    // getActiveOrCreate should recover context with checkpoint path, and recover old configuration
    testGetActiveOrCreate {
      ssc = StreamingContext.getActiveOrCreate(checkpointPath, () => creatingFunction())
      assert(ssc != null, "no context created")
      assert(!newContextCreated, "old context not recovered")
      assert(ssc.conf.get("someKey") === "someValue")
    }
  }

  test("multiple streaming contexts") {
    sc = new SparkContext(
      conf.clone.set("spark.streaming.clock", "org.apache.spark.util.ManualClock"))
    ssc = new StreamingContext(sc, Seconds(1))
    val input = addInputStream(ssc)
    input.foreachRDD { rdd => rdd.count }
    ssc.start()

    // Creating another streaming context should not create errors
    val anotherSsc = new StreamingContext(sc, Seconds(10))
    val anotherInput = addInputStream(anotherSsc)
    anotherInput.foreachRDD { rdd => rdd.count }

    val exception = intercept[IllegalStateException] {
      anotherSsc.start()
    }
    assert(exception.getMessage.contains("StreamingContext"), "Did not get the right exception")
  }

  test("DStream and generated RDD creation sites") {
    testPackage.test()
  }

  test("throw exception on using active or stopped context") {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      .set("spark.streaming.clock", "org.apache.spark.util.ManualClock")
    ssc = new StreamingContext(conf, batchDuration)
    require(ssc.getState() === StreamingContextState.INITIALIZED)
    val input = addInputStream(ssc)
    val transformed = input.map { x => x}
    transformed.foreachRDD { rdd => rdd.count }

    def testForException(clue: String, expectedErrorMsg: String)(body: => Unit): Unit = {
      withClue(clue) {
        val ex = intercept[IllegalStateException] {
          body
        }
        assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(expectedErrorMsg))
      }
    }

    ssc.start()
    require(ssc.getState() === StreamingContextState.ACTIVE)
    testForException("no error on adding input after start", "start") {
      addInputStream(ssc) }
    testForException("no error on adding transformation after start", "start") {
      input.map { x => x * 2 } }
    testForException("no error on adding output operation after start", "start") {
      transformed.foreachRDD { rdd => rdd.collect() } }

    ssc.stop()
    require(ssc.getState() === StreamingContextState.STOPPED)
    testForException("no error on adding input after stop", "stop") {
      addInputStream(ssc) }
    testForException("no error on adding transformation after stop", "stop") {
      input.map { x => x * 2 } }
    testForException("no error on adding output operation after stop", "stop") {
      transformed.foreachRDD { rdd => rdd.collect() } }
  }

  test("queueStream doesn't support checkpointing") {
    val checkpointDirectory = Utils.createTempDir().getAbsolutePath()
    def creatingFunction(): StreamingContext = {
      val _ssc = new StreamingContext(conf, batchDuration)
      val rdd = _ssc.sparkContext.parallelize(1 to 10)
      _ssc.checkpoint(checkpointDirectory)
      _ssc.queueStream[Int](Queue(rdd)).register()
      _ssc
    }
    ssc = StreamingContext.getOrCreate(checkpointDirectory, () => creatingFunction())
    ssc.start()
    eventually(timeout(10.seconds)) {
      assert(Checkpoint.getCheckpointFiles(checkpointDirectory).size > 1)
    }
    ssc.stop()
    val e = intercept[SparkException] {
      ssc = StreamingContext.getOrCreate(checkpointDirectory, () => creatingFunction())
    }
    // StreamingContext.validate changes the message, so use "contains" here
    assert(e.getCause.getMessage.contains("queueStream doesn't support checkpointing. " +
      "Please don't use queueStream when checkpointing is enabled."))
  }

  test("Creating an InputDStream but not using it should not crash") {
    ssc = new StreamingContext(master, appName, batchDuration)
    val input1 = addInputStream(ssc)
    val input2 = addInputStream(ssc)
    val output = new TestOutputStream(input2)
    output.register()
    val batchCount = new BatchCounter(ssc)
    ssc.start()
    // Just wait for completing 2 batches to make sure it triggers
    // `DStream.getMaxInputStreamRememberDuration`
    batchCount.waitUntilBatchesCompleted(2, 10000)
    // Throw the exception if crash
    ssc.awaitTerminationOrTimeout(1)
    ssc.stop()
  }

  test("SPARK-18560 Receiver data should be deserialized properly.") {
    // Start a two nodes cluster, so receiver will use one node, and Spark jobs will use the
    // other one. Then Spark jobs need to fetch remote blocks and it will trigger SPARK-18560.
    val conf = new SparkConf().setMaster("local-cluster[2,1,1024]").setAppName(appName)
    ssc = new StreamingContext(conf, Milliseconds(100))
    val input = ssc.receiverStream(new TestReceiver)
    val latch = new CountDownLatch(1)
    @volatile var stopping = false
    input.count().foreachRDD { rdd =>
      // Make sure we can read from BlockRDD
      if (rdd.collect().headOption.getOrElse(0L) > 0 && !stopping) {
        // Stop StreamingContext to unblock "awaitTerminationOrTimeout"
        stopping = true
        new Thread() {
          setDaemon(true)
          override def run(): Unit = {
            ssc.stop(stopSparkContext = true, stopGracefully = false)
            latch.countDown()
          }
        }.start()
      }
    }
    ssc.start()
    ssc.awaitTerminationOrTimeout(60000)
    // Wait until `ssc.top` returns. Otherwise, we may finish this test too fast and leak an active
    // SparkContext. Note: the stop codes in `after` will just do nothing if `ssc.stop` in this test
    // is running.
    assert(latch.await(60, TimeUnit.SECONDS))
  }

  def addInputStream(s: StreamingContext): DStream[Int] = {
    val input = (1 to 100).map(i => 1 to i)
    val inputStream = new TestInputStream(s, input, 1)
    inputStream
  }

  def createValidCheckpoint(): String = {
    val testDirectory = Utils.createTempDir().getAbsolutePath()
    val checkpointDirectory = Utils.createTempDir().getAbsolutePath()
    ssc = new StreamingContext(conf.clone.set("someKey", "someValue"), batchDuration)
    ssc.checkpoint(checkpointDirectory)
    ssc.textFileStream(testDirectory).foreachRDD { rdd => rdd.count() }
    ssc.start()
    try {
      eventually(timeout(30.seconds)) {
        assert(Checkpoint.getCheckpointFiles(checkpointDirectory).size > 1)
      }
    } finally {
      ssc.stop()
    }
    checkpointDirectory
  }

  def createCorruptedCheckpoint(): String = {
    val checkpointDirectory = Utils.createTempDir().getAbsolutePath()
    val fakeCheckpointFile = Checkpoint.checkpointFile(checkpointDirectory, Time(1000))
    FileUtils.write(new File(fakeCheckpointFile.toString()), "blablabla")
    assert(Checkpoint.getCheckpointFiles(checkpointDirectory).nonEmpty)
    checkpointDirectory
  }
}

class TestException(msg: String) extends Exception(msg)

/** Custom receiver for testing whether all data received by a receiver gets processed or not */
class TestReceiver extends Receiver[Int](StorageLevel.MEMORY_ONLY) with Logging {

  var receivingThreadOption: Option[Thread] = None

  def onStart() {
    val thread = new Thread() {
      override def run() {
        logInfo("Receiving started")
        while (!isStopped) {
          store(TestReceiver.counter.getAndIncrement)
        }
        logInfo("Receiving stopped at count value of " + TestReceiver.counter.get())
      }
    }
    receivingThreadOption = Some(thread)
    thread.start()
  }

  def onStop() {
    // no clean to be done, the receiving thread should stop on it own, so just wait for it.
    receivingThreadOption.foreach(_.join())
  }
}

object TestReceiver {
  val counter = new AtomicInteger(1)
}

/** Custom receiver for testing whether a slow receiver can be shutdown gracefully or not */
class SlowTestReceiver(totalRecords: Int, recordsPerSecond: Int)
  extends Receiver[Int](StorageLevel.MEMORY_ONLY) with Logging {

  var receivingThreadOption: Option[Thread] = None

  def onStart() {
    val thread = new Thread() {
      override def run() {
        logInfo("Receiving started")
        for(i <- 1 to totalRecords) {
          Thread.sleep(1000 / recordsPerSecond)
          store(i)
        }
        SlowTestReceiver.receivedAllRecords = true
        logInfo(s"Received all $totalRecords records")
      }
    }
    receivingThreadOption = Some(thread)
    thread.start()
  }

  def onStop() {
    // Simulate slow receiver by waiting for all records to be produced
    while (!SlowTestReceiver.receivedAllRecords) {
      Thread.sleep(100)
    }
    // no clean to be done, the receiving thread should stop on it own
  }
}

object SlowTestReceiver {
  var receivedAllRecords = false
}

/** Streaming application for testing DStream and RDD creation sites */
package object testPackage extends Assertions {
  def test() {
    val conf = new SparkConf().setMaster("local").setAppName("CreationSite test")
    val ssc = new StreamingContext(conf, Milliseconds(100))
    try {
      val inputStream = ssc.receiverStream(new TestReceiver)

      // Verify creation site of DStream
      val creationSite = inputStream.creationSite
      assert(creationSite.shortForm.contains("receiverStream") &&
        creationSite.shortForm.contains("StreamingContextSuite")
      )
      assert(creationSite.longForm.contains("testPackage"))

      // Verify creation site of generated RDDs
      var rddGenerated = false
      var rddCreationSiteCorrect = false
      var foreachCallSiteCorrect = false

      inputStream.foreachRDD { rdd =>
        rddCreationSiteCorrect = rdd.creationSite == creationSite
        foreachCallSiteCorrect =
          rdd.sparkContext.getCallSite().shortForm.contains("StreamingContextSuite")
        rddGenerated = true
      }
      ssc.start()

      eventually(timeout(10.seconds), interval(10.milliseconds)) {
        assert(rddGenerated && rddCreationSiteCorrect, "RDD creation site was not correct")
        assert(rddGenerated && foreachCallSiteCorrect, "Call site in foreachRDD was not correct")
      }
    } finally {
      ssc.stop()
    }
  }
}

/**
 * Helper methods for testing StreamingContextSuite
 * This includes methods to access private methods and fields in StreamingContext and MetricsSystem
 */
private object StreamingContextSuite extends PrivateMethodTester {
  private val _sources = PrivateMethod[ArrayBuffer[Source]]('sources)
  private def getSources(metricsSystem: MetricsSystem): ArrayBuffer[Source] = {
    metricsSystem.invokePrivate(_sources())
  }
  private val _streamingSource = PrivateMethod[StreamingSource]('streamingSource)
  private def getStreamingSource(streamingContext: StreamingContext): StreamingSource = {
    streamingContext.invokePrivate(_streamingSource())
  }
}
