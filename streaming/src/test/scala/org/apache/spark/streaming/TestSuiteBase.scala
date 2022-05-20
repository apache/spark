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

import java.io.{IOException, ObjectInputStream}
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually.timeout
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.time.{Seconds => ScalaTestSeconds, Span}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ForEachDStream, InputDStream}
import org.apache.spark.streaming.scheduler._
import org.apache.spark.util.{ManualClock, Utils}

/**
 * A dummy stream that does absolutely nothing.
 */
private[streaming] class DummyDStream(ssc: StreamingContext) extends DStream[Int](ssc) {
  override def dependencies: List[DStream[Int]] = List.empty
  override def slideDuration: Duration = Seconds(1)
  override def compute(time: Time): Option[RDD[Int]] = Some(ssc.sc.emptyRDD[Int])
}

/**
 * A dummy input stream that does absolutely nothing.
 */
private[streaming] class DummyInputDStream(ssc: StreamingContext) extends InputDStream[Int](ssc) {
  override def start(): Unit = { }
  override def stop(): Unit = { }
  override def compute(time: Time): Option[RDD[Int]] = Some(ssc.sc.emptyRDD[Int])
}

/**
 * This is a input stream just for the testsuites. This is equivalent to a checkpointable,
 * replayable, reliable message queue like Kafka. It requires a sequence as input, and
 * returns the i_th element at the i_th batch under manual clock.
 */
class TestInputStream[T: ClassTag](_ssc: StreamingContext, input: Seq[Seq[T]], numPartitions: Int)
  extends InputDStream[T](_ssc) {

  def start(): Unit = {}

  def stop(): Unit = {}

  def compute(validTime: Time): Option[RDD[T]] = {
    logInfo("Computing RDD for time " + validTime)
    val index = ((validTime - zeroTime) / slideDuration - 1).toInt
    val selectedInput = if (index < input.size) input(index) else Seq[T]()

    // lets us test cases where RDDs are not created
    if (selectedInput == null) {
      return None
    }

    // Report the input data's information to InputInfoTracker for testing
    val inputInfo = StreamInputInfo(id, selectedInput.length.toLong)
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)

    val rdd = ssc.sc.makeRDD(selectedInput, numPartitions)
    logInfo("Created RDD " + rdd.id + " with " + selectedInput)
    Some(rdd)
  }
}

/**
 * This is a output stream just for the testsuites. All the output is collected into a
 * ConcurrentLinkedQueue. This queue is wiped clean on being restored from checkpoint.
 *
 * The buffer contains a sequence of RDD's, each containing a sequence of items.
 */
class TestOutputStream[T: ClassTag](
    parent: DStream[T],
    val output: ConcurrentLinkedQueue[Seq[T]] =
      new ConcurrentLinkedQueue[Seq[T]]()
  ) extends ForEachDStream[T](parent, (rdd: RDD[T], t: Time) => {
    val collected = rdd.collect()
    output.add(collected)
  }, false) {

  // This is to clear the output buffer every it is read from a checkpoint
  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
    ois.defaultReadObject()
    output.clear()
  }
}

/**
 * This is a output stream just for the testsuites. All the output is collected into a
 * ConcurrentLinkedQueue. This queue is wiped clean on being restored from checkpoint.
 *
 * The queue contains a sequence of RDD's, each containing a sequence of partitions, each
 * containing a sequence of items.
 */
class TestOutputStreamWithPartitions[T: ClassTag](
    parent: DStream[T],
    val output: ConcurrentLinkedQueue[Seq[Seq[T]]] =
      new ConcurrentLinkedQueue[Seq[Seq[T]]]())
  extends ForEachDStream[T](parent, (rdd: RDD[T], t: Time) => {
    val collected = rdd.glom().collect().map(_.toSeq)
    output.add(collected)
  }, false) {

  // This is to clear the output buffer every it is read from a checkpoint
  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
    ois.defaultReadObject()
    output.clear()
  }
}

/**
 * An object that counts the number of started / completed batches. This is implemented using a
 * StreamingListener. Constructing a new instance automatically registers a StreamingListener on
 * the given StreamingContext.
 */
class BatchCounter(ssc: StreamingContext) {

  // All access to this state should be guarded by `BatchCounter.this.synchronized`
  private var numCompletedBatches = 0
  private var numStartedBatches = 0
  private var lastCompletedBatchTime: Time = null

  private val listener = new StreamingListener {
    override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit =
      BatchCounter.this.synchronized {
        numStartedBatches += 1
        BatchCounter.this.notifyAll()
      }
    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit =
      BatchCounter.this.synchronized {
        numCompletedBatches += 1
        lastCompletedBatchTime = batchCompleted.batchInfo.batchTime
        BatchCounter.this.notifyAll()
      }
  }
  ssc.addStreamingListener(listener)

  def getNumCompletedBatches: Int = this.synchronized {
    numCompletedBatches
  }

  def getNumStartedBatches: Int = this.synchronized {
    numStartedBatches
  }

  def getLastCompletedBatchTime: Time = this.synchronized {
    lastCompletedBatchTime
  }

  /**
   * Wait until `expectedNumCompletedBatches` batches are completed, or timeout. Return true if
   * `expectedNumCompletedBatches` batches are completed. Otherwise, return false to indicate it's
   * timeout.
   *
   * @param expectedNumCompletedBatches the `expectedNumCompletedBatches` batches to wait
   * @param timeout the maximum time to wait in milliseconds.
   */
  def waitUntilBatchesCompleted(expectedNumCompletedBatches: Int, timeout: Long): Boolean =
    waitUntilConditionBecomeTrue(numCompletedBatches >= expectedNumCompletedBatches, timeout)

  /**
   * Wait until `expectedNumStartedBatches` batches are completed, or timeout. Return true if
   * `expectedNumStartedBatches` batches are completed. Otherwise, return false to indicate it's
   * timeout.
   *
   * @param expectedNumStartedBatches the `expectedNumStartedBatches` batches to wait
   * @param timeout the maximum time to wait in milliseconds.
   */
  def waitUntilBatchesStarted(expectedNumStartedBatches: Int, timeout: Long): Boolean =
    waitUntilConditionBecomeTrue(numStartedBatches >= expectedNumStartedBatches, timeout)

  private def waitUntilConditionBecomeTrue(condition: => Boolean, timeout: Long): Boolean = {
    synchronized {
      var now = System.currentTimeMillis()
      val timeoutTick = now + timeout
      while (!condition && timeoutTick > now) {
        wait(timeoutTick - now)
        now = System.currentTimeMillis()
      }
      condition
    }
  }
}

/**
 * This is the base trait for Spark Streaming testsuites. This provides basic functionality
 * to run user-defined set of input on user-defined stream operations, and verify the output.
 */
trait TestSuiteBase extends SparkFunSuite with BeforeAndAfterEach with Logging {

  // Name of the framework for Spark context
  def framework: String = this.getClass.getSimpleName

  // Master for Spark context
  def master: String = "local[2]"

  // Batch duration
  def batchDuration: Duration = Seconds(1)

  // Directory where the checkpoint data will be saved
  lazy val checkpointDir: String = {
    val dir = Utils.createTempDir()
    logDebug(s"checkpointDir: $dir")
    dir.toString
  }

  // Number of partitions of the input parallel collections created for testing
  def numInputPartitions: Int = 2

  // Maximum time to wait before the test times out
  def maxWaitTimeMillis: Int = 10000

  // Whether to use manual clock or not
  def useManualClock: Boolean = true

  // Whether to actually wait in real time before changing manual clock
  def actuallyWait: Boolean = false

  // A SparkConf to use in tests. Can be modified before calling setupStreams to configure things.
  val conf = new SparkConf()
    .setMaster(master)
    .setAppName(framework)

  // Timeout for use in ScalaTest `eventually` blocks
  val eventuallyTimeout: PatienceConfiguration.Timeout = timeout(Span(10, ScalaTestSeconds))

  // Default before function for any streaming test suite. Override this
  // if you want to add your stuff to "beforeEach"
  def beforeFunction(): Unit = {
    if (useManualClock) {
      logInfo("Using manual clock")
      conf.set("spark.streaming.clock", "org.apache.spark.util.ManualClock")
    } else {
      logInfo("Using real clock")
      conf.set("spark.streaming.clock", "org.apache.spark.util.SystemClock")
    }
  }

  // Default after function for any streaming test suite. Override this
  // if you want to add your stuff to "afterEach"
  def afterFunction(): Unit = {
    System.clearProperty("spark.streaming.clock")
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    beforeFunction()
  }

  override def afterEach(): Unit = {
    try {
      afterFunction()
    } finally {
      super.afterEach()
    }

  }

  /**
   * Run a block of code with the given StreamingContext and automatically
   * stop the context when the block completes or when an exception is thrown.
   */
  def withStreamingContext[R](ssc: StreamingContext)(block: StreamingContext => R): R = {
    try {
      block(ssc)
    } finally {
      LocalStreamingContext.stop(ssc, stopSparkContext = true)
    }
  }

  /**
   * Run a block of code with the given TestServer and automatically
   * stop the server when the block completes or when an exception is thrown.
   */
  def withTestServer[R](testServer: TestServer)(block: TestServer => R): R = {
    try {
      block(testServer)
    } finally {
      try {
        testServer.stop()
      } catch {
        case e: Exception =>
          logError("Error stopping TestServer", e)
      }
    }
  }

  /**
   * Set up required DStreams to test the DStream operation using the two sequences
   * of input collections.
   */
  def setupStreams[U: ClassTag, V: ClassTag](
      input: Seq[Seq[U]],
      operation: DStream[U] => DStream[V],
      numPartitions: Int = numInputPartitions
    ): StreamingContext = {
    // Create StreamingContext
    val ssc = new StreamingContext(conf, batchDuration)
    if (checkpointDir != null) {
      ssc.checkpoint(checkpointDir)
    }

    // Setup the stream computation
    val inputStream = new TestInputStream(ssc, input, numPartitions)
    val operatedStream = operation(inputStream)
    val outputStream = new TestOutputStreamWithPartitions(operatedStream,
      new ConcurrentLinkedQueue[Seq[Seq[V]]])
    outputStream.register()
    ssc
  }

  /**
   * Set up required DStreams to test the binary operation using the sequence
   * of input collections.
   */
  def setupStreams[U: ClassTag, V: ClassTag, W: ClassTag](
      input1: Seq[Seq[U]],
      input2: Seq[Seq[V]],
      operation: (DStream[U], DStream[V]) => DStream[W]
    ): StreamingContext = {
    // Create StreamingContext
    val ssc = new StreamingContext(conf, batchDuration)
    if (checkpointDir != null) {
      ssc.checkpoint(checkpointDir)
    }

    // Setup the stream computation
    val inputStream1 = new TestInputStream(ssc, input1, numInputPartitions)
    val inputStream2 = new TestInputStream(ssc, input2, numInputPartitions)
    val operatedStream = operation(inputStream1, inputStream2)
    val outputStream = new TestOutputStreamWithPartitions(operatedStream,
      new ConcurrentLinkedQueue[Seq[Seq[W]]])
    outputStream.register()
    ssc
  }

  /**
   * Runs the streams set up in `ssc` on manual clock for `numBatches` batches and
   * returns the collected output. It will wait until `numExpectedOutput` number of
   * output data has been collected or timeout (set by `maxWaitTimeMillis`) is reached.
   *
   * Returns a sequence of items for each RDD.
   *
   * @param ssc The StreamingContext
   * @param numBatches The number of batches should be run
   * @param numExpectedOutput The number of expected output
   * @param preStop The function to run before stopping StreamingContext
   */
  def runStreams[V: ClassTag](
      ssc: StreamingContext,
      numBatches: Int,
      numExpectedOutput: Int,
      preStop: () => Unit = () => {}
    ): Seq[Seq[V]] = {
    // Flatten each RDD into a single Seq
    runStreamsWithPartitions(ssc, numBatches, numExpectedOutput, preStop).map(_.flatten.toSeq)
  }

  /**
   * Runs the streams set up in `ssc` on manual clock for `numBatches` batches and
   * returns the collected output. It will wait until `numExpectedOutput` number of
   * output data has been collected or timeout (set by `maxWaitTimeMillis`) is reached.
   *
   * Returns a sequence of RDD's. Each RDD is represented as several sequences of items, each
   * representing one partition.
   *
   * @param ssc The StreamingContext
   * @param numBatches The number of batches should be run
   * @param numExpectedOutput The number of expected output
   * @param preStop The function to run before stopping StreamingContext
   */
  def runStreamsWithPartitions[V: ClassTag](
      ssc: StreamingContext,
      numBatches: Int,
      numExpectedOutput: Int,
      preStop: () => Unit = () => {}
    ): Seq[Seq[Seq[V]]] = {
    assert(numBatches > 0, "Number of batches to run stream computation is zero")
    assert(numExpectedOutput > 0, "Number of expected outputs after " + numBatches + " is zero")
    logInfo("numBatches = " + numBatches + ", numExpectedOutput = " + numExpectedOutput)

    // Get the output buffer
    val outputStream = ssc.graph.getOutputStreams.
      filter(_.isInstanceOf[TestOutputStreamWithPartitions[_]]).
      head.asInstanceOf[TestOutputStreamWithPartitions[V]]
    val output = outputStream.output

    try {
      // Start computation
      ssc.start()

      // Advance manual clock
      val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
      logInfo("Manual clock before advancing = " + clock.getTimeMillis())
      if (actuallyWait) {
        for (i <- 1 to numBatches) {
          logInfo("Actually waiting for " + batchDuration)
          clock.advance(batchDuration.milliseconds)
          Thread.sleep(batchDuration.milliseconds)
        }
      } else {
        clock.advance(numBatches * batchDuration.milliseconds)
      }
      logInfo("Manual clock after advancing = " + clock.getTimeMillis())

      // Wait until expected number of output items have been generated
      val startTimeNs = System.nanoTime()
      while (output.size < numExpectedOutput &&
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs) < maxWaitTimeMillis) {
        logInfo("output.size = " + output.size + ", numExpectedOutput = " + numExpectedOutput)
        ssc.awaitTerminationOrTimeout(50)
      }
      val timeTaken = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)
      logInfo("Output generated in " + timeTaken + " milliseconds")
      output.asScala.foreach(x => logInfo("[" + x.mkString(",") + "]"))
      assert(timeTaken < maxWaitTimeMillis, "Operation timed out after " + timeTaken + " ms")
      assert(output.size === numExpectedOutput, "Unexpected number of outputs generated")

      Thread.sleep(100) // Give some time for the forgetting old RDDs to complete
      preStop()
    } finally {
      ssc.stop(stopSparkContext = true)
    }
    output.asScala.toSeq
  }

  /**
   * Verify whether the output values after running a DStream operation
   * is same as the expected output values, by comparing the output
   * collections either as lists (order matters) or sets (order does not matter)
   */
  def verifyOutput[V: ClassTag](
      output: Seq[Seq[V]],
      expectedOutput: Seq[Seq[V]],
      useSet: Boolean
    ): Unit = {
    logInfo("--------------------------------")
    logInfo("output.size = " + output.size)
    logInfo("output")
    output.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    logInfo("expected output.size = " + expectedOutput.size)
    logInfo("expected output")
    expectedOutput.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    logInfo("--------------------------------")

    // Match the output with the expected output
    for (i <- 0 until output.size) {
      if (useSet) {
        assert(
          output(i).toSet === expectedOutput(i).toSet,
          s"Set comparison failed\n" +
            s"Expected output (${expectedOutput.size} items):\n${expectedOutput.mkString("\n")}\n" +
            s"Generated output (${output.size} items): ${output.mkString("\n")}"
        )
      } else {
        assert(
          output(i).toList === expectedOutput(i).toList,
          s"Ordered list comparison failed\n" +
            s"Expected output (${expectedOutput.size} items):\n${expectedOutput.mkString("\n")}\n" +
            s"Generated output (${output.size} items): ${output.mkString("\n")}"
        )
      }
    }
    logInfo("Output verified successfully")
  }

  /**
   * Test unary DStream operation with a list of inputs, with number of
   * batches to run same as the number of expected output values
   */
  def testOperation[U: ClassTag, V: ClassTag](
      input: Seq[Seq[U]],
      operation: DStream[U] => DStream[V],
      expectedOutput: Seq[Seq[V]],
      useSet: Boolean = false
    ): Unit = {
    testOperation[U, V](input, operation, expectedOutput, -1, useSet)
  }

  /**
   * Test unary DStream operation with a list of inputs
   * @param input      Sequence of input collections
   * @param operation  Binary DStream operation to be applied to the 2 inputs
   * @param expectedOutput Sequence of expected output collections
   * @param numBatches Number of batches to run the operation for
   * @param useSet     Compare the output values with the expected output values
   *                   as sets (order matters) or as lists (order does not matter)
   */
  def testOperation[U: ClassTag, V: ClassTag](
      input: Seq[Seq[U]],
      operation: DStream[U] => DStream[V],
      expectedOutput: Seq[Seq[V]],
      numBatches: Int,
      useSet: Boolean
    ): Unit = {
    val numBatches_ = if (numBatches > 0) numBatches else expectedOutput.size
    withStreamingContext(setupStreams[U, V](input, operation)) { ssc =>
      val output = runStreams[V](ssc, numBatches_, expectedOutput.size)
      verifyOutput[V](output.toSeq, expectedOutput, useSet)
    }
  }

  /**
   * Test binary DStream operation with two lists of inputs, with number of
   * batches to run same as the number of expected output values
   */
  def testOperation[U: ClassTag, V: ClassTag, W: ClassTag](
      input1: Seq[Seq[U]],
      input2: Seq[Seq[V]],
      operation: (DStream[U], DStream[V]) => DStream[W],
      expectedOutput: Seq[Seq[W]],
      useSet: Boolean
    ): Unit = {
    testOperation[U, V, W](input1, input2, operation, expectedOutput, -1, useSet)
  }

  /**
   * Test binary DStream operation with two lists of inputs
   * @param input1     First sequence of input collections
   * @param input2     Second sequence of input collections
   * @param operation  Binary DStream operation to be applied to the 2 inputs
   * @param expectedOutput Sequence of expected output collections
   * @param numBatches Number of batches to run the operation for
   * @param useSet     Compare the output values with the expected output values
   *                   as sets (order matters) or as lists (order does not matter)
   */
  def testOperation[U: ClassTag, V: ClassTag, W: ClassTag](
      input1: Seq[Seq[U]],
      input2: Seq[Seq[V]],
      operation: (DStream[U], DStream[V]) => DStream[W],
      expectedOutput: Seq[Seq[W]],
      numBatches: Int,
      useSet: Boolean
    ): Unit = {
    val numBatches_ = if (numBatches > 0) numBatches else expectedOutput.size
    withStreamingContext(setupStreams[U, V, W](input1, input2, operation)) { ssc =>
      val output = runStreams[W](ssc, numBatches_, expectedOutput.size)
      verifyOutput[W](output.toSeq, expectedOutput, useSet)
    }
  }
}
