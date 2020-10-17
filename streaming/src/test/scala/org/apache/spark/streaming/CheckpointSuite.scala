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

import java.io._
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import com.google.common.io.Files
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.hadoop.mapreduce.lib.output.{TextOutputFormat => NewTextOutputFormat}
import org.mockito.Mockito.mock
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite, TestUtils}
import org.apache.spark.internal.config._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.scheduler._
import org.apache.spark.util.{Clock, ManualClock, MutableURLClassLoader, ResetSystemProperties,
  Utils}

/**
 * A input stream that records the times of restore() invoked
 */
private[streaming]
class CheckpointInputDStream(_ssc: StreamingContext) extends InputDStream[Int](_ssc) {
  protected[streaming] override val checkpointData = new FileInputDStreamCheckpointData
  override def start(): Unit = { }
  override def stop(): Unit = { }
  override def compute(time: Time): Option[RDD[Int]] = Some(ssc.sc.makeRDD(Seq(1)))
  private[streaming]
  class FileInputDStreamCheckpointData extends DStreamCheckpointData(this) {
    @transient
    var restoredTimes = 0
    override def restore() {
      restoredTimes += 1
      super.restore()
    }
  }
}

/**
 * A trait of that can be mixed in to get methods for testing DStream operations under
 * DStream checkpointing. Note that the implementations of this trait has to implement
 * the `setupCheckpointOperation`
 */
trait DStreamCheckpointTester { self: SparkFunSuite =>

  /**
   * Tests a streaming operation under checkpointing, by restarting the operation
   * from checkpoint file and verifying whether the final output is correct.
   * The output is assumed to have come from a reliable queue which a replay
   * data as required.
   *
   * NOTE: This takes into consideration that the last batch processed before
   * master failure will be re-processed after restart/recovery.
   */
  protected def testCheckpointedOperation[U: ClassTag, V: ClassTag](
      input: Seq[Seq[U]],
      operation: DStream[U] => DStream[V],
      expectedOutput: Seq[Seq[V]],
      numBatchesBeforeRestart: Int,
      batchDuration: Duration = Milliseconds(500),
      stopSparkContextAfterTest: Boolean = true
    ) {
    require(numBatchesBeforeRestart < expectedOutput.size,
      "Number of batches before context restart less than number of expected output " +
        "(i.e. number of total batches to run)")
    require(StreamingContext.getActive().isEmpty,
      "Cannot run test with already active streaming context")

    // Current code assumes that number of batches to be run = number of inputs
    val totalNumBatches = input.size
    val batchDurationMillis = batchDuration.milliseconds

    // Setup the stream computation
    val checkpointDir = Utils.createTempDir(namePrefix = this.getClass.getSimpleName()).toString
    logDebug(s"Using checkpoint directory $checkpointDir")
    val ssc = createContextForCheckpointOperation(batchDuration)
    require(ssc.conf.get("spark.streaming.clock") === classOf[ManualClock].getName,
      "Cannot run test without manual clock in the conf")

    val inputStream = new TestInputStream(ssc, input, numPartitions = 2)
    val operatedStream = operation(inputStream)
    operatedStream.print()
    val outputStream = new TestOutputStreamWithPartitions(operatedStream,
      new ConcurrentLinkedQueue[Seq[Seq[V]]])
    outputStream.register()
    ssc.checkpoint(checkpointDir)

    // Do the computation for initial number of batches, create checkpoint file and quit
    val beforeRestartOutput = generateOutput[V](ssc,
      Time(batchDurationMillis * numBatchesBeforeRestart), checkpointDir, stopSparkContextAfterTest)
    assertOutput(beforeRestartOutput, expectedOutput, beforeRestart = true)
    // Restart and complete the computation from checkpoint file
    logInfo(
      "\n-------------------------------------------\n" +
        "        Restarting stream computation          " +
        "\n-------------------------------------------\n"
    )

    val restartedSsc = new StreamingContext(checkpointDir)
    val afterRestartOutput = generateOutput[V](restartedSsc,
      Time(batchDurationMillis * totalNumBatches), checkpointDir, stopSparkContextAfterTest)
    assertOutput(afterRestartOutput, expectedOutput, beforeRestart = false)
  }

  protected def createContextForCheckpointOperation(batchDuration: Duration): StreamingContext = {
    val conf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)
    conf.set("spark.streaming.clock", classOf[ManualClock].getName())
    new StreamingContext(SparkContext.getOrCreate(conf), batchDuration)
  }

  /**
   * Get the first TestOutputStreamWithPartitions, does not check the provided generic type.
   */
  protected def getTestOutputStream[V: ClassTag](streams: Array[DStream[_]]):
    TestOutputStreamWithPartitions[V] = {
    streams.collect {
      case ds: TestOutputStreamWithPartitions[V @unchecked] => ds
    }.head
  }


  protected def generateOutput[V: ClassTag](
      ssc: StreamingContext,
      targetBatchTime: Time,
      checkpointDir: String,
      stopSparkContext: Boolean
    ): Seq[Seq[V]] = {
    try {
      val batchCounter = new BatchCounter(ssc)
      ssc.start()
      val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]

      logInfo("Manual clock before advancing = " + clock.getTimeMillis())
      clock.setTime(targetBatchTime.milliseconds)
      logInfo("Manual clock after advancing = " + clock.getTimeMillis())

      val outputStream = getTestOutputStream[V](ssc.graph.getOutputStreams())

      eventually(timeout(10 seconds)) {
        ssc.awaitTerminationOrTimeout(10)
        assert(batchCounter.getLastCompletedBatchTime === targetBatchTime)
      }

      eventually(timeout(10 seconds)) {
        val checkpointFilesOfLatestTime = Checkpoint.getCheckpointFiles(checkpointDir).filter {
          _.getName.contains(clock.getTimeMillis.toString)
        }
        // Checkpoint files are written twice for every batch interval. So assert that both
        // are written to make sure that both of them have been written.
        assert(checkpointFilesOfLatestTime.size === 2)
      }
      outputStream.output.asScala.map(_.flatten).toSeq

    } finally {
      ssc.stop(stopSparkContext = stopSparkContext)
    }
  }

  private def assertOutput[V: ClassTag](
      output: Seq[Seq[V]],
      expectedOutput: Seq[Seq[V]],
      beforeRestart: Boolean): Unit = {
    val expectedPartialOutput = if (beforeRestart) {
      expectedOutput.take(output.size)
    } else {
      expectedOutput.takeRight(output.size)
    }
    val setComparison = output.zip(expectedPartialOutput).forall {
      case (o, e) => o.toSet === e.toSet
    }
    assert(setComparison, s"set comparison failed\n" +
      s"Expected output items:\n${expectedPartialOutput.mkString("\n")}\n" +
      s"Generated output items: ${output.mkString("\n")}"
    )
  }
}

/**
 * This test suites tests the checkpointing functionality of DStreams -
 * the checkpointing of a DStream's RDDs as well as the checkpointing of
 * the whole DStream graph.
 */
class CheckpointSuite extends TestSuiteBase with DStreamCheckpointTester
  with ResetSystemProperties {

  var ssc: StreamingContext = null

  override def batchDuration: Duration = Milliseconds(500)

  override def beforeFunction() {
    super.beforeFunction()
    Utils.deleteRecursively(new File(checkpointDir))
  }

  override def afterFunction() {
    try {
      if (ssc != null) { ssc.stop() }
      Utils.deleteRecursively(new File(checkpointDir))
    } finally {
      super.afterFunction()
    }
  }

  test("non-existent checkpoint dir") {
    // SPARK-13211
    intercept[IllegalArgumentException](new StreamingContext("nosuchdirectory"))
  }

  test("basic rdd checkpoints + dstream graph checkpoint recovery") {

    assert(batchDuration === Milliseconds(500), "batchDuration for this test must be 1 second")

    conf.set("spark.streaming.clock", "org.apache.spark.util.ManualClock")

    val stateStreamCheckpointInterval = Seconds(1)
    val fs = FileSystem.getLocal(new Configuration())
    // this ensure checkpointing occurs at least once
    val firstNumBatches = (stateStreamCheckpointInterval / batchDuration).toLong * 2
    val secondNumBatches = firstNumBatches

    // Setup the streams
    val input = (1 to 10).map(_ => Seq("a")).toSeq
    val operation = (st: DStream[String]) => {
      val updateFunc = (values: Seq[Int], state: Option[Int]) => {
        Some(values.sum + state.getOrElse(0))
      }
      st.map(x => (x, 1))
      .updateStateByKey(updateFunc)
      .checkpoint(stateStreamCheckpointInterval)
      .map(t => (t._1, t._2))
    }
    var ssc = setupStreams(input, operation)
    var stateStream = ssc.graph.getOutputStreams().head.dependencies.head.dependencies.head

    // Run till a time such that at least one RDD in the stream should have been checkpointed,
    // then check whether some RDD has been checkpointed or not
    ssc.start()
    advanceTimeWithRealDelay(ssc, firstNumBatches)
    logInfo("Checkpoint data of state stream = \n" + stateStream.checkpointData)
    assert(!stateStream.checkpointData.currentCheckpointFiles.isEmpty,
      "No checkpointed RDDs in state stream before first failure")
    stateStream.checkpointData.currentCheckpointFiles.foreach {
      case (time, file) =>
        assert(fs.exists(new Path(file)), "Checkpoint file '" + file +"' for time " + time +
            " for state stream before first failure does not exist")
    }

    // Run till a further time such that previous checkpoint files in the stream would be deleted
    // and check whether the earlier checkpoint files are deleted
    val checkpointFiles = stateStream.checkpointData.currentCheckpointFiles.map(x => new File(x._2))
    advanceTimeWithRealDelay(ssc, secondNumBatches)
    checkpointFiles.foreach(file =>
      assert(!file.exists, "Checkpoint file '" + file + "' was not deleted"))
    ssc.stop()

    // Restart stream computation using the checkpoint file and check whether
    // checkpointed RDDs have been restored or not
    ssc = new StreamingContext(checkpointDir)
    stateStream = ssc.graph.getOutputStreams().head.dependencies.head.dependencies.head
    logInfo("Restored data of state stream = \n[" + stateStream.generatedRDDs.mkString("\n") + "]")
    assert(!stateStream.generatedRDDs.isEmpty,
      "No restored RDDs in state stream after recovery from first failure")


    // Run one batch to generate a new checkpoint file and check whether some RDD
    // is present in the checkpoint data or not
    ssc.start()
    advanceTimeWithRealDelay(ssc, 1)
    assert(!stateStream.checkpointData.currentCheckpointFiles.isEmpty,
      "No checkpointed RDDs in state stream before second failure")
    stateStream.checkpointData.currentCheckpointFiles.foreach {
      case (time, file) =>
        assert(fs.exists(new Path(file)), "Checkpoint file '" + file +"' for time " + time +
          " for state stream before seconds failure does not exist")
    }
    ssc.stop()

    // Restart stream computation from the new checkpoint file to see whether that file has
    // correct checkpoint data
    ssc = new StreamingContext(checkpointDir)
    stateStream = ssc.graph.getOutputStreams().head.dependencies.head.dependencies.head
    logInfo("Restored data of state stream = \n[" + stateStream.generatedRDDs.mkString("\n") + "]")
    assert(!stateStream.generatedRDDs.isEmpty,
      "No restored RDDs in state stream after recovery from second failure")

    // Adjust manual clock time as if it is being restarted after a delay; this is a hack because
    // we modify the conf object, but it works for this one property
    ssc.conf.set("spark.streaming.manualClock.jump", (batchDuration.milliseconds * 7).toString)
    ssc.start()
    advanceTimeWithRealDelay(ssc, 4)
    ssc.stop()
    ssc = null
  }

  // This tests whether spark conf persists through checkpoints, and certain
  // configs gets scrubbed
  test("recovery of conf through checkpoints") {
    val key = "spark.mykey"
    val value = "myvalue"
    System.setProperty(key, value)
    ssc = new StreamingContext(master, framework, batchDuration)
    val originalConf = ssc.conf

    val cp = new Checkpoint(ssc, Time(1000))
    val cpConf = cp.createSparkConf()
    assert(cpConf.get("spark.master") === originalConf.get("spark.master"))
    assert(cpConf.get("spark.app.name") === originalConf.get("spark.app.name"))
    assert(cpConf.get(key) === value)
    ssc.stop()

    // Serialize/deserialize to simulate write to storage and reading it back
    val newCp = Utils.deserialize[Checkpoint](Utils.serialize(cp))

    // Verify new SparkConf has all the previous properties
    val newCpConf = newCp.createSparkConf()
    assert(newCpConf.get("spark.master") === originalConf.get("spark.master"))
    assert(newCpConf.get("spark.app.name") === originalConf.get("spark.app.name"))
    assert(newCpConf.get(key) === value)
    assert(!newCpConf.contains("spark.driver.host"))
    assert(!newCpConf.contains("spark.driver.port"))

    // Check if all the parameters have been restored
    ssc = new StreamingContext(null, newCp, null)
    val restoredConf = ssc.conf
    assert(restoredConf.get(key) === value)
    ssc.stop()

    // Verify new SparkConf picks up new master url if it is set in the properties. See SPARK-6331.
    try {
      val newMaster = "local[100]"
      System.setProperty("spark.master", newMaster)
      val newCpConf = newCp.createSparkConf()
      assert(newCpConf.get("spark.master") === newMaster)
      assert(newCpConf.get("spark.app.name") === originalConf.get("spark.app.name"))
      ssc = new StreamingContext(null, newCp, null)
      assert(ssc.sparkContext.master === newMaster)
    } finally {
      System.clearProperty("spark.master")
    }
  }

  // This tests if "spark.driver.host" and "spark.driver.port" is set by user, can be recovered
  // with correct value.
  test("get correct spark.driver.[host|port] from checkpoint") {
    val conf = Map("spark.driver.host" -> "localhost", "spark.driver.port" -> "9999")
    conf.foreach(kv => System.setProperty(kv._1, kv._2))
    ssc = new StreamingContext(master, framework, batchDuration)
    val originalConf = ssc.conf
    assert(originalConf.get("spark.driver.host") === "localhost")
    assert(originalConf.get("spark.driver.port") === "9999")

    val cp = new Checkpoint(ssc, Time(1000))
    ssc.stop()

    // Serialize/deserialize to simulate write to storage and reading it back
    val newCp = Utils.deserialize[Checkpoint](Utils.serialize(cp))

    val newCpConf = newCp.createSparkConf()
    assert(newCpConf.contains("spark.driver.host"))
    assert(newCpConf.contains("spark.driver.port"))
    assert(newCpConf.get("spark.driver.host") === "localhost")
    assert(newCpConf.get("spark.driver.port") === "9999")

    // Check if all the parameters have been restored
    ssc = new StreamingContext(null, newCp, null)
    val restoredConf = ssc.conf
    assert(restoredConf.get("spark.driver.host") === "localhost")
    assert(restoredConf.get("spark.driver.port") === "9999")
    ssc.stop()

    // If spark.driver.host and spark.driver.host is not set in system property, these two
    // parameters should not be presented in the newly recovered conf.
    conf.foreach(kv => System.clearProperty(kv._1))
    val newCpConf1 = newCp.createSparkConf()
    assert(!newCpConf1.contains("spark.driver.host"))
    assert(!newCpConf1.contains("spark.driver.port"))

    // Spark itself will dispatch a random, not-used port for spark.driver.port if it is not set
    // explicitly.
    ssc = new StreamingContext(null, newCp, null)
    val restoredConf1 = ssc.conf
    val defaultConf = new SparkConf()
    assert(restoredConf1.get("spark.driver.host") === defaultConf.get(DRIVER_HOST_ADDRESS))
    assert(restoredConf1.get("spark.driver.port") !== "9999")
  }

  test("SPARK-30199 get ui port and blockmanager port") {
    val conf = Map("spark.ui.port" -> "30001", "spark.blockManager.port" -> "30002")
    conf.foreach { case (k, v) => System.setProperty(k, v) }
    ssc = new StreamingContext(master, framework, batchDuration)
    conf.foreach { case (k, v) => assert(ssc.conf.get(k) === v) }

    val cp = new Checkpoint(ssc, Time(1000))
    ssc.stop()

    // Serialize/deserialize to simulate write to storage and reading it back
    val newCp = Utils.deserialize[Checkpoint](Utils.serialize(cp))

    val newCpConf = newCp.createSparkConf()
    conf.foreach { case (k, v) => assert(newCpConf.contains(k) && newCpConf.get(k) === v) }

    // Check if all the parameters have been restored
    ssc = new StreamingContext(null, newCp, null)
    conf.foreach { case (k, v) => assert(ssc.conf.get(k) === v) }
    ssc.stop()

    // If port numbers are not set in system property, these parameters should not be presented
    // in the newly recovered conf.
    conf.foreach(kv => System.clearProperty(kv._1))
    val newCpConf1 = newCp.createSparkConf()
    conf.foreach { case (k, _) => assert(!newCpConf1.contains(k)) }
  }

  // This tests whether the system can recover from a master failure with simple
  // non-stateful operations. This assumes as reliable, replayable input
  // source - TestInputDStream.
  test("recovery with map and reduceByKey operations") {
    testCheckpointedOperation(
      Seq( Seq("a", "a", "b"), Seq("", ""), Seq(), Seq("a", "a", "b"), Seq("", ""), Seq() ),
      (s: DStream[String]) => s.map(x => (x, 1)).reduceByKey(_ + _),
      Seq(
        Seq(("a", 2), ("b", 1)),
        Seq(("", 2)),
        Seq(),
        Seq(("a", 2), ("b", 1)),
        Seq(("", 2)),
        Seq()
      ),
      3
    )
  }


  // This tests whether the ReduceWindowedDStream's RDD checkpoints works correctly such
  // that the system can recover from a master failure. This assumes as reliable,
  // replayable input source - TestInputDStream.
  test("recovery with invertible reduceByKeyAndWindow operation") {
    val n = 10
    val w = 4
    val input = (1 to n).map(_ => Seq("a")).toSeq
    val output = Seq(
      Seq(("a", 1)), Seq(("a", 2)), Seq(("a", 3))) ++ (1 to (n - w + 1)).map(x => Seq(("a", 4)))
    val operation = (st: DStream[String]) => {
      st.map(x => (x, 1))
        .reduceByKeyAndWindow(_ + _, _ - _, batchDuration * w, batchDuration)
        .checkpoint(batchDuration * 2)
    }
    testCheckpointedOperation(input, operation, output, 7)
  }

  test("recovery with saveAsHadoopFiles operation") {
    val tempDir = Utils.createTempDir()
    try {
      testCheckpointedOperation(
        Seq(Seq("a", "a", "b"), Seq("", ""), Seq(), Seq("a", "a", "b"), Seq("", ""), Seq()),
        (s: DStream[String]) => {
          val output = s.map(x => (x, 1)).reduceByKey(_ + _)
          output.saveAsHadoopFiles(
            tempDir.toURI.toString,
            "result",
            classOf[Text],
            classOf[IntWritable],
            classOf[TextOutputFormat[Text, IntWritable]])
          output
        },
        Seq(
          Seq(("a", 2), ("b", 1)),
          Seq(("", 2)),
          Seq(),
          Seq(("a", 2), ("b", 1)),
          Seq(("", 2)),
          Seq()),
        3
      )
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }

  test("recovery with saveAsNewAPIHadoopFiles operation") {
    val tempDir = Utils.createTempDir()
    try {
      testCheckpointedOperation(
        Seq(Seq("a", "a", "b"), Seq("", ""), Seq(), Seq("a", "a", "b"), Seq("", ""), Seq()),
        (s: DStream[String]) => {
          val output = s.map(x => (x, 1)).reduceByKey(_ + _)
          output.saveAsNewAPIHadoopFiles(
            tempDir.toURI.toString,
            "result",
            classOf[Text],
            classOf[IntWritable],
            classOf[NewTextOutputFormat[Text, IntWritable]])
          output
        },
        Seq(
          Seq(("a", 2), ("b", 1)),
          Seq(("", 2)),
          Seq(),
          Seq(("a", 2), ("b", 1)),
          Seq(("", 2)),
          Seq()),
        3
      )
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }

  test("recovery with saveAsHadoopFile inside transform operation") {
    // Regression test for SPARK-4835.
    //
    // In that issue, the problem was that `saveAsHadoopFile(s)` would fail when the last batch
    // was restarted from a checkpoint since the output directory would already exist.  However,
    // the other saveAsHadoopFile* tests couldn't catch this because they only tested whether the
    // output matched correctly and not whether the post-restart batch had successfully finished
    // without throwing any errors.  The following test reproduces the same bug with a test that
    // actually fails because the error in saveAsHadoopFile causes transform() to fail, which
    // prevents the expected output from being written to the output stream.
    //
    // This is not actually a valid use of transform, but it's being used here so that we can test
    // the fix for SPARK-4835 independently of additional test cleanup.
    //
    // After SPARK-5079 is addressed, should be able to remove this test since a strengthened
    // version of the other saveAsHadoopFile* tests would prevent regressions for this issue.
    val tempDir = Utils.createTempDir()
    try {
      testCheckpointedOperation(
        Seq(Seq("a", "a", "b"), Seq("", ""), Seq(), Seq("a", "a", "b"), Seq("", ""), Seq()),
        (s: DStream[String]) => {
          s.transform { (rdd, time) =>
            val output = rdd.map(x => (x, 1)).reduceByKey(_ + _)
            output.saveAsHadoopFile(
              new File(tempDir, "result-" + time.milliseconds).getAbsolutePath,
              classOf[Text],
              classOf[IntWritable],
              classOf[TextOutputFormat[Text, IntWritable]])
            output
          }
        },
        Seq(
          Seq(("a", 2), ("b", 1)),
          Seq(("", 2)),
          Seq(),
          Seq(("a", 2), ("b", 1)),
          Seq(("", 2)),
          Seq()),
        3
      )
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }

  // This tests whether the StateDStream's RDD checkpoints works correctly such
  // that the system can recover from a master failure. This assumes as reliable,
  // replayable input source - TestInputDStream.
  test("recovery with updateStateByKey operation") {
    val input = (1 to 10).map(_ => Seq("a")).toSeq
    val output = (1 to 10).map(x => Seq(("a", x))).toSeq
    val operation = (st: DStream[String]) => {
      val updateFunc = (values: Seq[Int], state: Option[Int]) => {
        Some((values.sum + state.getOrElse(0)))
      }
      st.map(x => (x, 1))
        .updateStateByKey(updateFunc)
        .checkpoint(batchDuration * 2)
        .map(t => (t._1, t._2))
    }
    testCheckpointedOperation(input, operation, output, 7)
  }

  test("recovery maintains rate controller") {
    ssc = new StreamingContext(conf, batchDuration)
    ssc.checkpoint(checkpointDir)

    val dstream = new RateTestInputDStream(ssc) {
      override val rateController =
        Some(new ReceiverRateController(id, new ConstantEstimator(200)))
    }

    val output = new TestOutputStreamWithPartitions(dstream.checkpoint(batchDuration * 2))
    output.register()
    runStreams(ssc, 5, 5)

    ssc = new StreamingContext(checkpointDir)
    ssc.start()

    eventually(timeout(10.seconds)) {
      assert(RateTestReceiver.getActive().nonEmpty)
    }

    advanceTimeWithRealDelay(ssc, 2)

    eventually(timeout(10.seconds)) {
      assert(RateTestReceiver.getActive().get.getDefaultBlockGeneratorRateLimit() === 200)
    }
    ssc.stop()
  }

  // This tests whether file input stream remembers what files were seen before
  // the master failure and uses them again to process a large window operation.
  // It also tests whether batches, whose processing was incomplete due to the
  // failure, are re-processed or not.
  test("recovery with file input stream") {
    // Set up the streaming context and input streams
    val batchDuration = Seconds(2)  // Due to 1-second resolution of setLastModified() on some OS's.
    val testDir = Utils.createTempDir()
    val outputBuffer = new ConcurrentLinkedQueue[Seq[Int]]

    /**
     * Writes a file named `i` (which contains the number `i`) to the test directory and sets its
     * modification time to `clock`'s current time.
     */
    def writeFile(i: Int, clock: Clock): Unit = {
      val file = new File(testDir, i.toString)
      Files.write(i + "\n", file, StandardCharsets.UTF_8)
      assert(file.setLastModified(clock.getTimeMillis()))
      // Check that the file's modification date is actually the value we wrote, since rounding or
      // truncation will break the test:
      assert(file.lastModified() === clock.getTimeMillis())
    }

    /**
     * Returns ids that identify which files which have been recorded by the file input stream.
     */
    def recordedFiles(ssc: StreamingContext): Seq[Int] = {
      val fileInputDStream =
        ssc.graph.getInputStreams().head.asInstanceOf[FileInputDStream[_, _, _]]
      val filenames = fileInputDStream.batchTimeToSelectedFiles.synchronized
         { fileInputDStream.batchTimeToSelectedFiles.values.flatten }
      filenames.map(_.split("/").last.toInt).toSeq.sorted
    }

    try {
      // This is a var because it's re-assigned when we restart from a checkpoint
      var clock: ManualClock = null
      withStreamingContext(new StreamingContext(conf, batchDuration)) { ssc =>
        ssc.checkpoint(checkpointDir)
        clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
        val batchCounter = new BatchCounter(ssc)
        val fileStream = ssc.textFileStream(testDir.toString)
        // Make value 3 take a large time to process, to ensure that the driver
        // shuts down in the middle of processing the 3rd batch
        CheckpointSuite.batchThreeShouldBlockALongTime = true
        val mappedStream = fileStream.map { s =>
          val i = s.toInt
          if (i == 3) {
            if (CheckpointSuite.batchThreeShouldBlockALongTime) {
              // It's not a good idea to let the thread run forever
              // as resource won't be correctly released
              Thread.sleep(6000)
            }
          }
          i
        }

        // Reducing over a large window to ensure that recovery from driver failure
        // requires reprocessing of all the files seen before the failure
        val reducedStream = mappedStream.reduceByWindow(_ + _, batchDuration * 30, batchDuration)
        val outputStream = new TestOutputStream(reducedStream, outputBuffer)
        outputStream.register()
        ssc.start()

        // Advance half a batch so that the first file is created after the StreamingContext starts
        clock.advance(batchDuration.milliseconds / 2)
        // Create files and advance manual clock to process them
        for (i <- Seq(1, 2, 3)) {
          writeFile(i, clock)
          // Advance the clock after creating the file to avoid a race when
          // setting its modification time
          clock.advance(batchDuration.milliseconds)
          if (i != 3) {
            // Since we want to shut down while the 3rd batch is processing
            eventually(eventuallyTimeout) {
              assert(batchCounter.getNumCompletedBatches === i)
            }
          }
        }
        eventually(eventuallyTimeout) {
          // Wait until all files have been recorded and all batches have started
          assert(recordedFiles(ssc) === Seq(1, 2, 3) && batchCounter.getNumStartedBatches === 3)
        }
        clock.advance(batchDuration.milliseconds)
        // Wait for a checkpoint to be written
        eventually(eventuallyTimeout) {
          assert(Checkpoint.getCheckpointFiles(checkpointDir).size === 6)
        }
        ssc.stop()
        // Check that we shut down while the third batch was being processed
        assert(batchCounter.getNumCompletedBatches === 2)
        assert(outputStream.output.asScala.toSeq.flatten === Seq(1, 3))
      }

      // The original StreamingContext has now been stopped.
      CheckpointSuite.batchThreeShouldBlockALongTime = false

      // Create files while the streaming driver is down
      for (i <- Seq(4, 5, 6)) {
        writeFile(i, clock)
        // Advance the clock after creating the file to avoid a race when
        // setting its modification time
        clock.advance(batchDuration.milliseconds)
      }

      // Recover context from checkpoint file and verify whether the files that were
      // recorded before failure were saved and successfully recovered
      logInfo("*********** RESTARTING ************")
      withStreamingContext(new StreamingContext(checkpointDir)) { ssc =>
        // "batchDuration.milliseconds * 3" has gone before restarting StreamingContext. And because
        // the recovery time is read from the checkpoint time but the original clock doesn't align
        // with the batch time, we need to add the offset "batchDuration.milliseconds / 2".
        ssc.conf.set("spark.streaming.manualClock.jump",
          (batchDuration.milliseconds / 2 + batchDuration.milliseconds * 3).toString)
        val oldClockTime = clock.getTimeMillis() // 15000ms
        clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
        val batchCounter = new BatchCounter(ssc)
        val outputStream = ssc.graph.getOutputStreams().head.asInstanceOf[TestOutputStream[Int]]
        // Check that we remember files that were recorded before the restart
        assert(recordedFiles(ssc) === Seq(1, 2, 3))

        // Restart stream computation
        ssc.start()
        // Verify that the clock has traveled forward to the expected time
        eventually(eventuallyTimeout) {
          assert(clock.getTimeMillis() === oldClockTime)
        }
        // There are 5 batches between 6000ms and 15000ms (inclusive).
        val numBatchesAfterRestart = 5
        eventually(eventuallyTimeout) {
          assert(batchCounter.getNumCompletedBatches === numBatchesAfterRestart)
        }
        for ((i, index) <- Seq(7, 8, 9).zipWithIndex) {
          writeFile(i, clock)
          // Advance the clock after creating the file to avoid a race when
          // setting its modification time
          clock.advance(batchDuration.milliseconds)
          eventually(eventuallyTimeout) {
            assert(batchCounter.getNumCompletedBatches === index + numBatchesAfterRestart + 1)
          }
        }
        logInfo("Output after restart = " + outputStream.output.asScala.mkString("[", ", ", "]"))
        assert(outputStream.output.size > 0, "No files processed after restart")
        ssc.stop()

        // Verify whether files created while the driver was down (4, 5, 6) and files created after
        // recovery (7, 8, 9) have been recorded
        assert(recordedFiles(ssc) === (1 to 9))

        // Append the new output to the old buffer
        outputBuffer.addAll(outputStream.output)

        // Verify whether all the elements received are as expected
        val expectedOutput = Seq(1, 3, 6, 10, 15, 21, 28, 36, 45)
        assert(outputBuffer.asScala.flatten.toSet === expectedOutput.toSet)
      }
    } finally {
      try {
        // As the driver shuts down in the middle of processing and the thread above sleeps
        // for a while, `testDir` can be not closed correctly at this point which causes the
        // test failure on Windows.
        Utils.deleteRecursively(testDir)
      } catch {
        case e: IOException if Utils.isWindows =>
          logWarning(e.getMessage)
      }
    }
  }

  test("DStreamCheckpointData.restore invoking times") {
    withStreamingContext(new StreamingContext(conf, batchDuration)) { ssc =>
      ssc.checkpoint(checkpointDir)
      val inputDStream = new CheckpointInputDStream(ssc)
      val checkpointData = inputDStream.checkpointData
      val mappedDStream = inputDStream.map(_ + 100)
      val outputStream = new TestOutputStreamWithPartitions(mappedDStream)
      outputStream.register()
      // do two more times output
      mappedDStream.foreachRDD(rdd => rdd.count())
      mappedDStream.foreachRDD(rdd => rdd.count())
      assert(checkpointData.restoredTimes === 0)
      val batchDurationMillis = ssc.progressListener.batchDuration
      generateOutput(ssc, Time(batchDurationMillis * 3), checkpointDir, stopSparkContext = true)
      assert(checkpointData.restoredTimes === 0)
    }
    logInfo("*********** RESTARTING ************")
    withStreamingContext(new StreamingContext(checkpointDir)) { ssc =>
      val checkpointData =
        ssc.graph.getInputStreams().head.asInstanceOf[CheckpointInputDStream].checkpointData
      assert(checkpointData.restoredTimes === 1)
      ssc.start()
      ssc.stop()
      assert(checkpointData.restoredTimes === 1)
    }
  }

  // This tests whether spark can deserialize array object
  // refer to SPARK-5569
  test("recovery from checkpoint contains array object") {
    // create a class which is invisible to app class loader
    val jar = TestUtils.createJarWithClasses(
      classNames = Seq("testClz"),
      toStringValue = "testStringValue"
      )

    // invisible to current class loader
    val appClassLoader = getClass.getClassLoader
    intercept[ClassNotFoundException](appClassLoader.loadClass("testClz"))

    // visible to mutableURLClassLoader
    val loader = new MutableURLClassLoader(
      Array(jar), appClassLoader)
    assert(loader.loadClass("testClz").newInstance().toString == "testStringValue")

    // create and serialize Array[testClz]
    // scalastyle:off classforname
    val arrayObj = Class.forName("[LtestClz;", false, loader)
    // scalastyle:on classforname
    val bos = new ByteArrayOutputStream()
    new ObjectOutputStream(bos).writeObject(arrayObj)

    // deserialize the Array[testClz]
    val ois = new ObjectInputStreamWithLoader(
      new ByteArrayInputStream(bos.toByteArray), loader)
    assert(ois.readObject().asInstanceOf[Class[_]].getName == "[LtestClz;")
    ois.close()
  }

  test("SPARK-11267: the race condition of two checkpoints in a batch") {
    val jobGenerator = mock(classOf[JobGenerator])
    val checkpointDir = Utils.createTempDir().toString
    val checkpointWriter =
      new CheckpointWriter(jobGenerator, conf, checkpointDir, new Configuration())
    val bytes1 = Array.fill[Byte](10)(1)
    new checkpointWriter.CheckpointWriteHandler(
      Time(2000), bytes1, clearCheckpointDataLater = false).run()
    val bytes2 = Array.fill[Byte](10)(2)
    new checkpointWriter.CheckpointWriteHandler(
      Time(1000), bytes2, clearCheckpointDataLater = true).run()
    val checkpointFiles = Checkpoint.getCheckpointFiles(checkpointDir).reverse.map { path =>
      new File(path.toUri)
    }
    assert(checkpointFiles.size === 2)
    // Although bytes2 was written with an old time, it contains the latest status, so we should
    // try to read from it at first.
    assert(Files.toByteArray(checkpointFiles(0)) === bytes2)
    assert(Files.toByteArray(checkpointFiles(1)) === bytes1)
    checkpointWriter.stop()
  }

  test("SPARK-28912: Fix MatchError in getCheckpointFiles") {
    val tempDir = Utils.createTempDir()
    try {
      val fs = FileSystem.get(tempDir.toURI, new Configuration)
      val checkpointDir = tempDir.getAbsolutePath + "/checkpoint-01"

      assert(Checkpoint.getCheckpointFiles(checkpointDir, Some(fs)).length === 0)

      // Ignore files whose parent path match.
      fs.create(new Path(checkpointDir, "this-is-matched-before-due-to-parent-path")).close()
      assert(Checkpoint.getCheckpointFiles(checkpointDir, Some(fs)).length === 0)

      // Ignore directories whose names match.
      fs.mkdirs(new Path(checkpointDir, "checkpoint-1000000000"))
      assert(Checkpoint.getCheckpointFiles(checkpointDir, Some(fs)).length === 0)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }

  test("SPARK-6847: stack overflow when updateStateByKey is followed by a checkpointed dstream") {
    // In this test, there are two updateStateByKey operators. The RDD DAG is as follows:
    //
    //     batch 1            batch 2            batch 3     ...
    //
    // 1) input rdd          input rdd          input rdd
    //       |                  |                  |
    //       v                  v                  v
    // 2) cogroup rdd   ---> cogroup rdd   ---> cogroup rdd  ...
    //       |         /        |         /        |
    //       v        /         v        /         v
    // 3)  map rdd ---        map rdd ---        map rdd     ...
    //       |                  |                  |
    //       v                  v                  v
    // 4) cogroup rdd   ---> cogroup rdd   ---> cogroup rdd  ...
    //       |         /        |         /        |
    //       v        /         v        /         v
    // 5)  map rdd ---        map rdd ---        map rdd     ...
    //
    // Every batch depends on its previous batch, so "updateStateByKey" needs to do checkpoint to
    // break the RDD chain. However, before SPARK-6847, when the state RDD (layer 5) of the second
    // "updateStateByKey" does checkpoint, it won't checkpoint the state RDD (layer 3) of the first
    // "updateStateByKey" (Note: "updateStateByKey" has already marked that its state RDD (layer 3)
    // should be checkpointed). Hence, the connections between layer 2 and layer 3 won't be broken
    // and the RDD chain will grow infinitely and cause StackOverflow.
    //
    // Therefore SPARK-6847 introduces "spark.checkpoint.checkpointAllMarked" to force checkpointing
    // all marked RDDs in the DAG to resolve this issue. (For the previous example, it will break
    // connections between layer 2 and layer 3)
    ssc = new StreamingContext(master, framework, batchDuration)
    val batchCounter = new BatchCounter(ssc)
    ssc.checkpoint(checkpointDir)
    val inputDStream = new CheckpointInputDStream(ssc)
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      Some(values.sum + state.getOrElse(0))
    }
    @volatile var shouldCheckpointAllMarkedRDDs = false
    @volatile var rddsCheckpointed = false
    inputDStream.map(i => (i, i))
      .updateStateByKey(updateFunc).checkpoint(batchDuration)
      .updateStateByKey(updateFunc).checkpoint(batchDuration)
      .foreachRDD { rdd =>
        /**
         * Find all RDDs that are marked for checkpointing in the specified RDD and its ancestors.
         */
        def findAllMarkedRDDs(rdd: RDD[_]): List[RDD[_]] = {
          val markedRDDs = rdd.dependencies.flatMap(dep => findAllMarkedRDDs(dep.rdd)).toList
          if (rdd.checkpointData.isDefined) {
            rdd :: markedRDDs
          } else {
            markedRDDs
          }
        }

        shouldCheckpointAllMarkedRDDs =
          Option(rdd.sparkContext.getLocalProperty(RDD.CHECKPOINT_ALL_MARKED_ANCESTORS)).
            map(_.toBoolean).getOrElse(false)

        val stateRDDs = findAllMarkedRDDs(rdd)
        rdd.count()
        // Check the two state RDDs are both checkpointed
        rddsCheckpointed = stateRDDs.size == 2 && stateRDDs.forall(_.isCheckpointed)
      }
    ssc.start()
    batchCounter.waitUntilBatchesCompleted(1, 10000)
    assert(shouldCheckpointAllMarkedRDDs === true)
    assert(rddsCheckpointed === true)
  }

  /**
   * Advances the manual clock on the streaming scheduler by given number of batches.
   * It also waits for the expected amount of time for each batch.
   */
  def advanceTimeWithRealDelay[V: ClassTag](ssc: StreamingContext, numBatches: Long):
      Iterable[Seq[V]] =
  {
    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    logInfo("Manual clock before advancing = " + clock.getTimeMillis())
    for (i <- 1 to numBatches.toInt) {
      clock.advance(batchDuration.milliseconds)
      Thread.sleep(batchDuration.milliseconds)
    }
    logInfo("Manual clock after advancing = " + clock.getTimeMillis())
    Thread.sleep(batchDuration.milliseconds)

    val outputStream = getTestOutputStream[V](ssc.graph.getOutputStreams())
    outputStream.output.asScala.map(_.flatten)
  }
}

private object CheckpointSuite extends Serializable {
  var batchThreeShouldBlockALongTime: Boolean = true
}
