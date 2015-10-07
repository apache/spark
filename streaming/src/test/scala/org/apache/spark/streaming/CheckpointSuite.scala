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

import java.io.File

import scala.collection.mutable.{ArrayBuffer, SynchronizedBuffer}
import scala.reflect.ClassTag

import com.google.common.base.Charsets
import com.google.common.io.Files
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.hadoop.mapreduce.lib.output.{TextOutputFormat => NewTextOutputFormat}
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.streaming.dstream.{DStream, FileInputDStream}
import org.apache.spark.streaming.scheduler.{ConstantEstimator, RateTestInputDStream, RateTestReceiver}
import org.apache.spark.util.{Clock, ManualClock, Utils}

/**
 * This test suites tests the checkpointing functionality of DStreams -
 * the checkpointing of a DStream's RDDs as well as the checkpointing of
 * the whole DStream graph.
 */
class CheckpointSuite extends TestSuiteBase {

  var ssc: StreamingContext = null

  override def batchDuration: Duration = Milliseconds(500)

  override def beforeFunction() {
    super.beforeFunction()
    Utils.deleteRecursively(new File(checkpointDir))
  }

  override def afterFunction() {
    super.afterFunction()
    if (ssc != null) ssc.stop()
    Utils.deleteRecursively(new File(checkpointDir))
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
      case (time, file) => {
        assert(fs.exists(new Path(file)), "Checkpoint file '" + file +"' for time " + time +
            " for state stream before first failure does not exist")
      }
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
      case (time, file) => {
        assert(fs.exists(new Path(file)), "Checkpoint file '" + file +"' for time " + time +
          " for state stream before seconds failure does not exist")
      }
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
    assert(restoredConf1.get("spark.driver.host") === "localhost")
    assert(restoredConf1.get("spark.driver.port") !== "9999")
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
        Seq(("", 2)), Seq() ),
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
    val outputBuffer = new ArrayBuffer[Seq[Int]] with SynchronizedBuffer[Seq[Int]]

    /**
     * Writes a file named `i` (which contains the number `i`) to the test directory and sets its
     * modification time to `clock`'s current time.
     */
    def writeFile(i: Int, clock: Clock): Unit = {
      val file = new File(testDir, i.toString)
      Files.write(i + "\n", file, Charsets.UTF_8)
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
      val filenames = fileInputDStream.batchTimeToSelectedFiles.values.flatten
      filenames.map(_.split(File.separator).last.toInt).toSeq.sorted
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
        CheckpointSuite.batchThreeShouldBlockIndefinitely = true
        val mappedStream = fileStream.map(s => {
          val i = s.toInt
          if (i == 3) {
            while (CheckpointSuite.batchThreeShouldBlockIndefinitely) {
              Thread.sleep(Long.MaxValue)
            }
          }
          i
        })

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
        assert(outputStream.output.flatten === Seq(1, 3))
      }

      // The original StreamingContext has now been stopped.
      CheckpointSuite.batchThreeShouldBlockIndefinitely = false

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
        logInfo("Output after restart = " + outputStream.output.mkString("[", ", ", "]"))
        assert(outputStream.output.size > 0, "No files processed after restart")
        ssc.stop()

        // Verify whether files created while the driver was down (4, 5, 6) and files created after
        // recovery (7, 8, 9) have been recorded
        assert(recordedFiles(ssc) === (1 to 9))

        // Append the new output to the old buffer
        outputBuffer ++= outputStream.output

        // Verify whether all the elements received are as expected
        val expectedOutput = Seq(1, 3, 6, 10, 15, 21, 28, 36, 45)
        assert(outputBuffer.flatten.toSet === expectedOutput.toSet)
      }
    } finally {
      Utils.deleteRecursively(testDir)
    }
  }


  /**
   * Tests a streaming operation under checkpointing, by restarting the operation
   * from checkpoint file and verifying whether the final output is correct.
   * The output is assumed to have come from a reliable queue which an replay
   * data as required.
   *
   * NOTE: This takes into consideration that the last batch processed before
   * master failure will be re-processed after restart/recovery.
   */
  def testCheckpointedOperation[U: ClassTag, V: ClassTag](
    input: Seq[Seq[U]],
    operation: DStream[U] => DStream[V],
    expectedOutput: Seq[Seq[V]],
    initialNumBatches: Int
  ) {

    // Current code assumes that:
    // number of inputs = number of outputs = number of batches to be run
    val totalNumBatches = input.size
    val nextNumBatches = totalNumBatches - initialNumBatches
    val initialNumExpectedOutputs = initialNumBatches
    val nextNumExpectedOutputs = expectedOutput.size - initialNumExpectedOutputs + 1
    // because the last batch will be processed again

    // Do the computation for initial number of batches, create checkpoint file and quit
    ssc = setupStreams[U, V](input, operation)
    ssc.start()
    val output = advanceTimeWithRealDelay[V](ssc, initialNumBatches)
    ssc.stop()
    verifyOutput[V](output, expectedOutput.take(initialNumBatches), true)
    Thread.sleep(1000)

    // Restart and complete the computation from checkpoint file
    logInfo(
      "\n-------------------------------------------\n" +
      "        Restarting stream computation          " +
      "\n-------------------------------------------\n"
    )
    ssc = new StreamingContext(checkpointDir)
    ssc.start()
    val outputNew = advanceTimeWithRealDelay[V](ssc, nextNumBatches)
    // the first element will be re-processed data of the last batch before restart
    verifyOutput[V](outputNew, expectedOutput.takeRight(nextNumExpectedOutputs), true)
    ssc.stop()
    ssc = null
  }

  /**
   * Advances the manual clock on the streaming scheduler by given number of batches.
   * It also waits for the expected amount of time for each batch.
   */
  def advanceTimeWithRealDelay[V: ClassTag](ssc: StreamingContext, numBatches: Long): Seq[Seq[V]] =
  {
    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    logInfo("Manual clock before advancing = " + clock.getTimeMillis())
    for (i <- 1 to numBatches.toInt) {
      clock.advance(batchDuration.milliseconds)
      Thread.sleep(batchDuration.milliseconds)
    }
    logInfo("Manual clock after advancing = " + clock.getTimeMillis())
    Thread.sleep(batchDuration.milliseconds)

    val outputStream = ssc.graph.getOutputStreams().filter { dstream =>
      dstream.isInstanceOf[TestOutputStreamWithPartitions[V]]
    }.head.asInstanceOf[TestOutputStreamWithPartitions[V]]
    outputStream.output.map(_.flatten)
  }
}

private object CheckpointSuite extends Serializable {
  var batchThreeShouldBlockIndefinitely: Boolean = true
}
