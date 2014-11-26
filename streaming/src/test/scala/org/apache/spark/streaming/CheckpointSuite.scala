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
import java.nio.charset.Charset

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import com.google.common.io.Files
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.hadoop.mapreduce.lib.output.{TextOutputFormat => NewTextOutputFormat}

import org.apache.spark.streaming.dstream.{DStream, FileInputDStream}
import org.apache.spark.streaming.util.ManualClock
import org.apache.spark.util.Utils

/**
 * This test suites tests the checkpointing functionality of DStreams -
 * the checkpointing of a DStream's RDDs as well as the checkpointing of
 * the whole DStream graph.
 */
class CheckpointSuite extends TestSuiteBase {

  var ssc: StreamingContext = null

  override def batchDuration = Milliseconds(500)

  override def actuallyWait = true // to allow checkpoints to be written

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

    conf.set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")

    val stateStreamCheckpointInterval = Seconds(1)
    val fs = FileSystem.getLocal(new Configuration())
    // this ensure checkpointing occurs at least once
    val firstNumBatches = (stateStreamCheckpointInterval / batchDuration).toLong * 2
    val secondNumBatches = firstNumBatches

    // Setup the streams
    val input = (1 to 10).map(_ => Seq("a")).toSeq
    val operation = (st: DStream[String]) => {
      val updateFunc = (values: Seq[Int], state: Option[Int]) => {
        Some((values.sum + state.getOrElse(0)))
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
    System.clearProperty("spark.streaming.manualClock.jump")
    ssc = null
  }

  // This tests whether spark conf persists through checkpoints, and certain
  // configs gets scrubbed
  test("persistence of conf through checkpoints") {
    val key = "spark.mykey"
    val value = "myvalue"
    System.setProperty(key, value)
    ssc = new StreamingContext(master, framework, batchDuration)
    val originalConf = ssc.conf

    val cp = new Checkpoint(ssc, Time(1000))
    val cpConf = cp.sparkConf
    assert(cpConf.get("spark.master") === originalConf.get("spark.master"))
    assert(cpConf.get("spark.app.name") === originalConf.get("spark.app.name"))
    assert(cpConf.get(key) === value)
    ssc.stop()

    // Serialize/deserialize to simulate write to storage and reading it back
    val newCp = Utils.deserialize[Checkpoint](Utils.serialize(cp))

    val newCpConf = newCp.sparkConf
    assert(newCpConf.get("spark.master") === originalConf.get("spark.master"))
    assert(newCpConf.get("spark.app.name") === originalConf.get("spark.app.name"))
    assert(newCpConf.get(key) === value)
    assert(!newCpConf.contains("spark.driver.host"))
    assert(!newCpConf.contains("spark.driver.port"))

    // Check if all the parameters have been restored
    ssc = new StreamingContext(null, newCp, null)
    val restoredConf = ssc.conf
    assert(restoredConf.get(key) === value)
  }


  // This tests whether the systm can recover from a master failure with simple
  // non-stateful operations. This assumes as reliable, replayable input
  // source - TestInputDStream.
  test("recovery with map and reduceByKey operations") {
    testCheckpointedOperation(
      Seq( Seq("a", "a", "b"), Seq("", ""), Seq(), Seq("a", "a", "b"), Seq("", ""), Seq() ),
      (s: DStream[String]) => s.map(x => (x, 1)).reduceByKey(_ + _),
      Seq( Seq(("a", 2), ("b", 1)), Seq(("", 2)), Seq(), Seq(("a", 2), ("b", 1)), Seq(("", 2)), Seq() ),
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
    val output = Seq(Seq(("a", 1)), Seq(("a", 2)), Seq(("a", 3))) ++ (1 to (n - w + 1)).map(x => Seq(("a", 4)))
    val operation = (st: DStream[String]) => {
      st.map(x => (x, 1))
        .reduceByKeyAndWindow(_ + _, _ - _, batchDuration * w, batchDuration)
        .checkpoint(batchDuration * 2)
    }
    testCheckpointedOperation(input, operation, output, 7)
  }

  test("recovery with saveAsHadoopFiles operation") {
    val tempDir = Files.createTempDir()
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
        Seq(Seq(("a", 2), ("b", 1)), Seq(("", 2)), Seq(), Seq(("a", 2), ("b", 1)), Seq(("", 2)), Seq()),
        3
      )
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }

  test("recovery with saveAsNewAPIHadoopFiles operation") {
    val tempDir = Files.createTempDir()
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
        Seq(Seq(("a", 2), ("b", 1)), Seq(("", 2)), Seq(), Seq(("a", 2), ("b", 1)), Seq(("", 2)), Seq()),
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


  // This tests whether file input stream remembers what files were seen before
  // the master failure and uses them again to process a large window operation.
  // It also tests whether batches, whose processing was incomplete due to the
  // failure, are re-processed or not.
  test("recovery with file input stream") {
    // Set up the streaming context and input streams
    val testDir = Utils.createTempDir()
    var ssc = new StreamingContext(master, framework, Seconds(1))
    ssc.checkpoint(checkpointDir)
    val fileStream = ssc.textFileStream(testDir.toString)
    // Making value 3 take large time to process, to ensure that the master
    // shuts down in the middle of processing the 3rd batch
    val mappedStream = fileStream.map(s => {
      val i = s.toInt
      if (i == 3) Thread.sleep(2000)
      i
    })

    // Reducing over a large window to ensure that recovery from master failure
    // requires reprocessing of all the files seen before the failure
    val reducedStream = mappedStream.reduceByWindow(_ + _, Seconds(30), Seconds(1))
    val outputBuffer = new ArrayBuffer[Seq[Int]]
    var outputStream = new TestOutputStream(reducedStream, outputBuffer)
    outputStream.register()
    ssc.start()

    // Create files and advance manual clock to process them
    // var clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    Thread.sleep(1000)
    for (i <- Seq(1, 2, 3)) {
      Files.write(i + "\n", new File(testDir, i.toString), Charset.forName("UTF-8"))
      // wait to make sure that the file is written such that it gets shown in the file listings
      Thread.sleep(1000)
    }
    logInfo("Output = " + outputStream.output.mkString(","))
    assert(outputStream.output.size > 0, "No files processed before restart")
    ssc.stop()

    // Verify whether files created have been recorded correctly or not
    var fileInputDStream = ssc.graph.getInputStreams().head.asInstanceOf[FileInputDStream[_, _, _]]
    def recordedFiles = fileInputDStream.batchTimeToSelectedFiles.values.flatten
    assert(!recordedFiles.filter(_.endsWith("1")).isEmpty)
    assert(!recordedFiles.filter(_.endsWith("2")).isEmpty)
    assert(!recordedFiles.filter(_.endsWith("3")).isEmpty)

    // Create files while the master is down
    for (i <- Seq(4, 5, 6)) {
      Files.write(i + "\n", new File(testDir, i.toString), Charset.forName("UTF-8"))
      Thread.sleep(1000)
    }

    // Recover context from checkpoint file and verify whether the files that were
    // recorded before failure were saved and successfully recovered
    logInfo("*********** RESTARTING ************")
    ssc = new StreamingContext(checkpointDir)
    fileInputDStream = ssc.graph.getInputStreams().head.asInstanceOf[FileInputDStream[_, _, _]]
    assert(!recordedFiles.filter(_.endsWith("1")).isEmpty)
    assert(!recordedFiles.filter(_.endsWith("2")).isEmpty)
    assert(!recordedFiles.filter(_.endsWith("3")).isEmpty)

    // Restart stream computation
    ssc.start()
    for (i <- Seq(7, 8, 9)) {
      Files.write(i + "\n", new File(testDir, i.toString), Charset.forName("UTF-8"))
      Thread.sleep(1000)
    }
    Thread.sleep(1000)
    logInfo("Output = " + outputStream.output.mkString("[", ", ", "]"))
    assert(outputStream.output.size > 0, "No files processed after restart")
    ssc.stop()

    // Verify whether files created while the driver was down have been recorded or not
    assert(!recordedFiles.filter(_.endsWith("4")).isEmpty)
    assert(!recordedFiles.filter(_.endsWith("5")).isEmpty)
    assert(!recordedFiles.filter(_.endsWith("6")).isEmpty)

    // Verify whether new files created after recover have been recorded or not
    assert(!recordedFiles.filter(_.endsWith("7")).isEmpty)
    assert(!recordedFiles.filter(_.endsWith("8")).isEmpty)
    assert(!recordedFiles.filter(_.endsWith("9")).isEmpty)

    // Append the new output to the old buffer
    outputStream = ssc.graph.getOutputStreams().head.asInstanceOf[TestOutputStream[Int]]
    outputBuffer ++= outputStream.output

    val expectedOutput = Seq(1, 3, 6, 10, 15, 21, 28, 36, 45)
    logInfo("--------------------------------")
    logInfo("output, size = " + outputBuffer.size)
    outputBuffer.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    logInfo("expected output, size = " + expectedOutput.size)
    expectedOutput.foreach(x => logInfo("[" + x + "]"))
    logInfo("--------------------------------")

    // Verify whether all the elements received are as expected
    val output = outputBuffer.flatMap(x => x)
    assert(output.contains(6))  // To ensure that the 3rd input (i.e., 3) was processed
    output.foreach(o =>         // To ensure all the inputs are correctly added cumulatively
      assert(expectedOutput.contains(o), "Expected value " + o + " not found")
    )
    // To ensure that all the inputs were received correctly
    assert(expectedOutput.last === output.last)
    Utils.deleteRecursively(testDir)
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
  def advanceTimeWithRealDelay[V: ClassTag](ssc: StreamingContext, numBatches: Long): Seq[Seq[V]] = {
    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    logInfo("Manual clock before advancing = " + clock.time)
    for (i <- 1 to numBatches.toInt) {
      clock.addToTime(batchDuration.milliseconds)
      Thread.sleep(batchDuration.milliseconds)
    }
    logInfo("Manual clock after advancing = " + clock.time)
    Thread.sleep(batchDuration.milliseconds)

    val outputStream = ssc.graph.getOutputStreams.filter { dstream =>
      dstream.isInstanceOf[TestOutputStreamWithPartitions[V]]
    }.head.asInstanceOf[TestOutputStreamWithPartitions[V]]
    outputStream.output.map(_.flatten)
  }
}
