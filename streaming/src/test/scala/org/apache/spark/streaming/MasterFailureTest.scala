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

import java.io.{File, IOException}
import java.nio.charset.StandardCharsets
import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random

import com.google.common.io.Files
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.Utils

private[streaming]
object MasterFailureTest extends Logging {

  @volatile var killed = false
  @volatile var killCount = 0
  @volatile var setupCalled = false

  def main(args: Array[String]) {
    // scalastyle:off println
    if (args.size < 2) {
      println(
        "Usage: MasterFailureTest <local/HDFS directory> <# batches> " +
          "[<batch size in milliseconds>]")
      System.exit(1)
    }
    val directory = args(0)
    val numBatches = args(1).toInt
    val batchDuration = if (args.size > 2) Milliseconds(args(2).toInt) else Seconds(1)

    println("\n\n========================= MAP TEST =========================\n\n")
    testMap(directory, numBatches, batchDuration)

    println("\n\n================= UPDATE-STATE-BY-KEY TEST =================\n\n")
    testUpdateStateByKey(directory, numBatches, batchDuration)

    println("\n\nSUCCESS\n\n")
    // scalastyle:on println
  }

  def testMap(directory: String, numBatches: Int, batchDuration: Duration) {
    // Input: time=1 ==> [ 1 ] , time=2 ==> [ 2 ] , time=3 ==> [ 3 ] , ...
    val input = (1 to numBatches).map(_.toString).toSeq
    // Expected output: time=1 ==> [ 1 ] , time=2 ==> [ 2 ] , time=3 ==> [ 3 ] , ...
    val expectedOutput = (1 to numBatches)

    val operation = (st: DStream[String]) => st.map(_.toInt)

    // Run streaming operation with multiple master failures
    val output = testOperation(directory, batchDuration, input, operation, expectedOutput)

    logInfo("Expected output, size = " + expectedOutput.size)
    logInfo(expectedOutput.mkString("[", ",", "]"))
    logInfo("Output, size = " + output.size)
    logInfo(output.mkString("[", ",", "]"))

    // Verify whether all the values of the expected output is present
    // in the output
    assert(output.distinct.toSet == expectedOutput.toSet)
  }


  def testUpdateStateByKey(directory: String, numBatches: Int, batchDuration: Duration) {
    // Input: time=1 ==> [ a ] , time=2 ==> [ a, a ] , time=3 ==> [ a, a, a ] , ...
    val input = (1 to numBatches).map(i => (1 to i).map(_ => "a").mkString(" ")).toSeq
    // Expected output: time=1 ==> [ (a, 1) ] , time=2 ==> [ (a, 3) ] , time=3 ==> [ (a,6) ] , ...
    val expectedOutput = (1L to numBatches).map(i => (1L to i).sum).map(j => ("a", j))

    val operation = (st: DStream[String]) => {
      val updateFunc = (values: Seq[Long], state: Option[Long]) => {
        Some(values.foldLeft(0L)(_ + _) + state.getOrElse(0L))
      }
      st.flatMap(_.split(" "))
        .map(x => (x, 1L))
        .updateStateByKey[Long](updateFunc)
        .checkpoint(batchDuration * 5)
    }

    // Run streaming operation with multiple master failures
    val output = testOperation(directory, batchDuration, input, operation, expectedOutput)

    logInfo("Expected output, size = " + expectedOutput.size + "\n" + expectedOutput)
    logInfo("Output, size = " + output.size + "\n" + output)

    // Verify whether all the values in the output are among the expected output values
    output.foreach(o =>
      assert(expectedOutput.contains(o), "Expected value " + o + " not found")
    )

    // Verify whether the last expected output value has been generated, there by
    // confirming that none of the inputs have been missed
    assert(output.last == expectedOutput.last)
  }

  /**
   * Tests stream operation with multiple master failures, and verifies whether the
   * final set of output values is as expected or not.
   */
  def testOperation[T: ClassTag](
    directory: String,
    batchDuration: Duration,
    input: Seq[String],
    operation: DStream[String] => DStream[T],
    expectedOutput: Seq[T]
  ): Seq[T] = {

    // Just making sure that the expected output does not have duplicates
    assert(expectedOutput.distinct.toSet == expectedOutput.toSet)

    // Reset all state
    reset()

    // Create the directories for this test
    val uuid = UUID.randomUUID().toString
    val rootDir = new Path(directory, uuid)
    val fs = rootDir.getFileSystem(new Configuration())
    val checkpointDir = new Path(rootDir, "checkpoint")
    val testDir = new Path(rootDir, "test")
    fs.mkdirs(checkpointDir)
    fs.mkdirs(testDir)

    // Setup the stream computation with the given operation
    val ssc = StreamingContext.getOrCreate(checkpointDir.toString, () => {
      setupStreams(batchDuration, operation, checkpointDir, testDir)
    })

    // Check if setupStream was called to create StreamingContext
    // (and not created from checkpoint file)
    assert(setupCalled, "Setup was not called in the first call to StreamingContext.getOrCreate")

    // Start generating files in the a different thread
    val fileGeneratingThread = new FileGeneratingThread(input, testDir, batchDuration.milliseconds)
    fileGeneratingThread.start()

    // Run the streams and repeatedly kill it until the last expected output
    // has been generated, or until it has run for twice the expected time
    val lastExpectedOutput = expectedOutput.last
    val maxTimeToRun = expectedOutput.size * batchDuration.milliseconds * 2
    val mergedOutput = runStreams(ssc, lastExpectedOutput, maxTimeToRun)

    fileGeneratingThread.join()
    fs.delete(checkpointDir, true)
    fs.delete(testDir, true)
    logInfo("Finished test after " + killCount + " failures")
    mergedOutput
  }

  /**
   * Sets up the stream computation with the given operation, directory (local or HDFS),
   * and batch duration. Returns the streaming context and the directory to which
   * files should be written for testing.
   */
  private def setupStreams[T: ClassTag](
      batchDuration: Duration,
      operation: DStream[String] => DStream[T],
      checkpointDir: Path,
      testDir: Path
    ): StreamingContext = {
    // Mark that setup was called
    setupCalled = true

    // Setup the streaming computation with the given operation
    val ssc = new StreamingContext("local[4]", "MasterFailureTest", batchDuration, null, Nil,
      Map())
    ssc.checkpoint(checkpointDir.toString)
    val inputStream = ssc.textFileStream(testDir.toString)
    val operatedStream = operation(inputStream)
    val outputStream = new TestOutputStream(operatedStream)
    outputStream.register()
    ssc
  }


  /**
   * Repeatedly starts and kills the streaming context until timed out or
   * the last expected output is generated. Finally, return
   */
  private def runStreams[T: ClassTag](
      _ssc: StreamingContext,
      lastExpectedOutput: T,
      maxTimeToRun: Long
   ): Seq[T] = {

    var ssc = _ssc
    var totalTimeRan = 0L
    var isLastOutputGenerated = false
    var isTimedOut = false
    val mergedOutput = new ArrayBuffer[T]()
    val checkpointDir = ssc.checkpointDir
    val batchDuration = ssc.graph.batchDuration

    while(!isLastOutputGenerated && !isTimedOut) {
      // Get the output buffer
      val outputQueue = ssc.graph.getOutputStreams().head.asInstanceOf[TestOutputStream[T]].output
      def output = outputQueue.asScala.flatten

      // Start the thread to kill the streaming after some time
      killed = false
      val killingThread = new KillingThread(ssc, batchDuration.milliseconds * 10)
      killingThread.start()

      var timeRan = 0L
      try {
        // Start the streaming computation and let it run while ...
        // (i) StreamingContext has not been shut down yet
        // (ii) The last expected output has not been generated yet
        // (iii) Its not timed out yet
        System.clearProperty("spark.streaming.clock")
        System.clearProperty("spark.driver.port")
        ssc.start()
        val startTime = System.currentTimeMillis()
        while (!killed && !isLastOutputGenerated && !isTimedOut) {
          Thread.sleep(100)
          timeRan = System.currentTimeMillis() - startTime
          isLastOutputGenerated = (output.nonEmpty && output.last == lastExpectedOutput)
          isTimedOut = (timeRan + totalTimeRan > maxTimeToRun)
        }
      } catch {
        case e: Exception => logError("Error running streaming context", e)
      } finally {
        ssc.stop()
      }
      if (killingThread.isAlive) {
        killingThread.interrupt()
        // SparkContext.stop will set SparkEnv.env to null. We need to make sure SparkContext is
        // stopped before running the next test. Otherwise, it's possible that we set SparkEnv.env
        // to null after the next test creates the new SparkContext and fail the test.
        killingThread.join()
      }

      logInfo("Has been killed = " + killed)
      logInfo("Is last output generated = " + isLastOutputGenerated)
      logInfo("Is timed out = " + isTimedOut)

      // Verify whether the output of each batch has only one element or no element
      // and then merge the new output with all the earlier output
      mergedOutput ++= output.toSeq
      totalTimeRan += timeRan
      logInfo("New output = " + output.toSeq)
      logInfo("Merged output = " + mergedOutput)
      logInfo("Time ran = " + timeRan)
      logInfo("Total time ran = " + totalTimeRan)

      if (!isLastOutputGenerated && !isTimedOut) {
        val sleepTime = Random.nextInt(batchDuration.milliseconds.toInt * 10)
        logInfo(
          "\n-------------------------------------------\n" +
            "   Restarting stream computation in " + sleepTime + " ms   " +
            "\n-------------------------------------------\n"
        )
        Thread.sleep(sleepTime)
        // Recreate the streaming context from checkpoint
        ssc = StreamingContext.getOrCreate(checkpointDir, () => {
          throw new Exception("Trying to create new context when it " +
            "should be reading from checkpoint file")
        })
      }
    }
    mergedOutput
  }

  /**
   * Verifies the output value are the same as expected. Since failures can lead to
   * a batch being processed twice, a batches output may appear more than once
   * consecutively. To avoid getting confused with those, we eliminate consecutive
   * duplicate batch outputs of values from the `output`. As a result, the
   * expected output should not have consecutive batches with the same values as output.
   */
  private def verifyOutput[T: ClassTag](output: Seq[T], expectedOutput: Seq[T]) {
    // Verify whether expected outputs do not consecutive batches with same output
    for (i <- 0 until expectedOutput.size - 1) {
      assert(expectedOutput(i) != expectedOutput(i + 1),
        "Expected output has consecutive duplicate sequence of values")
    }

    // Log the output
    // scalastyle:off println
    println("Expected output, size = " + expectedOutput.size)
    println(expectedOutput.mkString("[", ",", "]"))
    println("Output, size = " + output.size)
    println(output.mkString("[", ",", "]"))
    // scalastyle:on println

    // Match the output with the expected output
    output.foreach(o =>
      assert(expectedOutput.contains(o), "Expected value " + o + " not found")
    )
  }

  /** Resets counter to prepare for the test */
  private def reset() {
    killed = false
    killCount = 0
    setupCalled = false
  }
}

/**
 * Thread to kill streaming context after a random period of time.
 */
private[streaming]
class KillingThread(ssc: StreamingContext, maxKillWaitTime: Long) extends Thread with Logging {

  override def run() {
    try {
      // If it is the first killing, then allow the first checkpoint to be created
      var minKillWaitTime = if (MasterFailureTest.killCount == 0) 5000 else 2000
      val killWaitTime = minKillWaitTime + math.abs(Random.nextLong % maxKillWaitTime)
      logInfo("Kill wait time = " + killWaitTime)
      Thread.sleep(killWaitTime)
      logInfo(
        "\n---------------------------------------\n" +
          "Killing streaming context after " + killWaitTime + " ms" +
          "\n---------------------------------------\n"
      )
      if (ssc != null) {
        ssc.stop()
        MasterFailureTest.killed = true
        MasterFailureTest.killCount += 1
      }
      logInfo("Killing thread finished normally")
    } catch {
      case ie: InterruptedException => logInfo("Killing thread interrupted")
      case e: Exception => logWarning("Exception in killing thread", e)
    }

  }
}


/**
 * Thread to generate input files periodically with the desired text.
 */
private[streaming]
class FileGeneratingThread(input: Seq[String], testDir: Path, interval: Long)
  extends Thread with Logging {

  override def run() {
    val localTestDir = Utils.createTempDir()
    var fs = testDir.getFileSystem(new Configuration())
    val maxTries = 3
    try {
      Thread.sleep(5000) // To make sure that all the streaming context has been set up
      for (i <- 0 until input.size) {
        // Write the data to a local file and then move it to the target test directory
        val localFile = new File(localTestDir, (i + 1).toString)
        val hadoopFile = new Path(testDir, (i + 1).toString)
        val tempHadoopFile = new Path(testDir, ".tmp_" + (i + 1).toString)
        Files.write(input(i) + "\n", localFile, StandardCharsets.UTF_8)
        var tries = 0
        var done = false
            while (!done && tries < maxTries) {
              tries += 1
              try {
                // fs.copyFromLocalFile(new Path(localFile.toString), hadoopFile)
                fs.copyFromLocalFile(new Path(localFile.toString), tempHadoopFile)
                fs.rename(tempHadoopFile, hadoopFile)
            done = true
          } catch {
            case ioe: IOException =>
                  fs = testDir.getFileSystem(new Configuration())
                  logWarning("Attempt " + tries + " at generating file " + hadoopFile + " failed.",
                    ioe)
          }
        }
        if (!done) {
          logError("Could not generate file " + hadoopFile)
        } else {
          logInfo("Generated file " + hadoopFile + " at " + System.currentTimeMillis)
        }
        Thread.sleep(interval)
        localFile.delete()
      }
      logInfo("File generating thread finished normally")
    } catch {
      case ie: InterruptedException => logInfo("File generating thread interrupted")
      case e: Exception => logWarning("File generating in killing thread", e)
    } finally {
      fs.close()
      Utils.deleteRecursively(localTestDir)
    }
  }
}
