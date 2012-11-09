package spark.streaming

import org.scalatest.BeforeAndAfter
import org.apache.commons.io.FileUtils
import java.io.File
import scala.runtime.RichInt
import scala.util.Random
import spark.streaming.StreamingContext._
import collection.mutable.ArrayBuffer
import spark.Logging

/**
 * This testsuite tests master failures at random times while the stream is running using
 * the real clock.
 */
class FailureSuite extends TestSuiteBase with BeforeAndAfter {

  before {
    FileUtils.deleteDirectory(new File(checkpointDir))
  }

  after {
    FailureSuite.reset()
    FileUtils.deleteDirectory(new File(checkpointDir))
  }

  override def framework = "CheckpointSuite"

  override def batchDuration = Milliseconds(500)

  override def checkpointDir = "checkpoint"

  override def checkpointInterval = batchDuration

  test("multiple failures with updateStateByKey") {
    val n = 30
    // Input: time=1 ==> [ a ] , time=2 ==> [ a, a ] , time=3 ==> [ a, a, a ] , ...
    val input = (1 to n).map(i => (1 to i).map(_ =>"a").toSeq).toSeq
    // Last output: [ (a, 465) ]   for n=30
    val lastOutput = Seq( ("a", (1 to n).reduce(_ + _)) )

    val operation = (st: DStream[String]) => {
     val updateFunc = (values: Seq[Int], state: Option[RichInt]) => {
       Some(new RichInt(values.foldLeft(0)(_ + _) + state.map(_.self).getOrElse(0)))
     }
     st.map(x => (x, 1))
     .updateStateByKey[RichInt](updateFunc)
     .checkpoint(Seconds(2))
     .map(t => (t._1, t._2.self))
    }

    testOperationWithMultipleFailures(input, operation, lastOutput, n, n)
  }

  test("multiple failures with reduceByKeyAndWindow") {
    val n = 30
    val w = 100
    assert(w > n, "Window should be much larger than the number of input sets in this test")
    // Input: time=1 ==> [ a ] , time=2 ==> [ a, a ] , time=3 ==> [ a, a, a ] , ...
    val input = (1 to n).map(i => (1 to i).map(_ =>"a").toSeq).toSeq
    // Last output: [ (a, 465) ]
    val lastOutput = Seq( ("a", (1 to n).reduce(_ + _)) )

    val operation = (st: DStream[String]) => {
      st.map(x => (x, 1))
        .reduceByKeyAndWindow(_ + _, _ - _, batchDuration * w, batchDuration)
        .checkpoint(Seconds(2))
    }

    testOperationWithMultipleFailures(input, operation, lastOutput, n, n)
  }


  /**
   * Tests stream operation with multiple master failures, and verifies whether the
   * final set of output values is as expected or not. Checking the final value is
   * proof that no intermediate data was lost due to master failures.
   */
  def testOperationWithMultipleFailures[U: ClassManifest, V: ClassManifest](
    input: Seq[Seq[U]],
    operation: DStream[U] => DStream[V],
    lastExpectedOutput: Seq[V],
    numBatches: Int,
    numExpectedOutput: Int
  ) {
    var ssc = setupStreams[U, V](input, operation)
    val mergedOutput = new ArrayBuffer[Seq[V]]()

    var totalTimeRan = 0L
    while(totalTimeRan <= numBatches * batchDuration.milliseconds * 2) {
      new KillingThread(ssc, numBatches * batchDuration.milliseconds.toInt / 4).start()
      val (output, timeRan) = runStreamsWithRealClock[V](ssc, numBatches, numExpectedOutput)

      mergedOutput ++= output
      totalTimeRan += timeRan
      logInfo("New output = " + output)
      logInfo("Merged output = " + mergedOutput)
      logInfo("Total time spent = " + totalTimeRan)
      val sleepTime = Random.nextInt(numBatches * batchDuration.milliseconds.toInt / 8)
      logInfo(
        "\n-------------------------------------------\n" +
        "   Restarting stream computation in " + sleepTime + " ms   " +
        "\n-------------------------------------------\n"
      )
      Thread.sleep(sleepTime)
      FailureSuite.failed = false
      ssc = new StreamingContext(checkpointDir)
    }
    ssc.stop()
    ssc = null

    // Verify whether the last output is the expected one
    val lastOutput = mergedOutput(mergedOutput.lastIndexWhere(!_.isEmpty))
    assert(lastOutput.toSet === lastExpectedOutput.toSet)
    logInfo("Finished computation after " + FailureSuite.failureCount + " failures")
  }

  /**
   * Runs the streams set up in `ssc` on real clock until the expected max number of
   */
  def runStreamsWithRealClock[V: ClassManifest](
    ssc: StreamingContext,
    numBatches: Int,
    maxExpectedOutput: Int
  ): (Seq[Seq[V]], Long) = {

    System.clearProperty("spark.streaming.clock")

    assert(numBatches > 0, "Number of batches to run stream computation is zero")
    assert(maxExpectedOutput > 0, "Max expected outputs after " + numBatches + " is zero")
    logInfo("numBatches = " + numBatches + ", maxExpectedOutput = " + maxExpectedOutput)

    // Get the output buffer
    val outputStream = ssc.graph.getOutputStreams.head.asInstanceOf[TestOutputStream[V]]
    val output = outputStream.output
    val waitTime = (batchDuration.millis * (numBatches.toDouble + 0.5)).toLong
    val startTime = System.currentTimeMillis()

    try {
      // Start computation
      ssc.start()

      // Wait until expected number of output items have been generated
      while (output.size < maxExpectedOutput && System.currentTimeMillis() - startTime < waitTime && !FailureSuite.failed) {
        logInfo("output.size = " + output.size + ", maxExpectedOutput = " + maxExpectedOutput)
        Thread.sleep(100)
      }
    } catch {
      case e: Exception => logInfo("Exception while running streams: " + e)
    } finally {
      ssc.stop()
    }
    val timeTaken = System.currentTimeMillis() - startTime
    logInfo("" + output.size + " sets of output generated in " + timeTaken + " ms")
    (output, timeTaken)
  }


}

object FailureSuite {
  var failed = false
  var failureCount = 0

  def reset() {
    failed = false
    failureCount = 0
  }
}

class KillingThread(ssc: StreamingContext, maxKillWaitTime: Int) extends Thread with Logging {
  initLogging()

  override def run() {
    var minKillWaitTime = if (FailureSuite.failureCount == 0) 3000 else 1000 // to allow the first checkpoint
    val killWaitTime = minKillWaitTime + Random.nextInt(maxKillWaitTime)
    logInfo("Kill wait time = " + killWaitTime)
    Thread.sleep(killWaitTime.toLong)
    logInfo(
      "\n---------------------------------------\n" +
      "Killing streaming context after " + killWaitTime + " ms" +
      "\n---------------------------------------\n"
    )
    if (ssc != null) ssc.stop()
    FailureSuite.failed = true
    FailureSuite.failureCount += 1
  }
}
