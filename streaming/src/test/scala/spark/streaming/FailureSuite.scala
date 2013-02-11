package spark.streaming

import org.scalatest.{FunSuite, BeforeAndAfter}
import org.apache.commons.io.FileUtils
import java.io.File
import scala.runtime.RichInt
import scala.util.Random
import spark.streaming.StreamingContext._
import collection.mutable.{SynchronizedBuffer, ArrayBuffer}
import spark.Logging
import com.google.common.io.Files

/**
 * This testsuite tests master failures at random times while the stream is running using
 * the real clock.
 */
class FailureSuite extends FunSuite with BeforeAndAfter with Logging {

  var testDir: File = null
  var checkpointDir: File = null
  val batchDuration = Milliseconds(500)

  before {
    testDir = Files.createTempDir()
    checkpointDir = Files.createTempDir()
  }

  after {
    FailureSuite.reset()
    FileUtils.deleteDirectory(checkpointDir)
    FileUtils.deleteDirectory(testDir)

    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }

  test("multiple failures with updateStateByKey") {
    val n = 30
    // Input: time=1 ==> [ a ] , time=2 ==> [ a, a ] , time=3 ==> [ a, a, a ] , ...
    val input = (1 to n).map(i => (1 to i).map(_ => "a").mkString(" ")).toSeq
    // Expected output: time=1 ==> [ (a, 1) ] , time=2 ==> [ (a, 3) ] , time=3 ==> [ (a,6) ] , ...
    val expectedOutput = (1 to n).map(i => (1 to i).reduce(_ + _)).map(j => ("a", j))

    val operation = (st: DStream[String]) => {
     val updateFunc = (values: Seq[Int], state: Option[RichInt]) => {
       Some(new RichInt(values.foldLeft(0)(_ + _) + state.map(_.self).getOrElse(0)))
     }
     st.flatMap(_.split(" "))
       .map(x => (x, 1))
       .updateStateByKey[RichInt](updateFunc)
       .checkpoint(Seconds(2))
       .map(t => (t._1, t._2.self))
    }

    testOperationWithMultipleFailures(input, operation, expectedOutput)
  }

  test("multiple failures with reduceByKeyAndWindow") {
    val n = 30
    val w = 100
    assert(w > n, "Window should be much larger than the number of input sets in this test")
    // Input: time=1 ==> [ a ] , time=2 ==> [ a, a ] , time=3 ==> [ a, a, a ] , ...
    val input = (1 to n).map(i => (1 to i).map(_ => "a").mkString(" ")).toSeq
    // Expected output: time=1 ==> [ (a, 1) ] , time=2 ==> [ (a, 3) ] , time=3 ==> [ (a,6) ] , ...
    val expectedOutput = (1 to n).map(i => (1 to i).reduce(_ + _)).map(j => ("a", j))

    val operation = (st: DStream[String]) => {
      st.flatMap(_.split(" "))
        .map(x => (x, 1))
        .reduceByKeyAndWindow(_ + _, _ - _, batchDuration * w, batchDuration)
        .checkpoint(Seconds(2))
    }

    testOperationWithMultipleFailures(input, operation, expectedOutput)
  }


  /**
   * Tests stream operation with multiple master failures, and verifies whether the
   * final set of output values is as expected or not. Checking the final value is
   * proof that no intermediate data was lost due to master failures.
   */
  def testOperationWithMultipleFailures(
    input: Seq[String],
    operation: DStream[String] => DStream[(String, Int)],
    expectedOutput: Seq[(String, Int)]
  ) {
    var ssc = setupStreamsWithFileStream(operation)

    val mergedOutput = new ArrayBuffer[(String, Int)]()
    val lastExpectedOutput = expectedOutput.last

    val maxTimeToRun = expectedOutput.size * batchDuration.milliseconds * 2
    var totalTimeRan = 0L

    // Start generating files in the a different thread
    val fileGeneratingThread = new FileGeneratingThread(input, testDir.getPath, batchDuration.milliseconds)
    fileGeneratingThread.start()

    // Repeatedly start and kill the streaming context until timed out or
    // all expected output is generated
    while(!FailureSuite.outputGenerated && !FailureSuite.timedOut) {

      // Start the thread to kill the streaming after some time
      FailureSuite.failed = false
      val killingThread = new KillingThread(ssc, batchDuration.milliseconds * 10)
      killingThread.start()

      // Run the streams with real clock until last expected output is seen or timed out
      val (output, timeRan) = runStreamsWithRealClock(ssc, lastExpectedOutput, maxTimeToRun - totalTimeRan)
      if (killingThread.isAlive) killingThread.interrupt()

      // Merge output and time ran and see whether already timed out or not
      mergedOutput ++= output
      totalTimeRan += timeRan
      logInfo("New output = " + output)
      logInfo("Merged output = " + mergedOutput)
      logInfo("Total time spent = " + totalTimeRan)
      if (totalTimeRan > maxTimeToRun) {
        FailureSuite.timedOut = true
      }

      if (!FailureSuite.outputGenerated && !FailureSuite.timedOut) {
        val sleepTime = Random.nextInt(batchDuration.milliseconds.toInt * 2)
        logInfo(
          "\n-------------------------------------------\n" +
            "   Restarting stream computation in " + sleepTime + " ms   " +
            "\n-------------------------------------------\n"
        )
        Thread.sleep(sleepTime)
      }

      // Recreate the streaming context from checkpoint
      ssc = new StreamingContext(checkpointDir.getPath)
    }
    ssc.stop()
    ssc = null
    logInfo("Finished test after " + FailureSuite.failureCount + " failures")

    if (FailureSuite.timedOut) {
      logWarning("Timed out with run time of "+ maxTimeToRun + " ms for " +
        expectedOutput.size + " batches of " + batchDuration)
    }

    // Verify whether the output is as expected
    verifyOutput(mergedOutput, expectedOutput)
    if (fileGeneratingThread.isAlive) fileGeneratingThread.interrupt()
  }

  /** Sets up the stream operations with file input stream */
  def setupStreamsWithFileStream(
      operation: DStream[String] => DStream[(String, Int)]
  ): StreamingContext = {
    val ssc = new StreamingContext("local[4]", "FailureSuite", batchDuration)
    ssc.checkpoint(checkpointDir.getPath)
    val inputStream = ssc.textFileStream(testDir.getPath)
    val operatedStream = operation(inputStream)
    val outputBuffer = new ArrayBuffer[Seq[(String, Int)]] with SynchronizedBuffer[Seq[(String, Int)]]
    val outputStream = new TestOutputStream(operatedStream, outputBuffer)
    ssc.registerOutputStream(outputStream)
    ssc
  }

  /**
   * Runs the streams set up in `ssc` on real clock.
   */
  def runStreamsWithRealClock(
      ssc: StreamingContext,
      lastExpectedOutput: (String, Int),
      timeout: Long
  ): (Seq[(String, Int)], Long) = {

    System.clearProperty("spark.streaming.clock")

    // Get the output buffer
    val outputStream = ssc.graph.getOutputStreams.head.asInstanceOf[TestOutputStream[(String, Int)]]
    val output = outputStream.output
    val startTime = System.currentTimeMillis()

    // Functions to detect various conditions
    def hasFailed = FailureSuite.failed
    def isLastOutputGenerated = !output.flatMap(x => x).isEmpty && output(output.lastIndexWhere(!_.isEmpty)).head == lastExpectedOutput
    def isTimedOut = System.currentTimeMillis() - startTime > timeout

    // Start the streaming computation and let it run while ...
    // (i) StreamingContext has not been shut down yet
    // (ii) The last expected output has not been generated yet
    // (iii) Its not timed out yet
    try {
      ssc.start()
      while (!hasFailed && !isLastOutputGenerated && !isTimedOut) {
        Thread.sleep(100)
      }
      logInfo("Has failed = " + hasFailed)
      logInfo("Is last output generated = " + isLastOutputGenerated)
      logInfo("Is timed out = " + isTimedOut)
    } catch {
      case e: Exception => logInfo("Exception while running streams: " + e)
    } finally {
      ssc.stop()
    }

    // Verify whether the output of each batch has only one element
    assert(output.forall(_.size <= 1), "output of each batch should have only one element")

    // Set appropriate flags is timed out or output has been generated
    if (isTimedOut) FailureSuite.timedOut = true
    if (isLastOutputGenerated) FailureSuite.outputGenerated = true

    val timeTaken = System.currentTimeMillis() - startTime
    logInfo("" + output.size + " sets of output generated in " + timeTaken + " ms")
    (output.flatMap(_.headOption), timeTaken)
  }

  /**
   * Verifies the output value are the same as expected. Since failures can lead to
   * a batch being processed twice, a batches output may appear more than once
   * consecutively. To avoid getting confused with those, we eliminate consecutive
   * duplicate batch outputs of values from the `output`. As a result, the
   * expected output should not have consecutive batches with the same values as output.
   */
  def verifyOutput(output: Seq[(String, Int)], expectedOutput: Seq[(String, Int)]) {
    // Verify whether expected outputs do not consecutive batches with same output
    for (i <- 0 until expectedOutput.size - 1) {
      assert(expectedOutput(i) != expectedOutput(i+1),
        "Expected output has consecutive duplicate sequence of values")
    }

    // Match the output with the expected output
    logInfo(
      "\n-------------------------------------------\n" +
        "                Verifying output " +
        "\n-------------------------------------------\n"
    )
    logInfo("Expected output, size = " + expectedOutput.size)
    logInfo(expectedOutput.mkString("[", ",", "]"))
    logInfo("Output, size = " + output.size)
    logInfo(output.mkString("[", ",", "]"))
    output.foreach(o =>
      assert(expectedOutput.contains(o), "Expected value " + o + " not found")
    )
  }
}

object FailureSuite {
  var failed = false
  var outputGenerated = false
  var timedOut = false
  var failureCount = 0

  def reset() {
    failed = false
    outputGenerated = false
    timedOut = false
    failureCount = 0
  }
}

/**
 * Thread to kill streaming context after some time.
 */
class KillingThread(ssc: StreamingContext, maxKillWaitTime: Long) extends Thread with Logging {
  initLogging()

  override def run() {
    try {
      var minKillWaitTime = if (FailureSuite.failureCount == 0) 5000 else 1000 // to allow the first checkpoint
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
        FailureSuite.failed = true
        FailureSuite.failureCount += 1
      }
      logInfo("Killing thread exited")
    } catch {
      case ie: InterruptedException => logInfo("Killing thread interrupted")
      case e: Exception => logWarning("Exception in killing thread", e)
    }
  }
}

/**
 * Thread to generate input files periodically with the desired text
 */
class FileGeneratingThread(input: Seq[String], testDir: String, interval: Long)
  extends Thread with Logging {
  initLogging()

  override def run() {
    try {
      Thread.sleep(5000) // To make sure that all the streaming context has been set up
      for (i <- 0 until input.size) {
        FileUtils.writeStringToFile(new File(testDir, i.toString), input(i).toString + "\n")
        Thread.sleep(interval)
      }
      logInfo("File generating thread exited")
    } catch {
      case ie: InterruptedException => logInfo("File generating thread interrupted")
      case e: Exception => logWarning("File generating in killing thread", e)
    }
  }
}

