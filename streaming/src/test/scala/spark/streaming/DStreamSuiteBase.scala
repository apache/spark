package spark.streaming

import spark.{RDD, Logging}
import util.ManualClock
import collection.mutable.ArrayBuffer
import org.scalatest.FunSuite
import collection.mutable.SynchronizedBuffer

class TestInputStream[T: ClassManifest](ssc_ : StreamingContext, val input: Seq[Seq[T]])
  extends InputDStream[T](ssc_) {
  var currentIndex = 0

  def start() {}

  def stop() {}

  def compute(validTime: Time): Option[RDD[T]] = {
    logInfo("Computing RDD for time " + validTime)
    val rdd = if (currentIndex < input.size) {
      ssc.sc.makeRDD(input(currentIndex), 2)
    } else {
      ssc.sc.makeRDD(Seq[T](), 2)
    }
    logInfo("Created RDD " + rdd.id)
    currentIndex += 1
    Some(rdd)
  }
}

class TestOutputStream[T: ClassManifest](parent: DStream[T], val output: ArrayBuffer[Seq[T]])
  extends PerRDDForEachDStream[T](parent, (rdd: RDD[T], t: Time) => {
    val collected = rdd.collect()
    output += collected
  })

trait DStreamSuiteBase extends FunSuite with Logging {

  def framework() = "DStreamSuiteBase"

  def master() = "local[2]"

  def batchDuration() = Seconds(1)

  def checkpointFile() = null.asInstanceOf[String]

  def checkpointInterval() = batchDuration

  def maxWaitTimeMillis() = 10000

  def setupStreams[U: ClassManifest, V: ClassManifest](
      input: Seq[Seq[U]],
      operation: DStream[U] => DStream[V]
    ): StreamingContext = {

    // Create StreamingContext
    val ssc = new StreamingContext(master, framework)
    ssc.setBatchDuration(batchDuration)
    if (checkpointFile != null) {
      ssc.setCheckpointDetails(checkpointFile, checkpointInterval())
    }

    // Setup the stream computation
    val inputStream = new TestInputStream(ssc, input)
    ssc.registerInputStream(inputStream)
    val operatedStream = operation(inputStream)
    val outputStream = new TestOutputStream(operatedStream, new ArrayBuffer[Seq[V]] with SynchronizedBuffer[Seq[V]])
    ssc.registerOutputStream(outputStream)
    ssc
  }

  def runStreams[V: ClassManifest](
      ssc: StreamingContext,
      numBatches: Int,
      numExpectedOutput: Int
    ): Seq[Seq[V]] = {
    logInfo("numBatches = " + numBatches + ", numExpectedOutput = " + numExpectedOutput)

    // Get the output buffer
    val outputStream = ssc.graph.getOutputStreams.head.asInstanceOf[TestOutputStream[V]]
    val output = outputStream.output

    try {
      // Start computation
      ssc.start()

      // Advance manual clock
      val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
      logInfo("Manual clock before advancing = " + clock.time)
      clock.addToTime(numBatches * batchDuration.milliseconds)
      logInfo("Manual clock after advancing = " + clock.time)

      // Wait until expected number of output items have been generated
      val startTime = System.currentTimeMillis()
      while (output.size < numExpectedOutput && System.currentTimeMillis() - startTime < maxWaitTimeMillis) {
        logInfo("output.size = " + output.size + ", numExpectedOutput = " + numExpectedOutput)
        Thread.sleep(100)
      }
      val timeTaken = System.currentTimeMillis() - startTime

      assert(timeTaken < maxWaitTimeMillis, "Operation timed out after " + timeTaken + " ms")
      assert(output.size === numExpectedOutput, "Unexpected number of outputs generated")
    } catch {
      case e: Exception => e.printStackTrace(); throw e;
    } finally {
      ssc.stop()
    }

    output
  }

  def verifyOutput[V: ClassManifest](
      output: Seq[Seq[V]],
      expectedOutput: Seq[Seq[V]],
      useSet: Boolean
    ) {
    logInfo("--------------------------------")
    logInfo("output.size = " + output.size)
    logInfo("output")
    output.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    logInfo("expected output.size = " + expectedOutput.size)
    logInfo("expected output")
    expectedOutput.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    logInfo("--------------------------------")

    // Match the output with the expected output
    assert(output.size === expectedOutput.size, "Number of outputs do not match")
    for (i <- 0 until output.size) {
      if (useSet) {
        assert(output(i).toSet === expectedOutput(i).toSet)
      } else {
        assert(output(i).toList === expectedOutput(i).toList)
      }
    }
    logInfo("Output verified successfully")
  }

  def testOperation[U: ClassManifest, V: ClassManifest](
      input: Seq[Seq[U]],
      operation: DStream[U] => DStream[V],
      expectedOutput: Seq[Seq[V]],
      useSet: Boolean = false
    ) {
    testOperation[U, V](input, operation, expectedOutput, -1, useSet)
  }

  def testOperation[U: ClassManifest, V: ClassManifest](
      input: Seq[Seq[U]],
      operation: DStream[U] => DStream[V],
      expectedOutput: Seq[Seq[V]],
      numBatches: Int,
      useSet: Boolean
    ) {
    System.setProperty("spark.streaming.clock", "spark.streaming.util.ManualClock")

    val numBatches_ = if (numBatches > 0) numBatches else expectedOutput.size
    val ssc = setupStreams[U, V](input, operation)
    val output = runStreams[V](ssc, numBatches_, expectedOutput.size)
    verifyOutput[V](output, expectedOutput, useSet)
  }
}
