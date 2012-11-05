package spark.streaming

import spark.{RDD, Logging}
import util.ManualClock
import collection.mutable.ArrayBuffer
import org.scalatest.FunSuite
import collection.mutable.SynchronizedBuffer
import java.io.{ObjectInputStream, IOException}


/**
 * This is a input stream just for the testsuites. This is equivalent to a checkpointable,
 * replayable, reliable message queue like Kafka. It requires a sequence as input, and
 * returns the i_th element at the i_th batch unde manual clock.
 */
class TestInputStream[T: ClassManifest](ssc_ : StreamingContext, input: Seq[Seq[T]], numPartitions: Int)
  extends InputDStream[T](ssc_) {

  def start() {}

  def stop() {}

  def compute(validTime: Time): Option[RDD[T]] = {
    logInfo("Computing RDD for time " + validTime)
    val index = ((validTime - zeroTime) / slideTime - 1).toInt
    val rdd = if (index < input.size) {
      ssc.sc.makeRDD(input(index), numPartitions)
    } else {
      ssc.sc.makeRDD(Seq[T](), numPartitions)
    }
    logInfo("Created RDD " + rdd.id)
    Some(rdd)
  }
}

/**
 * This is a output stream just for the testsuites. All the output is collected into a
 * ArrayBuffer. This buffer is wiped clean on being restored from checkpoint.
 */
class TestOutputStream[T: ClassManifest](parent: DStream[T], val output: ArrayBuffer[Seq[T]])
  extends PerRDDForEachDStream[T](parent, (rdd: RDD[T], t: Time) => {
    val collected = rdd.collect()
    output += collected
  }) {

  // This is to clear the output buffer every it is read from a checkpoint
  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream) {
    ois.defaultReadObject()
    output.clear()
  }
}

/**
 * This is the base trait for Spark Streaming testsuites. This provides basic functionality
 * to run user-defined set of input on user-defined stream operations, and verify the output.
 */
trait TestSuiteBase extends FunSuite with Logging {

  def framework = "TestSuiteBase"

  def master = "local[2]"

  def batchDuration = Seconds(1)

  def checkpointDir = null.asInstanceOf[String]

  def checkpointInterval = batchDuration

  def numInputPartitions = 2

  def maxWaitTimeMillis = 10000

  def actuallyWait = false

  def setupStreams[U: ClassManifest, V: ClassManifest](
      input: Seq[Seq[U]],
      operation: DStream[U] => DStream[V]
    ): StreamingContext = {

    // Create StreamingContext
    val ssc = new StreamingContext(master, framework)
    ssc.setBatchDuration(batchDuration)
    if (checkpointDir != null) {
      ssc.checkpoint(checkpointDir, checkpointInterval)
    }

    // Setup the stream computation
    val inputStream = new TestInputStream(ssc, input, numInputPartitions)
    val operatedStream = operation(inputStream)
    val outputStream = new TestOutputStream(operatedStream, new ArrayBuffer[Seq[V]] with SynchronizedBuffer[Seq[V]])
    ssc.registerInputStream(inputStream)
    ssc.registerOutputStream(outputStream)
    ssc
  }

  def setupStreams[U: ClassManifest, V: ClassManifest, W: ClassManifest](
      input1: Seq[Seq[U]],
      input2: Seq[Seq[V]],
      operation: (DStream[U], DStream[V]) => DStream[W]
    ): StreamingContext = {

    // Create StreamingContext
    val ssc = new StreamingContext(master, framework)
    ssc.setBatchDuration(batchDuration)
    if (checkpointDir != null) {
      ssc.checkpoint(checkpointDir, checkpointInterval)
    }

    // Setup the stream computation
    val inputStream1 = new TestInputStream(ssc, input1, numInputPartitions)
    val inputStream2 = new TestInputStream(ssc, input2, numInputPartitions)
    val operatedStream = operation(inputStream1, inputStream2)
    val outputStream = new TestOutputStream(operatedStream, new ArrayBuffer[Seq[W]] with SynchronizedBuffer[Seq[W]])
    ssc.registerInputStream(inputStream1)
    ssc.registerInputStream(inputStream2)
    ssc.registerOutputStream(outputStream)
    ssc
  }

  /**
   * Runs the streams set up in `ssc` on manual clock for `numBatches` batches and
   * returns the collected output. It will wait until `numExpectedOutput` number of
   * output data has been collected or timeout (set by `maxWaitTimeMillis`) is reached.
   */
  def runStreams[V: ClassManifest](
      ssc: StreamingContext,
      numBatches: Int,
      numExpectedOutput: Int
    ): Seq[Seq[V]] = {

    System.setProperty("spark.streaming.clock", "spark.streaming.util.ManualClock")

    assert(numBatches > 0, "Number of batches to run stream computation is zero")
    assert(numExpectedOutput > 0, "Number of expected outputs after " + numBatches + " is zero")
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
      if (actuallyWait) {
        for (i <- 1 to numBatches) {
          logInfo("Actually waiting for " + batchDuration)
          clock.addToTime(batchDuration.milliseconds)
          Thread.sleep(batchDuration.milliseconds)
        }
      } else {
        clock.addToTime(numBatches * batchDuration.milliseconds)
      }
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

      Thread.sleep(500) // Give some time for the forgetting old RDDs to complete
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
    val numBatches_ = if (numBatches > 0) numBatches else expectedOutput.size
    val ssc = setupStreams[U, V](input, operation)
    val output = runStreams[V](ssc, numBatches_, expectedOutput.size)
    verifyOutput[V](output, expectedOutput, useSet)
  }

  def testOperation[U: ClassManifest, V: ClassManifest, W: ClassManifest](
      input1: Seq[Seq[U]],
      input2: Seq[Seq[V]],
      operation: (DStream[U], DStream[V]) => DStream[W],
      expectedOutput: Seq[Seq[W]],
      useSet: Boolean
    ) {
    testOperation[U, V, W](input1, input2, operation, expectedOutput, -1, useSet)
  }

  def testOperation[U: ClassManifest, V: ClassManifest, W: ClassManifest](
      input1: Seq[Seq[U]],
      input2: Seq[Seq[V]],
      operation: (DStream[U], DStream[V]) => DStream[W],
      expectedOutput: Seq[Seq[W]],
      numBatches: Int,
      useSet: Boolean
    ) {
    val numBatches_ = if (numBatches > 0) numBatches else expectedOutput.size
    val ssc = setupStreams[U, V, W](input1, input2, operation)
    val output = runStreams[W](ssc, numBatches_, expectedOutput.size)
    verifyOutput[W](output, expectedOutput, useSet)
  }
}
