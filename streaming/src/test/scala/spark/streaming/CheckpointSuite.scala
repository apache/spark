package spark.streaming

import spark.streaming.StreamingContext._
import java.io.File
import runtime.RichInt
import org.scalatest.BeforeAndAfter
import org.apache.commons.io.FileUtils
import collection.mutable.{SynchronizedBuffer, ArrayBuffer}
import util.{Clock, ManualClock}

class CheckpointSuite extends TestSuiteBase with BeforeAndAfter {

  before {
    FileUtils.deleteDirectory(new File(checkpointDir))
  }

  after {

    if (ssc != null) ssc.stop()
    FileUtils.deleteDirectory(new File(checkpointDir))
  }

  var ssc: StreamingContext = null

  override def framework = "CheckpointSuite"

  override def batchDuration = Milliseconds(500)

  override def checkpointDir = "checkpoint"

  override def checkpointInterval = batchDuration

  override def actuallyWait = true

  test("basic stream+rdd recovery") {

    assert(batchDuration === Milliseconds(500), "batchDuration for this test must be 1 second")
    assert(checkpointInterval === batchDuration, "checkpointInterval for this test much be same as batchDuration")

    System.setProperty("spark.streaming.clock", "spark.streaming.util.ManualClock")

    val stateStreamCheckpointInterval = Seconds(1)

    // this ensure checkpointing occurs at least once
    val firstNumBatches = (stateStreamCheckpointInterval / batchDuration) * 2
    val secondNumBatches = firstNumBatches

    // Setup the streams
    val input = (1 to 10).map(_ => Seq("a")).toSeq
    val operation = (st: DStream[String]) => {
      val updateFunc = (values: Seq[Int], state: Option[RichInt]) => {
        Some(new RichInt(values.foldLeft(0)(_ + _) + state.map(_.self).getOrElse(0)))
      }
      st.map(x => (x, 1))
      .updateStateByKey[RichInt](updateFunc)
      .checkpoint(stateStreamCheckpointInterval)
      .map(t => (t._1, t._2.self))
    }
    var ssc = setupStreams(input, operation)
    var stateStream = ssc.graph.getOutputStreams().head.dependencies.head.dependencies.head

    // Run till a time such that at least one RDD in the stream should have been checkpointed,
    // then check whether some RDD has been checkpointed or not
    ssc.start()
    runStreamsWithRealDelay(ssc, firstNumBatches)
    logInfo("Checkpoint data of state stream = \n[" + stateStream.checkpointData.rdds.mkString(",\n") + "]")
    assert(!stateStream.checkpointData.rdds.isEmpty, "No checkpointed RDDs in state stream before first failure")
    stateStream.checkpointData.rdds.foreach {
      case (time, data) => {
        val file = new File(data.toString)
        assert(file.exists(), "Checkpoint file '" + file +"' for time " + time + " for state stream before first failure does not exist")
      }
    }

    // Run till a further time such that previous checkpoint files in the stream would be deleted
    // and check whether the earlier checkpoint files are deleted
    val checkpointFiles = stateStream.checkpointData.rdds.map(x => new File(x._2.toString))
    runStreamsWithRealDelay(ssc, secondNumBatches)
    checkpointFiles.foreach(file => assert(!file.exists, "Checkpoint file '" + file + "' was not deleted"))
    ssc.stop()

    // Restart stream computation using the checkpoint file and check whether
    // checkpointed RDDs have been restored or not
    ssc = new StreamingContext(checkpointDir)
    stateStream = ssc.graph.getOutputStreams().head.dependencies.head.dependencies.head
    logInfo("Restored data of state stream = \n[" + stateStream.generatedRDDs.mkString("\n") + "]")
    assert(!stateStream.generatedRDDs.isEmpty, "No restored RDDs in state stream after recovery from first failure")


    // Run one batch to generate a new checkpoint file and check whether some RDD
    // is present in the checkpoint data or not
    ssc.start()
    runStreamsWithRealDelay(ssc, 1)
    assert(!stateStream.checkpointData.rdds.isEmpty, "No checkpointed RDDs in state stream before second failure")
    stateStream.checkpointData.rdds.foreach {
      case (time, data) => {
        val file = new File(data.toString)
        assert(file.exists(),
          "Checkpoint file '" + file +"' for time " + time + " for state stream before seconds failure does not exist")
      }
    }
    ssc.stop()

    // Restart stream computation from the new checkpoint file to see whether that file has
    // correct checkpoint data
    ssc = new StreamingContext(checkpointDir)
    stateStream = ssc.graph.getOutputStreams().head.dependencies.head.dependencies.head
    logInfo("Restored data of state stream = \n[" + stateStream.generatedRDDs.mkString("\n") + "]")
    assert(!stateStream.generatedRDDs.isEmpty, "No restored RDDs in state stream after recovery from second failure")

    // Adjust manual clock time as if it is being restarted after a delay
    System.setProperty("spark.streaming.manualClock.jump", (batchDuration.milliseconds * 7).toString)
    ssc.start()
    runStreamsWithRealDelay(ssc, 4)
    ssc.stop()
    System.clearProperty("spark.streaming.manualClock.jump")
    ssc = null
  }

  test("map and reduceByKey") {
    testCheckpointedOperation(
      Seq( Seq("a", "a", "b"), Seq("", ""), Seq(), Seq("a", "a", "b"), Seq("", ""), Seq() ),
      (s: DStream[String]) => s.map(x => (x, 1)).reduceByKey(_ + _),
      Seq( Seq(("a", 2), ("b", 1)), Seq(("", 2)), Seq(), Seq(("a", 2), ("b", 1)), Seq(("", 2)), Seq() ),
      3
    )
  }

  test("reduceByKeyAndWindowInv") {
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

  test("updateStateByKey") {
    val input = (1 to 10).map(_ => Seq("a")).toSeq
    val output = (1 to 10).map(x => Seq(("a", x))).toSeq
    val operation = (st: DStream[String]) => {
      val updateFunc = (values: Seq[Int], state: Option[RichInt]) => {
        Some(new RichInt(values.foldLeft(0)(_ + _) + state.map(_.self).getOrElse(0)))
      }
      st.map(x => (x, 1))
        .updateStateByKey[RichInt](updateFunc)
        .checkpoint(batchDuration * 2)
        .map(t => (t._1, t._2.self))
    }
    testCheckpointedOperation(input, operation, output, 7)
  }

  /**
   * Tests a streaming operation under checkpointing, by restart the operation
   * from checkpoint file and verifying whether the final output is correct.
   * The output is assumed to have come from a reliable queue which an replay
   * data as required.
   */
  def testCheckpointedOperation[U: ClassManifest, V: ClassManifest](
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
    val nextNumExpectedOutputs = expectedOutput.size - initialNumExpectedOutputs

    // Do the computation for initial number of batches, create checkpoint file and quit
    ssc = setupStreams[U, V](input, operation)
    val output = runStreams[V](ssc, initialNumBatches, initialNumExpectedOutputs)
    verifyOutput[V](output, expectedOutput.take(initialNumBatches), true)
    Thread.sleep(1000)

    // Restart and complete the computation from checkpoint file
    logInfo(
      "\n-------------------------------------------\n" +
      "        Restarting stream computation          " +
      "\n-------------------------------------------\n"
    )
    ssc = new StreamingContext(checkpointDir)
    val outputNew = runStreams[V](ssc, nextNumBatches, nextNumExpectedOutputs)
    verifyOutput[V](outputNew, expectedOutput.takeRight(nextNumExpectedOutputs), true)
    ssc = null
  }

  /**
   * Advances the manual clock on the streaming scheduler by given number of batches.
   * It also wait for the expected amount of time for each batch.
   */
  def runStreamsWithRealDelay(ssc: StreamingContext, numBatches: Long) {
    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    logInfo("Manual clock before advancing = " + clock.time)
    for (i <- 1 to numBatches.toInt) {
      clock.addToTime(batchDuration.milliseconds)
      Thread.sleep(batchDuration.milliseconds)
    }
    logInfo("Manual clock after advancing = " + clock.time)
    Thread.sleep(batchDuration.milliseconds)
  }

}