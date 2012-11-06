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
    FileUtils.deleteDirectory(new File(checkpointDir))
  }

  override def framework = "CheckpointSuite"

  override def batchDuration = Milliseconds(500)

  override def checkpointDir = "checkpoint"

  override def checkpointInterval = batchDuration

  override def actuallyWait = true

  test("basic stream+rdd recovery") {

    assert(batchDuration === Milliseconds(500), "batchDuration for this test must be 1 second")
    assert(checkpointInterval === batchDuration, "checkpointInterval for this test much be same as batchDuration")

    System.setProperty("spark.streaming.clock", "spark.streaming.util.ManualClock")

    val stateStreamCheckpointInterval = Seconds(2)

    // this ensure checkpointing occurs at least once
    val firstNumBatches = (stateStreamCheckpointInterval.millis / batchDuration.millis) * 2
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
    val ssc = setupStreams(input, operation)
    val stateStream = ssc.graph.getOutputStreams().head.dependencies.head.dependencies.head

    // Run till a time such that at least one RDD in the stream should have been checkpointed
    ssc.start()
    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    advanceClock(clock, firstNumBatches)

    // Check whether some RDD has been checkpointed or not
    logInfo("Checkpoint data of state stream = \n[" + stateStream.checkpointData.mkString(",\n") + "]")
    assert(!stateStream.checkpointData.isEmpty, "No checkpointed RDDs in state stream before first failure")
    stateStream.checkpointData.foreach {
      case (time, data) => {
        val file = new File(data.toString)
        assert(file.exists(), "Checkpoint file '" + file +"' for time " + time + " for state stream before first failure does not exist")
      }
    }

    // Run till a further time such that previous checkpoint files in the stream would be deleted
    // and check whether the earlier checkpoint files are deleted
    val checkpointFiles = stateStream.checkpointData.map(x => new File(x._2.toString))
    advanceClock(clock, secondNumBatches)
    checkpointFiles.foreach(file => assert(!file.exists, "Checkpoint file '" + file + "' was not deleted"))

    // Restart stream computation using the checkpoint file and check whether
    // checkpointed RDDs have been restored or not
    ssc.stop()
    val sscNew = new StreamingContext(checkpointDir)
    val stateStreamNew = sscNew.graph.getOutputStreams().head.dependencies.head.dependencies.head
    logInfo("Restored data of state stream = \n[" + stateStreamNew.generatedRDDs.mkString("\n") + "]")
    assert(!stateStreamNew.generatedRDDs.isEmpty, "No restored RDDs in state stream after recovery from first failure")


    // Run one batch to generate a new checkpoint file
    sscNew.start()
    val clockNew = sscNew.scheduler.clock.asInstanceOf[ManualClock]
    advanceClock(clockNew, 1)

    // Check whether some RDD is present in the checkpoint data or not
    assert(!stateStreamNew.checkpointData.isEmpty, "No checkpointed RDDs in state stream before second failure")
    stateStream.checkpointData.foreach {
      case (time, data) => {
        val file = new File(data.toString)
        assert(file.exists(), "Checkpoint file '" + file +"' for time " + time + " for state stream before seconds failure does not exist")
      }
    }

    // Restart stream computation from the new checkpoint file to see whether that file has
    // correct checkpoint data
    sscNew.stop()
    val sscNewNew = new StreamingContext(checkpointDir)
    val stateStreamNewNew = sscNew.graph.getOutputStreams().head.dependencies.head.dependencies.head
    logInfo("Restored data of state stream = \n[" + stateStreamNew.generatedRDDs.mkString("\n") + "]")
    assert(!stateStreamNewNew.generatedRDDs.isEmpty, "No restored RDDs in state stream after recovery from second failure")
    sscNewNew.start()
    advanceClock(sscNewNew.scheduler.clock.asInstanceOf[ManualClock], 1)
    sscNewNew.stop()
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
    val input = (1 to n).map(x => Seq("a")).toSeq
    val output = Seq(Seq(("a", 1)), Seq(("a", 2)), Seq(("a", 3))) ++ (1 to (n - w + 1)).map(x => Seq(("a", 4)))
    val operation = (st: DStream[String]) => {
      st.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, batchDuration * 4, batchDuration)
    }
    for (i <- Seq(2, 3, 4)) {
      testCheckpointedOperation(input, operation, output, i)
    }
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
        .checkpoint(Seconds(2))
        .map(t => (t._1, t._2.self))
    }
    for (i <- Seq(2, 3, 4)) {
      testCheckpointedOperation(input, operation, output, i)
    }
  }



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

    // Do half the computation (half the number of batches), create checkpoint file and quit

    val ssc = setupStreams[U, V](input, operation)
    val output = runStreams[V](ssc, initialNumBatches, initialNumExpectedOutputs)
    verifyOutput[V](output, expectedOutput.take(initialNumBatches), true)
    Thread.sleep(1000)

    // Restart and complete the computation from checkpoint file
    logInfo(
      "\n-------------------------------------------\n" +
      "        Restarting stream computation          " +
      "\n-------------------------------------------\n"
    )
    val sscNew = new StreamingContext(checkpointDir)
    val outputNew = runStreams[V](sscNew, nextNumBatches, nextNumExpectedOutputs)
    verifyOutput[V](outputNew, expectedOutput.takeRight(nextNumExpectedOutputs), true)
  }

  def advanceClock(clock: ManualClock, numBatches: Long) {
    logInfo("Manual clock before advancing = " + clock.time)
    for (i <- 1 to numBatches.toInt) {
      clock.addToTime(batchDuration.milliseconds)
      Thread.sleep(batchDuration.milliseconds)
    }
    logInfo("Manual clock after advancing = " + clock.time)
    Thread.sleep(batchDuration.milliseconds)
  }
}