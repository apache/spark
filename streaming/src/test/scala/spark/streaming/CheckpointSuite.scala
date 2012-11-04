package spark.streaming

import spark.streaming.StreamingContext._
import java.io.File
import collection.mutable.ArrayBuffer
import runtime.RichInt
import org.scalatest.BeforeAndAfter
import org.apache.hadoop.fs.Path
import org.apache.commons.io.FileUtils

class CheckpointSuite extends TestSuiteBase with BeforeAndAfter {

  before {
    FileUtils.deleteDirectory(new File(checkpointDir))
  }

  after {
    FileUtils.deleteDirectory(new File(checkpointDir))
  }

  override def framework() = "CheckpointSuite"

  override def batchDuration() = Seconds(1)

  override def checkpointDir() = "checkpoint"

  override def checkpointInterval() = batchDuration

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
    val sscNew = new StreamingContext(checkpointDir)
    //sscNew.checkpoint(null, null)
    val outputNew = runStreams[V](sscNew, nextNumBatches, nextNumExpectedOutputs)
    verifyOutput[V](outputNew, expectedOutput.takeRight(nextNumExpectedOutputs), true)
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
      st.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(w), Seconds(1))
    }
    for (i <- Seq(3, 5, 7)) {
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
        .checkpoint(Seconds(5))
        .map(t => (t._1, t._2.self))
    }
    for (i <- Seq(3, 5, 7)) {
      testCheckpointedOperation(input, operation, output, i)
    }
  }

}