package spark.streaming

import spark.streaming.StreamingContext._

class CheckpointSuite extends DStreamSuiteBase {

  override def framework() = "CheckpointSuite"

  override def checkpointFile() = "checkpoint"

  def testCheckpointedOperation[U: ClassManifest, V: ClassManifest](
      input: Seq[Seq[U]],
      operation: DStream[U] => DStream[V],
      expectedOutput: Seq[Seq[V]],
      useSet: Boolean = false
    ) {
    System.setProperty("spark.streaming.clock", "spark.streaming.util.ManualClock")

    // Current code assumes that:
    // number of inputs = number of outputs = number of batches to be run

    // Do half the computation (half the number of batches), create checkpoint file and quit
    val totalNumBatches = input.size
    val initialNumBatches = input.size / 2
    val nextNumBatches = totalNumBatches - initialNumBatches
    val initialNumExpectedOutputs = initialNumBatches

    val ssc = setupStreams[U, V](input, operation)
    val output = runStreams[V](ssc, initialNumBatches, initialNumExpectedOutputs)
    verifyOutput[V](output, expectedOutput.take(initialNumBatches), useSet)
    Thread.sleep(1000)

    // Restart and complete the computation from checkpoint file
    val sscNew = new StreamingContext(checkpointFile)
    sscNew.setCheckpointDetails(null, null)
    val outputNew = runStreams[V](sscNew, nextNumBatches, expectedOutput.size)
    verifyOutput[V](outputNew, expectedOutput, useSet)
  }

  test("simple per-batch operation") {
    testCheckpointedOperation(
      Seq( Seq("a", "a", "b"), Seq("", ""), Seq(), Seq("a", "a", "b"), Seq("", ""), Seq() ),
      (s: DStream[String]) => s.map(x => (x, 1)).reduceByKey(_ + _),
      Seq( Seq(("a", 2), ("b", 1)), Seq(("", 2)), Seq(), Seq(("a", 2), ("b", 1)), Seq(("", 2)), Seq() ),
      true
    )
  }
}