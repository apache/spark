package spark.streaming

import spark.streaming.StreamingContext._
import scala.runtime.RichInt
import util.ManualClock

class DStreamBasicSuite extends DStreamSuiteBase {

  test("map") {
    val input = Seq(1 to 4, 5 to 8, 9 to 12)
    testOperation(
      input,
      (r: DStream[Int]) => r.map(_.toString),
      input.map(_.map(_.toString))
    )
  }

  test("flatmap") {
    val input = Seq(1 to 4, 5 to 8, 9 to 12)
    testOperation(
      input,
      (r: DStream[Int]) => r.flatMap(x => Seq(x, x * 2)),
      input.map(_.flatMap(x => Array(x, x * 2)))
    )
  }

  test("reduceByKey") {
    testOperation(
      Seq( Seq("a", "a", "b"), Seq("", ""), Seq() ),
      (s: DStream[String]) => s.map(x => (x, 1)).reduceByKey(_ + _),
      Seq( Seq(("a", 2), ("b", 1)), Seq(("", 2)), Seq() ),
      true
    )
  }

  test("reduce") {
    testOperation(
      Seq(1 to 4, 5 to 8, 9 to 12),
      (s: DStream[Int]) => s.reduce(_ + _),
      Seq(Seq(10), Seq(26), Seq(42))
    )
  }

  test("stateful operations") {
    val inputData =
      Seq(
        Seq("a"),
        Seq("a", "b"),
        Seq("a", "b", "c"),
        Seq("a", "b"),
        Seq("a"),
        Seq()
      )

    val outputData =
      Seq(
        Seq(("a", 1)),
        Seq(("a", 2), ("b", 1)),
        Seq(("a", 3), ("b", 2), ("c", 1)),
        Seq(("a", 4), ("b", 3), ("c", 1)),
        Seq(("a", 5), ("b", 3), ("c", 1)),
        Seq(("a", 5), ("b", 3), ("c", 1))
      )

    val updateStateOperation = (s: DStream[String]) => {
      val updateFunc = (values: Seq[Int], state: RichInt) => {
        var newState = 0
        if (values != null && values.size > 0) newState += values.reduce(_ + _)
        if (state != null) newState += state.self
        new RichInt(newState)
      }
      s.map(x => (x, 1)).updateStateByKey[RichInt](updateFunc).map(t => (t._1, t._2.self))
    }

    testOperation(inputData, updateStateOperation, outputData, true)
  }

  test("forgetting of RDDs") {
    assert(batchDuration === Seconds(1), "Batch duration has changed from 1 second")

    val input = Seq(1 to 4, 5 to 8, 9 to 12, 13 to 16, 17 to 20, 21 to 24, 25 to 28, 29 to 32)

    assert(input.size % 4 === 0, "Number of inputs should be a multiple of 4")

    def operation(s: DStream[Int]): DStream[(Int, Int)] = {
      s.map(x => (x % 10, 1))
       .window(Seconds(2), Seconds(1))
       .reduceByKeyAndWindow(_ + _, _ - _, Seconds(4), Seconds(1))
    }

    val ssc = setupStreams(input, operation _)
    runStreams[(Int, Int)](ssc, input.size, input.size)

    val reducedWindowedStream = ssc.graph.getOutputStreams().head.dependencies.head
                                   .asInstanceOf[ReducedWindowedDStream[Int, Int]]
    val windowedStream = reducedWindowedStream.dependencies.head.dependencies.head
                                              .asInstanceOf[WindowedDStream[(Int, Int)]]
    val mappedStream = windowedStream.dependencies.head

    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    val finalTime = Seconds(7)
    //assert(clock.time === finalTime.milliseconds)

    // ReducedWindowedStream should remember the last RDD created
    assert(reducedWindowedStream.generatedRDDs.contains(finalTime))

    // ReducedWindowedStream should have forgotten the previous to last RDD created
    assert(!reducedWindowedStream.generatedRDDs.contains(finalTime - reducedWindowedStream.slideTime))

    // WindowedStream should remember the last RDD created
    assert(windowedStream.generatedRDDs.contains(finalTime))

    // WindowedStream should still remember the previous to last RDD created
    // as the last RDD of ReducedWindowedStream requires that RDD
    assert(windowedStream.generatedRDDs.contains(finalTime - windowedStream.slideTime))

    // WindowedStream should have forgotten this RDD as the last RDD of
    // ReducedWindowedStream DOES NOT require this RDD
    assert(!windowedStream.generatedRDDs.contains(finalTime - windowedStream.slideTime - reducedWindowedStream.windowTime))

    // MappedStream should remember the last RDD created
    assert(mappedStream.generatedRDDs.contains(finalTime))

    // MappedStream should still remember the previous to last RDD created
    // as the last RDD of WindowedStream requires that RDD
    assert(mappedStream.generatedRDDs.contains(finalTime - mappedStream.slideTime))

    // MappedStream should still remember this RDD as the last RDD of
    // ReducedWindowedStream requires that RDD (even though the last RDD of
    // WindowedStream does not need it)
    assert(mappedStream.generatedRDDs.contains(finalTime - windowedStream.windowTime))

    // MappedStream should have forgotten this RDD as the last RDD of
    // ReducedWindowedStream DOES NOT require this RDD
    assert(!mappedStream.generatedRDDs.contains(finalTime - mappedStream.slideTime - windowedStream.windowTime - reducedWindowedStream.windowTime))
  }
}
