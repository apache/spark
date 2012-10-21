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

  test("updateStateByKey") {
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

  test("forgetting of RDDs - map and window operations") {
    assert(batchDuration === Seconds(1), "Batch duration has changed from 1 second")

    val input = (0 until 10).map(x => Seq(x, x + 1)).toSeq
    val rememberDuration = Seconds(3)

    assert(input.size === 10, "Number of inputs have changed")

    def operation(s: DStream[Int]): DStream[(Int, Int)] = {
      s.map(x => (x % 10, 1))
       .window(Seconds(2), Seconds(1))
       .window(Seconds(4), Seconds(2))
    }

    val ssc = setupStreams(input, operation _)
    ssc.setRememberDuration(rememberDuration)
    runStreams[(Int, Int)](ssc, input.size, input.size / 2)

    val windowedStream2 = ssc.graph.getOutputStreams().head.dependencies.head
    val windowedStream1 = windowedStream2.dependencies.head
    val mappedStream = windowedStream1.dependencies.head

    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    assert(clock.time === Seconds(10).milliseconds)

    // IDEALLY
    // WindowedStream2 should remember till 7 seconds: 10, 8,
    // WindowedStream1 should remember till 4 seconds: 10, 9, 8, 7, 6, 5
    // MappedStream should remember till 7 seconds:    10, 9, 8, 7, 6, 5, 4, 3,

    // IN THIS TEST
    // WindowedStream2 should remember till 7 seconds: 10, 8,
    // WindowedStream1 should remember till 4 seconds: 10, 9, 8, 7, 6, 5, 4
    // MappedStream should remember till 7 seconds:    10, 9, 8, 7, 6, 5, 4, 3, 2

    // WindowedStream2
    assert(windowedStream2.generatedRDDs.contains(Seconds(10)))
    assert(windowedStream2.generatedRDDs.contains(Seconds(8)))
    assert(!windowedStream2.generatedRDDs.contains(Seconds(6)))

    // WindowedStream1
    assert(windowedStream1.generatedRDDs.contains(Seconds(10)))
    assert(windowedStream1.generatedRDDs.contains(Seconds(4)))
    assert(!windowedStream1.generatedRDDs.contains(Seconds(3)))

    // MappedStream
    assert(mappedStream.generatedRDDs.contains(Seconds(10)))
    assert(mappedStream.generatedRDDs.contains(Seconds(2)))
    assert(!mappedStream.generatedRDDs.contains(Seconds(1)))
  }
}
