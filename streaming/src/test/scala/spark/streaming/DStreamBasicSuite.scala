package spark.streaming

import spark.streaming.StreamingContext._
import scala.runtime.RichInt

class DStreamBasicSuite extends DStreamSuiteBase {

  test("map-like operations") {
    val input = Seq(1 to 4, 5 to 8, 9 to 12)
    
    // map
    testOperation(input, (r: DStream[Int]) => r.map(_.toString), input.map(_.map(_.toString)))
    
    // flatMap
    testOperation(
      input,
      (r: DStream[Int]) => r.flatMap(x => Seq(x, x * 2)),
      input.map(_.flatMap(x => Array(x, x * 2)))
    )
  }

  test("shuffle-based operations") {
    // reduceByKey
    testOperation(
      Seq(Seq("a", "a", "b"), Seq("", ""), Seq()),
      (s: DStream[String]) => s.map(x => (x, 1)).reduceByKey(_ + _),
      Seq(Seq(("a", 2), ("b", 1)), Seq(("", 2)), Seq()),
      true
    )

    // reduce
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

    val updateStateOp = (s: DStream[String]) => {
      val updateFunc = (values: Seq[Int], state: RichInt) => {
        var newState = 0
        if (values != null && values.size > 0) newState += values.reduce(_ + _)
        if (state != null) newState += state.self
        //println("values = " + values + ", state = " + state + ", " + " new state = " + newState)
        new RichInt(newState)
      }
      s.map(x => (x, 1)).updateStateByKey[RichInt](updateFunc).map(t => (t._1, t._2.self))
    }

    testOperation(inputData, updateStateOp, outputData, true)
  }
}
