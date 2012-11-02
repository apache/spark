package spark.streaming

import spark.streaming.StreamingContext._
import collection.mutable.ArrayBuffer

class WindowOperationsSuite extends TestSuiteBase {

  override def framework() = "WindowOperationsSuite"

  override def maxWaitTimeMillis() = 20000

  override def batchDuration() = Seconds(1)

  val largerSlideInput = Seq(
    Seq(("a", 1)),
    Seq(("a", 2)),  // 1st window from here
    Seq(("a", 3)),
    Seq(("a", 4)),  // 2nd window from here
    Seq(("a", 5)),
    Seq(("a", 6)),  // 3rd window from here
    Seq(),
    Seq()           // 4th window from here
  )

  val largerSlideReduceOutput = Seq(
    Seq(("a", 3)),
    Seq(("a", 10)),
    Seq(("a", 18)),
    Seq(("a", 11))
  )


  val bigInput = Seq(
    Seq(("a", 1)),
    Seq(("a", 1), ("b", 1)),
    Seq(("a", 1), ("b", 1), ("c", 1)),
    Seq(("a", 1), ("b", 1)),
    Seq(("a", 1)),
    Seq(),
    Seq(("a", 1)),
    Seq(("a", 1), ("b", 1)),
    Seq(("a", 1), ("b", 1), ("c", 1)),
    Seq(("a", 1), ("b", 1)),
    Seq(("a", 1)),
    Seq()
  )

  val bigGroupByOutput = Seq(
    Seq(("a", Seq(1))),
    Seq(("a", Seq(1, 1)), ("b", Seq(1))),
    Seq(("a", Seq(1, 1)), ("b", Seq(1, 1)), ("c", Seq(1))),
    Seq(("a", Seq(1, 1)), ("b", Seq(1, 1)), ("c", Seq(1))),
    Seq(("a", Seq(1, 1)), ("b", Seq(1))),
    Seq(("a", Seq(1))),
    Seq(("a", Seq(1))),
    Seq(("a", Seq(1, 1)), ("b", Seq(1))),
    Seq(("a", Seq(1, 1)), ("b", Seq(1, 1)), ("c", Seq(1))),
    Seq(("a", Seq(1, 1)), ("b", Seq(1, 1)), ("c", Seq(1))),
    Seq(("a", Seq(1, 1)), ("b", Seq(1))),
    Seq(("a", Seq(1)))
  )


  val bigReduceOutput = Seq(
    Seq(("a", 1)),
    Seq(("a", 2), ("b", 1)),
    Seq(("a", 2), ("b", 2), ("c", 1)),
    Seq(("a", 2), ("b", 2), ("c", 1)),
    Seq(("a", 2), ("b", 1)),
    Seq(("a", 1)),
    Seq(("a", 1)),
    Seq(("a", 2), ("b", 1)),
    Seq(("a", 2), ("b", 2), ("c", 1)),
    Seq(("a", 2), ("b", 2), ("c", 1)),
    Seq(("a", 2), ("b", 1)),
    Seq(("a", 1))
  )

  /*
  The output of the reduceByKeyAndWindow with inverse reduce function is
  different from the naive reduceByKeyAndWindow. Even if the count of a
  particular key is 0, the key does not get eliminated from the RDDs of
  ReducedWindowedDStream. This causes the number of keys in these RDDs to
  increase forever. A more generalized version that allows elimination of
  keys should be considered.
  */

  val bigReduceInvOutput = Seq(
    Seq(("a", 1)),
    Seq(("a", 2), ("b", 1)),
    Seq(("a", 2), ("b", 2), ("c", 1)),
    Seq(("a", 2), ("b", 2), ("c", 1)),
    Seq(("a", 2), ("b", 1), ("c", 0)),
    Seq(("a", 1), ("b", 0), ("c", 0)),
    Seq(("a", 1), ("b", 0), ("c", 0)),
    Seq(("a", 2), ("b", 1), ("c", 0)),
    Seq(("a", 2), ("b", 2), ("c", 1)),
    Seq(("a", 2), ("b", 2), ("c", 1)),
    Seq(("a", 2), ("b", 1), ("c", 0)),
    Seq(("a", 1), ("b", 0), ("c", 0))
  )

  // Testing window operation

  testWindow(
    "basic window",
    Seq( Seq(0), Seq(1), Seq(2), Seq(3), Seq(4), Seq(5)),
    Seq( Seq(0), Seq(0, 1), Seq(1, 2), Seq(2, 3), Seq(3, 4), Seq(4, 5))
  )

  testWindow(
    "tumbling window",
    Seq( Seq(0), Seq(1), Seq(2), Seq(3), Seq(4), Seq(5)),
    Seq( Seq(0, 1), Seq(2, 3), Seq(4, 5)),
    Seconds(2),
    Seconds(2)
  )

  testWindow(
    "larger window",
    Seq( Seq(0), Seq(1), Seq(2), Seq(3), Seq(4), Seq(5)),
    Seq( Seq(0, 1), Seq(0, 1, 2, 3), Seq(2, 3, 4, 5), Seq(4, 5)),
    Seconds(4),
    Seconds(2)
  )

  testWindow(
    "non-overlapping window",
    Seq( Seq(0), Seq(1), Seq(2), Seq(3), Seq(4), Seq(5)),
    Seq( Seq(1, 2), Seq(4, 5)),
    Seconds(2),
    Seconds(3)
  )

  // Testing naive reduceByKeyAndWindow (without invertible function)

  testReduceByKeyAndWindow(
    "basic reduction",
    Seq( Seq(("a", 1), ("a", 3)) ),
    Seq( Seq(("a", 4)) )
  )

  testReduceByKeyAndWindow(
    "key already in window and new value added into window",
    Seq( Seq(("a", 1)), Seq(("a", 1)) ),
    Seq( Seq(("a", 1)), Seq(("a", 2)) )
  )

  testReduceByKeyAndWindow(
    "new key added into window",
    Seq( Seq(("a", 1)), Seq(("a", 1), ("b", 1)) ),
    Seq( Seq(("a", 1)), Seq(("a", 2), ("b", 1)) )
  )

  testReduceByKeyAndWindow(
    "key removed from window",
    Seq( Seq(("a", 1)), Seq(("a", 1)), Seq(), Seq() ),
    Seq( Seq(("a", 1)), Seq(("a", 2)), Seq(("a", 1)), Seq() )
  )

  testReduceByKeyAndWindow(
    "larger slide time",
    largerSlideInput,
    largerSlideReduceOutput,
    Seconds(4),
    Seconds(2)
  )

  testReduceByKeyAndWindow("big test", bigInput, bigReduceOutput)

  // Testing reduceByKeyAndWindow (with invertible reduce function)

  testReduceByKeyAndWindowInv(
    "basic reduction",
    Seq(Seq(("a", 1), ("a", 3)) ),
    Seq(Seq(("a", 4)) )
  )

  testReduceByKeyAndWindowInv(
    "key already in window and new value added into window",
    Seq( Seq(("a", 1)), Seq(("a", 1)) ),
    Seq( Seq(("a", 1)), Seq(("a", 2)) )
  )

  testReduceByKeyAndWindowInv(
    "new key added into window",
    Seq( Seq(("a", 1)), Seq(("a", 1), ("b", 1)) ),
    Seq( Seq(("a", 1)), Seq(("a", 2), ("b", 1)) )
  )

  testReduceByKeyAndWindowInv(
    "key removed from window",
    Seq( Seq(("a", 1)), Seq(("a", 1)), Seq(), Seq() ),
    Seq( Seq(("a", 1)), Seq(("a", 2)), Seq(("a", 1)), Seq(("a", 0)) )
  )

  testReduceByKeyAndWindowInv(
    "larger slide time",
    largerSlideInput,
    largerSlideReduceOutput,
    Seconds(4),
    Seconds(2)
  )

  testReduceByKeyAndWindowInv("big test", bigInput, bigReduceInvOutput)

  test("groupByKeyAndWindow") {
    val input = bigInput
    val expectedOutput = bigGroupByOutput.map(_.map(x => (x._1, x._2.toSet)))
    val windowTime = Seconds(2)
    val slideTime = Seconds(1)
    val numBatches = expectedOutput.size * (slideTime.millis / batchDuration.millis).toInt
    val operation = (s: DStream[(String, Int)]) => {
      s.groupByKeyAndWindow(windowTime, slideTime)
       .map(x => (x._1, x._2.toSet))
       .persist()
    }
    testOperation(input, operation, expectedOutput, numBatches, true)
  }

  test("countByWindow") {
    val input = Seq(Seq(1), Seq(1), Seq(1, 2), Seq(0), Seq(), Seq() )
    val expectedOutput = Seq( Seq(1), Seq(2), Seq(3), Seq(3), Seq(1), Seq(0))
    val windowTime = Seconds(2)
    val slideTime = Seconds(1)
    val numBatches = expectedOutput.size * (slideTime.millis / batchDuration.millis).toInt
    val operation = (s: DStream[Int]) => s.countByWindow(windowTime, slideTime)
    testOperation(input, operation, expectedOutput, numBatches, true)
  }

  test("countByKeyAndWindow") {
    val input = Seq(Seq(("a", 1)), Seq(("b", 1), ("b", 2)), Seq(("a", 10), ("b", 20)))
    val expectedOutput = Seq( Seq(("a", 1)), Seq(("a", 1), ("b", 2)), Seq(("a", 1), ("b", 3)))
    val windowTime = Seconds(2)
    val slideTime = Seconds(1)
    val numBatches = expectedOutput.size * (slideTime.millis / batchDuration.millis).toInt
    val operation = (s: DStream[(String, Int)]) => {
      s.countByKeyAndWindow(windowTime, slideTime).map(x => (x._1, x._2.toInt))
    }
    testOperation(input, operation, expectedOutput, numBatches, true)
  }


  // Helper functions

  def testWindow(
    name: String,
    input: Seq[Seq[Int]],
    expectedOutput: Seq[Seq[Int]],
    windowTime: Time = Seconds(2),
    slideTime: Time = Seconds(1)
    ) {
    test("window - " + name) {
      val numBatches = expectedOutput.size * (slideTime.millis / batchDuration.millis).toInt
      val operation = (s: DStream[Int]) => s.window(windowTime, slideTime)
      testOperation(input, operation, expectedOutput, numBatches, true)
    }
  }

  def testReduceByKeyAndWindow(
    name: String,
    input: Seq[Seq[(String, Int)]],
    expectedOutput: Seq[Seq[(String, Int)]],
    windowTime: Time = Seconds(2),
    slideTime: Time = Seconds(1)
    ) {
    test("reduceByKeyAndWindow - " + name) {
      val numBatches = expectedOutput.size * (slideTime.millis / batchDuration.millis).toInt
      val operation = (s: DStream[(String, Int)]) => {
        s.reduceByKeyAndWindow(_ + _, windowTime, slideTime).persist()
      }
      testOperation(input, operation, expectedOutput, numBatches, true)
    }
  }

  def testReduceByKeyAndWindowInv(
    name: String,
    input: Seq[Seq[(String, Int)]],
    expectedOutput: Seq[Seq[(String, Int)]],
    windowTime: Time = Seconds(2),
    slideTime: Time = Seconds(1)
  ) {
    test("reduceByKeyAndWindowInv - " + name) {
      val numBatches = expectedOutput.size * (slideTime.millis / batchDuration.millis).toInt
      val operation = (s: DStream[(String, Int)]) => {
        s.reduceByKeyAndWindow(_ + _, _ - _, windowTime, slideTime).persist()
      }
      testOperation(input, operation, expectedOutput, numBatches, true)
    }
  }
}
