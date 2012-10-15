package spark.streaming

import spark.streaming.StreamingContext._

class DStreamWindowSuite extends DStreamSuiteBase {

  override def framework() = "DStreamWindowSuite"

  override def maxWaitTimeMillis() = 20000

  val largerSlideInput = Seq(
    Seq(("a", 1)),  // 1st window from here
    Seq(("a", 2)),
    Seq(("a", 3)),  // 2nd window from here
    Seq(("a", 4)),
    Seq(("a", 5)),  // 3rd window from here
    Seq(("a", 6)),
    Seq(),          // 4th window from here
    Seq(),
    Seq()           // 5th window from here
  )

  val largerSlideOutput = Seq(
    Seq(("a", 1)),
    Seq(("a", 6)),
    Seq(("a", 14)),
    Seq(("a", 15)),
    Seq(("a", 6))
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

  val bigOutput = Seq(
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
  difference from the naive reduceByKeyAndWindow. Even if the count of a
  particular key is 0, the key does not get eliminated from the RDDs of
  ReducedWindowedDStream. This causes the number of keys in these RDDs to
  increase forever. A more generalized version that allows elimination of
  keys should be considered.
  */
  val bigOutputInv = Seq(
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

  def testReduceByKeyAndWindow(
    name: String,
    input: Seq[Seq[(String, Int)]],
    expectedOutput: Seq[Seq[(String, Int)]],
    windowTime: Time = batchDuration * 2,
    slideTime: Time = batchDuration
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
    windowTime: Time = batchDuration * 2,
    slideTime: Time = batchDuration
  ) {
    test("reduceByKeyAndWindowInv - " + name) {
      val numBatches = expectedOutput.size * (slideTime.millis / batchDuration.millis).toInt
      val operation = (s: DStream[(String, Int)]) => {
        s.reduceByKeyAndWindow(_ + _, _ - _, windowTime, slideTime).persist()
      }
      testOperation(input, operation, expectedOutput, numBatches, true)
    }
  }


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
    largerSlideOutput,
    Seconds(4),
    Seconds(2)
  )

  testReduceByKeyAndWindow("big test", bigInput, bigOutput)


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
    largerSlideOutput,
    Seconds(4),
    Seconds(2)
  )

  testReduceByKeyAndWindowInv("big test", bigInput, bigOutputInv)
}
