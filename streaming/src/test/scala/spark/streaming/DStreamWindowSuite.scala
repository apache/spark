package spark.streaming

import spark.streaming.StreamingContext._

class DStreamWindowSuite extends DStreamSuiteBase {

  def testReduceByKeyAndWindow(
    name: String,
    input: Seq[Seq[(String, Int)]],
    expectedOutput: Seq[Seq[(String, Int)]],
    windowTime: Time = Seconds(2),
    slideTime: Time = Seconds(1)
  ) {
    test("reduceByKeyAndWindow - " + name) {
      testOperation(
        input,
        (s: DStream[(String, Int)]) => s.reduceByKeyAndWindow(_ + _, _ - _, windowTime, slideTime).persist(),
        expectedOutput,
        true
      )
    }
  }

  testReduceByKeyAndWindow(
    "basic reduction",
    Seq(Seq(("a", 1), ("a", 3)) ),
    Seq(Seq(("a", 4)) )
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
    Seq( Seq(("a", 1)), Seq(("a", 2)), Seq(("a", 1)), Seq(("a", 0)) )
  )

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

  testReduceByKeyAndWindow(
    "larger slide time",
    largerSlideInput,
    largerSlideOutput,
    Seconds(4),
    Seconds(2)
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
    Seq(("a", 2), ("b", 1), ("c", 0)),
    Seq(("a", 1), ("b", 0), ("c", 0)),
    Seq(("a", 1), ("b", 0), ("c", 0)),
    Seq(("a", 2), ("b", 1), ("c", 0)),
    Seq(("a", 2), ("b", 2), ("c", 1)),
    Seq(("a", 2), ("b", 2), ("c", 1)),
    Seq(("a", 2), ("b", 1), ("c", 0)),
    Seq(("a", 1), ("b", 0), ("c", 0))
  )

  testReduceByKeyAndWindow("big test", bigInput, bigOutput)
}
