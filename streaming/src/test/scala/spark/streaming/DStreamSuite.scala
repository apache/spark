package spark.streaming

import spark.Logging
import spark.streaming.StreamingContext._
import spark.streaming.util.ManualClock

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import scala.collection.mutable.ArrayBuffer
import scala.runtime.RichInt

class DStreamSuite extends FunSuite with BeforeAndAfter with Logging {
  
  var ssc: StreamingContext = null
  val batchDurationMillis = 1000
  
  System.setProperty("spark.streaming.clock", "spark.streaming.util.ManualClock")
  
  def testOp[U: ClassManifest, V: ClassManifest](
      input: Seq[Seq[U]],
      operation: DStream[U] => DStream[V],
      expectedOutput: Seq[Seq[V]],
      useSet: Boolean = false
    ) {
    try {      
      ssc = new StreamingContext("local", "test")
      ssc.setBatchDuration(Milliseconds(batchDurationMillis))
      
      val inputStream = ssc.createQueueStream(input.map(ssc.sc.makeRDD(_, 2)).toIterator)
      val outputStream = operation(inputStream)
      val outputQueue = outputStream.toQueue
      
      ssc.start()
      val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
      clock.addToTime(input.size * batchDurationMillis)
      
      Thread.sleep(1000)
      
      val output = new ArrayBuffer[Seq[V]]()
      while(outputQueue.size > 0) {
        val rdd = outputQueue.take()        
        output += (rdd.collect())
      }

      assert(output.size === expectedOutput.size)
      for (i <- 0 until output.size) {
        if (useSet) {
          assert(output(i).toSet === expectedOutput(i).toSet)
        } else {
          assert(output(i).toList === expectedOutput(i).toList)
        }
      }
    } finally {
      ssc.stop()
    }     
  }

  test("map-like operations") {
    val inputData = Seq(1 to 4, 5 to 8, 9 to 12)
    
    // map
    testOp(inputData, (r: DStream[Int]) => r.map(_.toString), inputData.map(_.map(_.toString)))
    
    // flatMap
    testOp(
      inputData,
      (r: DStream[Int]) => r.flatMap(x => Seq(x, x * 2)),
      inputData.map(_.flatMap(x => Array(x, x * 2)))
    )
  }

  test("shuffle-based operations") {
    // reduceByKey
    testOp(
      Seq(Seq("a", "a", "b"), Seq("", ""), Seq()),
      (s: DStream[String]) => s.map(x => (x, 1)).reduceByKey(_ + _),
      Seq(Seq(("a", 2), ("b", 1)), Seq(("", 2)), Seq()),
      true
    )

    // reduce
    testOp(
      Seq(1 to 4, 5 to 8, 9 to 12),
      (s: DStream[Int]) => s.reduce(_ + _),
      Seq(Seq(10), Seq(26), Seq(42))
    )
  }

  test("window-based operations") {

  }


  test("stateful operations") {
    val inputData =
      Seq(
        Seq("a", "b", "c"),
        Seq("a", "b", "c"),
        Seq("a", "b", "c")
      )

    val outputData =
      Seq(
        Seq(("a", 1), ("b", 1), ("c", 1)),
        Seq(("a", 2), ("b", 2), ("c", 2)),
        Seq(("a", 3), ("b", 3), ("c", 3))
      )

    val updateStateOp = (s: DStream[String]) => {
      val updateFunc = (values: Seq[Int], state: RichInt) => {
        var newState = 0
        if (values != null) newState += values.reduce(_ + _)
        if (state != null) newState += state.self
        println("values = " + values + ", state = " + state + ", " + " new state = " + newState)
        new RichInt(newState)
      }
      s.map(x => (x, 1)).updateStateByKey[RichInt](updateFunc).map(t => (t._1, t._2.self))
    }

    testOp(inputData, updateStateOp, outputData, true)
  }
}
