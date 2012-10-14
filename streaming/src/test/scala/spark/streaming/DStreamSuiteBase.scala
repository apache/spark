package spark.streaming

import spark.{RDD, Logging}
import util.ManualClock
import collection.mutable.ArrayBuffer
import org.scalatest.FunSuite
import scala.collection.mutable.Queue


trait DStreamSuiteBase extends FunSuite with Logging {

  def batchDuration() = Seconds(1)

  def maxWaitTimeMillis() = 10000

  def testOperation[U: ClassManifest, V: ClassManifest](
      input: Seq[Seq[U]],
      operation: DStream[U] => DStream[V],
      expectedOutput: Seq[Seq[V]],
      useSet: Boolean = false
    ) {

    val manualClock = true

    if (manualClock) {
      System.setProperty("spark.streaming.clock", "spark.streaming.util.ManualClock")
    }

    val ssc = new StreamingContext("local", "test")

    try {
      ssc.setBatchDuration(Milliseconds(batchDuration))

      val inputQueue = new Queue[RDD[U]]()
      inputQueue ++= input.map(ssc.sc.makeRDD(_, 2))
      val emptyRDD = ssc.sc.makeRDD(Seq[U](), 2)

      val inputStream = ssc.createQueueStream(inputQueue, true, emptyRDD)
      val outputStream = operation(inputStream)

      val output = new ArrayBuffer[Seq[V]]()
      outputStream.foreachRDD(rdd => output += rdd.collect())

      ssc.start()

      val clock = ssc.scheduler.clock
      if (clock.isInstanceOf[ManualClock]) {
        clock.asInstanceOf[ManualClock].addToTime((input.size - 1) * batchDuration.milliseconds)
      }

      val startTime = System.currentTimeMillis()
      while (output.size < expectedOutput.size && System.currentTimeMillis() - startTime < maxWaitTimeMillis) {
        println("output.size = " + output.size + ", expectedOutput.size = " + expectedOutput.size)
        Thread.sleep(500)
      }

      println("output.size = " + output.size)
      println("output")
      output.foreach(x => println("[" + x.mkString(",") + "]"))

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
}
