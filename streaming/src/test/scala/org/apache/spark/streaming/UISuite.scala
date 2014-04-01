package org.apache.spark.streaming

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, FunSuite}
import org.apache.spark.streaming.dstream.InputDStream
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import scala.util.Random

class UISuite extends FunSuite with BeforeAndAfterAll {

  test("Testing") {
    runStreaming(1000000)
  }

  def runStreaming(duration: Long) {
    val ssc = new StreamingContext("local[10]", "test", Seconds(1))
    val servers = (1 to 5).map { i => new TestServer(10000 + i) }

    val inputStream = ssc.union(servers.map(server => ssc.socketTextStream("localhost", server.port)))
    inputStream.count.print

    ssc.start()
    servers.foreach(_.start())
    val startTime = System.currentTimeMillis()
    while (System.currentTimeMillis() - startTime < duration) {
      servers.map(_.send(Random.nextString(10) + "\n"))
      //Thread.sleep(1)
    }
    ssc.stop()
    servers.foreach(_.stop())
  }
}

class FunctionBasedInputDStream[T: ClassTag](
    ssc_ : StreamingContext,
    function: (StreamingContext, Time) => Option[RDD[T]]
  ) extends InputDStream[T](ssc_) {

  def start(): Unit = {}

  def stop(): Unit = {}

  def compute(validTime: Time): Option[RDD[T]] = function(ssc, validTime)
}








