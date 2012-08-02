package spark.streaming

import spark.RDD

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.SynchronizedQueue

class RDSSuite extends FunSuite with BeforeAndAfter {
  
  var ssc: SparkStreamContext = null
  val batchDurationMillis = 1000
  
  def testOp[U: ClassManifest, V: ClassManifest](
      input: Seq[Seq[U]],
      operation: RDS[U] => RDS[V],  
      expectedOutput: Seq[Seq[V]]) = {
    try {
      ssc = new SparkStreamContext("local", "test")
      ssc.setBatchDuration(Milliseconds(batchDurationMillis))
      
      val inputStream = ssc.createQueueStream(input.map(ssc.sc.makeRDD(_, 2)).toIterator)
      val outputStream = operation(inputStream)
      val outputQueue = outputStream.toQueue
      
      ssc.start()
      Thread.sleep(batchDurationMillis * input.size)
      
      val output = new ArrayBuffer[Seq[V]]()
      while(outputQueue.size > 0) {
        val rdd = outputQueue.take()
        println("Collecting RDD " + rdd.id + ", " + rdd.getClass().getSimpleName() + ", " + rdd.splits.size)
        output += (rdd.collect())
      }
      assert(output.size === expectedOutput.size)
      for (i <- 0 until output.size) {
        assert(output(i).toList === expectedOutput(i).toList)
      }
    } finally {
      ssc.stop()
    }     
  }
  
  test("basic operations") {
    val inputData = Array(1 to 4, 5 to 8, 9 to 12)    
    
    // map
    testOp(inputData, (r: RDS[Int]) => r.map(_.toString), inputData.map(_.map(_.toString)))
    
    // flatMap
    testOp(inputData, (r: RDS[Int]) => r.flatMap(x => Array(x, x * 2)),
        inputData.map(_.flatMap(x => Array(x, x * 2)))
    )
  }
}

object RDSSuite {
  def main(args: Array[String]) {
    val r = new RDSSuite()
    val inputData = Array(1 to 4, 5 to 8, 9 to 12)    
    r.testOp(inputData, (r: RDS[Int]) => r.map(_.toString), inputData.map(_.map(_.toString)))
  }
}