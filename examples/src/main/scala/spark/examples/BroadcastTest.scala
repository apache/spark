package spark.examples

import spark.SparkContext

object BroadcastTest {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: BroadcastTest <master> [<slices>] [numElem]")
      System.exit(1)
    }  
    
    val spark = new SparkContext(args(0), "Broadcast Test")
    val slices = if (args.length > 1) args(1).toInt else 2
    val num = if (args.length > 2) args(2).toInt else 1000000

    var arr1 = new Array[Int](num)
    for (i <- 0 until arr1.length) 
      arr1(i) = i
    
    for (i <- 0 until 2) {
      println("Iteration " + i)
      println("===========")
      val barr1 = spark.broadcast(arr1)
      spark.parallelize(1 to 10, slices).foreach {
        i => println(barr1.value.size)
      }
    }

    System.exit(0)
  }
}
