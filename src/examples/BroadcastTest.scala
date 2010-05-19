import spark.SparkContext

object BroadcastTest {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: BroadcastTest <host> [<slices>] [<array-len>]")
      System.exit(1)
    }  
    val spark = new SparkContext(args(0), "Broadcast Test")
    val slices = if (args.length > 1) args(1).toInt else 2
    val num = if (args.length > 2) args(2).toInt else 1000000

    var arr = new Array[Int](num)
    for (i <- 0 until arr.length) 
      arr(i) = i
    
    val start = System.nanoTime
    val barr = spark.broadcast(arr)
    spark.parallelize(1 to 10, slices).foreach {
      println("in task: barr = " + barr)
      i => println(barr.value.size)
    }
    val time = (System.nanoTime - start) / 1e9
    println("BroadcastTest took " + time + " s")                     
  }
}

