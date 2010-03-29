import spark._
import SparkContext._

object SparkPi {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: SparkLR <host> [<slices>]")
      System.exit(1)
    }
    val spark = new SparkContext(args(0), "SparkPi")
    val slices = if (args.length > 1) args(1).toInt else 2
    var count = spark.accumulator(0)
    for (i <- spark.parallelize(1 to 100000, slices)) {
      val x = Math.random * 2 - 1
      val y = Math.random * 2 - 1
      if (x*x + y*y < 1) count += 1
    }
    println("Pi is roughly " + 4 * count.value / 100000.0)
  }
}