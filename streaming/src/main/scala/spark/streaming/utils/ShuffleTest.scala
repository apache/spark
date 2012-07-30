package spark.streaming
import spark.SparkContext
import SparkContext._

object ShuffleTest {
  def main(args: Array[String]) {

    if (args.length < 1) {
      println ("Usage: ShuffleTest <host>")
      System.exit(1)
    }

    val sc = new spark.SparkContext(args(0), "ShuffleTest")
    val rdd = sc.parallelize(1 to 1000, 500).cache

    def time(f: => Unit) { val start = System.nanoTime; f; println((System.nanoTime - start) * 1.0e-6) }

    time { for (i <- 0 until 50) time { rdd.map(x => (x % 100, x)).reduceByKey(_ + _, 10).count } }
    System.exit(0)
  }
}

