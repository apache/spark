package spark.examples

import java.util.Random
import scala.math.exp
import spark.util.Vector
import spark._

/**
 * Logistic regression based classification.
 */
object SparkHdfsLR {
  val D = 10   // Numer of dimensions
  val rand = new Random(42)

  case class DataPoint(x: Vector, y: Double)

  def parsePoint(line: String): DataPoint = {
    //val nums = line.split(' ').map(_.toDouble)
    //return DataPoint(new Vector(nums.slice(1, D+1)), nums(0))
    val tok = new java.util.StringTokenizer(line, " ")
    var y = tok.nextToken.toDouble
    var x = new Array[Double](D)
    var i = 0
    while (i < D) {
      x(i) = tok.nextToken.toDouble; i += 1
    }
    return DataPoint(new Vector(x), y)
  }

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: SparkHdfsLR <master> <file> <iters>")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "SparkHdfsLR",
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))
    val lines = sc.textFile(args(1))
    val points = lines.map(parsePoint _).cache()
    val ITERATIONS = args(2).toInt

    // Initialize w to a random value
    var w = Vector(D, _ => 2 * rand.nextDouble - 1)
    println("Initial w: " + w)

    for (i <- 1 to ITERATIONS) {
      println("On iteration " + i)
      val gradient = points.map { p =>
        (1 / (1 + exp(-p.y * (w dot p.x))) - 1) * p.y * p.x
      }.reduce(_ + _)
      w -= gradient
    }

    println("Final w: " + w)
    System.exit(0)
  }
}
