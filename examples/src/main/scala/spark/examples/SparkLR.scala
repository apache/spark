package spark.examples

import java.util.Random
import scala.math.exp
import spark.util.Vector
import spark._

/**
 * Logistic regression based classification.
 */
object SparkLR {
  val N = 10000  // Number of data points
  val D = 10   // Numer of dimensions
  val R = 0.7  // Scaling factor
  val ITERATIONS = 5
  val rand = new Random(42)

  case class DataPoint(x: Vector, y: Double)

  def generateData = {
    def generatePoint(i: Int) = {
      val y = if(i % 2 == 0) -1 else 1
      val x = Vector(D, _ => rand.nextGaussian + y * R)
      DataPoint(x, y)
    }
    Array.tabulate(N)(generatePoint)
  }

  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: SparkLR <master> [<slices>]")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "SparkLR",
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))
    val numSlices = if (args.length > 1) args(1).toInt else 2
    val points = sc.parallelize(generateData, numSlices).cache()

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
