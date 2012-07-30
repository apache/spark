package spark.examples

import java.util.Random
import spark.util.Vector

object LocalFileLR {
  val D = 10   // Numer of dimensions
  val rand = new Random(42)

  case class DataPoint(x: Vector, y: Double)

  def parsePoint(line: String): DataPoint = {
    val nums = line.split(' ').map(_.toDouble)
    return DataPoint(new Vector(nums.slice(1, D+1)), nums(0))
  }

  def main(args: Array[String]) {
    val lines = scala.io.Source.fromFile(args(0)).getLines().toArray
    val points = lines.map(parsePoint _)
    val ITERATIONS = args(1).toInt

    // Initialize w to a random value
    var w = Vector(D, _ => 2 * rand.nextDouble - 1)
    println("Initial w: " + w)

    for (i <- 1 to ITERATIONS) {
      println("On iteration " + i)
      var gradient = Vector.zeros(D)
      for (p <- points) {
        val scale = (1 / (1 + math.exp(-p.y * (w dot p.x))) - 1) * p.y
        gradient +=  scale * p.x
      }
      w -= gradient
    }

    println("Final w: " + w)
  }
}
