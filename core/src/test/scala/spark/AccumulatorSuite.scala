package spark

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import collection.mutable
import java.util.Random
import scala.math.exp
import scala.math.signum
import spark.SparkContext._

class AccumulatorSuite extends FunSuite with ShouldMatchers {

  test ("basic accumulation"){
    val sc = new SparkContext("local", "test")
    val acc : Accumulator[Int] = sc.accumulator(0)

    val d = sc.parallelize(1 to 20)
    d.foreach{x => acc += x}
    acc.value should be (210)
    sc.stop()
  }

  test ("value not assignable from tasks") {
    val sc = new SparkContext("local", "test")
    val acc : Accumulator[Int] = sc.accumulator(0)

    val d = sc.parallelize(1 to 20)
    evaluating {d.foreach{x => acc.value = x}} should produce [Exception]
    sc.stop()
  }

  test ("add value to collection accumulators") {
    import SetAccum._
    val maxI = 1000
    for (nThreads <- List(1, 10)) { //test single & multi-threaded
      val sc = new SparkContext("local[" + nThreads + "]", "test")
      val acc: Accumulable[mutable.Set[Any], Any] = sc.accumulable(new mutable.HashSet[Any]())
      val d = sc.parallelize(1 to maxI)
      d.foreach {
        x => acc += x
      }
      val v = acc.value.asInstanceOf[mutable.Set[Int]]
      for (i <- 1 to maxI) {
        v should contain(i)
      }
      sc.stop()
    }
  }


  implicit object SetAccum extends AccumulableParam[mutable.Set[Any], Any] {
    def addInPlace(t1: mutable.Set[Any], t2: mutable.Set[Any]) : mutable.Set[Any] = {
      t1 ++= t2
      t1
    }
    def addToAccum(t1: mutable.Set[Any], t2: Any) : mutable.Set[Any] = {
      t1 += t2
      t1
    }
    def zero(t: mutable.Set[Any]) : mutable.Set[Any] = {
      new mutable.HashSet[Any]()
    }
  }


  test ("value readable in tasks") {
    import Vector.VectorAccumParam._
    import Vector._
    //stochastic gradient descent with weights stored in accumulator -- should be able to read value as we go

    //really easy data
    val N = 10000  // Number of data points
    val D = 10   // Numer of dimensions
    val R = 0.7  // Scaling factor
    val ITERATIONS = 5
    val rand = new Random(42)

    case class DataPoint(x: Vector, y: Double)

    def generateData = {
      def generatePoint(i: Int) = {
        val y = if(i % 2 == 0) -1 else 1
        val goodX = Vector(D, _ => 0.0001 * rand.nextGaussian() + y)
        val noiseX = Vector(D, _ => rand.nextGaussian())
        val x = Vector((goodX.elements.toSeq ++ noiseX.elements.toSeq): _*)
        DataPoint(x, y)
      }
      Array.tabulate(N)(generatePoint)
    }

    val data = generateData
    for (nThreads <- List(1, 10)) {
      //test single & multi-threaded
      val sc = new SparkContext("local[" + nThreads + "]", "test")
      val weights = Vector.zeros(2*D)
      val weightDelta = sc.accumulator(Vector.zeros(2 * D))
      for (itr <- 1 to ITERATIONS) {
        val eta = 0.1 / itr
        val badErrs = sc.accumulator(0)
        sc.parallelize(data).foreach {
          p => {
            //XXX Note the call to .value here.  That is required for this to be an online gradient descent
            // instead of a batch version.  Should it change to .localValue, and should .value throw an error
            // if you try to do this??
            val prod = weightDelta.value.plusDot(weights, p.x)
            val trueClassProb = (1 / (1 + exp(-p.y * prod)))  // works b/c p(-z) = 1 - p(z) (where p is the logistic function)
            val update = p.x * trueClassProb * p.y * eta
            //we could also include a momentum term here if our weightDelta accumulator saved a momentum
            weightDelta.value += update
            if (trueClassProb <= 0.95)
              badErrs += 1
          }
        }
        println("Iteration " + itr + " had badErrs = " + badErrs.value)
        weights += weightDelta.value
        println(weights)
        //TODO I should check the number of bad errors here, but for some reason spark tries to serialize the assertion ...
//        val assertVal = badErrs.value
//        assert (assertVal < 100)
      }
    }
  }

}



//ugly copy and paste from examples ...
class Vector(val elements: Array[Double]) extends Serializable {
  def length = elements.length

  def apply(index: Int) = elements(index)

  def + (other: Vector): Vector = {
    if (length != other.length)
      throw new IllegalArgumentException("Vectors of different length")
    return Vector(length, i => this(i) + other(i))
  }

  def - (other: Vector): Vector = {
    if (length != other.length)
      throw new IllegalArgumentException("Vectors of different length")
    return Vector(length, i => this(i) - other(i))
  }

  def dot(other: Vector): Double = {
    if (length != other.length)
      throw new IllegalArgumentException("Vectors of different length")
    var ans = 0.0
    var i = 0
    while (i < length) {
      ans += this(i) * other(i)
      i += 1
    }
    return ans
  }

  def plusDot(plus: Vector, other: Vector): Double = {
    if (length != other.length)
      throw new IllegalArgumentException("Vectors of different length")
    if (length != plus.length)
      throw new IllegalArgumentException("Vectors of different length")
    var ans = 0.0
    var i = 0
    while (i < length) {
      ans += (this(i) + plus(i)) * other(i)
      i += 1
    }
    return ans
  }

  def += (other: Vector) {
    if (length != other.length)
      throw new IllegalArgumentException("Vectors of different length")
    var ans = 0.0
    var i = 0
    while (i < length) {
      elements(i) += other(i)
      i += 1
    }
  }


  def * (scale: Double): Vector = Vector(length, i => this(i) * scale)

  def / (d: Double): Vector = this * (1 / d)

  def unary_- = this * -1

  def sum = elements.reduceLeft(_ + _)

  def squaredDist(other: Vector): Double = {
    var ans = 0.0
    var i = 0
    while (i < length) {
      ans += (this(i) - other(i)) * (this(i) - other(i))
      i += 1
    }
    return ans
  }

  def dist(other: Vector): Double = math.sqrt(squaredDist(other))

  override def toString = elements.mkString("(", ", ", ")")
}

object Vector {
  def apply(elements: Array[Double]) = new Vector(elements)

  def apply(elements: Double*) = new Vector(elements.toArray)

  def apply(length: Int, initializer: Int => Double): Vector = {
    val elements = new Array[Double](length)
    for (i <- 0 until length)
      elements(i) = initializer(i)
    return new Vector(elements)
  }

  def zeros(length: Int) = new Vector(new Array[Double](length))

  def ones(length: Int) = Vector(length, _ => 1)

  class Multiplier(num: Double) {
    def * (vec: Vector) = vec * num
  }

  implicit def doubleToMultiplier(num: Double) = new Multiplier(num)

  implicit object VectorAccumParam extends spark.AccumulatorParam[Vector] {
    def addInPlace(t1: Vector, t2: Vector) = t1 + t2
    def zero(initialValue: Vector) = Vector.zeros(initialValue.length)
  }
}
