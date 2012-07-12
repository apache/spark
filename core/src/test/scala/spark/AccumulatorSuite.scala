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
    import spark.util.Vector
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