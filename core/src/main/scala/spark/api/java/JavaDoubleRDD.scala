package spark.api.java

import spark.RDD
import spark.SparkContext.doubleRDDToDoubleRDDFunctions
import spark.api.java.function.{Function => JFunction}

import java.lang.Double

class JavaDoubleRDD(val srdd: RDD[scala.Double]) extends JavaRDDLike[Double, JavaDoubleRDD] {

  val classManifest = implicitly[ClassManifest[Double]]

  lazy val rdd = srdd.map(x => Double.valueOf(x))

  def wrapRDD: (RDD[Double]) => JavaDoubleRDD = rdd => new JavaDoubleRDD(rdd.map(_.doubleValue))

  // Common RDD functions

  import JavaDoubleRDD.fromRDD

  def cache(): JavaDoubleRDD = fromRDD(srdd.cache())

  // first() has to be overriden here in order for its return type to be Double instead of Object.
  override def first(): Double = srdd.first()

  // Transformations (return a new RDD)

  def distinct() = fromRDD(srdd.distinct())

  def filter(f: JFunction[Double, java.lang.Boolean]) =
    fromRDD(srdd.filter(x => f(x).booleanValue()))

  def sample(withReplacement: Boolean, fraction: Double, seed: Int) =
    fromRDD(srdd.sample(withReplacement, fraction, seed))

  def union(other: JavaDoubleRDD) = fromRDD(srdd.union(other.srdd))

  // Double RDD functions

  def sum() = srdd.sum()

  def stats() = srdd.stats()

  def mean() = srdd.mean()

  def variance() = srdd.variance()

  def stdev() = srdd.stdev()

  def meanApprox(timeout: Long, confidence: Double) = srdd.meanApprox(timeout, confidence)

  def meanApprox(timeout: Long) = srdd.meanApprox(timeout)

  def sumApprox(timeout: Long, confidence: Double) = srdd.sumApprox(timeout, confidence)

  def sumApprox(timeout: Long) = srdd.sumApprox(timeout)
}

object JavaDoubleRDD {
  def fromRDD(rdd: RDD[scala.Double]): JavaDoubleRDD = new JavaDoubleRDD(rdd)

  implicit def toRDD(rdd: JavaDoubleRDD): RDD[scala.Double] = rdd.srdd
}
