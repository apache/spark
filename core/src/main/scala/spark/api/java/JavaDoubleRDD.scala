package spark.api.java

import spark.RDD
import spark.SparkContext.doubleRDDToDoubleRDDFunctions
import spark.api.java.function.{Function => JFunction}
import spark.util.StatCounter
import spark.partial.{BoundedDouble, PartialResult}
import spark.storage.StorageLevel

import java.lang.Double

class JavaDoubleRDD(val srdd: RDD[scala.Double]) extends JavaRDDLike[Double, JavaDoubleRDD] {

  override val classManifest: ClassManifest[Double] = implicitly[ClassManifest[Double]]

  override val rdd: RDD[Double] = srdd.map(x => Double.valueOf(x))

  override def wrapRDD(rdd: RDD[Double]): JavaDoubleRDD =
    new JavaDoubleRDD(rdd.map(_.doubleValue))

  // Common RDD functions

  import JavaDoubleRDD.fromRDD

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  def cache(): JavaDoubleRDD = fromRDD(srdd.cache())

  /** 
   * Set this RDD's storage level to persist its values across operations after the first time
   * it is computed. Can only be called once on each RDD.
   */
  def persist(newLevel: StorageLevel): JavaDoubleRDD = fromRDD(srdd.persist(newLevel))

  // first() has to be overriden here in order for its return type to be Double instead of Object.
  override def first(): Double = srdd.first()

  // Transformations (return a new RDD)

  /**
   * Return a new RDD containing the distinct elements in this RDD.
   */
  def distinct(): JavaDoubleRDD = fromRDD(srdd.distinct())

  /**
   * Return a new RDD containing the distinct elements in this RDD.
   */
  def distinct(numSplits: Int): JavaDoubleRDD = fromRDD(srdd.distinct(numSplits))

  /**
   * Return a new RDD containing only the elements that satisfy a predicate.
   */
  def filter(f: JFunction[Double, java.lang.Boolean]): JavaDoubleRDD =
    fromRDD(srdd.filter(x => f(x).booleanValue()))

  /**
   * Return a sampled subset of this RDD.
   */
  def sample(withReplacement: Boolean, fraction: Double, seed: Int): JavaDoubleRDD =
    fromRDD(srdd.sample(withReplacement, fraction, seed))

  /**
   * Return the union of this RDD and another one. Any identical elements will appear multiple
   * times (use `.distinct()` to eliminate them).
   */
  def union(other: JavaDoubleRDD): JavaDoubleRDD = fromRDD(srdd.union(other.srdd))

  // Double RDD functions

  /** Return the sum of the elements in this RDD. */
  def sum(): Double = srdd.sum()

  /** Return a [[spark.StatCounter]] describing the elements in this RDD. */
  def stats(): StatCounter = srdd.stats()

  /** Return the mean of the elements in this RDD. */
  def mean(): Double = srdd.mean()

  /** Return the variance of the elements in this RDD. */
  def variance(): Double = srdd.variance()

  /** Return the standard deviation of the elements in this RDD. */
  def stdev(): Double = srdd.stdev()

  /** Return the approximate mean of the elements in this RDD. */
  def meanApprox(timeout: Long, confidence: Double): PartialResult[BoundedDouble] =
    srdd.meanApprox(timeout, confidence)

  /** Return the approximate mean of the elements in this RDD. */
  def meanApprox(timeout: Long): PartialResult[BoundedDouble] = srdd.meanApprox(timeout)

  /** Return the approximate sum of the elements in this RDD. */
  def sumApprox(timeout: Long, confidence: Double): PartialResult[BoundedDouble] =
    srdd.sumApprox(timeout, confidence)
 
  /** Return the approximate sum of the elements in this RDD. */
  def sumApprox(timeout: Long): PartialResult[BoundedDouble] = srdd.sumApprox(timeout)
}

object JavaDoubleRDD {
  def fromRDD(rdd: RDD[scala.Double]): JavaDoubleRDD = new JavaDoubleRDD(rdd)

  implicit def toRDD(rdd: JavaDoubleRDD): RDD[scala.Double] = rdd.srdd
}
