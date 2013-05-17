package spark.api.java

import spark._
import spark.api.java.function.{Function => JFunction}
import spark.storage.StorageLevel

class JavaRDD[T](val rdd: RDD[T])(implicit val classManifest: ClassManifest[T]) extends
JavaRDDLike[T, JavaRDD[T]] {

  override def wrapRDD(rdd: RDD[T]): JavaRDD[T] = JavaRDD.fromRDD(rdd)

  // Common RDD functions

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  def cache(): JavaRDD[T] = wrapRDD(rdd.cache())

  /**
   * Set this RDD's storage level to persist its values across operations after the first time
   * it is computed. This can only be used to assign a new storage level if the RDD does not
   * have a storage level set yet..
   */
  def persist(newLevel: StorageLevel): JavaRDD[T] = wrapRDD(rdd.persist(newLevel))

  /**
   * Mark the RDD as non-persistent, and remove all blocks for it from memory and disk.
   */
  def unpersist(): JavaRDD[T] = wrapRDD(rdd.unpersist())

  // Transformations (return a new RDD)

  /**
   * Return a new RDD containing the distinct elements in this RDD.
   */
  def distinct(): JavaRDD[T] = wrapRDD(rdd.distinct())

  /**
   * Return a new RDD containing the distinct elements in this RDD.
   */
  def distinct(numPartitions: Int): JavaRDD[T] = wrapRDD(rdd.distinct(numPartitions))

  /**
   * Return a new RDD containing only the elements that satisfy a predicate.
   */
  def filter(f: JFunction[T, java.lang.Boolean]): JavaRDD[T] =
    wrapRDD(rdd.filter((x => f(x).booleanValue())))

  /**
   * Return a new RDD that is reduced into `numPartitions` partitions.
   */
  def coalesce(numPartitions: Int): JavaRDD[T] = rdd.coalesce(numPartitions)

  /**
   * Return a new RDD that is reduced into `numPartitions` partitions.
   */
  def coalesce(numPartitions: Int, shuffle: Boolean): JavaRDD[T] =
    rdd.coalesce(numPartitions, shuffle)

  /**
   * Return a sampled subset of this RDD.
   */
  def sample(withReplacement: Boolean, fraction: Double, seed: Int): JavaRDD[T] =
    wrapRDD(rdd.sample(withReplacement, fraction, seed))

  /**
   * Return the union of this RDD and another one. Any identical elements will appear multiple
   * times (use `.distinct()` to eliminate them).
   */
  def union(other: JavaRDD[T]): JavaRDD[T] = wrapRDD(rdd.union(other.rdd))

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   *
   * Uses `this` partitioner/partition size, because even if `other` is huge, the resulting
   * RDD will be <= us.
   */
  def subtract(other: JavaRDD[T]): JavaRDD[T] = wrapRDD(rdd.subtract(other))

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   */
  def subtract(other: JavaRDD[T], numPartitions: Int): JavaRDD[T] =
    wrapRDD(rdd.subtract(other, numPartitions))

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   */
  def subtract(other: JavaRDD[T], p: Partitioner): JavaRDD[T] =
    wrapRDD(rdd.subtract(other, p))

}

object JavaRDD {

  implicit def fromRDD[T: ClassManifest](rdd: RDD[T]): JavaRDD[T] = new JavaRDD[T](rdd)

  implicit def toRDD[T](rdd: JavaRDD[T]): RDD[T] = rdd.rdd
}

