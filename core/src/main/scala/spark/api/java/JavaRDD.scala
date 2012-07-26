package spark.api.java

import spark._
import spark.api.java.function.{Function => JFunction}
import spark.storage.StorageLevel

class JavaRDD[T](val rdd: RDD[T])(implicit val classManifest: ClassManifest[T]) extends
JavaRDDLike[T, JavaRDD[T]] {

  override def wrapRDD(rdd: RDD[T]): JavaRDD[T] = JavaRDD.fromRDD(rdd)

  // Common RDD functions

  def cache(): JavaRDD[T] = wrapRDD(rdd.cache())

  def persist(newLevel: StorageLevel): JavaRDD[T] = wrapRDD(rdd.persist(newLevel))

  // Transformations (return a new RDD)

  def distinct(): JavaRDD[T] = wrapRDD(rdd.distinct())

  def filter(f: JFunction[T, java.lang.Boolean]): JavaRDD[T] =
    wrapRDD(rdd.filter((x => f(x).booleanValue())))

  def sample(withReplacement: Boolean, fraction: Double, seed: Int): JavaRDD[T] =
    wrapRDD(rdd.sample(withReplacement, fraction, seed))

  def union(other: JavaRDD[T]): JavaRDD[T] = wrapRDD(rdd.union(other.rdd))

}

object JavaRDD {

  implicit def fromRDD[T: ClassManifest](rdd: RDD[T]): JavaRDD[T] = new JavaRDD[T](rdd)

  implicit def toRDD[T](rdd: JavaRDD[T]): RDD[T] = rdd.rdd
}

