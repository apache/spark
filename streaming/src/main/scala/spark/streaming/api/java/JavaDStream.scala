package spark.streaming.api.java

import spark.streaming.{Time, DStream}
import spark.api.java.function.{Function => JFunction}
import spark.api.java.JavaRDD
import java.util.{List => JList}
import spark.storage.StorageLevel

class JavaDStream[T](val dstream: DStream[T])(implicit val classManifest: ClassManifest[T])
    extends JavaDStreamLike[T, JavaDStream[T]] {

  /** Returns a new DStream containing only the elements that satisfy a predicate. */
  def filter(f: JFunction[T, java.lang.Boolean]): JavaDStream[T] =
    dstream.filter((x => f(x).booleanValue()))

  /** Persists RDDs of this DStream with the default storage level (MEMORY_ONLY_SER) */
  def cache(): JavaDStream[T] = dstream.cache()

  /** Persists RDDs of this DStream with the default storage level (MEMORY_ONLY_SER) */
  def persist(): JavaDStream[T] = dstream.cache()

  /** Persists the RDDs of this DStream with the given storage level */
  def persist(storageLevel: StorageLevel): JavaDStream[T] = dstream.persist(storageLevel)

  /** Method that generates a RDD for the given time */
  def compute(validTime: Time): JavaRDD[T] = {
    dstream.compute(validTime) match {
      case Some(rdd) => new JavaRDD(rdd)
      case None => null
    }
  }

  /**
   * Return a new DStream which is computed based on windowed batches of this DStream.
   * The new DStream generates RDDs with the same interval as this DStream.
   * @param windowTime width of the window; must be a multiple of this DStream's interval.
   * @return
   */
  def window(windowTime: Time): JavaDStream[T] =
    dstream.window(windowTime)

  /**
   * Return a new DStream which is computed based on windowed batches of this DStream.
   * @param windowTime duration (i.e., width) of the window;
   *                   must be a multiple of this DStream's interval
   * @param slideTime  sliding interval of the window (i.e., the interval after which
   *                   the new DStream will generate RDDs); must be a multiple of this
   *                   DStream's interval
   */
  def window(windowTime: Time, slideTime: Time): JavaDStream[T] =
    dstream.window(windowTime, slideTime)

  /**
   * Returns a new DStream which computed based on tumbling window on this DStream.
   * This is equivalent to window(batchTime, batchTime).
   * @param batchTime tumbling window duration; must be a multiple of this DStream's interval
   */
  def tumble(batchTime: Time): JavaDStream[T] =
    dstream.tumble(batchTime)

  /**
   * Returns a new DStream by unifying data of another DStream with this DStream.
   * @param that Another DStream having the same interval (i.e., slideTime) as this DStream.
   */
  def union(that: JavaDStream[T]): JavaDStream[T] =
    dstream.union(that.dstream)
}

object JavaDStream {
  implicit def fromDStream[T: ClassManifest](dstream: DStream[T]): JavaDStream[T] =
    new JavaDStream[T](dstream)
}
