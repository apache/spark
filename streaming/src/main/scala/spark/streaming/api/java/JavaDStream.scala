package spark.streaming.api.java

import spark.streaming.{Duration, Time, DStream}
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

  /** Method that generates a RDD for the given duration */
  def compute(validTime: Time): JavaRDD[T] = {
    dstream.compute(validTime) match {
      case Some(rdd) => new JavaRDD(rdd)
      case None => null
    }
  }

  /**
   * Return a new DStream which is computed based on windowed batches of this DStream.
   * The new DStream generates RDDs with the same interval as this DStream.
   * @param windowDuration width of the window; must be a multiple of this DStream's interval.
   * @return
   */
  def window(windowDuration: Duration): JavaDStream[T] =
    dstream.window(windowDuration)

  /**
   * Return a new DStream which is computed based on windowed batches of this DStream.
   * @param windowDuration duration (i.e., width) of the window;
   *                   must be a multiple of this DStream's interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                   the new DStream will generate RDDs); must be a multiple of this
   *                   DStream's interval
   */
  def window(windowDuration: Duration, slideDuration: Duration): JavaDStream[T] =
    dstream.window(windowDuration, slideDuration)

  /**
   * Returns a new DStream which computed based on tumbling window on this DStream.
   * This is equivalent to window(batchDuration, batchDuration).
   * @param batchDuration tumbling window duration; must be a multiple of this DStream's interval
   */
  def tumble(batchDuration: Duration): JavaDStream[T] =
    dstream.tumble(batchDuration)

  /**
   * Returns a new DStream by unifying data of another DStream with this DStream.
   * @param that Another DStream having the same interval (i.e., slideDuration) as this DStream.
   */
  def union(that: JavaDStream[T]): JavaDStream[T] =
    dstream.union(that.dstream)
}

object JavaDStream {
  implicit def fromDStream[T: ClassManifest](dstream: DStream[T]): JavaDStream[T] =
    new JavaDStream[T](dstream)
}
