package spark.streaming.api.java

import java.util.{List => JList}
import java.lang.{Long => JLong}

import scala.collection.JavaConversions._

import spark.streaming._
import spark.streaming.StreamingContext._
import spark.api.java.function.{Function => JFunction, Function2 => JFunction2}
import spark.Partitioner
import org.apache.hadoop.mapred.{JobConf, OutputFormat}
import org.apache.hadoop.mapreduce.{OutputFormat => NewOutputFormat}
import org.apache.hadoop.conf.Configuration
import spark.api.java.JavaPairRDD
import spark.storage.StorageLevel
import java.lang

class JavaPairDStream[K, V](val dstream: DStream[(K, V)])(
    implicit val kManifiest: ClassManifest[K],
    implicit val vManifest: ClassManifest[V])
    extends JavaDStreamLike[(K, V), JavaPairDStream[K, V]] {

  // =======================================================================
  // Methods common to all DStream's
  // =======================================================================

  /** Returns a new DStream containing only the elements that satisfy a predicate. */
  def filter(f: JFunction[(K, V), java.lang.Boolean]): JavaPairDStream[K, V] =
    dstream.filter((x => f(x).booleanValue()))

  /** Persists RDDs of this DStream with the default storage level (MEMORY_ONLY_SER) */
  def cache(): JavaPairDStream[K, V] = dstream.cache()

  /** Persists RDDs of this DStream with the default storage level (MEMORY_ONLY_SER) */
  def persist(): JavaPairDStream[K, V] = dstream.cache()

  /** Persists the RDDs of this DStream with the given storage level */
  def persist(storageLevel: StorageLevel): JavaPairDStream[K, V] = dstream.persist(storageLevel)

  /** Method that generates a RDD for the given Duration */
  def compute(validTime: Time): JavaPairRDD[K, V] = {
    dstream.compute(validTime) match {
      case Some(rdd) => new JavaPairRDD(rdd)
      case None => null
    }
  }

  /**
   * Return a new DStream which is computed based on windowed batches of this DStream.
   * The new DStream generates RDDs with the same interval as this DStream.
   * @param windowDuration width of the window; must be a multiple of this DStream's interval.
   * @return
   */
  def window(windowDuration: Duration): JavaPairDStream[K, V] =
    dstream.window(windowDuration)

  /**
   * Return a new DStream which is computed based on windowed batches of this DStream.
   * @param windowDuration duration (i.e., width) of the window;
   *                   must be a multiple of this DStream's interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                   the new DStream will generate RDDs); must be a multiple of this
   *                   DStream's interval
   */
  def window(windowDuration: Duration, slideDuration: Duration): JavaPairDStream[K, V] =
    dstream.window(windowDuration, slideDuration)

  /**
   * Returns a new DStream which computed based on tumbling window on this DStream.
   * This is equivalent to window(batchDuration, batchDuration).
   * @param batchDuration tumbling window duration; must be a multiple of this DStream's interval
   */
  def tumble(batchDuration: Duration): JavaPairDStream[K, V] =
    dstream.tumble(batchDuration)

  /**
   * Returns a new DStream by unifying data of another DStream with this DStream.
   * @param that Another DStream having the same interval (i.e., slideDuration) as this DStream.
   */
  def union(that: JavaPairDStream[K, V]): JavaPairDStream[K, V] =
    dstream.union(that.dstream)

  // =======================================================================
  // Methods only for PairDStream's
  // =======================================================================

  def groupByKey(): JavaPairDStream[K, JList[V]] =
    dstream.groupByKey().mapValues(seqAsJavaList _)

  def groupByKey(numPartitions: Int): JavaPairDStream[K, JList[V]] =
    dstream.groupByKey(numPartitions).mapValues(seqAsJavaList _)

  def groupByKey(partitioner: Partitioner): JavaPairDStream[K, JList[V]] =
    dstream.groupByKey(partitioner).mapValues(seqAsJavaList _)

  def reduceByKey(func: JFunction2[V, V, V]): JavaPairDStream[K, V] =
    dstream.reduceByKey(func)

  def reduceByKey(func: JFunction2[V, V, V], numPartitions: Int): JavaPairDStream[K, V] =
    dstream.reduceByKey(func, numPartitions)

  def combineByKey[C](createCombiner: JFunction[V, C],
    mergeValue: JFunction2[C, V, C],
    mergeCombiners: JFunction2[C, C, C],
    partitioner: Partitioner): JavaPairDStream[K, C] = {
    implicit val cm: ClassManifest[C] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[C]]
    dstream.combineByKey(createCombiner, mergeValue, mergeCombiners, partitioner)
  }

  def countByKey(numPartitions: Int): JavaPairDStream[K, JLong] = {
    JavaPairDStream.scalaToJavaLong(dstream.countByKey(numPartitions));
  }

  def countByKey(): JavaPairDStream[K, JLong] = {
    JavaPairDStream.scalaToJavaLong(dstream.countByKey());
  }

  def groupByKeyAndWindow(windowDuration: Duration, slideDuration: Duration): JavaPairDStream[K, JList[V]] = {
    dstream.groupByKeyAndWindow(windowDuration, slideDuration).mapValues(seqAsJavaList _)
  }

  def groupByKeyAndWindow(windowDuration: Duration, slideDuration: Duration, numPartitions: Int)
  :JavaPairDStream[K, JList[V]] = {
    dstream.groupByKeyAndWindow(windowDuration, slideDuration, numPartitions).mapValues(seqAsJavaList _)
  }

  def groupByKeyAndWindow(windowDuration: Duration, slideDuration: Duration, partitioner: Partitioner)
  :JavaPairDStream[K, JList[V]] = {
    dstream.groupByKeyAndWindow(windowDuration, slideDuration, partitioner).mapValues(seqAsJavaList _)
  }

  def reduceByKeyAndWindow(reduceFunc: Function2[V, V, V], windowDuration: Duration)
  :JavaPairDStream[K, V] = {
    dstream.reduceByKeyAndWindow(reduceFunc, windowDuration)
  }

  def reduceByKeyAndWindow(reduceFunc: Function2[V, V, V], windowDuration: Duration, slideDuration: Duration)
  :JavaPairDStream[K, V] = {
    dstream.reduceByKeyAndWindow(reduceFunc, windowDuration, slideDuration)
  }

  def reduceByKeyAndWindow(
    reduceFunc: Function2[V, V, V],
    windowDuration: Duration,
    slideDuration: Duration,
    numPartitions: Int): JavaPairDStream[K, V] = {
    dstream.reduceByKeyAndWindow(reduceFunc, windowDuration, slideDuration, numPartitions)
  }

  def reduceByKeyAndWindow(reduceFunc: Function2[V, V, V], windowDuration: Duration, slideDuration: Duration,
      partitioner: Partitioner): JavaPairDStream[K, V] = {
    dstream.reduceByKeyAndWindow(reduceFunc, windowDuration, slideDuration, partitioner)
  }

  def reduceByKeyAndWindow(reduceFunc: Function2[V, V, V], invReduceFunc: Function2[V, V, V],
      windowDuration: Duration, slideDuration: Duration): JavaPairDStream[K, V] = {
    dstream.reduceByKeyAndWindow(reduceFunc, invReduceFunc, windowDuration, slideDuration)
  }

  def reduceByKeyAndWindow(reduceFunc: Function2[V, V, V], invReduceFunc: Function2[V, V, V],
      windowDuration: Duration, slideDuration: Duration, numPartitions: Int): JavaPairDStream[K, V] = {
    dstream.reduceByKeyAndWindow(reduceFunc, invReduceFunc, windowDuration, slideDuration, numPartitions)
  }

  def reduceByKeyAndWindow(reduceFunc: Function2[V, V, V], invReduceFunc: Function2[V, V, V],
      windowDuration: Duration, slideDuration: Duration, partitioner: Partitioner)
      : JavaPairDStream[K, V] = {
    dstream.reduceByKeyAndWindow(reduceFunc, invReduceFunc, windowDuration, slideDuration, partitioner)
  }

  def countByKeyAndWindow(windowDuration: Duration, slideDuration: Duration): JavaPairDStream[K, JLong] = {
    JavaPairDStream.scalaToJavaLong(dstream.countByKeyAndWindow(windowDuration, slideDuration))
  }

  def countByKeyAndWindow(windowDuration: Duration, slideDuration: Duration, numPartitions: Int)
      : JavaPairDStream[K, Long] = {
    dstream.countByKeyAndWindow(windowDuration, slideDuration, numPartitions)
  }

  def mapValues[U](f: JFunction[V, U]): JavaPairDStream[K, U] = {
    implicit val cm: ClassManifest[U] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[U]]
    dstream.mapValues(f)
  }

  def flatMapValues[U](f: JFunction[V, java.lang.Iterable[U]]): JavaPairDStream[K, U] = {
    import scala.collection.JavaConverters._
    def fn = (x: V) => f.apply(x).asScala
    implicit val cm: ClassManifest[U] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[U]]
    dstream.flatMapValues(fn)
  }

  def cogroup[W](other: JavaPairDStream[K, W]): JavaPairDStream[K, (JList[V], JList[W])] = {
    implicit val cm: ClassManifest[W] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[W]]
    dstream.cogroup(other.dstream).mapValues(t => (seqAsJavaList(t._1), seqAsJavaList((t._2))))
  }

  def cogroup[W](other: JavaPairDStream[K, W], partitioner: Partitioner)
  : JavaPairDStream[K, (JList[V], JList[W])] = {
    implicit val cm: ClassManifest[W] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[W]]
    dstream.cogroup(other.dstream, partitioner)
        .mapValues(t => (seqAsJavaList(t._1), seqAsJavaList((t._2))))
  }

  def join[W](other: JavaPairDStream[K, W]): JavaPairDStream[K, (V, W)] = {
    implicit val cm: ClassManifest[W] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[W]]
    dstream.join(other.dstream)
  }

  def join[W](other: JavaPairDStream[K, W], partitioner: Partitioner)
  : JavaPairDStream[K, (V, W)] = {
    implicit val cm: ClassManifest[W] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[W]]
    dstream.join(other.dstream, partitioner)
  }

  def saveAsHadoopFiles[F <: OutputFormat[K, V]](prefix: String, suffix: String) {
    dstream.saveAsHadoopFiles(prefix, suffix)
  }

  def saveAsHadoopFiles(
    prefix: String,
    suffix: String,
    keyClass: Class[_],
    valueClass: Class[_],
    outputFormatClass: Class[_ <: OutputFormat[_, _]]) {
    dstream.saveAsHadoopFiles(prefix, suffix, keyClass, valueClass, outputFormatClass)
  }

  def saveAsHadoopFiles(
    prefix: String,
    suffix: String,
    keyClass: Class[_],
    valueClass: Class[_],
    outputFormatClass: Class[_ <: OutputFormat[_, _]],
    conf: JobConf) {
    dstream.saveAsHadoopFiles(prefix, suffix, keyClass, valueClass, outputFormatClass, conf)
  }

  def saveAsNewAPIHadoopFiles[F <: NewOutputFormat[K, V]](prefix: String, suffix: String) {
    dstream.saveAsNewAPIHadoopFiles(prefix, suffix)
  }

  def saveAsNewAPIHadoopFiles(
    prefix: String,
    suffix: String,
    keyClass: Class[_],
    valueClass: Class[_],
    outputFormatClass: Class[_ <: NewOutputFormat[_, _]]) {
    dstream.saveAsNewAPIHadoopFiles(prefix, suffix, keyClass, valueClass, outputFormatClass)
  }

  def saveAsNewAPIHadoopFiles(
    prefix: String,
    suffix: String,
    keyClass: Class[_],
    valueClass: Class[_],
    outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
    conf: Configuration = new Configuration) {
    dstream.saveAsNewAPIHadoopFiles(prefix, suffix, keyClass, valueClass, outputFormatClass, conf)
  }

  override val classManifest: ClassManifest[(K, V)] =
    implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[Tuple2[K, V]]]
}

object JavaPairDStream {
  implicit def fromPairDStream[K: ClassManifest, V: ClassManifest](dstream: DStream[(K, V)])
  :JavaPairDStream[K, V] =
    new JavaPairDStream[K, V](dstream)

  def fromJavaDStream[K, V](dstream: JavaDStream[(K, V)]): JavaPairDStream[K, V] = {
    implicit val cmk: ClassManifest[K] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[K]]
    implicit val cmv: ClassManifest[V] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[V]]
    new JavaPairDStream[K, V](dstream.dstream)
  }

  def scalaToJavaLong[K: ClassManifest](dstream: JavaPairDStream[K, Long]): JavaPairDStream[K, JLong] = {
    StreamingContext.toPairDStreamFunctions(dstream.dstream).mapValues(new JLong(_))
  }
}
