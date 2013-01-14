package spark.streaming

import spark.streaming.StreamingContext._
import spark.streaming.dstream.{ReducedWindowedDStream, StateDStream}
import spark.streaming.dstream.{CoGroupedDStream, ShuffledDStream}
import spark.streaming.dstream.{MapValuedDStream, FlatMapValuedDStream}

import spark.{Manifests, RDD, Partitioner, HashPartitioner}
import spark.SparkContext._
import spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.mapred.{JobConf, OutputFormat}
import org.apache.hadoop.mapreduce.{OutputFormat => NewOutputFormat}
import org.apache.hadoop.mapred.OutputFormat
import org.apache.hadoop.conf.Configuration

class PairDStreamFunctions[K: ClassManifest, V: ClassManifest](self: DStream[(K,V)])
extends Serializable {
 
  def ssc = self.ssc

  private[streaming] def defaultPartitioner(numPartitions: Int = self.ssc.sc.defaultParallelism) = {
    new HashPartitioner(numPartitions)
  }

  /**
   * Creates a new DStream by applying `groupByKey` on each RDD of `this` DStream.
   * Therefore, the values for each key in `this` DStream's RDDs are grouped into a
   * single sequence to generate the RDDs of the new DStream. Hash partitioning is
   * used to generate the RDDs with Spark's default number of partitions.
   */
  def groupByKey(): DStream[(K, Seq[V])] = {
    groupByKey(defaultPartitioner())
  }

  /**
   * Creates a new DStream by applying `groupByKey` on each RDD of `this` DStream.
   * Therefore, the values for each key in `this` DStream's RDDs are grouped into a
   * single sequence to generate the RDDs of the new DStream. Hash partitioning is
   * used to generate the RDDs with `numPartitions` partitions.
   */
  def groupByKey(numPartitions: Int): DStream[(K, Seq[V])] = {
    groupByKey(defaultPartitioner(numPartitions))
  }

  /**
   * Creates a new DStream by applying `groupByKey` on each RDD of `this` DStream.
   * Therefore, the values for each key in `this` DStream's RDDs are grouped into a
   * single sequence to generate the RDDs of the new DStream. [[spark.Partitioner]]
   * is used to control the partitioning of each RDD.
   */
  def groupByKey(partitioner: Partitioner): DStream[(K, Seq[V])] = {
    val createCombiner = (v: V) => ArrayBuffer[V](v)
    val mergeValue = (c: ArrayBuffer[V], v: V) => (c += v)
    val mergeCombiner = (c1: ArrayBuffer[V], c2: ArrayBuffer[V]) => (c1 ++ c2)
    combineByKey(createCombiner, mergeValue, mergeCombiner, partitioner)
      .asInstanceOf[DStream[(K, Seq[V])]]
  }

  /**
   * Creates a new DStream by applying `reduceByKey` on each RDD of `this` DStream.
   * Therefore, the values for each key in `this` DStream's RDDs is merged using the
   * associative reduce function to generate the RDDs of the new DStream.
   * Hash partitioning is used to generate the RDDs with Spark's default number of partitions.
   */
  def reduceByKey(reduceFunc: (V, V) => V): DStream[(K, V)] = {
    reduceByKey(reduceFunc, defaultPartitioner())
  }

  /**
   * Creates a new DStream by applying `reduceByKey` on each RDD of `this` DStream.
   * Therefore, the values for each key in `this` DStream's RDDs is merged using the
   * associative reduce function to generate the RDDs of the new DStream.
   * Hash partitioning is used to generate the RDDs with `numPartitions` partitions.
   */
  def reduceByKey(reduceFunc: (V, V) => V, numPartitions: Int): DStream[(K, V)] = {
    reduceByKey(reduceFunc, defaultPartitioner(numPartitions))
  }

  /**
   * Creates a new DStream by applying `reduceByKey` on each RDD of `this` DStream.
   * Therefore, the values for each key in `this` DStream's RDDs is merged using the
   * associative reduce function to generate the RDDs of the new DStream.
   * [[spark.Partitioner]] is used to control the partitioning of each RDD.
   */
  def reduceByKey(reduceFunc: (V, V) => V, partitioner: Partitioner): DStream[(K, V)] = {
    val cleanedReduceFunc = ssc.sc.clean(reduceFunc)
    combineByKey((v: V) => v, cleanedReduceFunc, cleanedReduceFunc, partitioner)
  }

  /**
   * Generic function to combine elements of each key in DStream's RDDs using custom function.
   * This is similar to the combineByKey for RDDs. Please refer to combineByKey in
   * [[spark.PairRDDFunctions]] for more information.
   */
  def combineByKey[C: ClassManifest](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiner: (C, C) => C,
    partitioner: Partitioner) : DStream[(K, C)] = {
    new ShuffledDStream[K, V, C](self, createCombiner, mergeValue, mergeCombiner, partitioner)
  }

  /**
   * Creates a new DStream by counting the number of values of each key in each RDD
   * of `this` DStream. Hash partitioning is used to generate the RDDs with Spark's
   * `numPartitions` partitions.
   */
  def countByKey(numPartitions: Int = self.ssc.sc.defaultParallelism): DStream[(K, Long)] = {
    self.map(x => (x._1, 1L)).reduceByKey((x: Long, y: Long) => x + y, numPartitions)
  }

  /**
   * Creates a new DStream by applying `groupByKey` over a sliding window on `this` DStream.
   * This is similar to `DStream.groupByKey()` but applies it over a sliding window.
   * The new DStream generates RDDs with the same interval as this DStream.
   * Hash partitioning is used to generate the RDDs with Spark's default number of partitions.
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   */
  def groupByKeyAndWindow(windowDuration: Duration): DStream[(K, Seq[V])] = {
    groupByKeyAndWindow(windowDuration, self.slideDuration, defaultPartitioner())
  }

  /**
   * Creates a new DStream by applying `groupByKey` over a sliding window on `this` DStream.
   * This is similar to `DStream.groupByKey()` but applies it over a sliding window.
   * Hash partitioning is used to generate the RDDs with Spark's default number of partitions.
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   */
  def groupByKeyAndWindow(windowDuration: Duration, slideDuration: Duration): DStream[(K, Seq[V])] = {
    groupByKeyAndWindow(windowDuration, slideDuration, defaultPartitioner())
  }

  /**
   * Creates a new DStream by applying `groupByKey` over a sliding window on `this` DStream.
   * This is similar to `DStream.groupByKey()` but applies it over a sliding window.
   * Hash partitioning is used to generate the RDDs with `numPartitions` partitions.
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   * @param numPartitions  Number of partitions of each RDD in the new DStream.
   */
  def groupByKeyAndWindow(
      windowDuration: Duration,
      slideDuration: Duration,
      numPartitions: Int
    ): DStream[(K, Seq[V])] = {
    groupByKeyAndWindow(windowDuration, slideDuration, defaultPartitioner(numPartitions))
  }

  /**
   * Creates a new DStream by applying `groupByKey` over a sliding window on `this` DStream.
   * This is similar to `DStream.groupByKey()` but applies it over a sliding window.
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   * @param partitioner Partitioner for controlling the partitioning of each RDD in the new DStream.
   */
  def groupByKeyAndWindow(
      windowDuration: Duration,
      slideDuration: Duration,
      partitioner: Partitioner
    ): DStream[(K, Seq[V])] = {
    self.window(windowDuration, slideDuration).groupByKey(partitioner)
  }

  /**
   * Creates a new DStream by applying `reduceByKey` over a sliding window on `this` DStream.
   * This is similar to `DStream.reduceByKey()` but applies it over a sliding window.
   * The new DStream generates RDDs with the same interval as this DStream.
   * Hash partitioning is used to generate the RDDs with Spark's default number of partitions.
   * @param reduceFunc associative reduce function
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   */
  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      windowDuration: Duration
    ): DStream[(K, V)] = {
    reduceByKeyAndWindow(reduceFunc, windowDuration, self.slideDuration, defaultPartitioner())
  }

  /**
   * Creates a new DStream by applying `reduceByKey` over a sliding window on `this` DStream.
   * This is similar to `DStream.reduceByKey()` but applies it over a sliding window.
   * Hash partitioning is used to generate the RDDs with Spark's default number of partitions.
   * @param reduceFunc associative reduce function
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   */
  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V, 
      windowDuration: Duration,
      slideDuration: Duration
    ): DStream[(K, V)] = {
    reduceByKeyAndWindow(reduceFunc, windowDuration, slideDuration, defaultPartitioner())
  }

  /**
   * Creates a new DStream by applying `reduceByKey` over a sliding window on `this` DStream.
   * This is similar to `DStream.reduceByKey()` but applies it over a sliding window.
   * Hash partitioning is used to generate the RDDs with `numPartitions` partitions.
   * @param reduceFunc associative reduce function
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   * @param numPartitions  Number of partitions of each RDD in the new DStream.
   */
  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V, 
      windowDuration: Duration,
      slideDuration: Duration,
      numPartitions: Int
    ): DStream[(K, V)] = {
    reduceByKeyAndWindow(reduceFunc, windowDuration, slideDuration, defaultPartitioner(numPartitions))
  }

  /**
   * Creates a new DStream by applying `reduceByKey` over a sliding window on `this` DStream.
   * This is similar to `DStream.reduceByKey()` but applies it over a sliding window.
   * @param reduceFunc associative reduce function
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   * @param partitioner Partitioner for controlling the partitioning of each RDD in the new DStream.
   */
  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      windowDuration: Duration,
      slideDuration: Duration,
      partitioner: Partitioner
    ): DStream[(K, V)] = {
    val cleanedReduceFunc = ssc.sc.clean(reduceFunc)
    self.reduceByKey(cleanedReduceFunc, partitioner)
        .window(windowDuration, slideDuration)
        .reduceByKey(cleanedReduceFunc, partitioner)
  }

  /**
   * Creates a new DStream by reducing over a window in a smarter way.
   * The reduced value of over a new window is calculated incrementally by using the
   * old window's reduce value :
   *  1. reduce the new values that entered the window (e.g., adding new counts)
   *  2. "inverse reduce" the old values that left the window (e.g., subtracting old counts)
   * This is more efficient that reduceByKeyAndWindow without "inverse reduce" function.
   * However, it is applicable to only "invertible reduce functions".
   * Hash partitioning is used to generate the RDDs with Spark's default number of partitions.
   * @param reduceFunc associative reduce function
   * @param invReduceFunc inverse function
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   */
  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      invReduceFunc: (V, V) => V,
      windowDuration: Duration,
      slideDuration: Duration
    ): DStream[(K, V)] = {

    reduceByKeyAndWindow(
      reduceFunc, invReduceFunc, windowDuration, slideDuration, defaultPartitioner())
  }

  /**
   * Creates a new DStream by reducing over a window in a smarter way.
   * The reduced value of over a new window is calculated incrementally by using the
   * old window's reduce value :
   *  1. reduce the new values that entered the window (e.g., adding new counts)
   *  2. "inverse reduce" the old values that left the window (e.g., subtracting old counts)
   * This is more efficient that reduceByKeyAndWindow without "inverse reduce" function.
   * However, it is applicable to only "invertible reduce functions".
   * Hash partitioning is used to generate the RDDs with `numPartitions` partitions.
   * @param reduceFunc associative reduce function
   * @param invReduceFunc inverse function
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   * @param numPartitions  Number of partitions of each RDD in the new DStream.
   */
  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      invReduceFunc: (V, V) => V,
      windowDuration: Duration,
      slideDuration: Duration,
      numPartitions: Int
    ): DStream[(K, V)] = {

    reduceByKeyAndWindow(
      reduceFunc, invReduceFunc, windowDuration, slideDuration, defaultPartitioner(numPartitions))
  }

  /**
   * Creates a new DStream by reducing over a window in a smarter way.
   * The reduced value of over a new window is calculated incrementally by using the
   * old window's reduce value :
   *  1. reduce the new values that entered the window (e.g., adding new counts)
   *  2. "inverse reduce" the old values that left the window (e.g., subtracting old counts)
   * This is more efficient that reduceByKeyAndWindow without "inverse reduce" function.
   * However, it is applicable to only "invertible reduce functions".
   * @param reduceFunc associative reduce function
   * @param invReduceFunc inverse function
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   * @param partitioner Partitioner for controlling the partitioning of each RDD in the new DStream.
   */
  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      invReduceFunc: (V, V) => V,
      windowDuration: Duration,
      slideDuration: Duration,
      partitioner: Partitioner
    ): DStream[(K, V)] = {

    val cleanedReduceFunc = ssc.sc.clean(reduceFunc)
    val cleanedInvReduceFunc = ssc.sc.clean(invReduceFunc)
    new ReducedWindowedDStream[K, V](
      self, cleanedReduceFunc, cleanedInvReduceFunc, windowDuration, slideDuration, partitioner)
  }

  /**
   * Creates a new DStream by counting the number of values for each key over a window.
   * Hash partitioning is used to generate the RDDs with `numPartitions` partitions.
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   * @param numPartitions  Number of partitions of each RDD in the new DStream.
   */
  def countByKeyAndWindow(
      windowDuration: Duration,
      slideDuration: Duration,
      numPartitions: Int = self.ssc.sc.defaultParallelism
    ): DStream[(K, Long)] = {

    self.map(x => (x._1, 1L)).reduceByKeyAndWindow(
      (x: Long, y: Long) => x + y,
      (x: Long, y: Long) => x - y,
      windowDuration,
      slideDuration,
      numPartitions
    )
  }

  /**
   * Creates a new "state" DStream where the state for each key is updated by applying
   * the given function on the previous state of the key and the new values of the key from
   * `this` DStream. Hash partitioning is used to generate the RDDs with Spark's default
   * number of partitions.
   * @param updateFunc State update function. If `this` function returns None, then
   *                   corresponding state key-value pair will be eliminated.
   * @tparam S State type
   */
  def updateStateByKey[S <: AnyRef : ClassManifest](
      updateFunc: (Seq[V], Option[S]) => Option[S]
    ): DStream[(K, S)] = {
    updateStateByKey(updateFunc, defaultPartitioner())
  }

  /**
   * Creates a new "state" DStream where the state for each key is updated by applying
   * the given function on the previous state of the key and the new values of the key from
   * `this` DStream. Hash partitioning is used to generate the RDDs with `numPartitions` partitions.
   * @param updateFunc State update function. If `this` function returns None, then
   *                   corresponding state key-value pair will be eliminated.
   * @param numPartitions Number of partitions of each RDD in the new DStream.
   * @tparam S State type
   */
  def updateStateByKey[S <: AnyRef : ClassManifest](
      updateFunc: (Seq[V], Option[S]) => Option[S],
      numPartitions: Int
    ): DStream[(K, S)] = {
    updateStateByKey(updateFunc, defaultPartitioner(numPartitions))
  }

  /**
   * Creates a new "state" DStream where the state for each key is updated by applying
   * the given function on the previous state of the key and the new values of the key from
   * `this` DStream. [[spark.Partitioner]] is used to control the partitioning of each RDD.
   * @param updateFunc State update function. If `this` function returns None, then
   *                   corresponding state key-value pair will be eliminated.
   * @param partitioner Partitioner for controlling the partitioning of each RDD in the new DStream.
   * @tparam S State type
   */
  def updateStateByKey[S <: AnyRef : ClassManifest](
      updateFunc: (Seq[V], Option[S]) => Option[S],
      partitioner: Partitioner
    ): DStream[(K, S)] = {
    val newUpdateFunc = (iterator: Iterator[(K, Seq[V], Option[S])]) => {
      iterator.flatMap(t => updateFunc(t._2, t._3).map(s => (t._1, s)))
    }
    updateStateByKey(newUpdateFunc, partitioner, true)
  }

  /**
   * Creates a new "state" DStream where the state for each key is updated by applying
   * the given function on the previous state of the key and the new values of the key from
   * `this` DStream. [[spark.Partitioner]] is used to control the partitioning of each RDD.
   * @param updateFunc State update function. If `this` function returns None, then
   *                   corresponding state key-value pair will be eliminated. Note, that
   *                   this function may generate a different a tuple with a different key
   *                   than the input key. It is up to the developer to decide whether to
   *                   remember the partitioner despite the key being changed.
   * @param partitioner Partitioner for controlling the partitioning of each RDD in the new DStream.
   * @param rememberPartitioner Whether to remember the paritioner object in the generated RDDs.
   * @tparam S State type
   */
  def updateStateByKey[S <: AnyRef : ClassManifest](
      updateFunc: (Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)],
      partitioner: Partitioner,
      rememberPartitioner: Boolean
    ): DStream[(K, S)] = {
     new StateDStream(self, ssc.sc.clean(updateFunc), partitioner, rememberPartitioner)
  }


  def mapValues[U: ClassManifest](mapValuesFunc: V => U): DStream[(K, U)] = {
    new MapValuedDStream[K, V, U](self, mapValuesFunc)
  }

  def flatMapValues[U: ClassManifest](
      flatMapValuesFunc: V => TraversableOnce[U]
    ): DStream[(K, U)] = {
    new FlatMapValuedDStream[K, V, U](self, flatMapValuesFunc)
  }

  /**
   * Cogroups `this` DStream with `other` DStream. Each RDD of the new DStream will
   * be generated by cogrouping RDDs from`this`and `other` DStreams. Therefore, for
   * each key k in corresponding RDDs of `this` or `other` DStreams, the generated RDD
   * will contains a tuple with the list of values for that key in both RDDs.
   * HashPartitioner is used to partition each generated RDD into default number of partitions.
   */
  def cogroup[W: ClassManifest](other: DStream[(K, W)]): DStream[(K, (Seq[V], Seq[W]))] = {
    cogroup(other, defaultPartitioner())
  }

  /**
   * Cogroups `this` DStream with `other` DStream. Each RDD of the new DStream will
   * be generated by cogrouping RDDs from`this`and `other` DStreams. Therefore, for
   * each key k in corresponding RDDs of `this` or `other` DStreams, the generated RDD
   * will contains a tuple with the list of values for that key in both RDDs.
   * Partitioner is used to partition each generated RDD.
   */
  def cogroup[W: ClassManifest](
      other: DStream[(K, W)],
      partitioner: Partitioner
    ): DStream[(K, (Seq[V], Seq[W]))] = {

    val cgd = new CoGroupedDStream[K](
      Seq(self.asInstanceOf[DStream[(_, _)]], other.asInstanceOf[DStream[(_, _)]]),
      partitioner
    )
    val pdfs = new PairDStreamFunctions[K, Seq[Seq[_]]](cgd)(
      classManifest[K],
      Manifests.seqSeqManifest
    )
    pdfs.mapValues {
      case Seq(vs, ws) =>
        (vs.asInstanceOf[Seq[V]], ws.asInstanceOf[Seq[W]])
    }
  }

  /**
   * Joins `this` DStream with `other` DStream. Each RDD of the new DStream will
   * be generated by joining RDDs from `this` and `other` DStreams. HashPartitioner is used
   * to partition each generated RDD into default number of partitions.
   */
  def join[W: ClassManifest](other: DStream[(K, W)]): DStream[(K, (V, W))] = {
    join[W](other, defaultPartitioner())
  }

  /**
   * Joins `this` DStream with `other` DStream, that is, each RDD of the new DStream will
   * be generated by joining RDDs from `this` and other DStream. Uses the given
   * Partitioner to partition each generated RDD.
   */
  def join[W: ClassManifest](
      other: DStream[(K, W)],
      partitioner: Partitioner
    ): DStream[(K, (V, W))] = {
    this.cogroup(other, partitioner)
        .flatMapValues{
      case (vs, ws) =>
        for (v <- vs.iterator; w <- ws.iterator) yield (v, w)
    }
  }

  /**
   * Saves each RDD in `this` DStream as a Hadoop file. The file name at each batch interval is generated
   * based on `prefix` and `suffix`: "prefix-TIME_IN_MS.suffix"
   */
  def saveAsHadoopFiles[F <: OutputFormat[K, V]](
      prefix: String,
      suffix: String
    )(implicit fm: ClassManifest[F]) {
    saveAsHadoopFiles(prefix, suffix, getKeyClass, getValueClass, fm.erasure.asInstanceOf[Class[F]])
  }

  /**
   * Saves each RDD in `this` DStream as a Hadoop file. The file name at each batch interval is generated
   * based on `prefix` and `suffix`: "prefix-TIME_IN_MS.suffix"
   */
  def saveAsHadoopFiles(
      prefix: String,
      suffix: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: OutputFormat[_, _]],
      conf: JobConf = new JobConf
    ) {
    val saveFunc = (rdd: RDD[(K, V)], time: Time) => {
      val file = rddToFileName(prefix, suffix, time)
      rdd.saveAsHadoopFile(file, keyClass, valueClass, outputFormatClass, conf)
    }
    self.foreach(saveFunc)
  }

  /**
   * Saves each RDD in `this` DStream as a Hadoop file. The file name at each batch interval is generated
   * based on `prefix` and `suffix`: "prefix-TIME_IN_MS.suffix".
   */
  def saveAsNewAPIHadoopFiles[F <: NewOutputFormat[K, V]](
      prefix: String,
      suffix: String
    )(implicit fm: ClassManifest[F])  {
    saveAsNewAPIHadoopFiles(prefix, suffix, getKeyClass, getValueClass, fm.erasure.asInstanceOf[Class[F]])
  }

  /**
   * Saves each RDD in `this` DStream as a Hadoop file. The file name at each batch interval is generated
   * based on `prefix` and `suffix`: "prefix-TIME_IN_MS.suffix".
   */
  def saveAsNewAPIHadoopFiles(
      prefix: String,
      suffix: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
      conf: Configuration = new Configuration
    ) {
    val saveFunc = (rdd: RDD[(K, V)], time: Time) => {
      val file = rddToFileName(prefix, suffix, time)
      rdd.saveAsNewAPIHadoopFile(file, keyClass, valueClass, outputFormatClass, conf)
    }
    self.foreach(saveFunc)
  }

  private def getKeyClass() = implicitly[ClassManifest[K]].erasure

  private def getValueClass() = implicitly[ClassManifest[V]].erasure
}


