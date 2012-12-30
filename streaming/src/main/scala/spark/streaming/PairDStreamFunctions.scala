package spark.streaming

import spark.streaming.StreamingContext._

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

  def defaultPartitioner(numPartitions: Int = self.ssc.sc.defaultParallelism) = {
    new HashPartitioner(numPartitions)
  }

  /* ---------------------------------- */
  /* DStream operations for key-value pairs */
  /* ---------------------------------- */

  def groupByKey(): DStream[(K, Seq[V])] = {
    groupByKey(defaultPartitioner())
  }

  def groupByKey(numPartitions: Int): DStream[(K, Seq[V])] = {
    groupByKey(defaultPartitioner(numPartitions))
  }

  def groupByKey(partitioner: Partitioner): DStream[(K, Seq[V])] = {
    val createCombiner = (v: V) => ArrayBuffer[V](v)
    val mergeValue = (c: ArrayBuffer[V], v: V) => (c += v)
    val mergeCombiner = (c1: ArrayBuffer[V], c2: ArrayBuffer[V]) => (c1 ++ c2)
    combineByKey(createCombiner, mergeValue, mergeCombiner, partitioner).asInstanceOf[DStream[(K, Seq[V])]]
  }

  def reduceByKey(reduceFunc: (V, V) => V): DStream[(K, V)] = {
    reduceByKey(reduceFunc, defaultPartitioner())
  }

  def reduceByKey(reduceFunc: (V, V) => V, numPartitions: Int): DStream[(K, V)] = {
    reduceByKey(reduceFunc, defaultPartitioner(numPartitions))
  }

  def reduceByKey(reduceFunc: (V, V) => V, partitioner: Partitioner): DStream[(K, V)] = {
    val cleanedReduceFunc = ssc.sc.clean(reduceFunc)
    combineByKey((v: V) => v, cleanedReduceFunc, cleanedReduceFunc, partitioner)
  }

  def combineByKey[C: ClassManifest](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiner: (C, C) => C,
    partitioner: Partitioner) : DStream[(K, C)] = {
    new ShuffledDStream[K, V, C](self, createCombiner, mergeValue, mergeCombiner, partitioner)
  }

  def countByKey(numPartitions: Int = self.ssc.sc.defaultParallelism): DStream[(K, Long)] = {
    self.map(x => (x._1, 1L)).reduceByKey((x: Long, y: Long) => x + y, numPartitions)
  }

  def groupByKeyAndWindow(windowTime: Time, slideTime: Time): DStream[(K, Seq[V])] = {
    groupByKeyAndWindow(windowTime, slideTime, defaultPartitioner())
  }

  def groupByKeyAndWindow(
      windowTime: Time, 
      slideTime: Time, 
      numPartitions: Int
    ): DStream[(K, Seq[V])] = {
    groupByKeyAndWindow(windowTime, slideTime, defaultPartitioner(numPartitions))
  }

  def groupByKeyAndWindow(
      windowTime: Time,
      slideTime: Time,
      partitioner: Partitioner
    ): DStream[(K, Seq[V])] = {
    self.window(windowTime, slideTime).groupByKey(partitioner)
  }

  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      windowTime: Time
    ): DStream[(K, V)] = {
    reduceByKeyAndWindow(reduceFunc, windowTime, self.slideTime, defaultPartitioner())
  }

  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V, 
      windowTime: Time, 
      slideTime: Time
    ): DStream[(K, V)] = {
    reduceByKeyAndWindow(reduceFunc, windowTime, slideTime, defaultPartitioner())
  }

  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V, 
      windowTime: Time, 
      slideTime: Time, 
      numPartitions: Int
    ): DStream[(K, V)] = {
    reduceByKeyAndWindow(reduceFunc, windowTime, slideTime, defaultPartitioner(numPartitions))
  }

  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      windowTime: Time,
      slideTime: Time,
      partitioner: Partitioner
    ): DStream[(K, V)] = {
    val cleanedReduceFunc = ssc.sc.clean(reduceFunc)
    self.reduceByKey(cleanedReduceFunc, partitioner)
        .window(windowTime, slideTime)
        .reduceByKey(cleanedReduceFunc, partitioner)
  }

  // This method is the efficient sliding window reduce operation,
  // which requires the specification of an inverse reduce function,
  // so that new elements introduced in the window can be "added" using
  // reduceFunc to the previous window's result and old elements can be
  // "subtracted using invReduceFunc.

  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      invReduceFunc: (V, V) => V,
      windowTime: Time,
      slideTime: Time
    ): DStream[(K, V)] = {

    reduceByKeyAndWindow(
      reduceFunc, invReduceFunc, windowTime, slideTime, defaultPartitioner())
  }

  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      invReduceFunc: (V, V) => V,
      windowTime: Time,
      slideTime: Time,
      numPartitions: Int
    ): DStream[(K, V)] = {

    reduceByKeyAndWindow(
      reduceFunc, invReduceFunc, windowTime, slideTime, defaultPartitioner(numPartitions))
  }

  def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      invReduceFunc: (V, V) => V,
      windowTime: Time,
      slideTime: Time,
      partitioner: Partitioner
    ): DStream[(K, V)] = {

    val cleanedReduceFunc = ssc.sc.clean(reduceFunc)
    val cleanedInvReduceFunc = ssc.sc.clean(invReduceFunc)
    new ReducedWindowedDStream[K, V](
      self, cleanedReduceFunc, cleanedInvReduceFunc, windowTime, slideTime, partitioner)
  }

  def countByKeyAndWindow(
      windowTime: Time,
      slideTime: Time,
      numPartitions: Int = self.ssc.sc.defaultParallelism
    ): DStream[(K, Long)] = {

    self.map(x => (x._1, 1L)).reduceByKeyAndWindow(
      (x: Long, y: Long) => x + y,
      (x: Long, y: Long) => x - y,
      windowTime,
      slideTime,
      numPartitions
    )
  }

  // TODO:
  //
  //
  //
  //
  def updateStateByKey[S <: AnyRef : ClassManifest](
      updateFunc: (Seq[V], Option[S]) => Option[S]
    ): DStream[(K, S)] = {
    updateStateByKey(updateFunc, defaultPartitioner())
  }

  def updateStateByKey[S <: AnyRef : ClassManifest](
      updateFunc: (Seq[V], Option[S]) => Option[S],
      numPartitions: Int
    ): DStream[(K, S)] = {
    updateStateByKey(updateFunc, defaultPartitioner(numPartitions))
  }

  def updateStateByKey[S <: AnyRef : ClassManifest](
      updateFunc: (Seq[V], Option[S]) => Option[S],
      partitioner: Partitioner
    ): DStream[(K, S)] = {
    val newUpdateFunc = (iterator: Iterator[(K, Seq[V], Option[S])]) => {
      iterator.flatMap(t => updateFunc(t._2, t._3).map(s => (t._1, s)))
    }
    updateStateByKey(newUpdateFunc, partitioner, true)
  }

  def updateStateByKey[S <: AnyRef : ClassManifest](
      updateFunc: (Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)],
      partitioner: Partitioner,
      rememberPartitioner: Boolean
    ): DStream[(K, S)] = {
     new StateDStream(self, ssc.sc.clean(updateFunc), partitioner, rememberPartitioner)
  }


  def mapValues[U: ClassManifest](mapValuesFunc: V => U): DStream[(K, U)] = {
    new MapValuesDStream[K, V, U](self, mapValuesFunc)
  }

  def flatMapValues[U: ClassManifest](
      flatMapValuesFunc: V => TraversableOnce[U]
    ): DStream[(K, U)] = {
    new FlatMapValuesDStream[K, V, U](self, flatMapValuesFunc)
  }

  def cogroup[W: ClassManifest](other: DStream[(K, W)]): DStream[(K, (Seq[V], Seq[W]))] = {
    cogroup(other, defaultPartitioner())
  }

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

  def join[W: ClassManifest](other: DStream[(K, W)]): DStream[(K, (V, W))] = {
    join[W](other, defaultPartitioner())
  }

  def join[W: ClassManifest](other: DStream[(K, W)], partitioner: Partitioner): DStream[(K, (V, W))] = {
    this.cogroup(other, partitioner)
        .flatMapValues{
      case (vs, ws) =>
        for (v <- vs.iterator; w <- ws.iterator) yield (v, w)
    }
  }

  def saveAsHadoopFiles[F <: OutputFormat[K, V]](
      prefix: String,
      suffix: String
    )(implicit fm: ClassManifest[F]) {
    saveAsHadoopFiles(prefix, suffix, getKeyClass, getValueClass, fm.erasure.asInstanceOf[Class[F]])
  }

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

  def saveAsNewAPIHadoopFiles[F <: NewOutputFormat[K, V]](
      prefix: String,
      suffix: String
    )(implicit fm: ClassManifest[F])  {
    saveAsNewAPIHadoopFiles(prefix, suffix, getKeyClass, getValueClass, fm.erasure.asInstanceOf[Class[F]])
  }

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


