package spark.streaming.api.java

import java.util.{List => JList}

import scala.collection.JavaConversions._

import spark.streaming._
import spark.streaming.StreamingContext._
import spark.api.java.function.{Function => JFunction, Function2 => JFunction2}
import spark.Partitioner

class JavaPairDStream[K, V](val dstream: DStream[(K, V)])(
    implicit val kManifiest: ClassManifest[K],
    implicit val vManifest: ClassManifest[V])
    extends JavaDStreamLike[(K, V), JavaPairDStream[K, V]] {

  def filter(f: JFunction[(K, V), java.lang.Boolean]): JavaPairDStream[K, V] = {
    dstream.filter((x => f(x).booleanValue()))
  }

  def groupByKey(): JavaPairDStream[K, JList[V]] = {
    dstream.groupByKey().mapValues(seqAsJavaList _)
  }

  def groupByKey(numPartitions: Int): JavaPairDStream[K, JList[V]] = {
    dstream.groupByKey(numPartitions).mapValues(seqAsJavaList _)
  }

  def groupByKey(partitioner: Partitioner): JavaPairDStream[K, JList[V]] = {
    dstream.groupByKey(partitioner).mapValues(seqAsJavaList _)
  }

  def reduceByKey(func: JFunction2[V, V, V]): JavaPairDStream[K, V] = {
    dstream.reduceByKey(func)
  }

  def reduceByKey(func: JFunction2[V, V, V], numPartitions: Int): JavaPairDStream[K, V] = {
    dstream.reduceByKey(func, numPartitions)
  }

  // TODO: TEST BELOW
  def combineByKey[C](createCombiner: Function[V, C],
                      mergeValue: JFunction2[C, V, C],
                      mergeCombiners: JFunction2[C, C, C],
                      partitioner: Partitioner): JavaPairDStream[K, C] = {
    implicit val cm: ClassManifest[C] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[C]]
    dstream.combineByKey(createCombiner, mergeValue, mergeCombiners, partitioner)
  }

  def countByKey(numPartitions: Int): JavaPairDStream[K, Long] = {
    dstream.countByKey(numPartitions);
  }

  def countByKey(): JavaPairDStream[K, Long] = {
    dstream.countByKey();
  }

  def groupByKeyAndWindow(windowTime: Time, slideTime: Time): JavaPairDStream[K, JList[V]] = {
    dstream.groupByKeyAndWindow(windowTime, slideTime).mapValues(seqAsJavaList _)
  }

  def groupByKeyAndWindow(windowTime: Time, slideTime: Time, numPartitions: Int):
      JavaPairDStream[K, JList[V]] = {
    dstream.groupByKeyAndWindow(windowTime, slideTime, numPartitions).mapValues(seqAsJavaList _)
  }

  def groupByKeyAndWindow(windowTime: Time, slideTime: Time, partitioner: Partitioner):
      JavaPairDStream[K, JList[V]] = {
    dstream.groupByKeyAndWindow(windowTime, slideTime, partitioner).mapValues(seqAsJavaList _)
  }

  def reduceByKeyAndWindow(reduceFunc: Function2[V, V, V], windowTime: Time):
      JavaPairDStream[K, V] = {
    dstream.reduceByKeyAndWindow(reduceFunc, windowTime)
  }

  def reduceByKeyAndWindow(reduceFunc: Function2[V, V, V], windowTime: Time, slideTime: Time):
      JavaPairDStream[K, V] = {
    dstream.reduceByKeyAndWindow(reduceFunc, windowTime, slideTime)
  }

  def reduceByKeyAndWindow(reduceFunc: Function2[V, V, V], windowTime: Time, slideTime: Time,
      numPartitions: Int): JavaPairDStream[K, V] = {
    dstream.reduceByKeyAndWindow(reduceFunc, windowTime, slideTime, numPartitions)
  }

  def reduceByKeyAndWindow(reduceFunc: Function2[V, V, V], windowTime: Time, slideTime: Time,
      partitioner: Partitioner): JavaPairDStream[K, V] = {
    dstream.reduceByKeyAndWindow(reduceFunc, windowTime, slideTime, partitioner)
  }

  def reduceByKeyAndWindow(reduceFunc: Function2[V, V, V], invReduceFunc: Function2[V, V, V],
      windowTime: Time, slideTime: Time): JavaPairDStream[K, V] = {
    dstream.reduceByKeyAndWindow(reduceFunc, invReduceFunc, windowTime, slideTime)
  }

  def reduceByKeyAndWindow(reduceFunc: Function2[V, V, V], invReduceFunc: Function2[V, V, V],
      windowTime: Time, slideTime: Time, numPartitions: Int): JavaPairDStream[K, V] = {
    dstream.reduceByKeyAndWindow(reduceFunc, invReduceFunc, windowTime, slideTime, numPartitions)
  }

  def reduceByKeyAndWindow(reduceFunc: Function2[V, V, V], invReduceFunc: Function2[V, V, V],
      windowTime: Time, slideTime: Time, partitioner: Partitioner)
      : JavaPairDStream[K, V] = {
    dstream.reduceByKeyAndWindow(reduceFunc, invReduceFunc, windowTime, slideTime, partitioner)
  }

  def countByKeyAndWindow(windowTime: Time, slideTime: Time): JavaPairDStream[K, Long] = {
    dstream.countByKeyAndWindow(windowTime, slideTime)
  }

  def countByKeyAndWindow(windowTime: Time, slideTime: Time, numPartitions: Int)
      : JavaPairDStream[K, Long] = {
    dstream.countByKeyAndWindow(windowTime, slideTime, numPartitions)
  }

  override val classManifest: ClassManifest[(K, V)] =
    implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[Tuple2[K, V]]]
}

object JavaPairDStream {
  implicit def fromPairDStream[K: ClassManifest, V: ClassManifest](dstream: DStream[(K, V)]):
      JavaPairDStream[K, V] =
    new JavaPairDStream[K, V](dstream)

  def fromJavaDStream[K, V](dstream: JavaDStream[(K, V)]): JavaPairDStream[K, V] = {
    implicit val cmk: ClassManifest[K] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[K]]
    implicit val cmv: ClassManifest[V] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[V]]
    new JavaPairDStream[K, V](dstream.dstream)
  }
}
