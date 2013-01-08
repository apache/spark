package spark.streaming.api.java

import java.util.{List => JList}

import scala.collection.JavaConversions._

import spark.streaming._
import spark.api.java.JavaRDD
import spark.api.java.function.{Function2 => JFunction2, Function => JFunction, _}
import java.util
import spark.RDD
import JavaDStream._

trait JavaDStreamLike[T, This <: JavaDStreamLike[T, This]] extends Serializable {
  implicit val classManifest: ClassManifest[T]

  def dstream: DStream[T]

  def print() = dstream.print()

  def count(): JavaDStream[Int] = dstream.count()

  def countByWindow(windowTime: Time, slideTime: Time) : JavaDStream[Int] = {
    dstream.countByWindow(windowTime, slideTime)
  }

  def glom(): JavaDStream[JList[T]] =
    new JavaDStream(dstream.glom().map(x => new java.util.ArrayList[T](x.toSeq)))

  def context(): StreamingContext = dstream.context()

  def map[R](f: JFunction[T, R]): JavaDStream[R] = {
    new JavaDStream(dstream.map(f)(f.returnType()))(f.returnType())
  }

  def map[K, V](f: PairFunction[T, K, V]): JavaPairDStream[K, V] = {
    def cm = implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[Tuple2[K, V]]]
    new JavaPairDStream(dstream.map(f)(cm))(f.keyType(), f.valueType())
  }

  def mapPartitions[U](f: FlatMapFunction[java.util.Iterator[T], U]): JavaDStream[U] = {
    def fn = (x: Iterator[T]) => asScalaIterator(f.apply(asJavaIterator(x)).iterator())
    new JavaDStream(dstream.mapPartitions(fn)(f.elementType()))(f.elementType())
  }

  def mapPartitions[K, V](f: PairFlatMapFunction[java.util.Iterator[T], K, V])
  : JavaPairDStream[K, V] = {
    def fn = (x: Iterator[T]) => asScalaIterator(f.apply(asJavaIterator(x)).iterator())
    new JavaPairDStream(dstream.mapPartitions(fn))(f.keyType(), f.valueType())
  }

  def reduce(f: JFunction2[T, T, T]): JavaDStream[T] = dstream.reduce(f)

  def reduceByWindow(
    reduceFunc: JFunction2[T, T, T],
    invReduceFunc: JFunction2[T, T, T],
    windowTime: Time,
    slideTime: Time): JavaDStream[T] = {
    dstream.reduceByWindow(reduceFunc, invReduceFunc, windowTime, slideTime)
  }

  def slice(fromTime: Time, toTime: Time): JList[JavaRDD[T]] = {
    new util.ArrayList(dstream.slice(fromTime, toTime).map(new JavaRDD(_)).toSeq)
  }

  def foreach(foreachFunc: JFunction[JavaRDD[T], Void]) {
    dstream.foreach(rdd => foreachFunc.call(new JavaRDD(rdd)))
  }

  def foreach(foreachFunc: JFunction2[JavaRDD[T], Time, Void]) {
    dstream.foreach((rdd, time) => foreachFunc.call(new JavaRDD(rdd), time))
  }

  def transform[U](transformFunc: JFunction[JavaRDD[T], JavaRDD[U]]): JavaDStream[U] = {
    implicit val cm: ClassManifest[U] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[U]]
    def scalaTransform (in: RDD[T]): RDD[U] =
      transformFunc.call(new JavaRDD[T](in)).rdd
    dstream.transform(scalaTransform(_))
  }

  def transform[U](transformFunc: JFunction2[JavaRDD[T], Time, JavaRDD[U]]): JavaDStream[U] = {
    implicit val cm: ClassManifest[U] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[U]]
    def scalaTransform (in: RDD[T], time: Time): RDD[U] =
      transformFunc.call(new JavaRDD[T](in), time).rdd
    dstream.transform(scalaTransform(_, _))
  }
}