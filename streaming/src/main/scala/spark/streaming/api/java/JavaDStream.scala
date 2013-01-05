package spark.streaming.api.java

import java.util.{List => JList}

import scala.collection.JavaConversions._

import spark.streaming._
import spark.api.java.JavaRDD
import spark.api.java.function.{Function2 => JFunction2, Function => JFunction, _}
import java.util
import spark.RDD

class JavaDStream[T](val dstream: DStream[T])(implicit val classManifest: ClassManifest[T]) {
  def print() = dstream.print()

  // TODO move to type-specific implementations
  def cache() : JavaDStream[T] = {
    dstream.cache()
  }

  def count() : JavaDStream[Int] = {
    dstream.count()
  }

  def countByWindow(windowTime: Time, slideTime: Time) : JavaDStream[Int] = {
    dstream.countByWindow(windowTime, slideTime)
  }

  def compute(validTime: Time): JavaRDD[T] = {
    dstream.compute(validTime) match {
      case Some(rdd) => new JavaRDD(rdd)
      case None => null
    }
  }

  def context(): StreamingContext = dstream.context()

  def window(windowTime: Time): JavaDStream[T] = {
    dstream.window(windowTime)
  }

  def window(windowTime: Time, slideTime: Time): JavaDStream[T] = {
    dstream.window(windowTime, slideTime)
  }

  def tumble(batchTime: Time): JavaDStream[T] = {
    dstream.tumble(batchTime)
  }

  def map[R](f: JFunction[T, R]): JavaDStream[R] = {
    new JavaDStream(dstream.map(f)(f.returnType()))(f.returnType())
  }

  def filter(f: JFunction[T, java.lang.Boolean]): JavaDStream[T] = {
    dstream.filter((x => f(x).booleanValue()))
  }

  def glom(): JavaDStream[JList[T]] = {
    new JavaDStream(dstream.glom().map(x => new java.util.ArrayList[T](x.toSeq)))
  }

  // TODO: Other map partitions
  def mapPartitions[U](f: FlatMapFunction[java.util.Iterator[T], U]): JavaDStream[U] = {
    def fn = (x: Iterator[T]) => asScalaIterator(f.apply(asJavaIterator(x)).iterator())
    new JavaDStream(dstream.mapPartitions(fn)(f.elementType()))(f.elementType())
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

  def foreach(foreachFunc: JFunction[JavaRDD[T], Void]) = {
    dstream.foreach(rdd => foreachFunc.call(new JavaRDD(rdd)))
  }

  def foreach(foreachFunc: JFunction2[JavaRDD[T], Time, Void]) = {
    dstream.foreach((rdd, time) => foreachFunc.call(new JavaRDD(rdd), time))
  }

  def transform[U](transformFunc: JFunction[JavaRDD[T], JavaRDD[U]]): JavaDStream[U] = {
    implicit val cm: ClassManifest[U] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[U]]
    def scalaTransform (in: RDD[T]): RDD[U] = {
      transformFunc.call(new JavaRDD[T](in)).rdd
    }
    dstream.transform(scalaTransform(_))
  }
  // TODO: transform with time

  def union(that: JavaDStream[T]): JavaDStream[T] = {
    dstream.union(that.dstream)
  }
}

object JavaDStream {
  implicit def fromDStream[T: ClassManifest](dstream: DStream[T]): JavaDStream[T] =
    new JavaDStream[T](dstream)

}