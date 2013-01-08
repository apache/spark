package spark.streaming.api.java

import spark.streaming.{Time, DStream}
import spark.api.java.function.{Function => JFunction}
import spark.api.java.JavaRDD
import java.util.{List => JList}

class JavaDStream[T](val dstream: DStream[T])(implicit val classManifest: ClassManifest[T])
    extends JavaDStreamLike[T, JavaDStream[T]] {

  def filter(f: JFunction[T, java.lang.Boolean]): JavaDStream[T] =
    dstream.filter((x => f(x).booleanValue()))

  def cache(): JavaDStream[T] = dstream.cache()

  def compute(validTime: Time): JavaRDD[T] = {
    dstream.compute(validTime) match {
      case Some(rdd) => new JavaRDD(rdd)
      case None => null
    }
  }

  def window(windowTime: Time): JavaDStream[T] =
    dstream.window(windowTime)

  def window(windowTime: Time, slideTime: Time): JavaDStream[T] =
    dstream.window(windowTime, slideTime)

  def tumble(batchTime: Time): JavaDStream[T] =
    dstream.tumble(batchTime)

  def union(that: JavaDStream[T]): JavaDStream[T] =
    dstream.union(that.dstream)
}

object JavaDStream {
  implicit def fromDStream[T: ClassManifest](dstream: DStream[T]): JavaDStream[T] =
    new JavaDStream[T](dstream)
}
