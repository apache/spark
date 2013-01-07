package spark.streaming.api.java

import spark.streaming.DStream
import spark.api.java.function.{Function => JFunction}

class JavaDStream[T](val dstream: DStream[T])(implicit val classManifest: ClassManifest[T])
    extends JavaDStreamLike[T, JavaDStream[T]] {

  def filter(f: JFunction[T, java.lang.Boolean]): JavaDStream[T] = {
    dstream.filter((x => f(x).booleanValue()))
  }
}

object JavaDStream {
  implicit def fromDStream[T: ClassManifest](dstream: DStream[T]): JavaDStream[T] =
    new JavaDStream[T](dstream)
}
