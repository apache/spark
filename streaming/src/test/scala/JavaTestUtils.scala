package spark.streaming

import collection.mutable.{SynchronizedBuffer, ArrayBuffer}
import java.util.{List => JList}
import api.java.{JavaPairDStream, JavaDStreamLike, JavaDStream, JavaStreamingContext}
import spark.streaming._
import java.util.ArrayList
import collection.JavaConversions._

/** Exposes core test functionality in a Java-friendly way. */
object JavaTestUtils extends TestSuiteBase {
  def attachTestInputStream[T](ssc: JavaStreamingContext,
    data: JList[JList[T]], numPartitions: Int) = {
    val seqData = data.map(Seq(_:_*))

    implicit val cm: ClassManifest[T] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[T]]
    val dstream = new TestInputStream[T](ssc.ssc, seqData, numPartitions)
    ssc.ssc.registerInputStream(dstream)
    new JavaDStream[T](dstream)
  }

  def attachTestOutputStream[T, This <: spark.streaming.api.java.JavaDStreamLike[T,This]]
    (dstream: JavaDStreamLike[T, This]) = {
    implicit val cm: ClassManifest[T] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[T]]
    val ostream = new TestOutputStream(dstream.dstream,
      new ArrayBuffer[Seq[T]] with SynchronizedBuffer[Seq[T]])
    dstream.dstream.ssc.registerOutputStream(ostream)
  }

  def runStreams[V](
    ssc: JavaStreamingContext, numBatches: Int, numExpectedOutput: Int): JList[JList[V]] = {
    implicit val cm: ClassManifest[V] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[V]]
    val res = runStreams[V](ssc.ssc, numBatches, numExpectedOutput)
    val out = new ArrayList[JList[V]]()
    res.map(entry => out.append(new ArrayList[V](entry)))
    out
  }
}

