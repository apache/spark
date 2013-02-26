package spark.streaming

import collection.mutable.{SynchronizedBuffer, ArrayBuffer}
import java.util.{List => JList}
import spark.streaming.api.java.{JavaPairDStream, JavaDStreamLike, JavaDStream, JavaStreamingContext}
import spark.streaming._
import java.util.ArrayList
import collection.JavaConversions._

/** Exposes streaming test functionality in a Java-friendly way. */
trait JavaTestBase extends TestSuiteBase {

  /**
   * Create a [[spark.streaming.TestInputStream]] and attach it to the supplied context.
   * The stream will be derived from the supplied lists of Java objects.
   **/
  def attachTestInputStream[T](
    ssc: JavaStreamingContext,
    data: JList[JList[T]],
    numPartitions: Int) = {
    val seqData = data.map(Seq(_:_*))

    implicit val cm: ClassManifest[T] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[T]]
    val dstream = new TestInputStream[T](ssc.ssc, seqData, numPartitions)
    ssc.ssc.registerInputStream(dstream)
    new JavaDStream[T](dstream)
  }

  /**
   * Attach a provided stream to it's associated StreamingContext as a
   * [[spark.streaming.TestOutputStream]].
   **/
  def attachTestOutputStream[T, This <: spark.streaming.api.java.JavaDStreamLike[T, This, R],
      R <: spark.api.java.JavaRDDLike[T, R]](
    dstream: JavaDStreamLike[T, This, R]) = {
    implicit val cm: ClassManifest[T] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[T]]
    val ostream = new TestOutputStream(dstream.dstream,
      new ArrayBuffer[Seq[T]] with SynchronizedBuffer[Seq[T]])
    dstream.dstream.ssc.registerOutputStream(ostream)
  }

  /**
   * Process all registered streams for a numBatches batches, failing if
   * numExpectedOutput RDD's are not generated. Generated RDD's are collected
   * and returned, represented as a list for each batch interval.
   */
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

object JavaTestUtils extends JavaTestBase {
  override def maxWaitTimeMillis = 20000

}

object JavaCheckpointTestUtils extends JavaTestBase {
  override def actuallyWait = true
}