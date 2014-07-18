<<<<<<< HEAD
/*

=======
>>>>>>> 69e9cd33a58b880f96cc9c3e5e62eaa415c49843
package org.apache.spark.streaming.api.python

import org.apache.spark.Accumulator
import org.apache.spark.api.python.PythonRDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.streaming.{Time, Duration}
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

<<<<<<< HEAD
class PythonTransformedDStream[T: ClassTag](
               parent: DStream[T],
=======
/**
 * Created by ken on 7/15/14.
 */
class PythonTransformedDStream[T: ClassTag](
               parents: Seq[DStream[T]],
>>>>>>> 69e9cd33a58b880f96cc9c3e5e62eaa415c49843
               command: Array[Byte],
               envVars: JMap[String, String],
               pythonIncludes: JList[String],
               preservePartitoning: Boolean,
               pythonExec: String,
               broadcastVars: JList[Broadcast[Array[Byte]]],
               accumulator: Accumulator[JList[Array[Byte]]]
               ) extends DStream[Array[Byte]](parent.ssc) {

  override def dependencies = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  //pythonDStream compute
  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
<<<<<<< HEAD

//    val parentRDDs = parents.map(_.getOrCompute(validTime).orNull).toSeq
//    parents.map(_.getOrCompute(validTime).orNull).to
//    parent = parents.head.asInstanceOf[RDD]
//    Some()
  }

  val asJavaDStream = JavaDStream.fromDStream(this)
}

*/
