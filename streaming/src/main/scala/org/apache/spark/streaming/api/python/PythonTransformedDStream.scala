/*

package org.apache.spark.streaming.api.python

import org.apache.spark.Accumulator
import org.apache.spark.api.python.PythonRDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.streaming.{Time, Duration}
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

class PythonTransformedDStream[T: ClassTag](
               parent: DStream[T],
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

//    val parentRDDs = parents.map(_.getOrCompute(validTime).orNull).toSeq
//    parents.map(_.getOrCompute(validTime).orNull).to
//    parent = parents.head.asInstanceOf[RDD]
//    Some()
  }

  val asJavaDStream = JavaDStream.fromDStream(this)
}

*/
