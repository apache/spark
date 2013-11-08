package org.apache.spark.graph.impl

import org.apache.spark.Partitioner
import org.apache.spark.graph.{Pid, Vid}
import org.apache.spark.rdd.{ShuffledRDD, RDD}


class VertexBroadcastMsg[@specialized(Int, Long, Double, Boolean) T](
    @transient var partition: Pid,
    var vid: Vid,
    var data: T)
  extends Product2[Pid, (Vid, T)] {

  override def _1 = partition

  override def _2 = (vid, data)

  override def canEqual(that: Any): Boolean = that.isInstanceOf[VertexBroadcastMsg[_]]
}


class AggregationMsg[@specialized(Int, Long, Double, Boolean) T](var vid: Vid, var data: T)
  extends Product2[Vid, T] {

  override def _1 = vid

  override def _2 = data

  override def canEqual(that: Any): Boolean = that.isInstanceOf[AggregationMsg[_]]
}


/**
 * A message used to send a specific value to a partition.
 * @param partition index of the target partition.
 * @param data value to send
 */
class MessageToPartition[@specialized(Int, Long, Double, Char, Boolean/*, AnyRef*/) T](
    @transient var partition: Pid,
    var data: T)
  extends Product2[Pid, T] {

  override def _1 = partition

  override def _2 = data

  override def canEqual(that: Any): Boolean = that.isInstanceOf[MessageToPartition[_]]
}


class VertexBroadcastMsgRDDFunctions[T: ClassManifest](self: RDD[VertexBroadcastMsg[T]]) {
  def partitionBy(partitioner: Partitioner): RDD[VertexBroadcastMsg[T]] = {
    val rdd = new ShuffledRDD[Pid, (Vid, T), VertexBroadcastMsg[T]](self, partitioner)

    // Set a custom serializer if the data is of int or double type.
    if (classManifest[T] == ClassManifest.Int) {
      rdd.setSerializer(classOf[IntVertexBroadcastMsgSerializer].getName)
    } else if (classManifest[T] == ClassManifest.Long) {
      rdd.setSerializer(classOf[LongVertexBroadcastMsgSerializer].getName)
    } else if (classManifest[T] == ClassManifest.Double) {
      rdd.setSerializer(classOf[DoubleVertexBroadcastMsgSerializer].getName)
    }
    rdd
  }
}


class AggregationMessageRDDFunctions[T: ClassManifest](self: RDD[AggregationMsg[T]]) {
  def partitionBy(partitioner: Partitioner): RDD[AggregationMsg[T]] = {
    val rdd = new ShuffledRDD[Vid, T, AggregationMsg[T]](self, partitioner)

    // Set a custom serializer if the data is of int or double type.
    if (classManifest[T] == ClassManifest.Int) {
      rdd.setSerializer(classOf[IntAggMsgSerializer].getName)
    } else if (classManifest[T] == ClassManifest.Long) {
      rdd.setSerializer(classOf[LongAggMsgSerializer].getName)
    } else if (classManifest[T] == ClassManifest.Double) {
      rdd.setSerializer(classOf[DoubleAggMsgSerializer].getName)
    }
    rdd
  }
}


class MsgRDDFunctions[T: ClassManifest](self: RDD[MessageToPartition[T]]) {

  /**
   * Return a copy of the RDD partitioned using the specified partitioner.
   */
  def partitionBy(partitioner: Partitioner): RDD[MessageToPartition[T]] = {
    new ShuffledRDD[Pid, T, MessageToPartition[T]](self, partitioner)
  }

}


object MsgRDDFunctions {
  implicit def rdd2PartitionRDDFunctions[T: ClassManifest](rdd: RDD[MessageToPartition[T]]) = {
    new MsgRDDFunctions(rdd)
  }

  implicit def rdd2vertexMessageRDDFunctions[T: ClassManifest](rdd: RDD[VertexBroadcastMsg[T]]) = {
    new VertexBroadcastMsgRDDFunctions(rdd)
  }

  implicit def rdd2aggMessageRDDFunctions[T: ClassManifest](rdd: RDD[AggregationMsg[T]]) = {
    new AggregationMessageRDDFunctions(rdd)
  }
}
