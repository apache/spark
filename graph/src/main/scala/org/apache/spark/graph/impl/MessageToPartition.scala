package org.apache.spark.graph.impl

import scala.reflect.{classTag, ClassTag}

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


class VertexBroadcastMsgRDDFunctions[T: ClassTag](self: RDD[VertexBroadcastMsg[T]]) {
  def partitionBy(partitioner: Partitioner): RDD[VertexBroadcastMsg[T]] = {
    val rdd = new ShuffledRDD[Pid, (Vid, T), VertexBroadcastMsg[T]](self, partitioner)

    // Set a custom serializer if the data is of int or double type.
    if (classTag[T] == ClassTag.Int) {
      rdd.setSerializer(classOf[IntVertexBroadcastMsgSerializer].getName)
    } else if (classTag[T] == ClassTag.Long) {
      rdd.setSerializer(classOf[LongVertexBroadcastMsgSerializer].getName)
    } else if (classTag[T] == ClassTag.Double) {
      rdd.setSerializer(classOf[DoubleVertexBroadcastMsgSerializer].getName)
    }
    rdd
  }
}


class MsgRDDFunctions[T: ClassTag](self: RDD[MessageToPartition[T]]) {

  /**
   * Return a copy of the RDD partitioned using the specified partitioner.
   */
  def partitionBy(partitioner: Partitioner): RDD[MessageToPartition[T]] = {
    new ShuffledRDD[Pid, T, MessageToPartition[T]](self, partitioner)
  }

}


object MsgRDDFunctions {
  implicit def rdd2PartitionRDDFunctions[T: ClassTag](rdd: RDD[MessageToPartition[T]]) = {
    new MsgRDDFunctions(rdd)
  }

  implicit def rdd2vertexMessageRDDFunctions[T: ClassTag](rdd: RDD[VertexBroadcastMsg[T]]) = {
    new VertexBroadcastMsgRDDFunctions(rdd)
  }

  def partitionForAggregation[T: ClassTag](msgs: RDD[(Vid, T)], partitioner: Partitioner) = {
    val rdd = new ShuffledRDD[Vid, T, (Vid, T)](msgs, partitioner)

    // Set a custom serializer if the data is of int or double type.
    if (classTag[T] == ClassTag.Int) {
      rdd.setSerializer(classOf[IntAggMsgSerializer].getName)
    } else if (classTag[T] == ClassTag.Long) {
      rdd.setSerializer(classOf[LongAggMsgSerializer].getName)
    } else if (classTag[T] == ClassTag.Double) {
      rdd.setSerializer(classOf[DoubleAggMsgSerializer].getName)
    }
    rdd
  }
}
