package org.apache.spark.graph.impl

import org.apache.spark.Partitioner
import org.apache.spark.graph.Pid
import org.apache.spark.rdd.{ShuffledRDD, RDD}


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

/**
 * Companion object for MessageToPartition.
 */
object MessageToPartition {
  def apply[T](partition: Pid, value: T) = new MessageToPartition(partition, value)
}


class MessageToPartitionRDDFunctions[T: ClassManifest](self: RDD[MessageToPartition[T]]) {

  /**
   * Return a copy of the RDD partitioned using the specified partitioner.
   */
  def partitionBy(partitioner: Partitioner): RDD[MessageToPartition[T]] = {
    new ShuffledRDD[Pid, T, MessageToPartition[T]](self, partitioner)
  }

}


object MessageToPartitionRDDFunctions {
  implicit def rdd2PartitionRDDFunctions[T: ClassManifest](rdd: RDD[MessageToPartition[T]]) = {
    new MessageToPartitionRDDFunctions(rdd)
  }
}
