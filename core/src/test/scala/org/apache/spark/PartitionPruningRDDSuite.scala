package org.apache.spark

import org.scalatest.FunSuite
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.PartitionPruningRDD


class PartitionPruningRDDSuite extends FunSuite with SharedSparkContext {

  test("Pruned Partitions inherit locality prefs correctly") {
    class TestPartition(i: Int) extends Partition {
      def index = i
    }
    val rdd = new RDD[Int](sc, Nil) {
      override protected def getPartitions = {
        Array[Partition](
            new TestPartition(1),
            new TestPartition(2), 
            new TestPartition(3))
      }
      def compute(split: Partition, context: TaskContext) = {Iterator()}
    }
    val prunedRDD = PartitionPruningRDD.create(rdd, {x => if (x==2) true else false})
    val p = prunedRDD.partitions(0)
    assert(p.index == 2)
    assert(prunedRDD.partitions.length == 1)
  }
}
