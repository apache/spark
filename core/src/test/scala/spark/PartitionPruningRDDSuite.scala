package spark

import org.scalatest.FunSuite
import spark.SparkContext._
import spark.rdd.PartitionPruningRDD


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
    println(prunedRDD.partitions.length)
    val p = prunedRDD.partitions(0)
    assert(p.index == 2)
  }
}