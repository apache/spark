package spark

import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer

import SparkContext._

class PartitioningSuite extends FunSuite with LocalSparkContext {
  
  test("HashPartitioner equality") {
    val p2 = new HashPartitioner(2)
    val p4 = new HashPartitioner(4)
    val anotherP4 = new HashPartitioner(4)
    assert(p2 === p2)
    assert(p4 === p4)
    assert(p2 != p4)
    assert(p4 != p2)
    assert(p4 === anotherP4)
    assert(anotherP4 === p4)
  }

  test("RangePartitioner equality") {
    sc = new SparkContext("local", "test")

    // Make an RDD where all the elements are the same so that the partition range bounds
    // are deterministically all the same.
    val rdd = sc.parallelize(Seq(1, 1, 1, 1)).map(x => (x, x))

    val p2 = new RangePartitioner(2, rdd)
    val p4 = new RangePartitioner(4, rdd)
    val anotherP4 = new RangePartitioner(4, rdd)
    val descendingP2 = new RangePartitioner(2, rdd, false)
    val descendingP4 = new RangePartitioner(4, rdd, false)

    assert(p2 === p2)
    assert(p4 === p4)
    assert(p2 != p4)
    assert(p4 != p2)
    assert(p4 === anotherP4)
    assert(anotherP4 === p4)
    assert(descendingP2 === descendingP2)
    assert(descendingP4 === descendingP4)
    assert(descendingP2 != descendingP4)
    assert(descendingP4 != descendingP2)
    assert(p2 != descendingP2)
    assert(p4 != descendingP4)
    assert(descendingP2 != p2)
    assert(descendingP4 != p4)
  }

  test("HashPartitioner not equal to RangePartitioner") {
    sc = new SparkContext("local", "test")
    val rdd = sc.parallelize(1 to 10).map(x => (x, x))
    val rangeP2 = new RangePartitioner(2, rdd)
    val hashP2 = new HashPartitioner(2)
    assert(rangeP2 === rangeP2)
    assert(hashP2 === hashP2)
    assert(hashP2 != rangeP2)
    assert(rangeP2 != hashP2)
  }

  test("partitioner preservation") {
    sc = new SparkContext("local", "test")

    val rdd = sc.parallelize(1 to 10, 4).map(x => (x, x))

    val grouped2 = rdd.groupByKey(2)
    val grouped4 = rdd.groupByKey(4)
    val reduced2 = rdd.reduceByKey(_ + _, 2)
    val reduced4 = rdd.reduceByKey(_ + _, 4)

    assert(rdd.partitioner === None)

    assert(grouped2.partitioner === Some(new HashPartitioner(2)))
    assert(grouped4.partitioner === Some(new HashPartitioner(4)))
    assert(reduced2.partitioner === Some(new HashPartitioner(2)))
    assert(reduced4.partitioner === Some(new HashPartitioner(4)))

    assert(grouped2.groupByKey().partitioner  === grouped2.partitioner)
    assert(grouped2.groupByKey(3).partitioner !=  grouped2.partitioner)
    assert(grouped2.groupByKey(2).partitioner === grouped2.partitioner)
    assert(grouped4.groupByKey().partitioner  === grouped4.partitioner)
    assert(grouped4.groupByKey(3).partitioner !=  grouped4.partitioner)
    assert(grouped4.groupByKey(4).partitioner === grouped4.partitioner)

    assert(grouped2.join(grouped4).partitioner === grouped4.partitioner)
    assert(grouped2.leftOuterJoin(grouped4).partitioner === grouped4.partitioner)
    assert(grouped2.rightOuterJoin(grouped4).partitioner === grouped4.partitioner)
    assert(grouped2.cogroup(grouped4).partitioner === grouped4.partitioner)

    assert(grouped2.join(reduced2).partitioner === grouped2.partitioner)
    assert(grouped2.leftOuterJoin(reduced2).partitioner === grouped2.partitioner)
    assert(grouped2.rightOuterJoin(reduced2).partitioner === grouped2.partitioner)
    assert(grouped2.cogroup(reduced2).partitioner === grouped2.partitioner)

    assert(grouped2.map(_ => 1).partitioner === None)
    assert(grouped2.mapValues(_ => 1).partitioner === grouped2.partitioner)
    assert(grouped2.flatMapValues(_ => Seq(1)).partitioner === grouped2.partitioner)
    assert(grouped2.filter(_._1 > 4).partitioner === grouped2.partitioner)
  }

  test("partitioning Java arrays should fail") {
    sc = new SparkContext("local", "test")
    val arrs: RDD[Array[Int]] = sc.parallelize(Array(1, 2, 3, 4), 2).map(x => Array(x))
    val arrPairs: RDD[(Array[Int], Int)] =
      sc.parallelize(Array(1, 2, 3, 4), 2).map(x => (Array(x), x))

    assert(intercept[SparkException]{ arrs.distinct() }.getMessage.contains("array"))
    // We can't catch all usages of arrays, since they might occur inside other collections:
    //assert(fails { arrPairs.distinct() })
    assert(intercept[SparkException]{ arrPairs.partitionBy(new HashPartitioner(2)) }.getMessage.contains("array"))
    assert(intercept[SparkException]{ arrPairs.join(arrPairs) }.getMessage.contains("array"))
    assert(intercept[SparkException]{ arrPairs.leftOuterJoin(arrPairs) }.getMessage.contains("array"))
    assert(intercept[SparkException]{ arrPairs.rightOuterJoin(arrPairs) }.getMessage.contains("array"))
    assert(intercept[SparkException]{ arrPairs.groupByKey() }.getMessage.contains("array"))
    assert(intercept[SparkException]{ arrPairs.countByKey() }.getMessage.contains("array"))
    assert(intercept[SparkException]{ arrPairs.countByKeyApprox(1) }.getMessage.contains("array"))
    assert(intercept[SparkException]{ arrPairs.cogroup(arrPairs) }.getMessage.contains("array"))
    assert(intercept[SparkException]{ arrPairs.reduceByKeyLocally(_ + _) }.getMessage.contains("array"))
    assert(intercept[SparkException]{ arrPairs.reduceByKey(_ + _) }.getMessage.contains("array"))
  }
}
