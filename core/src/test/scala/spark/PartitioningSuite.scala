package spark

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import scala.collection.mutable.ArrayBuffer

import SparkContext._

class PartitioningSuite extends FunSuite with BeforeAndAfter {
  
  var sc: SparkContext = _
  
  after {
    if(sc != null) {
      sc.stop()
      sc = null
    }
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.master.port")
  }
  
  
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

    assert(grouped2.join(grouped4).partitioner === grouped2.partitioner)
    assert(grouped2.leftOuterJoin(grouped4).partitioner === grouped2.partitioner)
    assert(grouped2.rightOuterJoin(grouped4).partitioner === grouped2.partitioner)
    assert(grouped2.cogroup(grouped4).partitioner === grouped2.partitioner)

    assert(grouped2.join(reduced2).partitioner === grouped2.partitioner)
    assert(grouped2.leftOuterJoin(reduced2).partitioner === grouped2.partitioner)
    assert(grouped2.rightOuterJoin(reduced2).partitioner === grouped2.partitioner)
    assert(grouped2.cogroup(reduced2).partitioner === grouped2.partitioner)
  }
}
