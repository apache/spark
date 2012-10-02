package spark

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.prop.Checkers
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import org.scalacheck.Prop._

import com.google.common.io.Files

import scala.collection.mutable.ArrayBuffer

import SparkContext._
import storage.StorageLevel

class DistributedSuite extends FunSuite with ShouldMatchers with BeforeAndAfter {

  val clusterUrl = "local-cluster[2,1,512]"
  
  @transient var sc: SparkContext = _
  
  after {
    if (sc != null) {
      sc.stop()
      sc = null
    }
  }

  test("simple groupByKey") {
    sc = new SparkContext(clusterUrl, "test")
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (2, 1)), 5)
    val groups = pairs.groupByKey(5).collect()
    assert(groups.size === 2)
    val valuesFor1 = groups.find(_._1 == 1).get._2
    assert(valuesFor1.toList.sorted === List(1, 2, 3))
    val valuesFor2 = groups.find(_._1 == 2).get._2
    assert(valuesFor2.toList.sorted === List(1))
  }
 
  test("accumulators") {
    sc = new SparkContext(clusterUrl, "test")
    val accum = sc.accumulator(0)
    sc.parallelize(1 to 10, 10).foreach(x => accum += x)
    assert(accum.value === 55)
  }

  test("broadcast variables") {
    sc = new SparkContext(clusterUrl, "test")
    val array = new Array[Int](100)
    val bv = sc.broadcast(array)
    array(2) = 3     // Change the array -- this should not be seen on workers
    val rdd = sc.parallelize(1 to 10, 10)
    val sum = rdd.map(x => bv.value.sum).reduce(_ + _)
    assert(sum === 0)
  }

  test("repeatedly failing task") {
    sc = new SparkContext(clusterUrl, "test")
    val accum = sc.accumulator(0)
    val thrown = intercept[SparkException] {
      sc.parallelize(1 to 10, 10).foreach(x => println(x / 0))
    }
    assert(thrown.getClass === classOf[SparkException])
    assert(thrown.getMessage.contains("more than 4 times"))
  }

  test("caching") {
    sc = new SparkContext(clusterUrl, "test")
    val data = sc.parallelize(1 to 1000, 10).cache()
    assert(data.count() === 1000)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
  }

  test("caching on disk") {
    sc = new SparkContext(clusterUrl, "test")
    val data = sc.parallelize(1 to 1000, 10).persist(StorageLevel.DISK_ONLY)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
  }

  test("caching in memory, replicated") {
    sc = new SparkContext(clusterUrl, "test")
    val data = sc.parallelize(1 to 1000, 10).persist(StorageLevel.MEMORY_ONLY_2)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
  }

  test("caching in memory, serialized, replicated") {
    sc = new SparkContext(clusterUrl, "test")
    val data = sc.parallelize(1 to 1000, 10).persist(StorageLevel.MEMORY_ONLY_SER_2)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
  }

  test("caching on disk, replicated") {
    sc = new SparkContext(clusterUrl, "test")
    val data = sc.parallelize(1 to 1000, 10).persist(StorageLevel.DISK_ONLY_2)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
  }

  test("caching in memory and disk, replicated") {
    sc = new SparkContext(clusterUrl, "test")
    val data = sc.parallelize(1 to 1000, 10).persist(StorageLevel.MEMORY_AND_DISK_2)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
  }

  test("caching in memory and disk, serialized, replicated") {
    sc = new SparkContext(clusterUrl, "test")
    val data = sc.parallelize(1 to 1000, 10).persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
  }
}
