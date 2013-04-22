package spark

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.ShouldMatchers
import SparkContext._

class SortingSuite extends FunSuite with LocalSparkContext with ShouldMatchers with Logging {
  
  test("sortByKey") {
    sc = new SparkContext("local", "test")
    val pairs = sc.parallelize(Array((1, 0), (2, 0), (0, 0), (3, 0)), 2)
    assert(pairs.sortByKey().collect() === Array((0,0), (1,0), (2,0), (3,0)))      
  }

  test("large array") {
    sc = new SparkContext("local", "test")
    val rand = new scala.util.Random()
    val pairArr = Array.fill(1000) { (rand.nextInt(), rand.nextInt()) }
    val pairs = sc.parallelize(pairArr, 2)
    val sorted = pairs.sortByKey()
    assert(sorted.partitions.size === 2)
    assert(sorted.collect() === pairArr.sortBy(_._1))
  }

  test("large array with one split") {
    sc = new SparkContext("local", "test")
    val rand = new scala.util.Random()
    val pairArr = Array.fill(1000) { (rand.nextInt(), rand.nextInt()) }
    val pairs = sc.parallelize(pairArr, 2)
    val sorted = pairs.sortByKey(true, 1)
    assert(sorted.partitions.size === 1)
    assert(sorted.collect() === pairArr.sortBy(_._1))
  }
  
  test("large array with many partitions") {
    sc = new SparkContext("local", "test")
    val rand = new scala.util.Random()
    val pairArr = Array.fill(1000) { (rand.nextInt(), rand.nextInt()) }
    val pairs = sc.parallelize(pairArr, 2)
    val sorted = pairs.sortByKey(true, 20)
    assert(sorted.partitions.size === 20)
    assert(sorted.collect() === pairArr.sortBy(_._1))
  }
  
  test("sort descending") {
    sc = new SparkContext("local", "test")
    val rand = new scala.util.Random()
    val pairArr = Array.fill(1000) { (rand.nextInt(), rand.nextInt()) }
    val pairs = sc.parallelize(pairArr, 2)
    assert(pairs.sortByKey(false).collect() === pairArr.sortWith((x, y) => x._1 > y._1))
  }

  test("sort descending with one split") {
    sc = new SparkContext("local", "test")
    val rand = new scala.util.Random()
    val pairArr = Array.fill(1000) { (rand.nextInt(), rand.nextInt()) }
    val pairs = sc.parallelize(pairArr, 1)
    assert(pairs.sortByKey(false, 1).collect() === pairArr.sortWith((x, y) => x._1 > y._1))
  }
  
  test("sort descending with many partitions") {
    sc = new SparkContext("local", "test")
    val rand = new scala.util.Random()
    val pairArr = Array.fill(1000) { (rand.nextInt(), rand.nextInt()) }
    val pairs = sc.parallelize(pairArr, 2)
    assert(pairs.sortByKey(false, 20).collect() === pairArr.sortWith((x, y) => x._1 > y._1))
  }

  test("more partitions than elements") {
    sc = new SparkContext("local", "test")
    val rand = new scala.util.Random()
    val pairArr = Array.fill(10) { (rand.nextInt(), rand.nextInt()) }
    val pairs = sc.parallelize(pairArr, 30)
    assert(pairs.sortByKey().collect() === pairArr.sortBy(_._1))
  }

  test("empty RDD") {
    sc = new SparkContext("local", "test")
    val pairArr = new Array[(Int, Int)](0)
    val pairs = sc.parallelize(pairArr, 2)
    assert(pairs.sortByKey().collect() === pairArr.sortBy(_._1))
  }

  test("partition balancing") {
    sc = new SparkContext("local", "test")
    val pairArr = (1 to 1000).map(x => (x, x)).toArray
    val sorted = sc.parallelize(pairArr, 4).sortByKey()
    assert(sorted.collect() === pairArr.sortBy(_._1))
    val partitions = sorted.collectPartitions()
    logInfo("Partition lengths: " + partitions.map(_.length).mkString(", "))
    partitions(0).length should be > 180
    partitions(1).length should be > 180
    partitions(2).length should be > 180
    partitions(3).length should be > 180
    partitions(0).last should be < partitions(1).head
    partitions(1).last should be < partitions(2).head
    partitions(2).last should be < partitions(3).head
  }

  test("partition balancing for descending sort") {
    sc = new SparkContext("local", "test")
    val pairArr = (1 to 1000).map(x => (x, x)).toArray
    val sorted = sc.parallelize(pairArr, 4).sortByKey(false)
    assert(sorted.collect() === pairArr.sortBy(_._1).reverse)
    val partitions = sorted.collectPartitions()
    logInfo("partition lengths: " + partitions.map(_.length).mkString(", "))
    partitions(0).length should be > 180
    partitions(1).length should be > 180
    partitions(2).length should be > 180
    partitions(3).length should be > 180
    partitions(0).last should be > partitions(1).head
    partitions(1).last should be > partitions(2).head
    partitions(2).last should be > partitions(3).head
  }
}

