package spark

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import SparkContext._

class SortingSuite extends FunSuite with BeforeAndAfter {
  
  var sc: SparkContext = _
  
  after{
    if(sc != null){
      sc.stop()
    }
  }
  
  test("sortByKey") {
      sc = new SparkContext("local", "test")
      val pairs = sc.parallelize(Array((1, 0), (2, 0), (0, 0), (3, 0)))
      assert(pairs.sortByKey().collect() === Array((0,0), (1,0), (2,0), (3,0)))      
  }

  test("sortLargeArray") {
      sc = new SparkContext("local", "test")
      val rand = new scala.util.Random()
      val pairArr = Array.fill(1000) { (rand.nextInt(), rand.nextInt()) }
      val pairs = sc.parallelize(pairArr)
      assert(pairs.sortByKey().collect() === pairArr.sortBy(_._1))
  }

  test("sortDescending") {
      sc = new SparkContext("local", "test")
      val rand = new scala.util.Random()
      val pairArr = Array.fill(1000) { (rand.nextInt(), rand.nextInt()) }
      val pairs = sc.parallelize(pairArr)
      assert(pairs.sortByKey(false).collect() === pairArr.sortWith((x, y) => x._1 > y._1))
  }

  test("morePartitionsThanElements") {
      sc = new SparkContext("local", "test")
      val rand = new scala.util.Random()
      val pairArr = Array.fill(10) { (rand.nextInt(), rand.nextInt()) }
      val pairs = sc.parallelize(pairArr, 30)
      assert(pairs.sortByKey().collect() === pairArr.sortBy(_._1))
  }

  test("emptyRDD") {
      sc = new SparkContext("local", "test")
      val rand = new scala.util.Random()
      val pairArr = new Array[(Int, Int)](0)
      val pairs = sc.parallelize(pairArr)
      assert(pairs.sortByKey().collect() === pairArr.sortBy(_._1))
  }
}

