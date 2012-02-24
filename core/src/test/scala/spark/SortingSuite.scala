package spark

import org.scalatest.FunSuite
import SparkContext._

class SortingSuite extends FunSuite {
  test("sortByKey") {
      val sc = new SparkContext("local", "test")
      val pairs = sc.parallelize(Array((1, 0), (2, 0), (0, 0), (3, 0)))
      assert(pairs.sortByKey().collect() === Array((0,0), (1,0), (2,0), (3,0)))
      sc.stop()
  }

  test("sortLargeArray") {
      val sc = new SparkContext("local", "test")
      val rand = new scala.util.Random()
      val pairArr = Array.fill(1000) { (rand.nextInt(), rand.nextInt()) }
      val pairs = sc.parallelize(pairArr)
      assert(pairs.sortByKey().collect() === pairArr.sortBy(_._1))
      sc.stop()
  }

  test("sortDescending") {
      val sc = new SparkContext("local", "test")
      val rand = new scala.util.Random()
      val pairArr = Array.fill(1000) { (rand.nextInt(), rand.nextInt()) }
      val pairs = sc.parallelize(pairArr)
      assert(pairs.sortByKey(false).collect() === pairArr.sortWith((x, y) => x._1 > y._1))
      sc.stop()
  }

  test("sortHighParallelism") {
      val sc = new SparkContext("local", "test")
      val rand = new scala.util.Random()
      val pairArr = Array.fill(3000) { (rand.nextInt(), rand.nextInt()) }
      val pairs = sc.parallelize(pairArr, 300)
      assert(pairs.sortByKey().collect() === pairArr.sortBy(_._1))
      sc.stop()
  }
}

