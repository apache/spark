package spark

import org.scalatest.FunSuite

class BroadcastSuite extends FunSuite with LocalSparkContext {
  
  test("basic broadcast") {
    sc = new SparkContext("local", "test")
    val list = List(1, 2, 3, 4)
    val listBroadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to 2).map(x => (x, listBroadcast.value.sum))
    assert(results.collect.toSet === Set((1, 10), (2, 10)))
  }

  test("broadcast variables accessed in multiple threads") {
    sc = new SparkContext("local[10]", "test")
    val list = List(1, 2, 3, 4)
    val listBroadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to 10).map(x => (x, listBroadcast.value.sum))
    assert(results.collect.toSet === (1 to 10).map(x => (x, 10)).toSet)
  }
}
