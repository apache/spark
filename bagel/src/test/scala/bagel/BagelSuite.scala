package spark.bagel

import org.scalatest.{FunSuite, Assertions, BeforeAndAfter}
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._

import scala.collection.mutable.ArrayBuffer

import spark._
import storage.StorageLevel

class TestVertex(val active: Boolean, val age: Int) extends Vertex with Serializable
class TestMessage(val targetId: String) extends Message[String] with Serializable

class BagelSuite extends FunSuite with Assertions with BeforeAndAfter with Timeouts {
  
  var sc: SparkContext = _
  
  after {
    if (sc != null) {
      sc.stop()
      sc = null
    }
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }

  test("halting by voting") {
    sc = new SparkContext("local", "test")
    val verts = sc.parallelize(Array("a", "b", "c", "d").map(id => (id, new TestVertex(true, 0))))
    val msgs = sc.parallelize(Array[(String, TestMessage)]())
    val numSupersteps = 5
    val result =
      Bagel.run(sc, verts, msgs, sc.defaultParallelism) {
        (self: TestVertex, msgs: Option[Array[TestMessage]], superstep: Int) =>
          (new TestVertex(superstep < numSupersteps - 1, self.age + 1), Array[TestMessage]())
      }
    for ((id, vert) <- result.collect) {
      assert(vert.age === numSupersteps)
    }
  }

  test("halting by message silence") {
    sc = new SparkContext("local", "test")
    val verts = sc.parallelize(Array("a", "b", "c", "d").map(id => (id, new TestVertex(false, 0))))
    val msgs = sc.parallelize(Array("a" -> new TestMessage("a")))
    val numSupersteps = 5
    val result =
      Bagel.run(sc, verts, msgs, sc.defaultParallelism) {
        (self: TestVertex, msgs: Option[Array[TestMessage]], superstep: Int) =>
          val msgsOut =
            msgs match {
              case Some(ms) if (superstep < numSupersteps - 1) =>
                ms
              case _ =>
                Array[TestMessage]()
            }
        (new TestVertex(self.active, self.age + 1), msgsOut)
      }
    for ((id, vert) <- result.collect) {
      assert(vert.age === numSupersteps)
    }
  }

  test("large number of iterations") {
    // This tests whether jobs with a large number of iterations finish in a reasonable time,
    // because non-memoized recursion in RDD or DAGScheduler used to cause them to hang
    failAfter(10 seconds) {
      sc = new SparkContext("local", "test")
      val verts = sc.parallelize((1 to 4).map(id => (id.toString, new TestVertex(true, 0))))
      val msgs = sc.parallelize(Array[(String, TestMessage)]())
      val numSupersteps = 50
      val result =
        Bagel.run(sc, verts, msgs, sc.defaultParallelism) {
          (self: TestVertex, msgs: Option[Array[TestMessage]], superstep: Int) =>
            (new TestVertex(superstep < numSupersteps - 1, self.age + 1), Array[TestMessage]())
        }
      for ((id, vert) <- result.collect) {
        assert(vert.age === numSupersteps)
      }
    }
  }

  test("using non-default persistence level") {
    failAfter(10 seconds) {
      sc = new SparkContext("local", "test")
      val verts = sc.parallelize((1 to 4).map(id => (id.toString, new TestVertex(true, 0))))
      val msgs = sc.parallelize(Array[(String, TestMessage)]())
      val numSupersteps = 50
      val result =
        Bagel.run(sc, verts, msgs, sc.defaultParallelism, StorageLevel.DISK_ONLY) {
          (self: TestVertex, msgs: Option[Array[TestMessage]], superstep: Int) =>
            (new TestVertex(superstep < numSupersteps - 1, self.age + 1), Array[TestMessage]())
        }
      for ((id, vert) <- result.collect) {
        assert(vert.age === numSupersteps)
      }
    }
  }
}
