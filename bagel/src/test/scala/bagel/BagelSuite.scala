package bagel

import org.scalatest.{FunSuite, Assertions}
import org.scalatest.prop.Checkers
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import org.scalacheck.Prop._

import scala.collection.mutable.ArrayBuffer

import spark._

import bagel.Pregel._

@serializable class TestVertex(val id: String, val active: Boolean, val age: Int) extends Vertex
@serializable class TestMessage(val targetId: String) extends Message

class BagelSuite extends FunSuite with Assertions {
  test("halting by voting") {
    val sc = new SparkContext("local", "test")
    val verts = sc.parallelize(Array("a", "b", "c", "d").map(id => (id, new TestVertex(id, true, 0))))
    val msgs = sc.parallelize(Array[(String, TestMessage)]())
    val numSupersteps = 5
    val result =
      Pregel.run(sc, verts, msgs)()(addAggregatorArg {
      (self: TestVertex, msgs: Option[ArrayBuffer[TestMessage]], superstep: Int) =>
        (new TestVertex(self.id, superstep < numSupersteps - 1, self.age + 1), Array[TestMessage]())
    })
    for (vert <- result.collect)
      assert(vert.age === numSupersteps)
  }

  test("halting by message silence") {
    val sc = new SparkContext("local", "test")
    val verts = sc.parallelize(Array("a", "b", "c", "d").map(id => (id, new TestVertex(id, false, 0))))
    val msgs = sc.parallelize(Array("a" -> new TestMessage("a")))
    val numSupersteps = 5
    val result =
      Pregel.run(sc, verts, msgs)()(addAggregatorArg {
      (self: TestVertex, msgs: Option[ArrayBuffer[TestMessage]], superstep: Int) =>
        val msgsOut =
          msgs match {
            case Some(ms) if (superstep < numSupersteps - 1) =>
              ms
            case _ =>
              new ArrayBuffer[TestMessage]()
          }
        (new TestVertex(self.id, self.active, self.age + 1), msgsOut)
    })
    for (vert <- result.collect)
      assert(vert.age === numSupersteps)
  }
}
