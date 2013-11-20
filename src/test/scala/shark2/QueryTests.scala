package catalyst
package shark2

import catalyst.analysis

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import analysis._
import expressions._
import plans.logical
import types._

import dsl._

class QueryTests extends FunSuite with BeforeAndAfterAll {
  override def beforeAll() {
    // By clearing the port we force Spark to pick a new one.  This allows us to rerun tests
    // without restarting the JVM.
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
  }

  val testShark = new TestShark
  import testShark._

  val testData =
    logical.LocalRelation('key.int, 'value.string)
      .loadData((1 to 100).map(i => (i, i.toString)))

  test("table scan") {
    assert(
      testData.toRdd.collect().toSeq === testData.data.map(_.productIterator.toIndexedSeq))
  }

  test("simple select") {
    assert(
      testData.where('key === 1).select('value).toRdd.collect().toSeq ===
      Seq(Seq("1")))
  }
}