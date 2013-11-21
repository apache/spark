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

  val testData2 =
    logical.LocalRelation('a.int, 'b.int).loadData(
      (1, 1) ::
      (1, 2) ::
      (2, 1) ::
      (2, 2) ::
      (3, 1) ::
      (3, 2) :: Nil
    )

  test("table scan") {
    assert(
      testData.toRdd.collect().toSeq === testData.data.map(_.productIterator.toIndexedSeq))
  }

  test("simple select") {
    assert(
      testData.where('key === 1).select('value).toRdd.collect().toSeq ===
      Seq(Seq("1")))
  }

  test("random sample") {
    testData.where(Rand > 0.5).orderBy(Rand.asc).toRdd.collect()
  }

  test("sorting") {
    assert(testData2.orderBy('a.asc, 'b.asc).toRdd.collect().toSeq === Seq((1,1), (1,2), (2,1), (2,2), (3,1), (3,2)).map(_.productIterator.toIndexedSeq))
    assert(testData2.orderBy('a.asc, 'b.desc).toRdd.collect().toSeq === Seq((1,2), (1,1), (2,2), (2,1), (3,2), (3,1)).map(_.productIterator.toIndexedSeq))
    assert(testData2.orderBy('a.desc, 'b.desc).toRdd.collect().toSeq === Seq((3,2), (3,1), (2,2), (2,1), (1,2), (1,1)).map(_.productIterator.toIndexedSeq))
    assert(testData2.orderBy('a.desc, 'b.asc).toRdd.collect().toSeq === Seq((3,1), (3,2), (2,1), (2,2), (1,1), (1,2)).map(_.productIterator.toIndexedSeq))
  }
}