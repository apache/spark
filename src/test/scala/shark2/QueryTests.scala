package catalyst
package shark2

import catalyst.analysis

import catalyst.plans.logical.LogicalPlan
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
    checkAnswer(
      testData,
      testData.data)
  }

  test("simple select") {
    checkAnswer(
      testData.where('key === 1).select('value),
      Seq(Seq("1")))
  }

  test("random sample") {
    testData.where(Rand > 0.5).orderBy(Rand.asc).toRdd.collect()
  }

  test("sorting") {
    checkAnswer(
      testData2.orderBy('a.asc, 'b.asc),
      Seq((1,1), (1,2), (2,1), (2,2), (3,1), (3,2)))

    checkAnswer(
      testData2.orderBy('a.asc, 'b.desc),
      Seq((1,2), (1,1), (2,2), (2,1), (3,2), (3,1)))

    checkAnswer(
      testData2.orderBy('a.desc, 'b.desc),
      Seq((3,2), (3,1), (2,2), (2,1), (1,2), (1,1)))

    checkAnswer(
      testData2.orderBy('a.desc, 'b.asc),
      Seq((3,1), (3,2), (2,1), (2,2), (1,1), (1,2)))
  }


  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * @param plan the query to be executed
   * @param expectedAnswer the expected result, can either be Seq[Product] or Seq[Seq[Any]]
   */
  protected def checkAnswer(plan: LogicalPlan, expectedAnswer: Seq[Any]): Unit = {
    val convertedAnswer =
      if(expectedAnswer.isEmpty)
        expectedAnswer
      else if(expectedAnswer.head.isInstanceOf[Product] && !expectedAnswer.head.isInstanceOf[::[_]])
        expectedAnswer.map(_.asInstanceOf[Product].productIterator.toSeq)
      else
        expectedAnswer

    val sharkAnswer = plan.toRdd.collect().toSeq
    assert(convertedAnswer === sharkAnswer)
  }
}