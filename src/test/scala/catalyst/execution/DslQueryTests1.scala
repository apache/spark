package catalyst
package execution

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import analysis._
import expressions._
import plans._
import plans.logical.LogicalPlan
import types._

/* Implicits */
import dsl._

class DslQueryTests1 extends FunSuite with BeforeAndAfterAll {
  override def beforeAll() {
    // By clearing the port we force Spark to pick a new one.  This allows us to rerun tests
    // without restarting the JVM.
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
  }

  import TestShark._
  import TestData._

  test("inner join and then group by using the same column") {
    val x = testData2.subquery('x)
    val y = testData3.subquery('y)
    checkAnswer(
      x.join(y).where("x.a".attr === "y.a".attr)
        .groupBy("x.a".attr)("x.a".attr, Count("x.b".attr)),
      (1,2) ::
      (2,2) :: Nil
    )
  }

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * @param plan the query to be executed
   * @param expectedAnswer the expected result, can either be an Any, Seq[Product], or Seq[ Seq[Any] ].
   */
  protected def checkAnswer(plan: LogicalPlan, expectedAnswer: Any): Unit = {
    val convertedAnswer = expectedAnswer match {
      case s: Seq[_] if s.isEmpty => s
      case s: Seq[_] if s.head.isInstanceOf[Product] &&
        !s.head.isInstanceOf[Seq[_]] => s.map(_.asInstanceOf[Product].productIterator.toIndexedSeq)
      case s: Seq[_] => s
      case singleItem => Seq(Seq(singleItem))
    }

    val isSorted = plan.collect { case s: logical.Sort => s}.nonEmpty
    def prepareAnswer(answer: Seq[Any]) = if (!isSorted) answer.sortBy(_.toString) else answer
    val sharkAnswer = try plan.toRdd.collect().toSeq catch {
      case e: Exception =>
        fail(
          s"""
            |Exception thrown while executing query:
            |$plan
            |== Physical Plan ==
            |${plan.executedPlan}
            |== Exception ==
            |$e
          """.stripMargin)
    }
    println(
      s"""
      |Logical plan:
      |$plan
      |== Physical Plan ==
      |${plan.executedPlan}
      """.stripMargin)
    assert(prepareAnswer(convertedAnswer) === prepareAnswer(sharkAnswer))
  }
}