package org.apache.spark.sql.catalyst.optimizer

<<<<<<< Updated upstream
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.ExpressionEvalHelper
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.StringType

class CollectionRulesSuite extends PlanTest with ExpressionEvalHelper {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Collection rules", FixedPoint(10),
      CombineCollectionFilters,
      CombineCollectionTransforms,
      PushDownArrayFilter,
      EliminateArraySort) :: Nil
  }

  val testRelation = LocalRelation('a.array(StringType))

  test("Combine array transforms") {
    val plan = testRelation
      .analyze


    val actual = Optimize.execute(plan)
    val correctAnswer = testRelation.analyze
    comparePlans(actual, correctAnswer)
  }
=======
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class CombineCollectionFiltersSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
>>>>>>> Stashed changes

}
