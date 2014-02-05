package catalyst
package execution

import org.scalatest.FunSuite

import catalyst.expressions._
import catalyst.plans.logical
import catalyst.dsl._

class PlannerSuite extends FunSuite {
  import TestData._

  test("Unions are collapsed") {
    val query = testData.unionAll(testData).unionAll(testData)
    val planned = TestShark.TrivialPlanner.BasicOperators(query).head
    val logicalUnions = query collect { case u: logical.Union => u}
    val physicalUnions = planned collect { case u: execution.Union => u}

    assert(logicalUnions.size === 2)
    assert(physicalUnions.size === 1)
  }

  test("counts are partially aggregated") {
    val query = testData.groupBy('value)(Count('key)).analyze
    val planned = TestShark.TrivialPlanner.PartialAggregation(query).head
    val aggregations = planned.collect { case a: Aggregate => a }

    assert(aggregations.size === 2)
  }

  test("counts are not partially aggregated") {
    val query = testData.groupBy('value)(CountDistinct('key :: Nil)).analyze
    val planned = TestShark.TrivialPlanner.PartialAggregation(query)
    assert(planned.isEmpty)
  }
}