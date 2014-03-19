package org.apache.spark.sql.columnar

import org.apache.spark.sql.{TestData, TestSqlContext, DslQueryTest}
import org.apache.spark.sql.execution.SparkLogicalPlan

class ColumnarQuerySuite extends DslQueryTest {
  import TestData._

  test("simple columnar query") {
    val plan = TestSqlContext.executePlan(testData).executedPlan
    val attributes = plan.output
    val relation = InMemoryColumnarRelation("t1", plan)
    val scan = SparkLogicalPlan(InMemoryColumnarTableScan(attributes, relation))

    checkAnswer(scan, testData.data)
  }
}
