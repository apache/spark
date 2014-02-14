package org.apache.spark.sql
package shark
package execution

import scala.collection.JavaConversions._

import TestShark._

class PartitionPruningSuite extends HiveComparisonTest {
  createPruningTest("Pruning with predicate on STRING partition key",
    "SELECT * FROM srcpart1 WHERE ds = '2008-04-08'",
    Seq(
      Seq("2008-04-08", "11"),
      Seq("2008-04-08", "12")))

  createPruningTest("Pruning with predicate on INT partition key",
    "SELECT * FROM srcpart1 WHERE hr < 12",
    Seq(
      Seq("2008-04-08", "11"),
      Seq("2008-04-09", "11")))

  createPruningTest("Select only 1 partition",
    "SELECT * FROM srcpart1 WHERE ds = '2008-04-08' AND hr < 12",
    Seq(
      Seq("2008-04-08", "11")))

  createPruningTest("All partitions pruned",
    "SELECT * FROM srcpart1 WHERE ds = '2014-01-27' AND hr = 11",
    Seq.empty)

  createPruningTest("Pruning with both column key and partition key",
    "SELECT * FROM srcpart1 WHERE value IS NOT NULL AND hr < 12",
    Seq(
      Seq("2008-04-08", "11"),
      Seq("2008-04-09", "11")))

  def createPruningTest(testCaseName: String, sql: String, expectedValues: Seq[Seq[String]]) = {
    test(testCaseName) {
      val plan = new TestShark.SqlQueryExecution(sql).executedPlan
      val prunedPartitions = plan.collect {
        case p @ HiveTableScan(_, relation, _) =>
          p.prunePartitions(relation.hiveQlPartitions)
      }.head
      val values = prunedPartitions.map(_.getValues)

      assert(prunedPartitions.size === expectedValues.size)

      for ((actual, expected) <- values.zip(expectedValues)) {
        assert(actual sameElements expected)
      }
    }
  }
}
