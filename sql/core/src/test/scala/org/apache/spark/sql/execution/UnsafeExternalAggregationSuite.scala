package org.apache.spark.sql.execution

import org.apache.spark.SparkEnv
import org.apache.spark.sql.{QueryTest, TestData, Row, SQLConf}
import org.scalatest.BeforeAndAfterAll

class UnsafeExternalAggregationSuite extends QueryTest  with BeforeAndAfterAll {

  TestData
  val sqlContext = org.apache.spark.sql.test.TestSQLContext
  import sqlContext.sql

  test("aggregation with codegen") {
    val originalValue = sqlContext.conf.codegenEnabled
    sqlContext.setConf(SQLConf.CODEGEN_ENABLED, true)
    val unsafeOriginalValue = sqlContext.conf.unsafeEnabled
    sqlContext.setConf(SQLConf.UNSAFE_ENABLED, true)
    SparkEnv.get.conf.set("spark.test.aggregate.spillFrequency","5")
    // Prepare a table that we can group some rows.
    sqlContext.table("testData")
      .unionAll(sqlContext.table("testData"))
      .unionAll(sqlContext.table("testData"))
      .registerTempTable("testData3x")

    def testCodeGen(sqlText: String, expectedResults: Seq[Row]): Unit = {
      val df = sql(sqlText)
      // First, check if we have GeneratedAggregate.
      var hasGeneratedAgg = false
      df.queryExecution.executedPlan.foreach {
        case generatedAgg: GeneratedAggregate => hasGeneratedAgg = true
        case _ =>
      }
      if (!hasGeneratedAgg) {
        fail(
          s"""
             |Codegen is enabled, but query $sqlText does not have GeneratedAggregate in the plan.
             |${df.queryExecution.simpleString}
           """.stripMargin)
      }
      // Then, check results.
      checkAnswer(df, expectedResults)
    }

    try {
      // Just to group rows.
      testCodeGen(
        "SELECT key FROM testData3x GROUP BY key",
        (1 to 100).map(Row(_)))
      // COUNT
      testCodeGen(
        "SELECT key, count(value) FROM testData3x GROUP BY key",
        (1 to 100).map(i => Row(i, 3)))
      testCodeGen(
        "SELECT count(key) FROM testData3x",
        Row(300) :: Nil)
      // COUNT DISTINCT ON int
      testCodeGen(
        "SELECT value, count(distinct key) FROM testData3x GROUP BY value",
        (1 to 100).map(i => Row(i.toString, 1)))
      testCodeGen(
        "SELECT count(distinct key) FROM testData3x",
        Row(100) :: Nil)
      // SUM
      testCodeGen(
        "SELECT value, sum(key) FROM testData3x GROUP BY value",
        (1 to 100).map(i => Row(i.toString, 3 * i)))
      testCodeGen(
        "SELECT sum(key), SUM(CAST(key as Double)) FROM testData3x",
        Row(5050 * 3, 5050 * 3.0) :: Nil)
      // AVERAGE
      testCodeGen(
        "SELECT value, avg(key) FROM testData3x GROUP BY value",
        (1 to 100).map(i => Row(i.toString, i)))
      testCodeGen(
        "SELECT avg(key) FROM testData3x",
        Row(50.5) :: Nil)
      // MAX
      testCodeGen(
        "SELECT value, max(key) FROM testData3x GROUP BY value",
        (1 to 100).map(i => Row(i.toString, i)))
      testCodeGen(
        "SELECT max(key) FROM testData3x",
        Row(100) :: Nil)
      // MIN
      testCodeGen(
        "SELECT value, min(key) FROM testData3x GROUP BY value",
        (1 to 100).map(i => Row(i.toString, i)))
      testCodeGen(
        "SELECT min(key) FROM testData3x",
        Row(1) :: Nil)
      // Some combinations.
      testCodeGen(
        """
          |SELECT
          |  value,
          |  sum(key),
          |  max(key),
          |  min(key),
          |  avg(key),
          |  count(key),
          |  count(distinct key)
          |FROM testData3x
          |GROUP BY value
        """.stripMargin,
        (1 to 100).map(i => Row(i.toString, i*3, i, i, i, 3, 1)))
      testCodeGen(
        "SELECT max(key), min(key), avg(key), count(key), count(distinct key) FROM testData3x",
        Row(100, 1, 50.5, 300, 100) :: Nil)
      // Aggregate with Code generation handling all null values
      testCodeGen(
        "SELECT  sum('a'), avg('a'), count(null) FROM testData",
        Row(0, null, 0) :: Nil)
    } finally {
      sqlContext.dropTempTable("testData3x")
      sqlContext.setConf(SQLConf.CODEGEN_ENABLED, originalValue)
      sqlContext.setConf(SQLConf.UNSAFE_ENABLED, unsafeOriginalValue)
      SparkEnv.get.conf.set("spark.test.aggregate.spillFrequency","0")
    }
  }
}
