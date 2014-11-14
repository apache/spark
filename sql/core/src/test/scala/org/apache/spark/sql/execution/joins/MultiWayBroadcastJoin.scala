package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.TestSQLContext

case class Data(key: Int, value: String)

class MultiWayBroadcastJoinSuite extends QueryTest {
  import TestSQLContext._



  test("multi way join") {
    sparkContext.parallelize(1 to 100).map(i => Data(i % 10, s"Large$i")).registerTempTable("largeTable")

    sparkContext.parallelize(0 to 9).map(i => Data(i, s"A$i")).registerTempTable("smallA")
    sparkContext.parallelize(0 to 9).map(i => Data(i, s"B$i")).registerTempTable("smallB")

    cacheTable("smallA")
    cacheTable("smallB")

    table("smallA").count()
    table("smallB").count()

    val query = sql(
      """
        |SELECT large.key, a.value, b.value
        |FROM largeTable large
        |JOIN smallA a
        |JOIN smallB b
        |WHERE large.key = a.key
        |  AND large.key = b.key
      """.stripMargin)


    println(table("smallA").queryExecution.analyzed.statistics)
    println(TestSQLContext.autoBroadcastJoinThreshold)

    val multiWayJoins = query.queryExecution.executedPlan.collect {
      case m if m.nodeName contains "MultiWay" => m
    }

    val otherJoins = query.queryExecution.executedPlan.collect {
      case j if j.nodeName contains "Join" && !(j.nodeName contains "MultiWay") => j
    }

    if (multiWayJoins.size != 1 || otherJoins.size != 0) {
      fail(s"Not using multi-way join\n${query.queryExecution}")
    }

    checkAnswer(query, (1 to 100).map(i => Row(i % 10, s"A${i % 10}", s"B${i % 10}")))
  }

  test("second run") {
    val query = sql(
      """
        |SELECT large.key, a.value, b.value
        |FROM largeTable large
        |JOIN smallA a
        |JOIN smallB b
        |WHERE large.key = a.key
        |  AND large.key = b.key
      """.stripMargin)
    checkAnswer(query, (1 to 100).map(i => Row(i % 10, s"A${i % 10}", s"B${i % 10}")))
  }
}