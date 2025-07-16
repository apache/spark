/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.jdbc.v2.join

import java.sql.Connection

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.{DockerIntegrationFunSuite, JdbcDialect}
import org.apache.spark.sql.jdbc.v2.V2JDBCPushdownTestUtils
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, DataTypes}
import org.apache.spark.tags.DockerTest

@DockerTest
trait JDBCJoinPushdownIntegrationSuite
  extends QueryTest
  with SharedSparkSession
  with DockerIntegrationFunSuite
  with V2JDBCPushdownTestUtils {
  val catalogName: String
  val namespaceOpt: Option[String] = None
  val joinTableName1: String = "join_table_1"
  val joinTableName2: String = "join_table_2"

  // Concrete suite must provide the dialect for its DB
  def jdbcDialect: JdbcDialect

  private def catalogAndNamespace =
    namespaceOpt.map(namespace => s"$catalogName.$namespace").getOrElse(catalogName)

  def fullyQualifiedTableName1: String = namespaceOpt
    .map(namespace => s"$namespace.$joinTableName1").getOrElse(joinTableName1)

  def fullyQualifiedTableName2: String = namespaceOpt
    .map(namespace => s"$namespace.$joinTableName2").getOrElse(joinTableName2)

  protected def getJDBCTypeString(dt: DataType): String = {
    JdbcUtils.getJdbcType(dt, jdbcDialect).databaseTypeDefinition.toUpperCase()
  }

  protected def caseConvert(tableName: String): String = tableName

  def dataPreparation(connection: Connection): Unit = {
    tablePreparation(connection)
    fillJoinTables(connection)
  }

  def tablePreparation(connection: Connection): Unit = {
    connection.prepareStatement(
      s"""CREATE TABLE $fullyQualifiedTableName1 (
         |  id ${getJDBCTypeString(DataTypes.IntegerType)},
         |  amount ${getJDBCTypeString(DataTypes.createDecimalType(10, 2))},
         |  name ${getJDBCTypeString(DataTypes.StringType)}
         |)""".stripMargin).executeUpdate()

    connection.prepareStatement(
      s"""CREATE TABLE $fullyQualifiedTableName2 (
         |  id ${getJDBCTypeString(DataTypes.LongType)},
         |  salary ${getJDBCTypeString(DataTypes.createDecimalType(10, 2))},
         |  surname ${getJDBCTypeString(DataTypes.StringType)}
         |)""".stripMargin).executeUpdate()
  }

  def fillJoinTables(connection: Connection): Unit = {
    val random = new java.util.Random(42)
    val table1Data = (1 to 100).map { i =>
      val id = i % 11
      val amount = BigDecimal.valueOf(random.nextDouble() * 10000)
        .setScale(2, BigDecimal.RoundingMode.HALF_UP)
      val name = s"name_$i"
      (id, amount, name)
    }
    val table2Data = (1 to 100).map { i =>
      val id = (i % 17).toLong
      val salary = BigDecimal.valueOf(random.nextDouble() * 50000)
        .setScale(2, BigDecimal.RoundingMode.HALF_UP)
      val surname = s"surname_$i"
      (id, salary, surname)
    }

    // Use parameterized queries to handle different data types properly
    val insertStmt1 = connection.prepareStatement(
      s"INSERT INTO $fullyQualifiedTableName1 (id, amount, name) VALUES (?, ?, ?)"
    )
    table1Data.foreach { case (id, amount, name) =>
      insertStmt1.setInt(1, id)
      insertStmt1.setBigDecimal(2, amount.bigDecimal)
      insertStmt1.setString(3, name)
      insertStmt1.executeUpdate()
    }
    insertStmt1.close()

    val insertStmt2 = connection.prepareStatement(
      s"INSERT INTO $fullyQualifiedTableName2 (id, salary, surname) VALUES (?, ?, ?)"
    )
    table2Data.foreach { case (id, salary, surname) =>
      insertStmt2.setLong(1, id)
      insertStmt2.setBigDecimal(2, salary.bigDecimal)
      insertStmt2.setString(3, surname)
      insertStmt2.executeUpdate()
    }
    insertStmt2.close()
  }

  /**
   * Runs the plan and makes sure the plans contains all of the keywords.
   */
  protected def checkKeywordsExistsInExplain(df: DataFrame, keywords: String*): Unit = {
    val output = new java.io.ByteArrayOutputStream()
    Console.withOut(output) {
      df.explain(extended = true)
    }
    val normalizedOutput = output.toString.replaceAll("#\\d+", "#x")
    for (key <- keywords) {
      assert(normalizedOutput.contains(key), s"Expected keyword '$key' not found in explain output")
    }
  }

  private def checkPushedInfo(df: DataFrame, expectedPlanFragment: String*): Unit = {
    withSQLConf(SQLConf.MAX_METADATA_STRING_LENGTH.key -> "1000") {
      df.queryExecution.optimizedPlan.collect {
        case _: DataSourceV2ScanRelation =>
          checkKeywordsExistsInExplain(df, expectedPlanFragment: _*)
      }
    }
  }

  private def checkJoinNotPushed(df: DataFrame): Unit = {
    val joinNodes = df.queryExecution.optimizedPlan.collect {
      case j: Join => j
    }
    assert(joinNodes.nonEmpty, "Join should not be pushed down")
  }

  private def checkJoinPushed(df: DataFrame, expectedTables: String*): Unit = {
    val joinNodes = df.queryExecution.optimizedPlan.collect {
      case j: Join => j
    }
    assert(joinNodes.isEmpty, "Join should be pushed down")
    if (expectedTables.nonEmpty) {
      checkPushedInfo(df, s"PushedJoins: [${expectedTables.mkString(", ")}]")
    }
  }

  test("Test basic inner join pushdown with column pruning") {
    val sqlQuery = s"""
      |SELECT t1.id, t1.name, t2.surname, t1.amount, t2.salary
      |FROM $catalogAndNamespace.${caseConvert(joinTableName1)} t1
      |JOIN $catalogAndNamespace.${caseConvert(joinTableName2)} t2 ON t1.id = t2.id
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    // Verify we have non-empty results
    assert(rows.nonEmpty, "Join should produce non-empty results")

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)
      checkJoinPushed(
        df,
        s"$catalogAndNamespace.${caseConvert(joinTableName1)}",
        s"$catalogAndNamespace.${caseConvert(joinTableName2)}"
      )
      checkAnswer(df, rows)
    }
  }


  test("Test join with additional filters") {
    val sqlQuery = s"""
      |SELECT t1.id, t1.name, t2.surname, t1.amount, t2.salary
      |FROM $catalogAndNamespace.${caseConvert(joinTableName1)} t1
      |JOIN $catalogAndNamespace.${caseConvert(joinTableName2)} t2 ON t1.id = t2.id
      |WHERE t1.amount > 5000 AND t2.salary > 25000
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)
      checkJoinPushed(
        df,
        s"$catalogAndNamespace.${caseConvert(joinTableName1)}",
        s"$catalogAndNamespace.${caseConvert(joinTableName2)}"
      )
      checkFilterPushed(df)
      checkAnswer(df, rows)
    }
  }

  test("Test self join should be pushed down") {
    val sqlQuery = s"""
      |SELECT t1.id, t1.name, t2.name as name2
      |FROM $catalogAndNamespace.${caseConvert(joinTableName1)} t1
      |JOIN $catalogAndNamespace.${caseConvert(joinTableName1)} t2 ON t1.id = t2.id
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)
      checkJoinPushed(
        df,
        s"$catalogAndNamespace.${caseConvert(joinTableName1)}",
        s"$catalogAndNamespace.${caseConvert(joinTableName1)}"
      )
      checkAnswer(df, rows)
    }
  }

  test("Test join without condition should not be pushed down") {
    val sqlQuery = s"""
      |SELECT t1.id, t1.name, t2.surname
      |FROM $catalogAndNamespace.${caseConvert(joinTableName1)} t1
      |CROSS JOIN $catalogAndNamespace.${caseConvert(joinTableName2)} t2
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)
      checkJoinNotPushed(df)
      checkAnswer(df, rows)
    }
  }

  test("Test join with complex condition") {
    val sqlQuery = s"""
      |SELECT t1.id, t1.name, t2.surname, t1.amount + t2.salary as total
      |FROM $catalogAndNamespace.${caseConvert(joinTableName1)} t1
      |JOIN $catalogAndNamespace.${caseConvert(joinTableName2)} t2
      |ON t1.id = t2.id AND t1.amount > 1000
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)
      checkJoinPushed(
        df,
        s"$catalogAndNamespace.${caseConvert(joinTableName1)}",
        s"$catalogAndNamespace.${caseConvert(joinTableName2)}"
      )
      checkAnswer(df, rows)
    }
  }

  test("Test join with limit and order") {
    // ORDER BY is used to have same ordering on Spark and database. Otherwise, different results
    // could be returned.
    val sqlQuery = s"""
      |SELECT t1.id, t1.name, t2.surname
      |FROM $catalogAndNamespace.${caseConvert(joinTableName1)} t1
      |JOIN $catalogAndNamespace.${caseConvert(joinTableName2)} t2 ON t1.id = t2.id
      |ORDER BY t1.id, t1.name, t2.surname
      |LIMIT 5
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)
      checkJoinPushed(
        df,
        s"$catalogAndNamespace.${caseConvert(joinTableName1)}",
        s"$catalogAndNamespace.${caseConvert(joinTableName2)}"
      )
      checkSortRemoved(df)
      checkLimitRemoved(df)
      checkAnswer(df, rows)
    }
  }

  test("Test join with order by") {
    val sqlQuery = s"""
      |SELECT t1.id, t1.name, t2.surname
      |FROM $catalogAndNamespace.${caseConvert(joinTableName1)} t1
      |JOIN $catalogAndNamespace.${caseConvert(joinTableName2)} t2 ON t1.id = t2.id
      |ORDER BY t1.id, t1.name, t2.surname
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)
      checkJoinPushed(
        df,
        s"$catalogAndNamespace.${caseConvert(joinTableName1)}",
        s"$catalogAndNamespace.${caseConvert(joinTableName2)}"
      )
      // Order without limit is not supported in DSv2
      checkSortRemoved(df, false)
      checkAnswer(df, rows)
    }
  }

  test("Test join with aggregation") {
    val sqlQuery = s"""
      |SELECT t1.id, COUNT(*), AVG(t1.amount), MAX(t2.salary)
      |FROM $catalogAndNamespace.${caseConvert(joinTableName1)} t1
      |JOIN $catalogAndNamespace.${caseConvert(joinTableName2)} t2 ON t1.id = t2.id
      |GROUP BY t1.id
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)
      checkJoinPushed(
        df,
        s"$catalogAndNamespace.${caseConvert(joinTableName1)}",
        s"$catalogAndNamespace.${caseConvert(joinTableName2)}"
      )
      checkAggregateRemoved(df)
      checkAnswer(df, rows)
    }
  }

  test("Test left outer join should not be pushed down") {
    val sqlQuery = s"""
      |SELECT t1.id, t1.name, t2.surname
      |FROM $catalogAndNamespace.${caseConvert(joinTableName1)} t1
      |LEFT JOIN $catalogAndNamespace.${caseConvert(joinTableName2)} t2 ON t1.id = t2.id
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)
      checkJoinNotPushed(df)
      checkAnswer(df, rows)
    }
  }

  test("Test right outer join should not be pushed down") {
    val sqlQuery = s"""
      |SELECT t1.id, t1.name, t2.surname
      |FROM $catalogAndNamespace.${caseConvert(joinTableName1)} t1
      |RIGHT JOIN $catalogAndNamespace.${caseConvert(joinTableName2)} t2 ON t1.id = t2.id
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)
      checkJoinNotPushed(df)
      checkAnswer(df, rows)
    }
  }

  test("Test full outer join should not be pushed down") {
    val sqlQuery = s"""
      |SELECT t1.id, t1.name, t2.surname
      |FROM $catalogAndNamespace.${caseConvert(joinTableName1)} t1
      |FULL OUTER JOIN $catalogAndNamespace.${caseConvert(joinTableName2)} t2 ON t1.id = t2.id
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)
      checkJoinNotPushed(df)
      checkAnswer(df, rows)
    }
  }

  test("Test join with subquery should be pushed down") {
    val sqlQuery = s"""
      |SELECT t1.id, t1.name, sub.surname
      |FROM $catalogAndNamespace.${caseConvert(joinTableName1)} t1
      |JOIN (
      |  SELECT id, surname FROM $catalogAndNamespace.${caseConvert(joinTableName2)}
      |  WHERE salary > 25000
      |) sub ON t1.id = sub.id
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)
      checkJoinPushed(
        df,
        s"$catalogAndNamespace.${caseConvert(joinTableName1)}",
        s"$catalogAndNamespace.${caseConvert(joinTableName2)}"
      )
      checkAnswer(df, rows)
    }
  }

  test("Test join with non-equality condition should be pushed down") {
    val sqlQuery = s"""
      |SELECT t1.id, t1.name, t2.surname
      |FROM $catalogAndNamespace.${caseConvert(joinTableName1)} t1
      |JOIN $catalogAndNamespace.${caseConvert(joinTableName2)} t2 ON t1.id > t2.id
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)
      checkJoinPushed(
        df,
        s"$catalogAndNamespace.${caseConvert(joinTableName1)}",
        s"$catalogAndNamespace.${caseConvert(joinTableName2)}"
      )
      checkAnswer(df, rows)
    }
  }
}
