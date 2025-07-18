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

package org.apache.spark.sql.jdbc.v2

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.connector.DataSourcePushdownTestUtils
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, DataTypes}

trait JDBCV2JoinPushdownIntegrationSuiteBase
  extends QueryTest
  with SharedSparkSession
  with DataSourcePushdownTestUtils {
  val catalogName: String = "join_pushdown_catalog"
  val namespaceOpt: Option[String] = None
  val url: String

  val joinTableName1: String = "join_table_1"
  val joinTableName2: String = "join_table_2"

  val jdbcDialect: JdbcDialect

  override def sparkConf: SparkConf = super.sparkConf
    .set(s"spark.sql.catalog.$catalogName", classOf[JDBCTableCatalog].getName)
    .set(s"spark.sql.catalog.$catalogName.url", url)
    .set(s"spark.sql.catalog.$catalogName.pushDownJoin", "true")
    .set(s"spark.sql.catalog.$catalogName.pushDownAggregate", "true")
    .set(s"spark.sql.catalog.$catalogName.pushDownLimit", "true")
    .set(s"spark.sql.catalog.$catalogName.pushDownOffset", "true")

  private def catalogAndNamespace =
    namespaceOpt.map(namespace => s"$catalogName.$namespace").getOrElse(catalogName)

  def qualifyTableName(tableName: String): String = namespaceOpt
    .map(namespace => s"$namespace.$tableName").getOrElse(tableName)

  private lazy val fullyQualifiedTableName1: String = qualifyTableName(joinTableName1)

  private lazy val fullyQualifiedTableName2: String = qualifyTableName(joinTableName2)

  protected def getJDBCTypeString(dt: DataType): String = {
    JdbcUtils.getJdbcType(dt, jdbcDialect).databaseTypeDefinition.toUpperCase()
  }

  protected def caseConvert(tableName: String): String = tableName

  protected def withConnection[T](f: Connection => T): T = {
    val conn = DriverManager.getConnection(url, new Properties())
    try {
      f(conn)
    } finally {
      conn.close()
    }
  }

  def dataPreparation(connection: Connection): Unit = {
    schemaPreparation(connection)
    tablePreparation(connection)
    fillJoinTables(connection)
  }

  def schemaPreparation(connection: Connection): Unit = {
    connection.prepareStatement(s"CREATE SCHEMA IF NOT EXISTS ${namespaceOpt.get}").executeUpdate()
  }

  def tablePreparation(connection: Connection): Unit = {
    connection.prepareStatement(
      s"""CREATE TABLE $fullyQualifiedTableName1 (
         |  ID ${getJDBCTypeString(DataTypes.IntegerType)},
         |  AMOUNT ${getJDBCTypeString(DataTypes.createDecimalType(10, 2))},
         |  ADDRESS ${getJDBCTypeString(DataTypes.StringType)}
         |)""".stripMargin
    ).executeUpdate()

    connection.prepareStatement(
      s"""CREATE TABLE $fullyQualifiedTableName2 (
         |  ID ${getJDBCTypeString(DataTypes.IntegerType)},
         |  NEXT_ID ${getJDBCTypeString(DataTypes.IntegerType)},
         |  SALARY ${getJDBCTypeString(DataTypes.createDecimalType(10, 2))},
         |  SURNAME ${getJDBCTypeString(DataTypes.StringType)}
         |)""".stripMargin
    ).executeUpdate()
  }

  def fillJoinTables(connection: Connection): Unit = {
    val random = new java.util.Random(42)
    val table1Data = (1 to 100).map { i =>
      val id = i % 11
      val amount = BigDecimal.valueOf(random.nextDouble() * 10000)
        .setScale(2, BigDecimal.RoundingMode.HALF_UP)
      val address = s"address_$i"
      (id, amount, address)
    }
    val table2Data = (1 to 100).map { i =>
      val id = (i % 17)
      val next_id = (id + 1) % 17
      val salary = BigDecimal.valueOf(random.nextDouble() * 50000)
        .setScale(2, BigDecimal.RoundingMode.HALF_UP)
      val surname = s"surname_$i"
      (id, next_id, salary, surname)
    }

    val insertStmt1 = connection.prepareStatement(
      s"INSERT INTO $fullyQualifiedTableName1 (id, amount, address) VALUES (?, ?, ?)"
    )
    table1Data.foreach { case (id, amount, address) =>
      insertStmt1.setInt(1, id)
      insertStmt1.setBigDecimal(2, amount.bigDecimal)
      insertStmt1.setString(3, address)
      insertStmt1.executeUpdate()
    }
    insertStmt1.close()

    val insertStmt2 = connection.prepareStatement(
      s"INSERT INTO $fullyQualifiedTableName2 (id, next_id, salary, surname) VALUES (?, ?, ?, ?)"
    )
    table2Data.foreach { case (id, next_id, salary, surname) =>
      insertStmt2.setInt(1, id)
      insertStmt2.setInt(2, next_id)
      insertStmt2.setBigDecimal(3, salary.bigDecimal)
      insertStmt2.setString(4, surname)
      insertStmt2.executeUpdate()
    }
    insertStmt2.close()
  }

  // Condition-less joins are not supported in join pushdown
  test("Test that 2-way join without condition should not have join pushed down") {
    val sqlQuery =
      s"""
         |SELECT * FROM
         |$catalogAndNamespace.${caseConvert(joinTableName1)} a,
         |$catalogAndNamespace.${caseConvert(joinTableName1)} b
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

  // Condition-less joins are not supported in join pushdown
  test("Test that multi-way join without condition should not have join pushed down") {
    val sqlQuery = s"""
      |SELECT * FROM
      |$catalogAndNamespace.${caseConvert(joinTableName1)} a,
      |$catalogAndNamespace.${caseConvert(joinTableName1)} b,
      |$catalogAndNamespace.${caseConvert(joinTableName1)} c
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

  test("Test self join with condition") {
    val sqlQuery = s"""
      |SELECT * FROM $catalogAndNamespace.${caseConvert(joinTableName1)} a
      |JOIN $catalogAndNamespace.${caseConvert(joinTableName1)} b
      |ON a.id = b.id + 1""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)

      checkJoinPushed(
        df,
        s"$catalogAndNamespace.${caseConvert(joinTableName1)}, " +
          s"$catalogAndNamespace.${caseConvert(joinTableName1)}"
      )
      checkAnswer(df, rows)
    }
  }

  test("Test multi-way self join with conditions") {
    val sqlQuery = s"""
      |SELECT * FROM
      |$catalogAndNamespace.${caseConvert(joinTableName1)} a
      |JOIN $catalogAndNamespace.${caseConvert(joinTableName1)} b ON b.id = a.id + 1
      |JOIN $catalogAndNamespace.${caseConvert(joinTableName1)} c ON c.id = b.id - 1
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    assert(!rows.isEmpty)

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)

      checkJoinPushed(df,
        s"$catalogAndNamespace.${caseConvert(joinTableName1)}, " +
          s"$catalogAndNamespace.${caseConvert(joinTableName1)}, " +
          s"$catalogAndNamespace.${caseConvert(joinTableName1)}"
      )
      checkAnswer(df, rows)
    }
  }

  test("Test self join with column pruning") {
    val sqlQuery = s"""
      |SELECT a.id + 2, b.id, b.amount FROM
      |$catalogAndNamespace.${caseConvert(joinTableName1)} a
      |JOIN $catalogAndNamespace.${caseConvert(joinTableName1)} b
      |ON a.id = b.id + 1
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)

      checkJoinPushed(
        df,
        s"$catalogAndNamespace.${caseConvert(joinTableName1)}, " +
          s"$catalogAndNamespace.${caseConvert(joinTableName1)}"
      )
      checkAnswer(df, rows)
    }
  }

  test("Test 2-way join with column pruning - different tables") {
    val sqlQuery = s"""
      |SELECT a.id, b.next_id FROM
      |$catalogAndNamespace.${caseConvert(joinTableName1)} a
      |JOIN $catalogAndNamespace.${caseConvert(joinTableName2)} b
      |ON a.id = b.next_id
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)

      checkJoinPushed(
        df,
        s"$catalogAndNamespace.${caseConvert(joinTableName1)}," +
          s" $catalogAndNamespace.${caseConvert(joinTableName2)}"
      )
      checkPushedInfo(df,
        "PushedFilters: [ID IS NOT NULL, NEXT_ID IS NOT NULL, ID = NEXT_ID]")
      checkAnswer(df, rows)
    }
  }

  test("Test multi-way self join with column pruning") {
    val sqlQuery = s"""
      |SELECT a.id, b.*, c.id, c.amount + a.amount
      |FROM $catalogAndNamespace.${caseConvert(joinTableName1)} a
      |JOIN $catalogAndNamespace.${caseConvert(joinTableName1)} b ON b.id = a.id + 1
      |JOIN $catalogAndNamespace.${caseConvert(joinTableName1)} c ON c.id = b.id - 1
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)

      checkJoinPushed(
        df,
        s"$catalogAndNamespace.${caseConvert(joinTableName1)}, " +
          s"$catalogAndNamespace.${caseConvert(joinTableName1)}, " +
          s"$catalogAndNamespace.${caseConvert(joinTableName1)}")
      checkAnswer(df, rows)
    }
  }

  test("Test aliases not supported in join pushdown") {
    val sqlQuery = s"""
      |SELECT a.id, bc.*
      |FROM $catalogAndNamespace.${caseConvert(joinTableName1)} a
      |JOIN (
      |  SELECT b.*, c.id AS c_id, c.amount AS c_amount
      |  FROM $catalogAndNamespace.${caseConvert(joinTableName1)} b
      |  JOIN $catalogAndNamespace.${caseConvert(joinTableName1)} c ON c.id = b.id - 1
      |) bc ON bc.id = a.id + 1
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

  test("Test join with dataframe with duplicated columns") {
    val df1 = sql(s"SELECT id FROM $catalogAndNamespace.${caseConvert(joinTableName1)}")
    val df2 = sql(s"SELECT id, id FROM $catalogAndNamespace.${caseConvert(joinTableName1)}")

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      df1.join(df2, "id").collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val joinDf = df1.join(df2, "id")

      checkJoinPushed(
        joinDf,
        s"$catalogAndNamespace.${caseConvert(joinTableName1)}, " +
          s"$catalogAndNamespace.${caseConvert(joinTableName1)}"
      )
      checkAnswer(joinDf, rows)
    }
  }

  test("Test aggregate on top of 2-way self join") {
    val sqlQuery = s"""
      |SELECT min(a.id + b.id), min(a.id)
      |FROM $catalogAndNamespace.${caseConvert(joinTableName1)} a
      |JOIN $catalogAndNamespace.${caseConvert(joinTableName1)} b ON a.id = b.id + 1
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)

      checkAggregateRemoved(df)
      checkJoinPushed(
        df,
        s"$catalogAndNamespace.${caseConvert(joinTableName1)}, " +
          s"$catalogAndNamespace.${caseConvert(joinTableName1)}"
      )

      checkAnswer(df, rows)
    }
  }

  test("Test aggregate on top of multi-way self join") {
    val sqlQuery = s"""
      |SELECT min(a.id + b.id), min(a.id), min(c.id - 2)
      |FROM $catalogAndNamespace.${caseConvert(joinTableName1)} a
      |JOIN $catalogAndNamespace.${caseConvert(joinTableName1)} b ON b.id = a.id + 1
      |JOIN $catalogAndNamespace.${caseConvert(joinTableName1)} c ON c.id = b.id - 1
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)

      checkJoinPushed(
        df,
        s"$catalogAndNamespace.${caseConvert(joinTableName1)}," +
          s" $catalogAndNamespace.${caseConvert(joinTableName1)}, " +
          s"$catalogAndNamespace.${caseConvert(joinTableName1)}")
      checkAnswer(df, rows)
    }
  }

  test("Test sort limit on top of join is pushed down") {
    val sqlQuery = s"""
      |SELECT min(a.id + b.id), a.id, b.id
      |FROM $catalogAndNamespace.${caseConvert(joinTableName1)} a
      |JOIN $catalogAndNamespace.${caseConvert(joinTableName1)} b ON b.id = a.id + 1
      |GROUP BY a.id, b.id
      |ORDER BY a.id
      |LIMIT 1
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(
      SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)

      checkSortRemoved(df)
      checkLimitRemoved(df)

      checkJoinPushed(
        df,
        s"$catalogAndNamespace.${caseConvert(joinTableName1)}, " +
          s"$catalogAndNamespace.${caseConvert(joinTableName1)}"
      )
      checkAnswer(df, rows)
    }
  }

  test("Test join with additional filters") {
    val sqlQuery =
      s"""
         |SELECT t1.id, t1.address, t2.surname, t1.amount, t2.salary
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

  test("Test join with complex condition") {
    val sqlQuery =
      s"""
         |SELECT t1.id, t1.address, t2.surname, t1.amount + t2.salary as total
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

  test("Test left outer join should not be pushed down") {
    val sqlQuery =
      s"""
         |SELECT t1.id, t1.address, t2.surname
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
    val sqlQuery =
      s"""
         |SELECT t1.id, t1.address, t2.surname
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
}
