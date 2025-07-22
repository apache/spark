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
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

trait JDBCV2JoinPushdownIntegrationSuiteBase
  extends QueryTest
  with SharedSparkSession
  with DataSourcePushdownTestUtils {
  val catalogName: String = "join_pushdown_catalog"
  val namespace: String = "join_schema"
  val url: String

  val joinTableName1: String = "join_table_1"
  val joinTableName2: String = "join_table_2"

  val jdbcDialect: JdbcDialect

  override def sparkConf: SparkConf = super.sparkConf
    .set(s"spark.sql.catalog.$catalogName", classOf[JDBCTableCatalog].getName)
    .set(SQLConf.ANSI_ENABLED.key, "true")
    .set(s"spark.sql.catalog.$catalogName.url", url)
    .set(s"spark.sql.catalog.$catalogName.pushDownJoin", "true")
    .set(s"spark.sql.catalog.$catalogName.pushDownAggregate", "true")
    .set(s"spark.sql.catalog.$catalogName.pushDownLimit", "true")
    .set(s"spark.sql.catalog.$catalogName.pushDownOffset", "true")
    .set(s"spark.sql.catalog.$catalogName.caseSensitive", "false")

  private def catalogAndNamespace = s"$catalogName.${caseConvert(namespace)}"
  private def casedJoinTableName1 = caseConvert(joinTableName1)
  private def casedJoinTableName2 = caseConvert(joinTableName2)

  def qualifyTableName(tableName: String): String = {
    val fullyQualifiedCasedNamespace = jdbcDialect.quoteIdentifier(caseConvert(namespace))
    val fullyQualifiedCasedTableName = jdbcDialect.quoteIdentifier(caseConvert(tableName))
    s"$fullyQualifiedCasedNamespace.$fullyQualifiedCasedTableName"
  }

  def quoteSchemaName(schemaName: String): String =
    jdbcDialect.quoteIdentifier(caseConvert(namespace))

  private lazy val fullyQualifiedTableName1: String = qualifyTableName(joinTableName1)

  private lazy val fullyQualifiedTableName2: String = qualifyTableName(joinTableName2)

  protected def getJDBCTypeString(dt: DataType): String = {
    JdbcUtils.getJdbcType(dt, jdbcDialect).databaseTypeDefinition.toUpperCase()
  }

  protected def caseConvert(identifier: String): String = identifier

  protected def withConnection[T](f: Connection => T): T = {
    val conn = DriverManager.getConnection(url, new Properties())
    try {
      f(conn)
    } finally {
      conn.close()
    }
  }

  protected val integerType = DataTypes.IntegerType

  protected val stringType = DataTypes.StringType

  protected val decimalType = DataTypes.createDecimalType(10, 2)

  /**
   * This method should cover the following:
   * <ul>
   *   <li>Create the schema where testing tables will be stored.
   *   <li>Create the testing tables {@code joinTableName1} and {@code joinTableName2}
   *   in above schema.
   *   <li>Populate the tables with the data.
   * </ul>
   */
  def dataPreparation(): Unit = {
    schemaPreparation()
    tablePreparation()
    fillJoinTables()
  }

  def schemaPreparation(): Unit = {
    withConnection {conn =>
      conn
        .prepareStatement(s"CREATE SCHEMA IF NOT EXISTS ${quoteSchemaName(namespace)}")
        .executeUpdate()
    }
  }

  def tablePreparation(): Unit = {
    withConnection{ conn =>
      conn.prepareStatement(
        s"""CREATE TABLE $fullyQualifiedTableName1 (
           |  ID ${getJDBCTypeString(integerType)},
           |  AMOUNT ${getJDBCTypeString(decimalType)},
           |  ADDRESS ${getJDBCTypeString(stringType)}
           |)""".stripMargin
      ).executeUpdate()

      conn.prepareStatement(
        s"""CREATE TABLE $fullyQualifiedTableName2 (
           |  ID ${getJDBCTypeString(integerType)},
           |  NEXT_ID ${getJDBCTypeString(integerType)},
           |  SALARY ${getJDBCTypeString(decimalType)},
           |  SURNAME ${getJDBCTypeString(stringType)}
           |)""".stripMargin
      ).executeUpdate()
    }
  }

  private val random = new java.util.Random(42)

  private val table1Data = (1 to 100).map { i =>
    val id = i % 11
    val amount = BigDecimal.valueOf(random.nextDouble() * 10000)
      .setScale(2, BigDecimal.RoundingMode.HALF_UP)
    val address = s"address_$i"
    (id, amount, address)
  }

  private val table2Data = (1 to 100).map { i =>
    val id = (i % 17)
    val next_id = (id + 1) % 17
    val salary = BigDecimal.valueOf(random.nextDouble() * 50000)
      .setScale(2, BigDecimal.RoundingMode.HALF_UP)
    val surname = s"surname_$i"
    (id, next_id, salary, surname)
  }

  def fillJoinTables(): Unit = {
    withConnection { conn =>
      val insertStmt1 = conn.prepareStatement(
        s"INSERT INTO $fullyQualifiedTableName1 (id, amount, address) VALUES (?, ?, ?)"
      )
      table1Data.foreach { case (id, amount, address) =>
        insertStmt1.setInt(1, id)
        insertStmt1.setBigDecimal(2, amount.bigDecimal)
        insertStmt1.setString(3, address)
        insertStmt1.addBatch()
      }
      insertStmt1.executeBatch()
      insertStmt1.close()

      val insertStmt2 = conn.prepareStatement(
        s"INSERT INTO $fullyQualifiedTableName2 (id, next_id, salary, surname) VALUES (?, ?, ?, ?)"
      )
      table2Data.foreach { case (id, next_id, salary, surname) =>
        insertStmt2.setInt(1, id)
        insertStmt2.setInt(2, next_id)
        insertStmt2.setBigDecimal(3, salary.bigDecimal)
        insertStmt2.setString(4, surname)
        insertStmt2.addBatch()
      }
      insertStmt2.executeBatch()
      insertStmt2.close()

    }
  }

  protected val supportsFilterPushdown: Boolean = true

  protected val supportsLimitPushdown: Boolean = true

  protected val supportsAggregatePushdown: Boolean = true

  protected val supportsSortPushdown: Boolean = true

  protected val supportsOffsetPushdown: Boolean = true

  protected val supportsColumnPruning: Boolean = true

  protected val supportsJoinPushdown: Boolean = true

  // Condition-less joins are not supported in join pushdown
  test("Test that 2-way join without condition should not have join pushed down") {
    val sqlQuery =
      s"""
         |SELECT * FROM
         |$catalogAndNamespace.$casedJoinTableName1 a,
         |$catalogAndNamespace.$casedJoinTableName1 b
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
      |$catalogAndNamespace.$casedJoinTableName1 a,
      |$catalogAndNamespace.$casedJoinTableName1 b,
      |$catalogAndNamespace.$casedJoinTableName1 c
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
      |SELECT * FROM $catalogAndNamespace.$casedJoinTableName1 a
      |JOIN $catalogAndNamespace.$casedJoinTableName1 b
      |ON a.id = b.id + 1""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)

      checkJoinPushed(
        df,
        expectedTables = s"$catalogAndNamespace.${caseConvert(joinTableName1)}, " +
          s"$catalogAndNamespace.${caseConvert(joinTableName1)}"
      )
      checkAnswer(df, rows)
    }
  }

  test("Test multi-way self join with conditions") {
    val sqlQuery = s"""
      |SELECT * FROM
      |$catalogAndNamespace.$casedJoinTableName1 a
      |JOIN $catalogAndNamespace.$casedJoinTableName1 b ON b.id = a.id + 1
      |JOIN $catalogAndNamespace.$casedJoinTableName1 c ON c.id = b.id - 1
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    assert(!rows.isEmpty)

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)

      checkJoinPushed(
        df,
        expectedTables = s"$catalogAndNamespace.${caseConvert(joinTableName1)}, " +
          s"$catalogAndNamespace.${caseConvert(joinTableName1)}, " +
          s"$catalogAndNamespace.${caseConvert(joinTableName1)}"
      )
      checkAnswer(df, rows)
    }
  }

  test("Test self join with column pruning") {
    val sqlQuery = s"""
      |SELECT a.id + 2, b.id, b.amount FROM
      |$catalogAndNamespace.$casedJoinTableName1 a
      |JOIN $catalogAndNamespace.$casedJoinTableName1 b
      |ON a.id = b.id + 1
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)

      val expectedSchemaWithoutNames = StructType(
        Seq(
          StructField("", integerType), // ID
          StructField("", integerType), // NEXT_ID
          StructField(caseConvert("amount"), decimalType) // AMOUNT
        )
      )
      checkPrunedColumnsDataTypeAndNullability(df, expectedSchemaWithoutNames)
      checkJoinPushed(
        df,
        expectedTables = s"$catalogAndNamespace.${caseConvert(joinTableName1)}, " +
          s"$catalogAndNamespace.${caseConvert(joinTableName1)}"
      )
      checkAnswer(df, rows)
    }
  }

  test("Test 2-way join with column pruning - different tables") {
    val sqlQuery = s"""
      |SELECT a.id, b.next_id FROM
      |$catalogAndNamespace.$casedJoinTableName1 a
      |JOIN $catalogAndNamespace.$casedJoinTableName2 b
      |ON a.id = b.next_id
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)

      val expectedSchemaWithoutNames = StructType(
        Seq(
          StructField(caseConvert("id"), integerType), // ID
          StructField(caseConvert("next_id"), integerType) // NEXT_ID
        )
      )
      checkPrunedColumnsDataTypeAndNullability(df, expectedSchemaWithoutNames)
      checkJoinPushed(
        df,
        expectedTables = s"$catalogAndNamespace.${caseConvert(joinTableName1)}",
          s"$catalogAndNamespace.${caseConvert(joinTableName2)}"
      )
      checkPushedInfo(df,
        s"PushedFilters: [${caseConvert("id")} IS NOT NULL, " +
          s"${caseConvert("next_id")} IS NOT NULL, " +
          s"${caseConvert("id")} = ${caseConvert("next_id")}]")
      checkAnswer(df, rows)
    }
  }

  test("Test multi-way self join with column pruning") {
    val sqlQuery = s"""
      |SELECT a.id, b.*, c.id, c.amount + a.amount
      |FROM $catalogAndNamespace.$casedJoinTableName1 a
      |JOIN $catalogAndNamespace.$casedJoinTableName1 b ON b.id = a.id + 1
      |JOIN $catalogAndNamespace.$casedJoinTableName1 c ON c.id = b.id - 1
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)

      val expectedSchemaWithoutNames = StructType(
        Seq(
          StructField("", integerType), // ID_UUID
          StructField("", decimalType), // AMOUNT_UUID
          StructField("", integerType), // ID_UUID
          StructField("", decimalType), // AMOUNT_UUID
          StructField(caseConvert("address"), stringType), // ADDRESS
          StructField(caseConvert("id"), integerType), // ID
          StructField(caseConvert("amount"), decimalType) // AMOUNT
        )
      )
      checkPrunedColumnsDataTypeAndNullability(df, expectedSchemaWithoutNames)
      checkJoinPushed(
        df,
        expectedTables = s"$catalogAndNamespace.${caseConvert(joinTableName1)}, " +
          s"$catalogAndNamespace.${caseConvert(joinTableName1)}, " +
          s"$catalogAndNamespace.${caseConvert(joinTableName1)}")
      checkAnswer(df, rows)
    }
  }

  test("Test aliases not supported in join pushdown") {
    val sqlQuery = s"""
      |SELECT a.id, bc.*
      |FROM $catalogAndNamespace.$casedJoinTableName1 a
      |JOIN (
      |  SELECT b.*, c.id AS c_id, c.amount AS c_amount
      |  FROM $catalogAndNamespace.$casedJoinTableName1 b
      |  JOIN $catalogAndNamespace.$casedJoinTableName1 c ON c.id = b.id - 1
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
    val df1 = sql(s"SELECT id FROM $catalogAndNamespace.$casedJoinTableName1")
    val df2 = sql(s"SELECT id, id FROM $catalogAndNamespace.$casedJoinTableName1")

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      df1.join(df2, "id").collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val joinDf = df1.join(df2, "id")

      checkJoinPushed(
        joinDf,
        expectedTables = s"$catalogAndNamespace.${caseConvert(joinTableName1)}, " +
          s"$catalogAndNamespace.${caseConvert(joinTableName1)}"
      )
      checkAnswer(joinDf, rows)
    }
  }

  test("Test aggregate on top of 2-way self join") {
    val sqlQuery = s"""
      |SELECT min(a.id + b.id), min(a.id)
      |FROM $catalogAndNamespace.$casedJoinTableName1 a
      |JOIN $catalogAndNamespace.$casedJoinTableName1 b ON a.id = b.id + 1
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)

      checkAggregateRemoved(df, supportsAggregatePushdown)
      checkJoinPushed(
        df,
        expectedTables = s"$catalogAndNamespace.${caseConvert(joinTableName1)}, " +
          s"$catalogAndNamespace.${caseConvert(joinTableName1)}"
      )

      checkAnswer(df, rows)
    }
  }

  test("Test aggregate on top of multi-way self join") {
    val sqlQuery = s"""
      |SELECT min(a.id + b.id), min(a.id), min(c.id - 2)
      |FROM $catalogAndNamespace.$casedJoinTableName1 a
      |JOIN $catalogAndNamespace.$casedJoinTableName1 b ON b.id = a.id + 1
      |JOIN $catalogAndNamespace.$casedJoinTableName1 c ON c.id = b.id - 1
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)

      checkJoinPushed(
        df,
        expectedTables = s"$catalogAndNamespace.${caseConvert(joinTableName1)}," +
          s" $catalogAndNamespace.${caseConvert(joinTableName1)}, " +
          s"$catalogAndNamespace.${caseConvert(joinTableName1)}")
      checkAnswer(df, rows)
    }
  }

  test("Test sort limit on top of join is pushed down") {
    val sqlQuery = s"""
      |SELECT min(a.id + b.id), a.id, b.id
      |FROM $catalogAndNamespace.$casedJoinTableName1 a
      |JOIN $catalogAndNamespace.$casedJoinTableName1 b ON b.id = a.id + 1
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

      checkSortRemoved(df, supportsSortPushdown)
      checkLimitRemoved(df, supportsLimitPushdown)

      checkJoinPushed(
        df,
        expectedTables = s"$catalogAndNamespace.${caseConvert(joinTableName1)}, " +
          s"$catalogAndNamespace.${caseConvert(joinTableName1)}"
      )
      checkAnswer(df, rows)
    }
  }

  test("Test join with additional filters") {
    val sqlQuery =
      s"""
         |SELECT t1.id, t1.address, t2.surname, t1.amount, t2.salary
         |FROM $catalogAndNamespace.$casedJoinTableName1 t1
         |JOIN $catalogAndNamespace.$casedJoinTableName2 t2 ON t1.id = t2.id
         |WHERE t1.amount > 5000 AND t2.salary > 25000
         |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)
      checkJoinPushed(
        df,
        expectedTables = s"$catalogAndNamespace.${caseConvert(joinTableName1)}",
          s"$catalogAndNamespace.${caseConvert(joinTableName2)}"
      )
      checkFilterPushed(df, supportsFilterPushdown)
      checkAnswer(df, rows)
    }
  }

  test("Test join with complex condition") {
    val sqlQuery =
      s"""
         |SELECT t1.id, t1.address, t2.surname, t1.amount + t2.salary as total
         |FROM $catalogAndNamespace.$casedJoinTableName1 t1
         |JOIN $catalogAndNamespace.$casedJoinTableName2 t2
         |ON t1.id = t2.id AND t1.amount > 1000
         |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)
      checkJoinPushed(
        df,
        expectedTables = s"$catalogAndNamespace.${caseConvert(joinTableName1)}",
          s"$catalogAndNamespace.${caseConvert(joinTableName2)}"
      )
      checkAnswer(df, rows)
    }
  }

  test("Test left outer join should not be pushed down") {
    val sqlQuery =
      s"""
         |SELECT t1.id, t1.address, t2.surname
         |FROM $catalogAndNamespace.$casedJoinTableName1 t1
         |LEFT JOIN $catalogAndNamespace.$casedJoinTableName2 t2 ON t1.id = t2.id
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
         |FROM $catalogAndNamespace.$casedJoinTableName1 t1
         |RIGHT JOIN $catalogAndNamespace.$casedJoinTableName2 t2 ON t1.id = t2.id
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
