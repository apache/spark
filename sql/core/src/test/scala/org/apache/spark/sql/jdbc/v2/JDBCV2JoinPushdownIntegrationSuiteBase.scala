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
import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.DataSourcePushdownTestUtils
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2ScanRelation, V1ScanWrapper}
import org.apache.spark.sql.execution.datasources.v2.jdbc.{JDBCScan, JDBCTableCatalog}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

trait JDBCV2JoinPushdownIntegrationSuiteBase
  extends SharedSparkSession
  with DataSourcePushdownTestUtils {
  val catalogName: String = "join_pushdown_catalog"
  val namespace: String = "join_schema"
  val url: String

  val joinTableName1: String = "join_table_1"
  val joinTableName2: String = "join_table_2"
  val joinTableName3: String = "join_table_3"
  val joinTableName4: String = "join_table_4"
  val existenceLeftTable: String = "existence_left"
  val existenceRightTable: String = "existence_right"

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

  protected def catalogAndNamespace = s"$catalogName.${caseConvert(namespace)}"
  protected def casedJoinTableName1 = caseConvert(joinTableName1)
  protected def casedJoinTableName2 = caseConvert(joinTableName2)
  protected def casedJoinTableName3 = caseConvert(joinTableName3)
  protected def casedJoinTableName4 = caseConvert(joinTableName4)

  def qualifyTableName(tableName: String): String = {
    val fullyQualifiedCasedNamespace = jdbcDialect.quoteIdentifier(caseConvert(namespace))
    val fullyQualifiedCasedTableName = jdbcDialect.quoteIdentifier(caseConvert(tableName))
    s"$fullyQualifiedCasedNamespace.$fullyQualifiedCasedTableName"
  }

  def quoteSchemaName(schemaName: String): String =
    jdbcDialect.quoteIdentifier(caseConvert(namespace))

  private lazy val fullyQualifiedTableName1: String = qualifyTableName(joinTableName1)
  private lazy val fullyQualifiedTableName2: String = qualifyTableName(joinTableName2)
  private lazy val fullyQualifiedTableName3: String = qualifyTableName(joinTableName3)
  private lazy val fullyQualifiedTableName4: String = qualifyTableName(joinTableName4)

  protected def getJDBCTypeString(dt: DataType): String = {
    JdbcUtils.getJdbcType(dt, jdbcDialect).databaseTypeDefinition.toUpperCase()
  }

  protected def caseConvert(identifier: String): String = identifier

  // Quote the identifier to remain original case, for example, MySql convert [`ID`, ID]
  // to [ID, id]
  protected def remainColumnCase(identifier: String): String = "\"" + identifier + "\""

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
    fillExistenceTables()
  }

  def schemaPreparation(): Unit = {
    withConnection {conn =>
      conn
        .prepareStatement(s"CREATE SCHEMA ${quoteSchemaName(namespace)}")
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

      // Complex situations with different capitalization and quotation marks.
      conn.prepareStatement(
        s"""CREATE TABLE $fullyQualifiedTableName3(
           |${remainColumnCase("id")} ${getJDBCTypeString(integerType)},
           |${remainColumnCase("id_1")} ${getJDBCTypeString(integerType)},
           |${remainColumnCase("id_2")} ${getJDBCTypeString(integerType)},
           |${remainColumnCase("id_1_1")} ${getJDBCTypeString(integerType)},
           |${remainColumnCase("sid")} ${getJDBCTypeString(integerType)}
           |)""".stripMargin
      ).executeUpdate()
      conn.prepareStatement(
        s"""CREATE TABLE $fullyQualifiedTableName4 (
           |${remainColumnCase("id")} ${getJDBCTypeString(integerType)},
           |${remainColumnCase("id_1")} ${getJDBCTypeString(integerType)},
           |${remainColumnCase("id_2")} ${getJDBCTypeString(integerType)},
           |${remainColumnCase("id_2_1")} ${getJDBCTypeString(integerType)},
           |${remainColumnCase("Sid")} ${getJDBCTypeString(integerType)}
           |)""".stripMargin
       ).executeUpdate()
      conn.prepareStatement(
        s"CREATE TABLE ${qualifyTableName(existenceLeftTable)} " +
          s"(ID ${getJDBCTypeString(integerType)}, NAME ${getJDBCTypeString(stringType)})"
      ).executeUpdate()
      conn.prepareStatement(
        s"CREATE TABLE ${qualifyTableName(existenceRightTable)} " +
          s"(ID ${getJDBCTypeString(integerType)}, FLAG ${getJDBCTypeString(integerType)})"
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

      conn.createStatement().execute(
        s"""INSERT INTO $fullyQualifiedTableName3 VALUES (0, 1, 2, 3, 4)""")
      conn.createStatement().execute(
        s"""INSERT INTO $fullyQualifiedTableName4 VALUES (0, -1, -2, -3, -4)""")
    }
  }

  def fillExistenceTables(): Unit = {
    withConnection { conn =>
      val left = conn.prepareStatement(
        s"INSERT INTO ${qualifyTableName(existenceLeftTable)} VALUES (?, ?)")
      Seq((Some(1), "first"), (Some(1), "second"), (Some(2), "third"),
        (None, "fourth"), (Some(3), "fifth")).foreach { case (id, name) =>
        id match {
          case Some(value) => left.setInt(1, value)
          case None => left.setNull(1, java.sql.Types.INTEGER)
        }
        left.setString(2, name)
        left.addBatch()
      }
      left.executeBatch()
      left.close()

      val right = conn.prepareStatement(
        s"INSERT INTO ${qualifyTableName(existenceRightTable)} VALUES (?, ?)")
      Seq((Some(1), 1), (Some(1), 1), (Some(3), 0), (None, 1)).foreach { case (id, flag) =>
        id match {
          case Some(value) => right.setInt(1, value)
          case None => right.setNull(1, java.sql.Types.INTEGER)
        }
        right.setInt(2, flag)
        right.addBatch()
      }
      right.executeBatch()
      right.close()
    }
  }

  protected val supportsFilterPushdown: Boolean = true

  protected val supportsLimitPushdown: Boolean = true

  protected val supportsAggregatePushdown: Boolean = true

  protected val supportsSortPushdown: Boolean = true

  protected val supportsOffsetPushdown: Boolean = true

  protected val supportsColumnPruning: Boolean = true

  test("Left semi and left anti existence joins") {
    val left = s"$catalogAndNamespace.${caseConvert(existenceLeftTable)}"
    val right = s"$catalogAndNamespace.${caseConvert(existenceRightTable)}"
    val cases = Seq(
      (s"SELECT * FROM $left a LEFT SEMI JOIN $right b ON a.id = b.id",
        "EXISTS", Seq("first", "second", "fifth")),
      (s"SELECT * FROM $left a LEFT ANTI JOIN $right b ON a.id = b.id",
        "NOT EXISTS", Seq("third", "fourth")),
      (s"SELECT * FROM $left a LEFT SEMI JOIN $right b ON a.id <=> b.id",
        "EXISTS", Seq("first", "second", "fourth", "fifth")),
      (s"SELECT * FROM $left a LEFT ANTI JOIN $right b ON a.id <=> b.id",
        "NOT EXISTS", Seq("third")),
      (s"SELECT * FROM $left a LEFT SEMI JOIN $right b ON a.id < b.id",
        "EXISTS", Seq("first", "second", "third")),
      (s"SELECT * FROM $left a LEFT ANTI JOIN $right b ON a.id < b.id",
        "NOT EXISTS", Seq("fourth", "fifth")),
      (s"SELECT * FROM $left a LEFT SEMI JOIN $right b ON a.id = b.id OR a.id < b.id",
        "EXISTS", Seq("first", "second", "third", "fifth")),
      (s"SELECT * FROM $left a LEFT ANTI JOIN $right b ON a.id = b.id OR a.id < b.id",
        "NOT EXISTS", Seq("fourth")),
      (s"SELECT * FROM $left a LEFT ANTI JOIN $right b " +
        "ON a.id = b.id AND b.flag = -1",
        "NOT EXISTS", Seq("first", "second", "third", "fourth", "fifth")),
      (s"SELECT * FROM $left a WHERE EXISTS " +
        s"(SELECT 1 FROM $right b WHERE a.id = b.id AND b.flag = 1)",
        "EXISTS", Seq("first", "second")),
      (s"SELECT * FROM $left a WHERE NOT EXISTS " +
        s"(SELECT 1 FROM $right b WHERE a.id = b.id AND b.flag = 1)",
        "NOT EXISTS", Seq("third", "fourth", "fifth")),
      (s"SELECT * FROM $left a WHERE EXISTS " +
        s"(SELECT 1 FROM $right b WHERE a.id <=> b.id)",
        "EXISTS", Seq("first", "second", "fourth", "fifth")),
      (s"SELECT * FROM $left a WHERE NOT EXISTS " +
        s"(SELECT 1 FROM $right b WHERE a.id <=> b.id)",
        "NOT EXISTS", Seq("third")),
      (s"SELECT * FROM $left a WHERE EXISTS " +
        s"(SELECT 1 FROM $right b WHERE a.id = b.id AND b.flag = 1) AND NOT EXISTS " +
        s"(SELECT 1 FROM $right c WHERE a.id = c.id AND c.flag = 0)",
        "NOT EXISTS", Seq("first", "second")))

    cases.foreach { case (query, keyword, expected) =>
      withClue(query) {
        withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
          checkAnswer(sql(query).select(caseConvert("name")), expected.map(Row(_)))
        }
        withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
          val df = sql(query)
          checkJoinPushed(df)
          assert(df.schema.fieldNames.toSeq == Seq("ID", "NAME").map(caseConvert))
          val scans = df.queryExecution.optimizedPlan.collect {
            case relation: DataSourceV2ScanRelation => relation.scan
          }
          assert(scans.size == 1)
          val jdbcScan = scans.head.asInstanceOf[V1ScanWrapper].v1Scan.asInstanceOf[JDBCScan]
          assert(jdbcScan.readSchema().fieldNames.toSeq == Seq("ID", "NAME").map(caseConvert))
          assert(jdbcScan.relation.jdbcOptions.tableOrQuery.contains(keyword))
          checkAnswer(df.select(caseConvert("name")), expected.map(Row(_)))
        }
      }
    }
  }

  test("NOT IN with a null on the right is not pushed down") {
    val left = s"$catalogAndNamespace.${caseConvert(existenceLeftTable)}"
    val right = s"$catalogAndNamespace.${caseConvert(existenceRightTable)}"
    val query = s"SELECT * FROM $left WHERE id NOT IN (SELECT id FROM $right)"

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(query)
      checkJoinNotPushed(df)
      checkAnswer(df, Seq.empty[Row])
    }
  }

  test("Left semi and left anti joins with a pruned left side") {
    val left = s"$catalogAndNamespace.${caseConvert(existenceLeftTable)}"
    val right = s"$catalogAndNamespace.${caseConvert(existenceRightTable)}"
    val cases = Seq(
      ("SEMI", Seq(Row("first"), Row("second"), Row("fifth"))),
      ("ANTI", Seq(Row("third"), Row("fourth"))))

    cases.foreach { case (joinType, expected) =>
      val query = s"SELECT a.name FROM $left a LEFT $joinType JOIN $right b ON a.id = b.id"
      withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
        val df = sql(query)
        checkJoinPushed(df)
        checkAnswer(df, expected)
        val ids = sql(s"SELECT a.id FROM $left a LEFT $joinType JOIN $right b ON a.id = b.id")
        checkJoinPushed(ids)
        val expectedIds = if (joinType == "SEMI") {
          Seq(Row(1), Row(1), Row(3))
        } else {
          Seq(Row(2), Row(null))
        }
        checkAnswer(ids, expectedIds)
        checkAnswer(sql(s"SELECT count(*) FROM $left a LEFT $joinType JOIN $right b " +
          "ON a.id = b.id"), Seq(Row(expected.size.toLong)))
      }
    }
  }

  test("Partitioned left semi and left anti joins are not pushed down") {
    val options = s"""WITH (
      |'partitionColumn' '${caseConvert("id")}',
      |'lowerBound' '0',
      |'upperBound' '3',
      |'numPartitions' '2')""".stripMargin
    val left = s"$catalogAndNamespace.${caseConvert(existenceLeftTable)} $options"
    val right = s"$catalogAndNamespace.${caseConvert(existenceRightTable)} $options"

    Seq("SEMI", "ANTI").foreach { joinType =>
      val query = s"SELECT a.name FROM $left a LEFT $joinType JOIN $right b ON a.id = b.id"
      withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
        val df = sql(query)
        checkJoinNotPushed(df)
        val expected = if (joinType == "SEMI") {
          Seq(Row("first"), Row("second"), Row("fifth"))
        } else {
          Seq(Row("third"), Row("fourth"))
        }
        checkAnswer(df, expected)
      }
    }
  }

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

      // scalastyle:off line.size.limit
      checkJoinPushed(
        df,
        s"""PushedFilters: [${caseConvert("id")} = (${caseConvert("id_1")} + 1)], PushedJoins:\u0020
           |[L]: Relation: $catalogAndNamespace.${caseConvert(joinTableName1)}
           |     PushedFilters: [${caseConvert("id")} IS NOT NULL]
           |[R]: Relation: $catalogAndNamespace.${caseConvert(joinTableName1)}
           |     PushedFilters: [${caseConvert("id")} IS NOT NULL]"""
          .stripMargin
      )
      // scalastyle:on line.size.limit
      checkAnswer(df, rows)
    }
  }

  gridTest("Join pushdown preserves partitioned input reads")(Seq(1, 2)) { numPartitions =>
    val tableOptions = s"""WITH (
      |'partitionColumn' '${caseConvert("id")}',
      |'lowerBound' '0',
      |'upperBound' '11',
      |'numPartitions' '$numPartitions')""".stripMargin
    val sqlQuery = s"""
      |SELECT a.id, b.id
      |FROM $catalogAndNamespace.$casedJoinTableName1 $tableOptions a
      |JOIN $catalogAndNamespace.$casedJoinTableName1 $tableOptions b ON a.id = b.id + 1
      |""".stripMargin

    val expectedRows = for {
      (leftId, _, _) <- table1Data
      (rightId, _, _) <- table1Data
      if leftId == rightId + 1
    } yield Row(leftId, rightId)

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)
      checkJoinNotPushed(df)
      checkAnswer(df, expectedRows)
    }
  }

  test("Test multi-way self join with conditions") {
    val sqlQuery = s"""
      |SELECT * FROM
      |$catalogAndNamespace.$casedJoinTableName1 a
      |JOIN $catalogAndNamespace.$casedJoinTableName1 b ON b.id = a.id + 1
      |JOIN $catalogAndNamespace.$casedJoinTableName1 c ON c.id = b.id - 1""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    assert(!rows.isEmpty)

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)

      // scalastyle:off line.size.limit
      checkJoinPushed(
        df,
        s"""PushedFilters: [${caseConvert("id_2")} = (${caseConvert("id_1")} - 1)], PushedJoins:\u0020
           |[L]: PushedFilters: [${caseConvert("id_1")} = (${caseConvert("id")} + 1)]
           |     PushedJoins:
           |     [L]: Relation: $catalogAndNamespace.${caseConvert(joinTableName1)}
           |          PushedFilters: [${caseConvert("id")} IS NOT NULL]
           |     [R]: Relation: $catalogAndNamespace.${caseConvert(joinTableName1)}
           |          PushedFilters: [${caseConvert("id")} IS NOT NULL]
           |[R]: Relation: $catalogAndNamespace.${caseConvert(joinTableName1)}
           |     PushedFilters: [${caseConvert("id")} IS NOT NULL]""".stripMargin
      )
      // scalastyle:on line.size.limit
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

      val expectedSchema = StructType(
        Seq(
          StructField(caseConvert("id"), integerType), // ID
          StructField(caseConvert("id_1"), integerType), // ID
          StructField(caseConvert("amount"), decimalType) // AMOUNT
        )
      )
      checkPrunedColumnsDataTypeAndNullability(df, expectedSchema)
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

      val expectedSchema = StructType(
        Seq(
          StructField(caseConvert("id"), integerType), // ID
          StructField(caseConvert("next_id"), integerType) // NEXT_ID
        )
      )
      checkPrunedColumnsDataTypeAndNullability(df, expectedSchema)
      checkPushedInfo(df,
        s"PushedFilters: [${caseConvert("id")} IS NOT NULL",
          s"${caseConvert("next_id")} IS NOT NULL",
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

      val expectedSchema = StructType(
        Seq(
          StructField(caseConvert("id"), integerType), // ID
          StructField(caseConvert("amount"), decimalType), // AMOUNT
          StructField(caseConvert("id_1"), integerType), // ID
          StructField(caseConvert("amount_1"), decimalType), // AMOUNT
          StructField(caseConvert("address"), stringType), // ADDRESS
          StructField(caseConvert("id_2"), integerType), // ID
          StructField(caseConvert("amount_2"), decimalType) // AMOUNT
        )
      )
      checkPrunedColumnsDataTypeAndNullability(df, expectedSchema)
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
      checkAnswer(df, rows)
    }
  }

  test("Join pushdown preserves aliases in partially pushed averages") {
    assume(supportsAggregatePushdown, "Aggregate pushdown is not supported")
    def sqlPartitionedQuery(numPartitions: Int): String = {
      val idCol = caseConvert("id")
      // numPartitions alone is valid and selects the partial aggregate path when greater than 1.
      val tableOptions = s"WITH ('numPartitions' '$numPartitions')"
      // Some databases return an integer for AVG over integer input, so use decimal input.
      s"""
        |SELECT avg(CAST(b.$idCol AS DECIMAL(10, 2)))
        |FROM $catalogAndNamespace.$casedJoinTableName1 $tableOptions a
        |JOIN $catalogAndNamespace.$casedJoinTableName1 $tableOptions b ON a.$idCol = b.$idCol + 1
        |""".stripMargin
    }

    val rowsWithJoinPushdown = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val completeAgg = sql(sqlPartitionedQuery(numPartitions = 1))
      checkJoinPushed(completeAgg)
      checkAggregateRemoved(completeAgg, pushed = true)
      val expectedRows = completeAgg.collect().toSeq
      assert(expectedRows.head.get(0) != null)

      val partialAgg = sql(sqlPartitionedQuery(numPartitions = 2))
      checkJoinPushed(partialAgg)
      checkAggregateRemoved(partialAgg, pushed = false)
      checkAnswer(partialAgg, expectedRows)
      expectedRows
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      val df = sql(sqlPartitionedQuery(numPartitions = 2))
      checkJoinNotPushed(df)
      checkAggregateRemoved(df, pushed = false)
      checkAnswer(df, rowsWithJoinPushdown)
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
      checkAnswer(df, rows)
    }
  }

  test("Test aggregate with group by on top of join") {
    val sqlQuery =
      s"""
         |SELECT t1.id, t1.address, min(t2.salary)
         |FROM $catalogAndNamespace.$casedJoinTableName1 t1
         |JOIN $catalogAndNamespace.$casedJoinTableName2 t2 ON t1.id = t2.id
         |WHERE t1.amount > 1000
         |GROUP BY t1.id, t1.address
         |""".stripMargin

    val rowsNoPushdown = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    assert(rowsNoPushdown.nonEmpty)

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)
      checkJoinPushed(df)
      checkAggregateRemoved(df, supportsAggregatePushdown)
      checkAnswer(df, rowsNoPushdown)
    }
  }

  test("Test multi-way join with function in join condition") {
    val sqlQuery =
      s"""
         |SELECT a.id, c.address, d.address, a.amount
         |FROM $catalogAndNamespace.$casedJoinTableName1 a
         |JOIN $catalogAndNamespace.$casedJoinTableName1 c
         |  ON a.address = c.address
         |JOIN $catalogAndNamespace.$casedJoinTableName1 d
         |  ON IF(a.id > 5, a.id - 1, a.id + 1) = d.id
         |WHERE a.amount >= 1000
         |""".stripMargin

    val rowsNoPushdown = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    assert(rowsNoPushdown.nonEmpty)

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)
      checkJoinPushed(df)
      checkAnswer(df, rowsNoPushdown)
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
      checkAnswer(df, rows)
    }
  }

  test("Test left outer join with condition should be pushed down") {
    val sqlQuery =
      s"""
         |SELECT t1.id, t1.address, t2.surname
         |FROM $catalogAndNamespace.$casedJoinTableName1 t1
         |LEFT JOIN $catalogAndNamespace.$casedJoinTableName2 t2
         |ON t1.id = t2.id
         |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    assert(rows.nonEmpty)
    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)
      checkJoinPushed(df)
      checkAnswer(df, rows)
    }
  }

  test("Test left outer join without condition - no pushdown") {
    val sqlQuery =
      s"""
         |SELECT * FROM
         |$catalogAndNamespace.$casedJoinTableName1 a
         |LEFT JOIN
         |$catalogAndNamespace.$casedJoinTableName2 b
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

  test("Test right outer join with condition should be pushed down") {
    val sqlQuery =
      s"""
         |SELECT t1.id, t1.address, t2.surname
         |FROM $catalogAndNamespace.$casedJoinTableName1 t1
         |RIGHT JOIN $catalogAndNamespace.$casedJoinTableName2 t2
         |ON t1.id = t2.id
         |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    assert(rows.nonEmpty)
    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)
      checkJoinPushed(df)
      checkAnswer(df, rows)
    }
  }

  test("Test right outer join without condition - no pushdown") {
    val sqlQuery =
      s"""
         |SELECT * FROM
         |$catalogAndNamespace.$casedJoinTableName1 a
         |RIGHT JOIN
         |$catalogAndNamespace.$casedJoinTableName2 b
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

  test("Test condition with aliased column") {
    // After the first join, columns will be aliased because we are doing self join in CTE.
    // Second join, is joining on aliased column, so the aliased value should be used in generated
    // SQL query.
    val sqlQuery = s"""
      |WITH ws_wh AS (
      |    SELECT
      |        ws1.ID,
      |        ws1.AMOUNT wh1,
      |        ws2.AMOUNT wh2
      |    FROM
      |        $catalogAndNamespace.$casedJoinTableName1 ws1,
      |        $catalogAndNamespace.$casedJoinTableName1 ws2
      |    WHERE
      |        ws1.ID = ws2.ID
      |        AND ws1.AMOUNT <> ws2.AMOUNT
      |)
      |SELECT
      |   NEXT_ID
      |FROM
      |   $catalogAndNamespace.$casedJoinTableName2,
      |   ws_wh
      |WHERE
      |   NEXT_ID = ws_wh.ID
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    assert(!rows.isEmpty)

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)

      checkJoinPushed(df)
      checkAnswer(df, rows)
    }
  }

  test("Test complex duplicate column name alias") {
    val sqlQuery = s"""
                      |SELECT
                      |    *
                      |FROM $catalogAndNamespace.$casedJoinTableName3 a
                      |JOIN $catalogAndNamespace.$casedJoinTableName4 b
                      |ON a.id = b.id""".stripMargin

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)
      val row = df.collect()(0)
      assert(row.toString == Row(0, 1, 2, 3, 4, 0, -1, -2, -3, -4).toString)

      assert(df.schema.fields.map(_.name) sameElements
        Array("id", "id_1", "id_2", "id_1_1", "sid",
          "id", "id_1", "id_2", "id_2_1", "Sid"),
        "Unexpected schema names: " + df.schema.fields.map(_.name).mkString(","))

      val schemaNames = df.queryExecution.optimizedPlan.collectFirst {
        case j: DataSourceV2ScanRelation => j
      }.get.schema.fields.map(_.name)
      assert(schemaNames sameElements
        Array("id", "id_1", "id_2", "id_1_1", "sid",
          "id_3", "id_1_2", "id_2_2", "id_2_1", "Sid_1"),
        "Unexpected schema names: " + schemaNames.mkString(","))

      checkJoinPushed(df)
    }
  }

  test("Test explain formatted") {
    val sqlQuery = s"""
      |SELECT * FROM $catalogAndNamespace.$casedJoinTableName1 a
      |JOIN $catalogAndNamespace.$casedJoinTableName2 b
      |ON a.id = b.id + 1
      |JOIN $catalogAndNamespace.$casedJoinTableName3 c
      |ON b.id = c.id + 1
      |JOIN $catalogAndNamespace.$casedJoinTableName4 d
      |ON c.id = d.id + 1
      |""".stripMargin

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)

      // scalastyle:off line.size.limit
      checkJoinPushed(
        df,
        s"""PushedFilters: [id_3 = (id_4 + 1)], PushedJoins:\u0020
           |[L]: PushedFilters: [${caseConvert("id_1")} = (id_3 + 1)]
           |     PushedJoins:
           |     [L]: PushedFilters: [${caseConvert("id")} = (${caseConvert("id_1")} + 1)]
           |          PushedJoins:
           |          [L]: Relation: $catalogAndNamespace.${caseConvert(joinTableName1)}
           |               PushedFilters: [${caseConvert("id")} IS NOT NULL]
           |          [R]: Relation: $catalogAndNamespace.${caseConvert(joinTableName2)}
           |               PushedFilters: [${caseConvert("id")} IS NOT NULL]
           |     [R]: Relation: $catalogAndNamespace.${caseConvert(joinTableName3)}
           |          PushedFilters: [id IS NOT NULL]
           |[R]: Relation: $catalogAndNamespace.${caseConvert(joinTableName4)}
           |     PushedFilters: [id IS NOT NULL]""".stripMargin
      )
      // scalastyle:on line.size.limit
    }
  }
}
