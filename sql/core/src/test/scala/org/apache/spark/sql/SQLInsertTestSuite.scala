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

package org.apache.spark.sql

import org.apache.spark.{SparkConf, SparkNumberFormatException, SparkThrowable}
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Hex
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.CommandResult
import org.apache.spark.sql.connector.catalog.InMemoryPartitionTableCatalog
import org.apache.spark.sql.execution.CommandResultExec
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.unsafe.types.UTF8String

/**
 * The base trait for SQL INSERT.
 */
trait SQLInsertTestSuite extends QueryTest with SQLTestUtils with AdaptiveSparkPlanHelper {

  import testImplicits._

  def format: String

  def checkV1AndV2Error(
      exception: SparkThrowable,
      v1ErrorClass: String,
      v2ErrorClass: String,
      v1Parameters: Map[String, String],
      v2Parameters: Map[String, String]): Unit

  protected def createTable(
      table: String,
      cols: Seq[String],
      colTypes: Seq[String],
      partCols: Seq[String] = Nil): Unit = {
    val values = cols.zip(colTypes).map(tuple => tuple._1 + " " + tuple._2).mkString("(", ", ", ")")
    val partitionSpec = if (partCols.nonEmpty) {
      partCols.mkString("PARTITIONED BY (", ",", ")")
    } else ""
    sql(s"CREATE TABLE $table$values USING $format $partitionSpec")
  }

  protected def processInsert(
      tableName: String,
      input: DataFrame,
      cols: Seq[String] = Nil,
      partitionExprs: Seq[String] = Nil,
      overwrite: Boolean,
      byName: Boolean = false): Unit = {
    val tmpView = "tmp_view"
    val partitionList = if (partitionExprs.nonEmpty) {
      partitionExprs.mkString("PARTITION (", ",", ")")
    } else ""
    withTempView(tmpView) {
      input.createOrReplaceTempView(tmpView)
      val overwriteStr = if (overwrite) "OVERWRITE" else "INTO"
      val columnList = if (cols.nonEmpty && !byName) cols.mkString("(", ",", ")") else ""
      val byNameStr = if (byName) "BY NAME" else ""
      sql(
        s"INSERT $overwriteStr TABLE $tableName $partitionList $byNameStr " +
          s"$columnList SELECT * FROM $tmpView")
    }
  }

  protected def verifyTable(tableName: String, expected: DataFrame): Unit = {
    checkAnswer(spark.table(tableName), expected)
  }

  test("insert with column list - follow table output order") {
    withTable("t1") {
      val df = Seq((1, 2L, "3")).toDF()
      val cols = Seq("c1", "c2", "c3")
      createTable("t1", cols, Seq("int", "long", "string"))
      Seq(false, true).foreach { m =>
        processInsert("t1", df, cols, overwrite = m)
        verifyTable("t1", df)
      }
    }
  }

  test("insert with column list - follow table output order + partitioned table") {
    val cols = Seq("c1", "c2", "c3", "c4")
    val df = Seq((1, 2, 3, 4)).toDF(cols: _*)
    withTable("t1") {
      createTable("t1", cols, Seq("int", "int", "int", "int"), cols.takeRight(2))
      Seq(false, true).foreach { m =>
        processInsert("t1", df, cols, overwrite = m)
        verifyTable("t1", df)
      }
    }

    withTable("t1") {
      createTable("t1", cols, Seq("int", "int", "int", "int"), cols.takeRight(2))
      Seq(false, true).foreach { m =>
        processInsert(
          "t1", df.selectExpr("c1", "c2"), cols.take(2), Seq("c3=3", "c4=4"), overwrite = m)
        verifyTable("t1", df)
      }
    }

    withTable("t1") {
      createTable("t1", cols, Seq("int", "int", "int", "int"), cols.takeRight(2))
      Seq(false, true).foreach { m =>
        processInsert("t1", df.selectExpr("c1", "c2", "c4"),
          cols.filterNot(_ == "c3"), Seq("c3=3", "c4"), overwrite = m)
        verifyTable("t1", df)
      }
    }
  }

  test("insert with column list - table output reorder") {
    withTable("t1") {
      val cols = Seq("c1", "c2", "c3")
      val df = Seq((1, 2, 3)).toDF(cols: _*)
      createTable("t1", cols, Seq("int", "int", "int"))
      Seq(false, true).foreach { m =>
        processInsert("t1", df, cols.reverse, overwrite = m)
        verifyTable("t1", df.selectExpr(cols.reverse: _*))
      }
    }
  }

  test("insert with column list - by name") {
    withTable("t1") {
      val cols = Seq("c1", "c2", "c3")
      val df = Seq((3, 2, 1)).toDF(cols.reverse: _*)
      createTable("t1", cols, Seq("int", "int", "int"))
      processInsert("t1", df, overwrite = false, byName = true)
      verifyTable("t1", df.selectExpr(cols: _*))
    }
  }

  test("insert with column list - by name + partitioned table") {
    val cols = Seq("c1", "c2", "c3", "c4")
    val df = Seq((4, 3, 2, 1)).toDF(cols.reverse: _*)
    withTable("t1") {
      createTable("t1", cols, Seq("int", "int", "int", "int"), cols.takeRight(2))
      processInsert("t1", df, overwrite = false, byName = true)
      verifyTable("t1", df.selectExpr(cols: _*))
    }

    withTable("t1") {
      createTable("t1", cols, Seq("int", "int", "int", "int"), cols.takeRight(2))
      processInsert("t1", df.selectExpr("c2", "c1", "c4"),
        partitionExprs = Seq("c3=3", "c4"), overwrite = false, byName = true)
      verifyTable("t1", df.selectExpr(cols: _*))
    }

    withTable("t1") {
      createTable("t1", cols, Seq("int", "int", "int", "int"), cols.takeRight(2))
      processInsert("t1", df.selectExpr("c2", "c1"),
        partitionExprs = Seq("c3=3", "c4=4"), overwrite = false, byName = true)
      verifyTable("t1", df.selectExpr(cols: _*))
    }
  }

  test("insert overwrite with column list - by name") {
    withTable("t1") {
      val cols = Seq("c1", "c2", "c3")
      val df = Seq((3, 2, 1)).toDF(cols.reverse: _*)
      createTable("t1", cols, Seq("int", "int", "int"))
      processInsert("t1", df, overwrite = false)
      verifyTable("t1", df.selectExpr(cols.reverse: _*))
      processInsert("t1", df, overwrite = true, byName = true)
      verifyTable("t1", df.selectExpr(cols: _*))
    }
  }

  test("insert overwrite with column list - by name + partitioned table") {
    val cols = Seq("c1", "c2", "c3", "c4")
    val df = Seq((4, 3, 2, 1)).toDF(cols.reverse: _*)
    withTable("t1") {
      createTable("t1", cols, Seq("int", "int", "int", "int"), cols.takeRight(2))
      processInsert("t1", df.selectExpr("c2", "c1", "c4"),
        partitionExprs = Seq("c3=3", "c4"), overwrite = false)
      verifyTable("t1", df.selectExpr("c2", "c1", "c3", "c4"))
      processInsert("t1", df.selectExpr("c2", "c1", "c4"),
        partitionExprs = Seq("c3=3", "c4"), overwrite = true, byName = true)
      verifyTable("t1", df.selectExpr(cols: _*))
    }

    withTable("t1") {
      createTable("t1", cols, Seq("int", "int", "int", "int"), cols.takeRight(2))
      processInsert("t1", df.selectExpr("c2", "c1"),
        partitionExprs = Seq("c3=3", "c4=4"), overwrite = false)
      verifyTable("t1", df.selectExpr("c2", "c1", "c3", "c4"))
      processInsert("t1", df.selectExpr("c2", "c1"),
        partitionExprs = Seq("c3=3", "c4=4"), overwrite = true, byName = true)
      verifyTable("t1", df.selectExpr(cols: _*))
    }
  }

  test("insert by name: mismatch column name") {
    withTable("t1") {
      val cols = Seq("c1", "c2", "c3")
      val cols2 = Seq("x1", "c2", "c3")
      val df = Seq((3, 2, 1)).toDF(cols2.reverse: _*)
      createTable("t1", cols, Seq("int", "int", "int"))
      checkV1AndV2Error(
        exception = intercept[AnalysisException] {
          processInsert("t1", df, overwrite = false, byName = true)
        },
        v1ErrorClass = "INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_COLUMNS",
        v2ErrorClass = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA",
        v1Parameters = Map("tableName" -> "`spark_catalog`.`default`.`t1`",
          "extraColumns" -> "`x1`"),
        v2Parameters = Map("tableName" -> "`testcat`.`t1`", "colName" -> "`c1`")
      )
      val df2 = Seq((3, 2, 1, 0)).toDF(Seq("c3", "c2", "c1", "c0"): _*)
      checkV1AndV2Error(
        exception = intercept[AnalysisException] {
          processInsert("t1", df2, overwrite = false, byName = true)
        },
        v1ErrorClass = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
        v2ErrorClass = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
        v1Parameters = Map("tableName" -> "`spark_catalog`.`default`.`t1`",
          "tableColumns" -> "`c1`, `c2`, `c3`",
          "dataColumns" -> "`c3`, `c2`, `c1`, `c0`"),
        v2Parameters = Map("tableName" -> "`testcat`.`t1`",
          "tableColumns" -> "`c1`, `c2`, `c3`",
          "dataColumns" -> "`c3`, `c2`, `c1`, `c0`")
      )
    }
  }

  test("insert with column list - table output reorder + partitioned table") {
    val cols = Seq("c1", "c2", "c3", "c4")
    val df = Seq((1, 2, 3, 4)).toDF(cols: _*)
    withTable("t1") {
      createTable("t1", cols, Seq("int", "int", "int", "int"), cols.takeRight(2))
      Seq(false, true).foreach { m =>
        processInsert("t1", df, cols.reverse, overwrite = m)
        verifyTable("t1", df.selectExpr(cols.reverse: _*))
      }
    }

    withTable("t1") {
      createTable("t1", cols, Seq("int", "int", "int", "int"), cols.takeRight(2))
      Seq(false, true).foreach { m =>
        processInsert(
          "t1", df.selectExpr("c1", "c2"), cols.take(2).reverse, Seq("c3=3", "c4=4"), overwrite = m)
        verifyTable("t1", df.selectExpr("c2", "c1", "c3", "c4"))
      }
    }

    withTable("t1") {
      createTable("t1", cols, Seq("int", "int", "int", "int"), cols.takeRight(2))
      Seq(false, true).foreach { m =>
        processInsert("t1",
          df.selectExpr("c1", "c2", "c4"), Seq("c4", "c2", "c1"), Seq("c3=3", "c4"), overwrite = m)
        verifyTable("t1", df.selectExpr("c4", "c2", "c3", "c1"))
      }
    }
  }

  test("insert with column list - duplicated columns") {
    withTable("t1") {
      val cols = Seq("c1", "c2", "c3")
      createTable("t1", cols, Seq("int", "long", "string"))
      checkError(
        exception = intercept[AnalysisException](
          sql(s"INSERT INTO t1 (c1, c2, c2) values(1, 2, 3)")),
        condition = "COLUMN_ALREADY_EXISTS",
        parameters = Map("columnName" -> "`c2`"))
    }
  }

  test("insert with column list - invalid columns") {
    withTable("t1") {
      val cols = Seq("c1", "c2", "c3")
      createTable("t1", cols, Seq("int", "long", "string"))
      checkError(
        exception =
          intercept[AnalysisException](sql(s"INSERT INTO t1 (c1, c2, c4) values(1, 2, 3)")),
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = None,
        parameters = Map("objectName" -> "`c4`", "proposal" -> "`c1`, `c2`, `c3`"),
        context = ExpectedContext(
          fragment = "INSERT INTO t1 (c1, c2, c4)", start = 0, stop = 26
        ))
    }
  }

  test("insert with column list - mismatched column list size") {
    def test: Unit = {
      withTable("t1") {
        val cols = Seq("c1", "c2", "c3")
        createTable("t1", cols, Seq("int", "long", "string"))
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"INSERT INTO t1 (c1, c2) values(1, 2, 3)")
          },
          sqlState = None,
          condition = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
          parameters = Map(
            "tableName" -> ".*`t1`",
            "tableColumns" -> "`c1`, `c2`",
            "dataColumns" -> "`col1`, `col2`, `col3`"),
          matchPVals = true
        )
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"INSERT INTO t1 (c1, c2, c3) values(1, 2)")
          },
          sqlState = None,
          condition = "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS",
          parameters = Map(
            "tableName" -> ".*`t1`",
            "tableColumns" -> "`c1`, `c2`, `c3`",
            "dataColumns" -> "`col1`, `col2`"),
          matchPVals = true
        )
      }
    }
    withSQLConf(SQLConf.ENABLE_DEFAULT_COLUMNS.key -> "false") {
      test
    }
    withSQLConf(SQLConf.ENABLE_DEFAULT_COLUMNS.key -> "true") {
      test
    }
  }

  test("SPARK-34223: static partition with null raise NPE") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c string) USING PARQUET PARTITIONED BY (c)")
      sql("INSERT OVERWRITE t PARTITION (c=null) VALUES ('1')")
      checkAnswer(spark.table("t"), Row("1", null))
    }
  }

  test("SPARK-33474: Support typed literals as partition spec values") {
    withTable("t1") {
      val binaryStr = "Spark SQL"
      val binaryHexStr = Hex.hex(UTF8String.fromString(binaryStr).getBytes).toString
      sql(
        """
          | CREATE TABLE t1(name STRING, part1 DATE, part2 TIMESTAMP, part3 BINARY,
          |  part4 STRING, part5 STRING, part6 STRING, part7 STRING)
          | USING PARQUET PARTITIONED BY (part1, part2, part3, part4, part5, part6, part7)
         """.stripMargin)

      sql(
        s"""
           | INSERT OVERWRITE t1 PARTITION(
           | part1 = date'2019-01-01',
           | part2 = timestamp'2019-01-01 11:11:11',
           | part3 = X'$binaryHexStr',
           | part4 = 'p1',
           | part5 = date'2019-01-01',
           | part6 = timestamp'2019-01-01 11:11:11',
           | part7 = X'$binaryHexStr'
           | ) VALUES('a')
        """.stripMargin)
      checkAnswer(sql(
        """
          | SELECT
          |   name,
          |   CAST(part1 AS STRING),
          |   CAST(part2 as STRING),
          |   CAST(part3 as STRING),
          |   part4,
          |   part5,
          |   part6,
          |   part7
          | FROM t1
        """.stripMargin),
        Row("a", "2019-01-01", "2019-01-01 11:11:11", "Spark SQL", "p1",
          "2019-01-01", "2019-01-01 11:11:11", "Spark SQL"))

      val e = intercept[AnalysisException] {
        sql("CREATE TABLE t2(name STRING, part INTERVAL) USING PARQUET PARTITIONED BY (part)")
      }.getMessage
      assert(e.contains("Cannot use \"INTERVAL\""))
    }
  }

  test("SPARK-34556: " +
    "checking duplicate static partition columns should respect case sensitive conf") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c string) USING PARQUET PARTITIONED BY (c)")
      checkError(
        exception = intercept[ParseException] {
          sql("INSERT OVERWRITE t PARTITION (c='2', C='3') VALUES (1)")
        },
        sqlState = None,
        condition = "DUPLICATE_KEY",
        parameters = Map("keyColumn" -> "`c`"),
        context = ExpectedContext("PARTITION (c='2', C='3')", 19, 42)
      )
    }
    // The following code is skipped for Hive because columns stored in Hive Metastore is always
    // case insensitive and we cannot create such table in Hive Metastore.
    if (!format.startsWith("hive")) {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        withTable("t") {
          sql(s"CREATE TABLE t(i int, c string, C string) USING PARQUET PARTITIONED BY (c, C)")
          sql("INSERT OVERWRITE t PARTITION (c='2', C='3') VALUES (1)")
          checkAnswer(spark.table("t"), Row(1, "2", "3"))
        }
      }
    }
  }

  test("SPARK-30844: static partition should also follow StoreAssignmentPolicy") {
    val testingPolicies = if (format == "foo") {
      // DS v2 doesn't support the legacy policy
      Seq(SQLConf.StoreAssignmentPolicy.ANSI, SQLConf.StoreAssignmentPolicy.STRICT)
    } else {
      SQLConf.StoreAssignmentPolicy.values
    }

    def shouldThrowException(policy: SQLConf.StoreAssignmentPolicy.Value): Boolean = policy match {
      case SQLConf.StoreAssignmentPolicy.ANSI | SQLConf.StoreAssignmentPolicy.STRICT =>
        true
      case SQLConf.StoreAssignmentPolicy.LEGACY =>
        false
    }

    testingPolicies.foreach { policy =>
      withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> policy.toString) {
        withTable("t") {
          sql("create table t(a int, b string) using parquet partitioned by (a)")
          if (shouldThrowException(policy)) {
            checkError(
              exception = intercept[SparkNumberFormatException] {
                sql("insert into t partition(a='ansi') values('ansi')")
              },
              condition = "CAST_INVALID_INPUT",
              parameters = Map(
                "expression" -> "'ansi'",
                "sourceType" -> "\"STRING\"",
                "targetType" -> "\"INT\""
              ),
              context = ExpectedContext("insert into t partition(a='ansi')", 0, 32)
            )
          } else {
            sql("insert into t partition(a='ansi') values('ansi')")
            checkAnswer(sql("select * from t"), Row("ansi", null) :: Nil)
          }
        }
      }
    }
  }

  test("SPARK-38228: legacy store assignment should not fail on error under ANSI mode") {
    // DS v2 doesn't support the legacy policy
    if (format != "foo") {
      Seq(true, false).foreach { ansiEnabled =>
        withSQLConf(
          SQLConf.STORE_ASSIGNMENT_POLICY.key -> SQLConf.StoreAssignmentPolicy.LEGACY.toString,
          SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString) {
          withTable("t") {
            sql("create table t(a int) using parquet")
            sql("insert into t values('ansi')")
            checkAnswer(spark.table("t"), Row(null))
          }
        }
      }
    }
  }

  test("SPARK-41982: treat the partition field as string literal " +
    "when keepPartitionSpecAsStringLiteral is enabled") {
    withSQLConf(SQLConf.LEGACY_KEEP_PARTITION_SPEC_AS_STRING_LITERAL.key -> "true") {
      withTable("t") {
        sql("create table t(i string, j int) using orc partitioned by (dt string)")
        sql("insert into t partition(dt=08) values('a', 10)")
        Seq(
          "select * from t where dt='08'",
          "select * from t where dt=08"
        ).foreach { query =>
          checkAnswer(sql(query), Seq(Row("a", 10, "08")))
        }
        checkError(
          exception = intercept[AnalysisException] {
            sql("alter table t drop partition(dt='8')")
          },
          condition = "PARTITIONS_NOT_FOUND",
          sqlState = None,
          parameters = Map(
            "partitionList" -> "PARTITION \\(`dt` = 8\\)",
            "tableName" -> ".*`t`"),
          matchPVals = true
        )
      }
    }

    withSQLConf(SQLConf.LEGACY_KEEP_PARTITION_SPEC_AS_STRING_LITERAL.key -> "false") {
      withTable("t") {
        sql("create table t(i string, j int) using orc partitioned by (dt string)")
        sql("insert into t partition(dt=08) values('a', 10)")
        checkAnswer(sql("select * from t where dt='08'"), sql("select * from t where dt='07'"))
        checkAnswer(sql("select * from t where dt=08"), Seq(Row("a", 10, "8")))
        checkError(
          exception = intercept[AnalysisException] {
            sql("alter table t drop partition(dt='08')")
          },
          condition = "PARTITIONS_NOT_FOUND",
          sqlState = None,
          parameters = Map(
            "partitionList" -> "PARTITION \\(`dt` = 08\\)",
            "tableName" -> ".*.`t`"),
          matchPVals = true
        )
      }
    }
  }

  test("SPARK-48817: test multi inserts") {
    withTable("t1", "t2", "t3") {
      createTable("t1", Seq("i"), Seq("int"))
      createTable("t2", Seq("i"), Seq("int"))
      createTable("t3", Seq("i"), Seq("int"))
      sql(s"INSERT INTO t1 VALUES (1), (2), (3)")
      val df = sql(
        """
          |FROM (select /*+ REPARTITION(3) */ i from t1)
          |INSERT INTO t2 SELECT i
          |INSERT INTO t3 SELECT i
          |""".stripMargin
      )
      checkAnswer(spark.table("t2"), Seq(Row(1), Row(2), Row(3)))
      checkAnswer(spark.table("t3"), Seq(Row(1), Row(2), Row(3)))

      val commandResults = df.queryExecution.executedPlan.collect {
        case c: CommandResultExec => c
      }
      assert(commandResults.size == 1)

      val reusedExchanges = collect(commandResults.head.commandPhysicalPlan) {
        case r: ReusedExchangeExec => r
      }
      assert(reusedExchanges.size == 1)
    }
  }
}

class FileSourceSQLInsertTestSuite extends SQLInsertTestSuite with SharedSparkSession {

  override def format: String = "parquet"

  override def checkV1AndV2Error(
      exception: SparkThrowable,
      v1ErrorClass: String,
      v2ErrorClass: String,
      v1Parameters: Map[String, String],
      v2Parameters: Map[String, String]): Unit = {
    checkError(exception = exception, sqlState = None, condition = v1ErrorClass,
      parameters = v1Parameters)
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set(SQLConf.USE_V1_SOURCE_LIST, format)
  }

  def getInsertIntoHadoopFsRelationCommandPartitionMembers(df: DataFrame):
  (Seq[CatalogTablePartition], TablePartitionSpec, Map[String, String], Boolean) = {
    val commandResults = df.queryExecution.optimizedPlan.collect {
      case _ @ CommandResult(_, _, commandPhysicalPlan, _) =>
        commandPhysicalPlan match {
          case d: DataWritingCommandExec => d.cmd.asInstanceOf[InsertIntoHadoopFsRelationCommand]
          case a: AdaptiveSparkPlanExec if a.inputPlan.isInstanceOf[DataWritingCommandExec] =>
            a.inputPlan.asInstanceOf[DataWritingCommandExec].cmd
              .asInstanceOf[InsertIntoHadoopFsRelationCommand]
        }
    }
    val insertIntoHadoopFsRelationCommand = commandResults.head
    val matchingPartitions = spark.sessionState.catalog.listPartitions(
      insertIntoHadoopFsRelationCommand.catalogTable.get.identifier,
      Some(insertIntoHadoopFsRelationCommand.staticPartitions
        ++ insertIntoHadoopFsRelationCommand.fillStaticPartitions))
    val staticPartitions = insertIntoHadoopFsRelationCommand.staticPartitions
    val fillStaticPartitions = insertIntoHadoopFsRelationCommand
      .fillStaticPartitions
    val dynamicPartitionOverwrite = insertIntoHadoopFsRelationCommand.dynamicPartitionOverwrite
    (matchingPartitions, staticPartitions, fillStaticPartitions, dynamicPartitionOverwrite)
  }

  test("SPARK-48881: test some dynamic partitions can be compensated to " +
    "specific partition values") {
    withTable("A", "B") {
      spark.sessionState.conf.setConfString("spark.sql.sources.partitionOverwriteMode",
        PartitionOverwriteMode.DYNAMIC.toString)
      sql("create table A(id int) using parquet partitioned by " +
        "(p1 string, p2 string)")
      spark.range(10).selectExpr("id").withColumns(Seq("p1", "p2"),
          Seq(col("id").cast("string"), col("id").cast("string"))).write
        .partitionBy("p1", "p2").saveAsTable("B")

      sql("insert overwrite A partition(p1='20240701', p2='1') values(11)")
      sql("insert overwrite A partition(p1='20240702', p2='2') values(12)")

      // insert overwrite t1 partition(p) select p from t where p = 1
      // this situation will be optimized by this optimization rule.
      val df = sql("insert overwrite A partition(p1, p2) " +
        "select id, p1, p2 from B where p1 in ('20240712') and id = 8")
      val (matchingPartitions, staticPartitions, fillStaticPartitions, dynamicPartitionOverwrite) =
        getInsertIntoHadoopFsRelationCommandPartitionMembers(df)

      assert(staticPartitions.isEmpty)
      assert(fillStaticPartitions == Map("p1" -> "20240712"))
      assert(dynamicPartitionOverwrite)
      assert(matchingPartitions.isEmpty)

      // insert overwrite t1 partition(p) select p from t where p = 1
      // this situation will not be optimized by this optimization rule.
      val df2 = sql("insert overwrite A partition(p1='20240712', p2) " +
        "select id, p2 from B where p1 in ('20240712') and id = 8")
      val (matchingPartitions2, staticPartitions2, fillStaticPartitions2,
      dynamicPartitionOverwrite2) = getInsertIntoHadoopFsRelationCommandPartitionMembers(df2)
      assert(staticPartitions2 == Map("p1" -> "20240712"))
      assert(fillStaticPartitions2.isEmpty)
      assert(dynamicPartitionOverwrite2)
      assert(matchingPartitions2.isEmpty)

      // insert overwrite t1 partition(p='1') select p from t where p = 1
      // this situation will not be optimized by this optimization rule.
      val df3 = sql("insert overwrite A partition(p1='20240712', p2='1') " +
        "select id from B where p1 in ('20240712') and id = 8")
      val (matchingPartitions3, staticPartitions3, fillStaticPartitions3,
      dynamicPartitionOverwrite3) = getInsertIntoHadoopFsRelationCommandPartitionMembers(df3)

      assert(staticPartitions3 == Map("p1" -> "20240712", "p2" -> "1"))
      assert(fillStaticPartitions3.isEmpty)
      assert(dynamicPartitionOverwrite3)
      assert(matchingPartitions3.map(_.spec) == Seq(
        Map("p1" -> "20240712", "p2" -> "1")
      ))

      // union situation will not be optimized by this optimization rule.
      val df4 = sql("insert overwrite A partition(p1, p2) " +
        "select id, p1, p2 from B where p1 in ('20240712') and id = 8 " +
        "union select id, p1, p2 from A where p1 in ('20240713') and id = 8 ")
      val (matchingPartitions4, staticPartitions4, fillStaticPartitions4,
      dynamicPartitionOverwrite4) = getInsertIntoHadoopFsRelationCommandPartitionMembers(df4)

      assert(staticPartitions4.isEmpty)
      assert(fillStaticPartitions4.isEmpty)
      assert(dynamicPartitionOverwrite4)
      assert(matchingPartitions4.map(_.spec).sortWith((x, y) =>
        x.values.toSeq.head.toLong < y.values.toSeq.head.toLong) == Seq(
        Map("p1" -> "20240701", "p2" -> "1"),
        Map("p1" -> "20240702", "p2" -> "2"),
        Map("p1" -> "20240712", "p2" -> "1")
      ))

      // join situation will not be  optimized by this optimization rule.
      val df5 = sql("insert overwrite A partition(p1, p2) " +
        "select t1.id, t1.p1, t1.p2 from B t1 left join A t2 " +
        "on t1.id = t2.id where t1.p1 in ('20240712') and t1.id = 8 and " +
        "t2.p1 in ('20240713') and t2.id = 8")
      val (matchingPartitions5, staticPartitions5, fillStaticPartitions5,
      dynamicPartitionOverwrite5) = getInsertIntoHadoopFsRelationCommandPartitionMembers(df5)

      assert(staticPartitions5.isEmpty)
      assert(fillStaticPartitions5.isEmpty)
      assert(dynamicPartitionOverwrite5)
      assert(matchingPartitions5.map(_.spec).sortWith((x, y) =>
        x.values.toSeq.head.toLong < y.values.toSeq.head.toLong) == Seq(
        Map("p1" -> "20240701", "p2" -> "1"),
        Map("p1" -> "20240702", "p2" -> "2"),
        Map("p1" -> "20240712", "p2" -> "1")
      ))

      // insert into table t1 partition(p) select (p+1) as p from t where p = 1
      // this situation will not be optimized by this optimization rule.
      val df6 = sql("insert overwrite A partition(p1, p2) " +
        "select id, (p1 + 1) as p1, p2 from B " +
        "where p1 in ('20240712') and id = 8")
      val (matchingPartitions6, staticPartitions6, fillStaticPartitions6,
      dynamicPartitionOverwrite6) = getInsertIntoHadoopFsRelationCommandPartitionMembers(df6)
      assert(staticPartitions6.isEmpty)
      assert(fillStaticPartitions6.isEmpty)
      assert(dynamicPartitionOverwrite6)
      assert(matchingPartitions6.size == 3 && matchingPartitions6.map(_.spec).sortWith((x, y) =>
        x.values.toSeq.head.toLong < y.values.toSeq.head.toLong) == Seq(
        Map("p1" -> "20240701", "p2" -> "1"),
        Map("p1" -> "20240702", "p2" -> "2"),
        Map("p1" -> "20240712", "p2" -> "1")
      ))

      // insert into table t1 partition(p) select p from t where p = 1 and p = 2
      // this situation will not be optimized by this optimization rule.
      val df7 = sql("insert overwrite A partition(p1, p2) " +
        "select id, p1, p2 from B " +
        "where p1 = '20240712' and p1 = '20240713'  and id = 8")
      val (matchingPartitions7, staticPartitions7, fillStaticPartitions7,
      dynamicPartitionOverwrite7) = getInsertIntoHadoopFsRelationCommandPartitionMembers(df7)

      assert(staticPartitions7.isEmpty)
      assert(fillStaticPartitions7.isEmpty)
      assert(dynamicPartitionOverwrite7)
      assert(matchingPartitions7.size == 3 && matchingPartitions6.map(_.spec).sortWith((x, y) =>
        x.values.toSeq.head.toLong < y.values.toSeq.head.toLong) == Seq(
        Map("p1" -> "20240701", "p2" -> "1"),
        Map("p1" -> "20240702", "p2" -> "2"),
        Map("p1" -> "20240712", "p2" -> "1")
      ) )

      // insert into table t1 partition(p1, p2) select * from t2 where p2=1
      // this situation can be optimized by using this optimization rule to avoid obtaining
      // all table partitions, which is in line with expectations.
      val df8 = sql("insert overwrite A partition(p1, p2) " +
        "select id, p1, p2 from B " +
        "where p2 = '1'  and id = 8")
      val (matchingPartitions8, staticPartitions8, fillStaticPartitions8,
      dynamicPartitionOverwrite8) = getInsertIntoHadoopFsRelationCommandPartitionMembers(df8)

      assert(staticPartitions8.isEmpty)
      assert(fillStaticPartitions8.size == 1 &&
        fillStaticPartitions8("p2") == "1")
      assert(dynamicPartitionOverwrite8)
      assert(matchingPartitions8.size == 2 && matchingPartitions8.map(_.spec).sortWith((x, y) =>
        x.values.toSeq.head.toLong < y.values.toSeq.head.toLong) == Seq(
        Map("p1" -> "20240701", "p2" -> "1"),
        Map("p1" -> "20240712", "p2" -> "1")
      ))
    }
  }

}

class DSV2SQLInsertTestSuite extends SQLInsertTestSuite with SharedSparkSession {

  override def format: String = "foo"

  override def checkV1AndV2Error(
      exception: SparkThrowable,
      v1ErrorClass: String,
      v2ErrorClass: String,
      v1Parameters: Map[String, String],
      v2Parameters: Map[String, String]): Unit = {
    checkError(exception = exception, sqlState = None, condition = v2ErrorClass,
      parameters = v2Parameters)
  }
  protected override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.catalog.testcat", classOf[InMemoryPartitionTableCatalog].getName)
      .set(SQLConf.DEFAULT_CATALOG.key, "testcat")
  }

  test("static partition column name should not be used in the column list") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c string) USING PARQUET PARTITIONED BY (c)")
      checkError(
        exception = intercept[AnalysisException] {
          sql("INSERT OVERWRITE t PARTITION (c='1') (c) VALUES ('2')")
        },
        condition = "STATIC_PARTITION_COLUMN_IN_INSERT_COLUMN_LIST",
        parameters = Map("staticName" -> "c"))
    }
  }

}
