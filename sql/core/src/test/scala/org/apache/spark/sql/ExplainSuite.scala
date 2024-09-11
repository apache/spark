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

import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.physical.RoundRobinPartitioning
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{DisableAdaptiveExecutionSuite, EnableAdaptiveExecutionSuite}
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.TestOptionsSource
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

trait ExplainSuiteHelper extends QueryTest with SharedSparkSession {

  protected def getNormalizedExplain(df: DataFrame, mode: ExplainMode): String = {
    val output = new java.io.ByteArrayOutputStream()
    Console.withOut(output) {
      df.explain(mode.name)
    }
    output.toString.replaceAll("#\\d+", "#x")
  }

  /**
   * Get the explain from a DataFrame and run the specified action on it.
   */
  protected def withNormalizedExplain(df: DataFrame, mode: ExplainMode)(f: String => Unit) = {
    f(getNormalizedExplain(df, mode))
  }

  /**
   * Get the explain by running the sql. The explain mode should be part of the
   * sql text itself.
   */
  protected def withNormalizedExplain(queryText: String)(f: String => Unit) = {
    val output = new java.io.ByteArrayOutputStream()
    Console.withOut(output) {
      sql(queryText).show(false)
    }
    val normalizedOutput = output.toString.replaceAll("#\\d+", "#x")
    f(normalizedOutput)
  }

  /**
   * Runs the plan and makes sure the plans contains all of the keywords.
   */
  protected def checkKeywordsExistsInExplain(
      df: DataFrame, mode: ExplainMode, keywords: String*): Unit = {
    withNormalizedExplain(df, mode) { normalizedOutput =>
      for (key <- keywords) {
        assert(normalizedOutput.contains(key))
      }
    }
  }

  protected def checkKeywordsExistsInExplain(df: DataFrame, keywords: String*): Unit = {
    checkKeywordsExistsInExplain(df, ExtendedMode, keywords: _*)
  }

  /**
   * Runs the plan and makes sure the plans does not contain any of the keywords.
   */
  protected def checkKeywordsNotExistsInExplain(
      df: DataFrame, mode: ExplainMode, keywords: String*): Unit = {
    withNormalizedExplain(df, mode) { normalizedOutput =>
      for (key <- keywords) {
        assert(!normalizedOutput.contains(key))
      }
    }
  }
}

class ExplainSuite extends ExplainSuiteHelper with DisableAdaptiveExecutionSuite {
  import testImplicits._

  test("SPARK-23034 show rdd names in RDD scan nodes (Dataset)") {
    val rddWithName = spark.sparkContext.parallelize(Row(1, "abc") :: Nil).setName("testRdd")
    val df = spark.createDataFrame(rddWithName, StructType.fromDDL("c0 int, c1 string"))
    checkKeywordsExistsInExplain(df, keywords = "Scan ExistingRDD testRdd")
  }

  test("SPARK-23034 show rdd names in RDD scan nodes (DataFrame)") {
    val rddWithName = spark.sparkContext.parallelize(ExplainSingleData(1) :: Nil).setName("testRdd")
    val df = spark.createDataFrame(rddWithName)
    checkKeywordsExistsInExplain(df, keywords = "Scan testRdd")
  }

  test("SPARK-24850 InMemoryRelation string representation does not include cached plan") {
    val df = Seq(1).toDF("a").cache()
    checkKeywordsExistsInExplain(df,
      keywords = "InMemoryRelation", "StorageLevel(disk, memory, deserialized, 1 replicas)")
  }

  test("optimized plan should show the rewritten expression") {
    withTempView("test_agg") {
      sql(
        """
          |CREATE TEMPORARY VIEW test_agg AS SELECT * FROM VALUES
          |  (1, true), (1, false),
          |  (2, true),
          |  (3, false), (3, null),
          |  (4, null), (4, null),
          |  (5, null), (5, true), (5, false) AS test_agg(k, v)
        """.stripMargin)

      // simple explain of queries having every/some/any aggregates. Optimized
      // plan should show the rewritten aggregate expression.
      val df = sql("SELECT k, every(v), some(v), any(v) FROM test_agg GROUP BY k")
      checkKeywordsExistsInExplain(df,
        "Aggregate [k#x], [k#x, every(v#x) AS every(v)#x, some(v#x) AS some(v)#x, " +
          "any(v#x) AS any(v)#x]")
    }

    withTable("t") {
      sql("CREATE TABLE t(col TIMESTAMP) USING parquet")
      val df = sql("SELECT date_part('month', col) FROM t")
      checkKeywordsExistsInExplain(df,
        "Project [month(cast(col#x as date)) AS date_part(month, col)#x]")
    }
  }

  test("explain inline tables cross-joins") {
    val df = sql(
      """
        |SELECT * FROM VALUES ('one', 1), ('three', null)
        |  CROSS JOIN VALUES ('one', 1), ('three', null)
      """.stripMargin)
    checkKeywordsExistsInExplain(df,
      "Join Cross",
      ":- LocalRelation [col1#x, col2#x]",
      "+- LocalRelation [col1#x, col2#x]")
  }

  test("explain table valued functions") {
    checkKeywordsExistsInExplain(sql("select * from RaNgE(2)"), "Range (0, 2, step=1)")
    checkKeywordsExistsInExplain(sql("SELECT * FROM range(3) CROSS JOIN range(3)"),
      "Join Cross",
      ":- Range (0, 3, step=1)",
      "+- Range (0, 3, step=1)")
  }

  test("explain lateral joins") {
    checkKeywordsExistsInExplain(
      sql("SELECT * FROM VALUES (0, 1) AS (a, b), LATERAL (SELECT a)"),
      "LateralJoin lateral-subquery#x [a#x], Inner",
      "Project [outer(a#x) AS a#x]"
    )
  }

  test("explain string functions") {
    // Check if catalyst combine nested `Concat`s
    val df1 = sql(
      """
        |SELECT (col1 || col2 || col3 || col4) col
        |  FROM (SELECT id col1, id col2, id col3, id col4 FROM range(10))
      """.stripMargin)
    checkKeywordsExistsInExplain(df1,
      "Project [concat(cast(id#xL as string), cast(id#xL as string), cast(id#xL as string)" +
        ", cast(id#xL as string)) AS col#x]")

    // Check if catalyst combine nested `Concat`s if concatBinaryAsString=false
    withSQLConf(SQLConf.CONCAT_BINARY_AS_STRING.key -> "false") {
      val df2 = sql(
        """
          |SELECT ((col1 || col2) || (col3 || col4)) col
          |FROM (
          |  SELECT
          |    string(id) col1,
          |    string(id + 1) col2,
          |    encode(string(id + 2), 'utf-8') col3,
          |    encode(string(id + 3), 'utf-8') col4
          |  FROM range(10)
          |)
        """.stripMargin)
      checkKeywordsExistsInExplain(df2,
        "Project [concat(concat(col1#x, col2#x), cast(concat(col3#x, col4#x) as string)) AS col#x]",
        "Project [cast(id#xL as string) AS col1#x, " +
          "cast((id#xL + cast(1 as bigint)) as string) AS col2#x, " +
          "encode(cast((id#xL + cast(2 as bigint)) as string), utf-8) AS col3#x, " +
          "encode(cast((id#xL + cast(3 as bigint)) as string), utf-8) AS col4#x]")

      val df3 = sql(
        """
          |SELECT (col1 || (col3 || col4)) col
          |FROM (
          |  SELECT
          |    string(id) col1,
          |    encode(string(id + 2), 'utf-8') col3,
          |    encode(string(id + 3), 'utf-8') col4
          |  FROM range(10)
          |)
        """.stripMargin)
      checkKeywordsExistsInExplain(df3,
        "Project [concat(col1#x, cast(concat(col3#x, col4#x) as string)) AS col#x]",
        "Project [cast(id#xL as string) AS col1#x, " +
          "encode(cast((id#xL + cast(2 as bigint)) as string), utf-8) AS col3#x, " +
          "encode(cast((id#xL + cast(3 as bigint)) as string), utf-8) AS col4#x]")
    }
  }

  test("check operator precedence") {
    // We follow Oracle operator precedence in the table below that lists the levels
    // of precedence among SQL operators from high to low:
    // ---------------------------------------------------------------------------------------
    // Operator                                          Operation
    // ---------------------------------------------------------------------------------------
    // +, -                                              identity, negation
    // *, /                                              multiplication, division
    // +, -, ||                                          addition, subtraction, concatenation
    // =, !=, <, >, <=, >=, IS NULL, LIKE, BETWEEN, IN   comparison
    // NOT                                               exponentiation, logical negation
    // AND                                               conjunction
    // OR                                                disjunction
    // ---------------------------------------------------------------------------------------
    checkKeywordsExistsInExplain(sql("select '1' || 1 + 2"),
      "Project [13", " AS (concat(1, 1) + 2)#x")
    checkKeywordsExistsInExplain(sql("select 1 - 2 || 'b'"),
      "Project [-1b AS concat((1 - 2), b)#x]")
    checkKeywordsExistsInExplain(sql("select 2 * 4  + 3 || 'b'"),
      "Project [11b AS concat(((2 * 4) + 3), b)#x]")
    checkKeywordsExistsInExplain(sql("select 3 + 1 || 'a' || 4 / 2"),
      "Project [4a2.0 AS concat(concat((3 + 1), a), (4 / 2))#x]")
    checkKeywordsExistsInExplain(sql("select 1 == 1 OR 'a' || 'b' ==  'ab'"),
      "Project [true AS ((1 = 1) OR (concat(a, b) = ab))#x]")
    checkKeywordsExistsInExplain(sql("select 'a' || 'c' == 'ac' AND 2 == 3"),
      "Project [false AS ((concat(a, c) = ac) AND (2 = 3))#x]")
  }

  test("explain for these functions; use range to avoid constant folding") {
    val df = sql("select ifnull(id, 1), nullif(id, 1), nvl(id, 1), nvl2(id, 1, 2) " +
      "from range(2)")
    checkKeywordsExistsInExplain(df,
      "Project [id#xL AS ifnull(id, 1)#xL, if ((id#xL = 1)) null " +
        "else id#xL AS nullif(id, 1)#xL, id#xL AS nvl(id, 1)#xL, 1 AS nvl2(id, 1, 2)#x]")
  }

  test("SPARK-26659: explain of DataWritingCommandExec should not contain duplicate cmd.nodeName") {
    withTable("temptable") {
      val df = sql("create table temptable using parquet as select * from range(2)")
      withNormalizedExplain(df, SimpleMode) { normalizedOutput =>
        // scalastyle:off
        // == Physical Plan ==
        // Execute CreateDataSourceTableAsSelectCommand
        //   +- CreateDataSourceTableAsSelectCommand `spark_catalog`.`default`.`temptable`, ErrorIfExists, Project [id#5L], [id]
        // scalastyle:on
        assert("Create\\w*?TableAsSelectCommand".r.findAllMatchIn(normalizedOutput).length == 2)
      }
    }
  }

  test("SPARK-33853: explain codegen - check presence of subquery") {
    withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
      withTempView("df") {
        val df1 = spark.range(1, 100)
        df1.createTempView("df")

        val sqlText = "EXPLAIN CODEGEN SELECT (SELECT min(id) FROM df)"
        val expectedText = "Found 3 WholeStageCodegen subtrees."

        withNormalizedExplain(sqlText) { normalizedOutput =>
          assert(normalizedOutput.contains(expectedText))
        }
      }
    }
  }

  test("explain formatted - check presence of subquery in case of DPP") {
    withTable("df1", "df2") {
      withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
        SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
          spark.range(1000).select(col("id"), col("id").as("k"))
            .write
            .partitionBy("k")
            .format("parquet")
            .mode("overwrite")
            .saveAsTable("df1")

          spark.range(100)
            .select(col("id"), col("id").as("k"))
            .write
            .partitionBy("k")
            .format("parquet")
            .mode("overwrite")
            .saveAsTable("df2")

          val sqlText =
            """
              |EXPLAIN FORMATTED SELECT df1.id, df2.k
              |FROM df1 JOIN df2 ON df1.k = df2.k AND df2.id < 2
              |""".stripMargin

          val expected_pattern1 =
            "Subquery:1 Hosting operator id = 1 Hosting Expression = k#xL IN subquery#x"
          val expected_pattern2 =
            "PartitionFilters: \\[isnotnull\\(k#xL\\), dynamicpruningexpression\\(k#xL " +
              "IN subquery#x\\)\\]"
          val expected_pattern3 =
            "Location: InMemoryFileIndex \\[\\S*org.apache.spark.sql.ExplainSuite" +
              "/df2/\\S*, ... 99 entries\\]"
          val expected_pattern4 =
            "Location: InMemoryFileIndex \\[\\S*org.apache.spark.sql.ExplainSuite" +
              "/df1/\\S*, ... 999 entries\\]"
          withNormalizedExplain(sqlText) { normalizedOutput =>
            assert(expected_pattern1.r.findAllMatchIn(normalizedOutput).length == 1)
            assert(expected_pattern2.r.findAllMatchIn(normalizedOutput).length == 1)
            assert(expected_pattern3.r.findAllMatchIn(normalizedOutput).length == 2)
            assert(expected_pattern4.r.findAllMatchIn(normalizedOutput).length == 1)
          }
        }
    }
  }

  test("SPARK-33850: explain formatted - check presence of subquery in case of AQE") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      withTempView("df") {
        val df = spark.range(1, 100)
        df.createTempView("df")

        val sqlText = "EXPLAIN FORMATTED SELECT (SELECT min(id) FROM df) as v"
        val expected_pattern =
          "Subquery:1 Hosting operator id = 2 Hosting Expression = Subquery subquery#x"

        withNormalizedExplain(sqlText) { normalizedOutput =>
          assert(expected_pattern.r.findAllMatchIn(normalizedOutput).length == 1)
        }
      }
    }
  }

  test("Support ExplainMode in Dataset.explain") {
    val df1 = Seq((1, 2), (2, 3)).toDF("k", "v1")
    val df2 = Seq((2, 3), (1, 1)).toDF("k", "v2")
    val testDf = df1.join(df2, "k").groupBy("k").agg(count("v1"), sum("v1"), avg("v2"))

    val simpleExplainOutput = getNormalizedExplain(testDf, SimpleMode)
    assert(simpleExplainOutput.startsWith("== Physical Plan =="))
    Seq("== Parsed Logical Plan ==",
        "== Analyzed Logical Plan ==",
        "== Optimized Logical Plan ==").foreach { planType =>
      assert(!simpleExplainOutput.contains(planType))
    }
    checkKeywordsExistsInExplain(
      testDf,
      ExtendedMode,
      "== Parsed Logical Plan ==" ::
        "== Analyzed Logical Plan ==" ::
        "== Optimized Logical Plan ==" ::
        "== Physical Plan ==" ::
        Nil: _*)
    checkKeywordsExistsInExplain(
      testDf,
      CostMode,
      "Statistics(sizeInBytes=" ::
        Nil: _*)
    checkKeywordsExistsInExplain(
      testDf,
      CodegenMode,
      "WholeStageCodegen subtrees" ::
        "Generated code:" ::
        Nil: _*)
    checkKeywordsExistsInExplain(
      testDf,
      FormattedMode,
      "* LocalTableScan (1)" ::
        "(1) LocalTableScan [codegen id :" ::
        Nil: _*)
  }

  test("Catch and log errors when failing to write to external data source") {
    val password = "MyPassWord"
    val token = "MyToken"
    val value = "value"
    val options = Map("password" -> password, "token" -> token, "key" -> value)
    val query = spark.range(10).logicalPlan
    val cmd = SaveIntoDataSourceCommand(query, null, options, SaveMode.Overwrite)

    checkError(
      exception = intercept[AnalysisException] {
        cmd.run(spark)
      },
      condition = "DATA_SOURCE_EXTERNAL_ERROR",
      sqlState = "KD00F",
      parameters = Map.empty
    )
  }

  test("SPARK-34970: Redact Map type options in explain output") {
    val password = "MyPassWord"
    val token = "MyToken"
    val value = "value"
    val options = Map("password" -> password, "token" -> token, "key" -> value)
    val cmd = SaveIntoDataSourceCommand(spark.range(10).logicalPlan, new TestOptionsSource,
      options, SaveMode.Overwrite)

    Seq(SimpleMode, ExtendedMode, FormattedMode).foreach { mode =>
      checkKeywordsExistsInExplain(cmd, mode, value)
    }
    Seq(SimpleMode, ExtendedMode, CodegenMode, CostMode, FormattedMode).foreach { mode =>
      checkKeywordsNotExistsInExplain(cmd, mode, password)
      checkKeywordsNotExistsInExplain(cmd, mode, token)
    }
  }

  test("SPARK-34970: Redact CaseInsensitiveMap type options in explain output") {
    val password = "MyPassWord"
    val token = "MyToken"
    val value = "value"
    val tableName = "t"
    withTable(tableName) {
      val df1 = spark.range(10).toDF()
      df1.write.format("json").saveAsTable(tableName)
      val df2 = spark.read
        .option("key", value)
        .option("password", password)
        .option("token", token)
        .table(tableName)

      checkKeywordsExistsInExplain(df2, ExtendedMode, value)
      Seq(SimpleMode, ExtendedMode, CodegenMode, CostMode, FormattedMode).foreach { mode =>
        checkKeywordsNotExistsInExplain(df2, mode, password)
        checkKeywordsNotExistsInExplain(df2, mode, token)
      }
    }
  }

  test("Dataset.toExplainString has mode as string") {
    val df = spark.range(10).toDF()
    def assertExplainOutput(mode: ExplainMode): Unit = {
      assert(df.queryExecution.explainString(mode).replaceAll("#\\d+", "#x").trim ===
        getNormalizedExplain(df, mode).trim)
    }
    assertExplainOutput(SimpleMode)
    assertExplainOutput(ExtendedMode)
    assertExplainOutput(CodegenMode)
    assertExplainOutput(CostMode)
    assertExplainOutput(FormattedMode)

    val errMsg = intercept[IllegalArgumentException] {
      ExplainMode.fromString("unknown")
    }.getMessage
    assert(errMsg.contains("Unknown explain mode: unknown"))
  }

  test("SPARK-31504: Output fields in formatted Explain should have determined order") {
    withTempPath { path =>
      spark.range(10).selectExpr("id as a", "id as b", "id as c", "id as d", "id as e")
        .write.mode("overwrite").parquet(path.getAbsolutePath)
      val df1 = spark.read.parquet(path.getAbsolutePath)
      val df2 = spark.read.parquet(path.getAbsolutePath)
      assert(getNormalizedExplain(df1, FormattedMode) === getNormalizedExplain(df2, FormattedMode))
    }
  }

  test("Coalesced bucket info should be a part of explain string") {
    withTable("t1", "t2") {
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0",
        SQLConf.COALESCE_BUCKETS_IN_JOIN_ENABLED.key -> "true") {
        Seq(1, 2).toDF("i").write.bucketBy(8, "i").saveAsTable("t1")
        Seq(2, 3).toDF("i").write.bucketBy(4, "i").saveAsTable("t2")
        val df1 = spark.table("t1")
        val df2 = spark.table("t2")
        val joined = df1.join(df2, df1("i") === df2("i"))
        checkKeywordsExistsInExplain(
          joined,
          SimpleMode,
          "SelectedBucketsCount: 8 out of 8 (Coalesced to 4)" :: Nil: _*)
      }
    }
  }

  test("Explain formatted output for scan operator for datasource V2") {
    withTempDir { dir =>
      Seq("parquet", "orc", "csv", "json").foreach { fmt =>
        val basePath = dir.getCanonicalPath + "/" + fmt

        val expectedPlanFragment =
          s"""
             |\\(1\\) BatchScan $fmt file:$basePath
             |Output \\[2\\]: \\[value#x, id#x\\]
             |DataFilters: \\[isnotnull\\(value#x\\), \\(value#x > 2\\)\\]
             |Format: $fmt
             |Location: InMemoryFileIndex\\([0-9]+ paths\\)\\[.*\\]
             |PartitionFilters: \\[isnotnull\\(id#x\\), \\(id#x > 1\\)\\]
             |PushedFilters: \\[IsNotNull\\(value\\), GreaterThan\\(value,2\\)\\]
             |ReadSchema: struct\\<value:int\\>
             |""".stripMargin.trim

        spark.range(10)
          .select(col("id"), col("id").as("value"))
          .write.option("header", true)
          .partitionBy("id")
          .format(fmt)
          .save(basePath)
        val readSchema =
          StructType(Seq(StructField("id", IntegerType), StructField("value", IntegerType)))
        withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
          val df = spark
            .read
            .schema(readSchema)
            .option("header", true)
            .format(fmt)
            .load(basePath).where($"id" > 1 && $"value" > 2)
          val normalizedOutput = getNormalizedExplain(df, FormattedMode)
          assert(expectedPlanFragment.r.findAllMatchIn(normalizedOutput).length == 1)
        }
      }
    }
  }

  test("Explain UnresolvedRelation with CaseInsensitiveStringMap options") {
    val tableName = "test"
    withTable(tableName) {
      val df1 = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df1.write.saveAsTable(tableName)
      val df2 = spark.read
        .option("key1", "value1")
        .option("KEY2", "VALUE2")
        .table(tableName)
      // == Parsed Logical Plan ==
      // 'UnresolvedRelation [test], [key1=value1, KEY2=VALUE2]
      checkKeywordsExistsInExplain(df2, keywords = "[key1=value1, KEY2=VALUE2]")
    }
  }

  test("SPARK-35225: Handle empty output for analyzed plan") {
    withTempView("test") {
      checkKeywordsExistsInExplain(
        sql("CREATE TEMPORARY VIEW test AS SELECT 1"),
        "== Analyzed Logical Plan ==\nCreateViewCommand")
    }
  }

  test("SPARK-39112: UnsupportedOperationException if explain cost command using v2 command") {
    withTempDir { dir =>
      sql("EXPLAIN COST CREATE DATABASE tmp")
      sql("EXPLAIN COST DESC DATABASE tmp")
      sql(s"EXPLAIN COST ALTER DATABASE tmp SET LOCATION '${dir.toURI.toString}'")
      sql("EXPLAIN COST USE tmp")
      sql("EXPLAIN COST CREATE TABLE t(c1 int) USING PARQUET")
      sql("EXPLAIN COST SHOW TABLES")
      sql("EXPLAIN COST SHOW CREATE TABLE t")
      sql("EXPLAIN COST SHOW TBLPROPERTIES t")
      sql("EXPLAIN COST DROP TABLE t")
      sql("EXPLAIN COST DROP DATABASE tmp")
    }
  }
}

class ExplainSuiteAE extends ExplainSuiteHelper with EnableAdaptiveExecutionSuite {
  import testImplicits._

  test("SPARK-35884: Explain Formatted") {
    val df1 = Seq((1, 2), (2, 3)).toDF("k", "v1")
    val df2 = Seq((2, 3), (1, 1)).toDF("k", "v2")
    val testDf = df1.join(df2, "k").groupBy("k").agg(count("v1"), sum("v1"), avg("v2"))
    // trigger the final plan for AQE
    testDf.collect()
    // AdaptiveSparkPlan (21)
    // +- == Final Plan ==
    //    * HashAggregate (12)
    //    +- AQEShuffleRead (11)
    //       +- ShuffleQueryStage (10)
    //          +- Exchange (9)
    //             +- * HashAggregate (8)
    //                +- * Project (7)
    //                   +- * BroadcastHashJoin Inner BuildRight (6)
    //                      :- * LocalTableScan (1)
    //                      +- BroadcastQueryStage (5)
    //                         +- BroadcastExchange (4)
    //                            +- * Project (3)
    //                               +- * LocalTableScan (2)
    // +- == Initial Plan ==
    //    HashAggregate (20)
    //    +- Exchange (19)
    //       +- HashAggregate (18)
    //          +- Project (17)
    //             +- BroadcastHashJoin Inner BuildRight (16)
    //                :- Project (14)
    //                :  +- LocalTableScan (13)
    //                +- BroadcastExchange (15)
    //                   +- Project (3)
    //                      +- LocalTableScan (2)
    checkKeywordsExistsInExplain(
      testDf,
      FormattedMode,
      """
        |(5) BroadcastQueryStage
        |Output [2]: [k#x, v2#x]
        |Arguments: 0""".stripMargin,
      """
        |(10) ShuffleQueryStage
        |Output [5]: [k#x, count#xL, sum#xL, sum#x, count#xL]
        |Arguments: 1""".stripMargin,
      """
        |(11) AQEShuffleRead
        |Input [5]: [k#x, count#xL, sum#xL, sum#x, count#xL]
        |Arguments: coalesced
        |""".stripMargin,
      """
        |(16) BroadcastHashJoin
        |Left keys [1]: [k#x]
        |Right keys [1]: [k#x]
        |Join type: Inner
        |Join condition: None
        |""".stripMargin,
      """
        |(19) Exchange
        |Input [5]: [k#x, count#xL, sum#xL, sum#x, count#xL]
        |""".stripMargin,
      """
        |(21) AdaptiveSparkPlan
        |Output [4]: [k#x, count(v1)#xL, sum(v1)#xL, avg(v2)#x]
        |Arguments: isFinalPlan=true
        |""".stripMargin
    )
    checkKeywordsNotExistsInExplain(testDf, FormattedMode, "unknown")
  }

  test("SPARK-35884: Explain should only display one plan before AQE takes effect") {
    val df = (0 to 10).toDF("id").where($"id" > 5)
    val modes = Seq(SimpleMode, ExtendedMode, CostMode, FormattedMode)
    modes.foreach { mode =>
      checkKeywordsExistsInExplain(df, mode, "AdaptiveSparkPlan")
      checkKeywordsNotExistsInExplain(df, mode, "Initial Plan", "Current Plan")
    }
    df.collect()
    modes.foreach { mode =>
      checkKeywordsExistsInExplain(df, mode, "Initial Plan", "Final Plan")
      checkKeywordsNotExistsInExplain(df, mode, "unknown")
    }
  }

  test("SPARK-35884: Explain formatted with subquery") {
    withTempView("t1", "t2") {
      spark.range(100).select($"id" % 10 as "key", $"id" as "value")
        .createOrReplaceTempView("t1")
      spark.range(10).createOrReplaceTempView("t2")
      val query =
        """
          |SELECT key, value FROM t1
          |JOIN t2 ON t1.key = t2.id
          |WHERE value > (SELECT MAX(id) FROM t2)
          |""".stripMargin
      val df = sql(query).toDF()
      df.collect()
      checkKeywordsExistsInExplain(df, FormattedMode,
        """
          |(2) Filter [codegen id : 2]
          |Input [1]: [id#xL]
          |Condition : ((id#xL > Subquery subquery#x, [id=#x]) AND isnotnull((id#xL % 10)))
          |""".stripMargin,
        """
          |(6) BroadcastQueryStage
          |Output [1]: [id#xL]
          |Arguments: 0""".stripMargin,
        """
          |(12) AdaptiveSparkPlan
          |Output [2]: [key#xL, value#xL]
          |Arguments: isFinalPlan=true
          |""".stripMargin,
        """
          |Subquery:1 Hosting operator id = 2 Hosting Expression = Subquery subquery#x, [id=#x]
          |""".stripMargin,
        """
          |(16) ShuffleQueryStage
          |Output [1]: [max#xL]
          |Arguments: 0""".stripMargin,
        """
          |(20) AdaptiveSparkPlan
          |Output [1]: [max(id)#xL]
          |Arguments: isFinalPlan=true
          |""".stripMargin
      )
      checkKeywordsNotExistsInExplain(df, FormattedMode, "unknown")
    }
  }

  test("SPARK-35133: explain codegen should work with AQE") {
    withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
      withTempView("df") {
        val df = spark.range(5).select(col("id").as("key"), col("id").as("value"))
        df.createTempView("df")

        val sqlText = "EXPLAIN CODEGEN SELECT key, MAX(value) FROM df GROUP BY key"
        val expectedCodegenText = "Found 1 WholeStageCodegen subtrees."
        val expectedNoCodegenText = "Found 0 WholeStageCodegen subtrees."
        withNormalizedExplain(sqlText) { normalizedOutput =>
          assert(normalizedOutput.contains(expectedNoCodegenText))
        }

        val aggDf = df.groupBy($"key").agg(max($"value"))
        withNormalizedExplain(aggDf, CodegenMode) { normalizedOutput =>
          assert(normalizedOutput.contains(expectedNoCodegenText))
        }

        // trigger the final plan for AQE
        aggDf.collect()
        withNormalizedExplain(aggDf, CodegenMode) { normalizedOutput =>
          assert(normalizedOutput.contains(expectedCodegenText))
        }
      }
    }
  }

  test("SPARK-32986: Bucketed scan info should be a part of explain string") {
    withTable("t1", "t2") {
      Seq((1, 2), (2, 3)).toDF("i", "j").write.bucketBy(8, "i").saveAsTable("t1")
      Seq(2, 3).toDF("i").write.bucketBy(8, "i").saveAsTable("t2")
      val df1 = spark.table("t1")
      val df2 = spark.table("t2")

      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0") {
        checkKeywordsExistsInExplain(
          df1.join(df2, df1("i") === df2("i")),
          "Bucketed: true")
      }

      withSQLConf(SQLConf.BUCKETING_ENABLED.key -> "false") {
        checkKeywordsExistsInExplain(
          df1.join(df2, df1("i") === df2("i")),
          "Bucketed: false (disabled by configuration)")
      }

      checkKeywordsExistsInExplain(df1, "Bucketed: false (disabled by query planner)" )

      checkKeywordsExistsInExplain(
        df1.select("j"),
        "Bucketed: false (bucket column(s) not read)")
    }
  }

  test("SPARK-36795: Node IDs should not be duplicated when InMemoryRelation present") {
    withTempView("t1", "t2") {
      Seq(1).toDF("k").write.saveAsTable("t1")
      Seq(1).toDF("key").write.saveAsTable("t2")
      spark.sql("SELECT * FROM t1").persist()
      val query = "SELECT * FROM (SELECT * FROM t1) join t2 " +
        "ON k = t2.key"
      val df = sql(query).toDF()
      df.collect()
      val inMemoryRelationRegex = """InMemoryRelation \(([0-9]+)\)""".r
      val columnarToRowRegex = """ColumnarToRow \(([0-9]+)\)""".r
      val explainString = getNormalizedExplain(df, FormattedMode)
      val inMemoryRelationNodeId = inMemoryRelationRegex.findAllIn(explainString).group(1)
      val columnarToRowNodeId = columnarToRowRegex.findAllIn(explainString).group(1)

      assert(inMemoryRelationNodeId != columnarToRowNodeId)
    }
  }

  test("SPARK-38232: Explain formatted does not collect subqueries under query stage in AQE") {
    withTable("t") {
      sql("CREATE TABLE t USING PARQUET AS SELECT 1 AS c")
      val expected =
        "Subquery:1 Hosting operator id = 2 Hosting Expression = Subquery subquery#x, [id=#x]"
      val df = sql("SELECT count(s) FROM (SELECT (SELECT c FROM t) as s)")
      df.collect()
      withNormalizedExplain(df, FormattedMode) { output =>
        assert(output.contains(expected))
      }
    }
  }

  test("SPARK-38322: Support query stage show runtime statistics in formatted explain mode") {
    val df = Seq(1, 2).toDF("c").distinct()
    val statistics = "Statistics(sizeInBytes=32.0 B, rowCount=2)"

    checkKeywordsNotExistsInExplain(
      df,
      FormattedMode,
      statistics)

    df.collect()
    checkKeywordsExistsInExplain(
      df,
      FormattedMode,
      statistics)
  }

  test("SPARK-42753: Process subtree for ReusedExchange with unknown child") {
    // Simulate a simplified subtree with a ReusedExchange pointing to an Exchange node that has
    // no ID. This is a rare edge case that could arise during AQE if there are multiple
    // ReusedExchanges. We check to make sure the child Exchange gets an ID and gets printed
    val exchange = ShuffleExchangeExec(RoundRobinPartitioning(10),
      RangeExec(org.apache.spark.sql.catalyst.plans.logical.Range(0, 1000, 1, 10)))
    val reused = ReusedExchangeExec(exchange.output, exchange)
    var results = ""
    def appendStr(str: String): Unit = {
      results = results + str
    }
    ExplainUtils.processPlan[SparkPlan](reused, appendStr(_))

    val expectedTree = """|ReusedExchange (1)
                          |
                          |
                          |(1) ReusedExchange [Reuses operator id: 3]
                          |Output [1]: [id#xL]
                          |
                          |===== Adaptively Optimized Out Exchanges =====
                          |
                          |Subplan:1
                          |Exchange (3)
                          |+- Range (2)
                          |
                          |
                          |(2) Range
                          |Output [1]: [id#xL]
                          |Arguments: Range (0, 1000, step=1, splits=Some(10))
                          |
                          |(3) Exchange
                          |Input [1]: [id#xL]
                          |Arguments: RoundRobinPartitioning(10), ENSURE_REQUIREMENTS, [plan_id=x]
                          |
                          |""".stripMargin

    results = results.replaceAll("#\\d+", "#x").replaceAll("plan_id=\\d+", "plan_id=x")
    assert(results == expectedTree)
  }

  test("SPARK-42753: Two ReusedExchange Sharing Same Subtree") {
    // Simulate a simplified subtree with a two ReusedExchange reusing the same exchange
    // Only one exchange node should be printed
    val exchange = ShuffleExchangeExec(RoundRobinPartitioning(10),
      RangeExec(org.apache.spark.sql.catalyst.plans.logical.Range(0, 1000, 1, 10)))
    val reused1 = ReusedExchangeExec(exchange.output, exchange)
    val reused2 = ReusedExchangeExec(exchange.output, exchange)
    val join = SortMergeJoinExec(reused1.output, reused2.output, Inner, None, reused1, reused2)

    var results = ""
    def appendStr(str: String): Unit = {
      results = results + str
    }

    ExplainUtils.processPlan[SparkPlan](join, appendStr(_))

    val expectedTree = """|SortMergeJoin Inner (3)
                          |:- ReusedExchange (1)
                          |+- ReusedExchange (2)
                          |
                          |
                          |(1) ReusedExchange [Reuses operator id: 5]
                          |Output [1]: [id#xL]
                          |
                          |(2) ReusedExchange [Reuses operator id: 5]
                          |Output [1]: [id#xL]
                          |
                          |(3) SortMergeJoin
                          |Left keys [1]: [id#xL]
                          |Right keys [1]: [id#xL]
                          |Join type: Inner
                          |Join condition: None
                          |
                          |===== Adaptively Optimized Out Exchanges =====
                          |
                          |Subplan:1
                          |Exchange (5)
                          |+- Range (4)
                          |
                          |
                          |(4) Range
                          |Output [1]: [id#xL]
                          |Arguments: Range (0, 1000, step=1, splits=Some(10))
                          |
                          |(5) Exchange
                          |Input [1]: [id#xL]
                          |Arguments: RoundRobinPartitioning(10), ENSURE_REQUIREMENTS, [plan_id=x]
                          |
                          |""".stripMargin
    results = results.replaceAll("#\\d+", "#x").replaceAll("plan_id=\\d+", "plan_id=x")
    assert(results == expectedTree)
  }

  test("SPARK-42753: Correctly separate two ReusedExchange not sharing subtree") {
    // Simulate two ReusedExchanges reusing two different Exchanges that appear similar
    // The two exchanges should have separate IDs and printed separately
    val exchange1 = ShuffleExchangeExec(RoundRobinPartitioning(10),
      RangeExec(org.apache.spark.sql.catalyst.plans.logical.Range(0, 1000, 1, 10)))
    val reused1 = ReusedExchangeExec(exchange1.output, exchange1)
    val exchange2 = ShuffleExchangeExec(RoundRobinPartitioning(10),
      RangeExec(org.apache.spark.sql.catalyst.plans.logical.Range(0, 1000, 1, 10)))
    val reused2 = ReusedExchangeExec(exchange2.output, exchange2)
    val join = SortMergeJoinExec(reused1.output, reused2.output, Inner, None, reused1, reused2)

    var results = ""
    def appendStr(str: String): Unit = {
      results = results + str
    }

    ExplainUtils.processPlan[SparkPlan](join, appendStr(_))

    val expectedTree = """|SortMergeJoin Inner (3)
                          |:- ReusedExchange (1)
                          |+- ReusedExchange (2)
                          |
                          |
                          |(1) ReusedExchange [Reuses operator id: 5]
                          |Output [1]: [id#xL]
                          |
                          |(2) ReusedExchange [Reuses operator id: 7]
                          |Output [1]: [id#xL]
                          |
                          |(3) SortMergeJoin
                          |Left keys [1]: [id#xL]
                          |Right keys [1]: [id#xL]
                          |Join type: Inner
                          |Join condition: None
                          |
                          |===== Adaptively Optimized Out Exchanges =====
                          |
                          |Subplan:1
                          |Exchange (5)
                          |+- Range (4)
                          |
                          |
                          |(4) Range
                          |Output [1]: [id#xL]
                          |Arguments: Range (0, 1000, step=1, splits=Some(10))
                          |
                          |(5) Exchange
                          |Input [1]: [id#xL]
                          |Arguments: RoundRobinPartitioning(10), ENSURE_REQUIREMENTS, [plan_id=x]
                          |
                          |Subplan:2
                          |Exchange (7)
                          |+- Range (6)
                          |
                          |
                          |(6) Range
                          |Output [1]: [id#xL]
                          |Arguments: Range (0, 1000, step=1, splits=Some(10))
                          |
                          |(7) Exchange
                          |Input [1]: [id#xL]
                          |Arguments: RoundRobinPartitioning(10), ENSURE_REQUIREMENTS, [plan_id=x]
                          |
                          |""".stripMargin
    results = results.replaceAll("#\\d+", "#x").replaceAll("plan_id=\\d+", "plan_id=x")
    assert(results == expectedTree)
  }
}

case class ExplainSingleData(id: Int)
