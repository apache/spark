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

package org.apache.spark.sql.jdbc

import java.sql.{Connection, DriverManager}
import java.util.Properties

import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{AnalysisException, DataFrame, ExplainSuiteHelper, QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.CannotReplaceMissingTableException
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, GlobalLimit, LocalLimit, Offset, Sort}
import org.apache.spark.sql.connector.{IntegralAverage, StrLen}
import org.apache.spark.sql.connector.catalog.{Catalogs, Identifier, TableCatalog}
import org.apache.spark.sql.connector.catalog.functions.{ScalarFunction, UnboundFunction}
import org.apache.spark.sql.connector.catalog.index.SupportsIndex
import org.apache.spark.sql.connector.expressions.Expression
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2ScanRelation, V1ScanWrapper}
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.functions.{abs, acos, asin, atan, atan2, avg, ceil, coalesce, cos, cosh, cot, count, count_distinct, degrees, exp, floor, lit, log => logarithm, log10, not, pow, radians, round, signum, sin, sinh, sqrt, sum, tan, tanh, udf, when}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}
import org.apache.spark.util.Utils

class JDBCV2Suite extends QueryTest with SharedSparkSession with ExplainSuiteHelper {
  import testImplicits._

  val tempDir = Utils.createTempDir()
  val url = s"jdbc:h2:${tempDir.getCanonicalPath};user=testUser;password=testPass"

  val testH2Dialect = new JdbcDialect {
    override def canHandle(url: String): Boolean = H2Dialect.canHandle(url)

    class H2SQLBuilder extends JDBCSQLBuilder {
      override def visitUserDefinedScalarFunction(
          funcName: String, canonicalName: String, inputs: Array[String]): String = {
        canonicalName match {
          case "h2.char_length" =>
            s"$funcName(${inputs.mkString(", ")})"
          case _ => super.visitUserDefinedScalarFunction(funcName, canonicalName, inputs)
        }
      }

      override def visitUserDefinedAggregateFunction(
          funcName: String,
          canonicalName: String,
          isDistinct: Boolean,
          inputs: Array[String]): String = {
        canonicalName match {
          case "h2.iavg" =>
            if (isDistinct) {
              s"AVG(DISTINCT ${inputs.mkString(", ")})"
            } else {
              s"AVG(${inputs.mkString(", ")})"
            }
          case _ =>
            super.visitUserDefinedAggregateFunction(funcName, canonicalName, isDistinct, inputs)
        }
      }
    }

    override def compileExpression(expr: Expression): Option[String] = {
      val h2SQLBuilder = new H2SQLBuilder()
      try {
        Some(h2SQLBuilder.build(expr))
      } catch {
        case NonFatal(e) =>
          logWarning("Error occurs while compiling V2 expression", e)
          None
      }
    }

    override def functions: Seq[(String, UnboundFunction)] = H2Dialect.functions
  }

  case object CharLength extends ScalarFunction[Int] {
    override def inputTypes(): Array[DataType] = Array(StringType)
    override def resultType(): DataType = IntegerType
    override def name(): String = "CHAR_LENGTH"
    override def canonicalName(): String = "h2.char_length"

    override def produceResult(input: InternalRow): Int = {
      val s = input.getString(0)
      s.length
    }
  }

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.h2", classOf[JDBCTableCatalog].getName)
    .set("spark.sql.catalog.h2.url", url)
    .set("spark.sql.catalog.h2.driver", "org.h2.Driver")
    .set("spark.sql.catalog.h2.pushDownAggregate", "true")
    .set("spark.sql.catalog.h2.pushDownLimit", "true")
    .set("spark.sql.catalog.h2.pushDownOffset", "true")

  private def withConnection[T](f: Connection => T): T = {
    val conn = DriverManager.getConnection(url, new Properties())
    try {
      f(conn)
    } finally {
      conn.close()
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    Utils.classForName("org.h2.Driver")
    withConnection { conn =>
      conn.prepareStatement("CREATE SCHEMA \"test\"").executeUpdate()
      conn.prepareStatement(
        "CREATE TABLE \"test\".\"empty_table\" (name TEXT(32) NOT NULL, id INTEGER NOT NULL)")
        .executeUpdate()
      conn.prepareStatement(
        "CREATE TABLE \"test\".\"people\" (name TEXT(32) NOT NULL, id INTEGER NOT NULL)")
        .executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"people\" VALUES ('fred', 1)").executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"people\" VALUES ('mary', 2)").executeUpdate()
      conn.prepareStatement(
        "CREATE TABLE \"test\".\"employee\" (dept INTEGER, name TEXT(32), salary NUMERIC(20, 2)," +
          " bonus DOUBLE, is_manager BOOLEAN)").executeUpdate()
      conn.prepareStatement(
        "INSERT INTO \"test\".\"employee\" VALUES (1, 'amy', 10000, 1000, true)").executeUpdate()
      conn.prepareStatement(
        "INSERT INTO \"test\".\"employee\" VALUES (2, 'alex', 12000, 1200, false)").executeUpdate()
      conn.prepareStatement(
        "INSERT INTO \"test\".\"employee\" VALUES (1, 'cathy', 9000, 1200, false)").executeUpdate()
      conn.prepareStatement(
        "INSERT INTO \"test\".\"employee\" VALUES (2, 'david', 10000, 1300, true)").executeUpdate()
      conn.prepareStatement(
        "INSERT INTO \"test\".\"employee\" VALUES (6, 'jen', 12000, 1200, true)").executeUpdate()
      conn.prepareStatement(
        "CREATE TABLE \"test\".\"dept\" (\"dept id\" INTEGER NOT NULL, \"dept.id\" INTEGER)")
        .executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"dept\" VALUES (1, 1)").executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"dept\" VALUES (2, 1)").executeUpdate()

      // scalastyle:off
      conn.prepareStatement(
        "CREATE TABLE \"test\".\"person\" (\"名\" INTEGER NOT NULL)").executeUpdate()
      // scalastyle:on
      conn.prepareStatement("INSERT INTO \"test\".\"person\" VALUES (1)").executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"person\" VALUES (2)").executeUpdate()
      conn.prepareStatement(
        """CREATE TABLE "test"."view1" ("|col1" INTEGER, "|col2" INTEGER)""").executeUpdate()
      conn.prepareStatement(
        """CREATE TABLE "test"."view2" ("|col1" INTEGER, "|col3" INTEGER)""").executeUpdate()

      conn.prepareStatement(
        "CREATE TABLE \"test\".\"item\" (id INTEGER, name TEXT(32), price NUMERIC(23, 3))")
        .executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"item\" VALUES " +
        "(1, 'bottle', 11111111111111111111.123)").executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"item\" VALUES " +
        "(1, 'bottle', 99999999999999999999.123)").executeUpdate()

      conn.prepareStatement(
        "CREATE TABLE \"test\".\"datetime\" (name TEXT(32), date1 DATE, time1 TIMESTAMP)")
        .executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"datetime\" VALUES " +
        "('amy', '2022-05-19', '2022-05-19 00:00:00')").executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"datetime\" VALUES " +
        "('alex', '2022-05-18', '2022-05-18 00:00:00')").executeUpdate()
    }
    H2Dialect.registerFunction("my_avg", IntegralAverage)
    H2Dialect.registerFunction("my_strlen", StrLen(CharLength))
  }

  override def afterAll(): Unit = {
    H2Dialect.clearFunctions()
    Utils.deleteRecursively(tempDir)
    super.afterAll()
  }

  test("simple scan") {
    checkAnswer(sql("SELECT * FROM h2.test.empty_table"), Seq())
    checkAnswer(sql("SELECT * FROM h2.test.people"), Seq(Row("fred", 1), Row("mary", 2)))
    checkAnswer(sql("SELECT name, id FROM h2.test.people"), Seq(Row("fred", 1), Row("mary", 2)))
  }

  private def checkPushedInfo(df: DataFrame, expectedPlanFragment: String*): Unit = {
    withSQLConf(SQLConf.MAX_METADATA_STRING_LENGTH.key -> "1000") {
      df.queryExecution.optimizedPlan.collect {
        case _: DataSourceV2ScanRelation =>
          checkKeywordsExistsInExplain(df, expectedPlanFragment: _*)
      }
    }
  }

  // TABLESAMPLE ({integer_expression | decimal_expression} PERCENT) and
  // TABLESAMPLE (BUCKET integer_expression OUT OF integer_expression)
  // are tested in JDBC dialect tests because TABLESAMPLE is not supported by all the DBMS
  test("TABLESAMPLE (integer_expression ROWS) is the same as LIMIT") {
    val df = sql("SELECT NAME FROM h2.test.employee TABLESAMPLE (3 ROWS)")
    checkSchemaNames(df, Seq("NAME"))
    checkPushedInfo(df, "PushedFilters: [], PushedLimit: LIMIT 3, ")
    checkAnswer(df, Seq(Row("amy"), Row("alex"), Row("cathy")))
  }

  private def checkSchemaNames(df: DataFrame, names: Seq[String]): Unit = {
    val scan = df.queryExecution.optimizedPlan.collectFirst {
      case s: DataSourceV2ScanRelation => s
    }.get
    assert(scan.schema.names.sameElements(names))
  }

  private def checkLimitRemoved(df: DataFrame, removed: Boolean = true): Unit = {
    val limits = df.queryExecution.optimizedPlan.collect {
      case g: GlobalLimit => g
      case limit: LocalLimit => limit
    }
    if (removed) {
      assert(limits.isEmpty)
    } else {
      assert(limits.nonEmpty)
    }
  }

  test("simple scan with LIMIT") {
    val df1 = spark.read.table("h2.test.employee")
      .where($"dept" === 1).limit(1)
    checkLimitRemoved(df1)
    checkPushedInfo(df1,
      "PushedFilters: [DEPT IS NOT NULL, DEPT = 1], PushedLimit: LIMIT 1, ")
    checkAnswer(df1, Seq(Row(1, "amy", 10000.00, 1000.0, true)))

    val df2 = spark.read
      .option("partitionColumn", "dept")
      .option("lowerBound", "0")
      .option("upperBound", "2")
      .option("numPartitions", "2")
      .table("h2.test.employee")
      .filter($"dept" > 1)
      .limit(1)
    checkLimitRemoved(df2, false)
    checkPushedInfo(df2,
      "PushedFilters: [DEPT IS NOT NULL, DEPT > 1], PushedLimit: LIMIT 1, ")
    checkAnswer(df2, Seq(Row(2, "alex", 12000.00, 1200.0, false)))

    val df3 = sql("SELECT name FROM h2.test.employee WHERE dept > 1 LIMIT 1")
    checkSchemaNames(df3, Seq("NAME"))
    checkLimitRemoved(df3)
    checkPushedInfo(df3,
      "PushedFilters: [DEPT IS NOT NULL, DEPT > 1], PushedLimit: LIMIT 1, ")
    checkAnswer(df3, Seq(Row("alex")))

    val df4 = spark.read
      .table("h2.test.employee")
      .groupBy("DEPT").sum("SALARY")
      .limit(1)
    checkLimitRemoved(df4, false)
    checkPushedInfo(df4,
      "PushedAggregates: [SUM(SALARY)], PushedFilters: [], PushedGroupByExpressions: [DEPT], ")
    checkAnswer(df4, Seq(Row(1, 19000.00)))

    val name = udf { (x: String) => x.matches("cat|dav|amy") }
    val sub = udf { (x: String) => x.substring(0, 3) }
    val df5 = spark.read
      .table("h2.test.employee")
      .select($"SALARY", $"BONUS", sub($"NAME").as("shortName"))
      .filter(name($"shortName"))
      .limit(1)
    checkLimitRemoved(df5, false)
    // LIMIT is pushed down only if all the filters are pushed down
    checkPushedInfo(df5, "PushedFilters: [], ")
    checkAnswer(df5, Seq(Row(10000.00, 1000.0, "amy")))
  }

  private def checkOffsetRemoved(df: DataFrame, removed: Boolean = true): Unit = {
    val offsets = df.queryExecution.optimizedPlan.collect {
      case offset: Offset => offset
    }
    if (removed) {
      assert(offsets.isEmpty)
    } else {
      assert(offsets.nonEmpty)
    }
  }

  test("simple scan with OFFSET") {
    val df1 = spark.read
      .table("h2.test.employee")
      .where($"dept" === 1)
      .offset(1)
    checkOffsetRemoved(df1)
    checkPushedInfo(df1,
      "PushedFilters: [DEPT IS NOT NULL, DEPT = 1], PushedOffset: OFFSET 1,")
    checkAnswer(df1, Seq(Row(1, "cathy", 9000.00, 1200.0, false)))

    val df2 = spark.read
      .option("pushDownOffset", "false")
      .table("h2.test.employee")
      .where($"dept" === 1)
      .offset(1)
    checkOffsetRemoved(df2, false)
    checkPushedInfo(df2,
      "PushedFilters: [DEPT IS NOT NULL, DEPT = 1], ReadSchema:")
    checkAnswer(df2, Seq(Row(1, "cathy", 9000.00, 1200.0, false)))

    val df3 = spark.read
      .table("h2.test.employee")
      .where($"dept" === 1)
      .sort($"salary")
      .offset(1)
    checkOffsetRemoved(df3, false)
    checkPushedInfo(df3,
      "PushedFilters: [DEPT IS NOT NULL, DEPT = 1], ReadSchema:")
    checkAnswer(df3, Seq(Row(1, "amy", 10000.00, 1000.0, true)))

    val df4 = spark.read
      .option("partitionColumn", "dept")
      .option("lowerBound", "0")
      .option("upperBound", "2")
      .option("numPartitions", "2")
      .table("h2.test.employee")
      .filter($"dept" > 1)
      .offset(1)
    checkOffsetRemoved(df4, false)
    checkPushedInfo(df4, "PushedFilters: [DEPT IS NOT NULL, DEPT > 1], ReadSchema:")
    checkAnswer(df4, Seq(Row(2, "david", 10000, 1300, true), Row(6, "jen", 12000, 1200, true)))

    val df5 = spark.read
      .table("h2.test.employee")
      .groupBy("DEPT").sum("SALARY")
      .offset(1)
    checkOffsetRemoved(df5, false)
    checkPushedInfo(df5,
      "PushedAggregates: [SUM(SALARY)], PushedFilters: [], PushedGroupByExpressions: [DEPT], ")
    checkAnswer(df5, Seq(Row(2, 22000.00), Row(6, 12000.00)))

    val name = udf { (x: String) => x.matches("cat|dav|amy") }
    val sub = udf { (x: String) => x.substring(0, 3) }
    val df6 = spark.read
      .table("h2.test.employee")
      .select($"SALARY", $"BONUS", sub($"NAME").as("shortName"))
      .filter(name($"shortName"))
      .offset(1)
    checkOffsetRemoved(df6, false)
    // OFFSET is pushed down only if all the filters are pushed down
    checkPushedInfo(df6, "PushedFilters: [], ")
    checkAnswer(df6, Seq(Row(10000.00, 1300.0, "dav"), Row(9000.00, 1200.0, "cat")))
  }

  test("simple scan with LIMIT and OFFSET") {
    val df1 = spark.read
      .table("h2.test.employee")
      .where($"dept" === 1)
      .limit(2)
      .offset(1)
    checkLimitRemoved(df1)
    checkOffsetRemoved(df1)
    checkPushedInfo(df1,
      "PushedFilters: [DEPT IS NOT NULL, DEPT = 1], PushedLimit: LIMIT 2, PushedOffset: OFFSET 1,")
    checkAnswer(df1, Seq(Row(1, "cathy", 9000.00, 1200.0, false)))

    val df2 = spark.read
      .option("pushDownLimit", "false")
      .table("h2.test.employee")
      .where($"dept" === 1)
      .limit(2)
      .offset(1)
    checkLimitRemoved(df2, false)
    checkOffsetRemoved(df2, false)
    checkPushedInfo(df2,
      "PushedFilters: [DEPT IS NOT NULL, DEPT = 1], ReadSchema:")
    checkAnswer(df2, Seq(Row(1, "cathy", 9000.00, 1200.0, false)))

    val df3 = spark.read
      .option("pushDownOffset", "false")
      .table("h2.test.employee")
      .where($"dept" === 1)
      .limit(2)
      .offset(1)
    checkLimitRemoved(df3)
    checkOffsetRemoved(df3, false)
    checkPushedInfo(df3,
      "PushedFilters: [DEPT IS NOT NULL, DEPT = 1], PushedLimit: LIMIT 2, ReadSchema:")
    checkAnswer(df3, Seq(Row(1, "cathy", 9000.00, 1200.0, false)))

    val df4 = spark.read
      .option("pushDownLimit", "false")
      .option("pushDownOffset", "false")
      .table("h2.test.employee")
      .where($"dept" === 1)
      .limit(2)
      .offset(1)
    checkLimitRemoved(df4, false)
    checkOffsetRemoved(df4, false)
    checkPushedInfo(df4,
      "PushedFilters: [DEPT IS NOT NULL, DEPT = 1], ReadSchema:")
    checkAnswer(df4, Seq(Row(1, "cathy", 9000.00, 1200.0, false)))

    val df5 = spark.read
      .table("h2.test.employee")
      .where($"dept" === 1)
      .sort($"salary")
      .limit(2)
      .offset(1)
    checkLimitRemoved(df5)
    checkOffsetRemoved(df5)
    checkPushedInfo(df5, "PushedFilters: [DEPT IS NOT NULL, DEPT = 1], " +
      "PushedOffset: OFFSET 1, PushedTopN: ORDER BY [SALARY ASC NULLS FIRST] LIMIT 2, ReadSchema:")
    checkAnswer(df5, Seq(Row(1, "amy", 10000.00, 1000.0, true)))

    val df6 = spark.read
      .option("pushDownLimit", "false")
      .table("h2.test.employee")
      .where($"dept" === 1)
      .sort($"salary")
      .limit(2)
      .offset(1)
    checkLimitRemoved(df6, false)
    checkOffsetRemoved(df6, false)
    checkPushedInfo(df6, "PushedFilters: [DEPT IS NOT NULL, DEPT = 1], ReadSchema:")
    checkAnswer(df6, Seq(Row(1, "amy", 10000.00, 1000.0, true)))

    val df7 = spark.read
      .option("pushDownOffset", "false")
      .table("h2.test.employee")
      .where($"dept" === 1)
      .sort($"salary")
      .limit(2)
      .offset(1)
    checkLimitRemoved(df7)
    checkOffsetRemoved(df7, false)
    checkPushedInfo(df7, "PushedFilters: [DEPT IS NOT NULL, DEPT = 1]," +
      " PushedTopN: ORDER BY [SALARY ASC NULLS FIRST] LIMIT 2, ReadSchema:")
    checkAnswer(df7, Seq(Row(1, "amy", 10000.00, 1000.0, true)))

    val df8 = spark.read
      .option("pushDownLimit", "false")
      .option("pushDownOffset", "false")
      .table("h2.test.employee")
      .where($"dept" === 1)
      .sort($"salary")
      .limit(2)
      .offset(1)
    checkLimitRemoved(df8, false)
    checkOffsetRemoved(df8, false)
    checkPushedInfo(df8, "PushedFilters: [DEPT IS NOT NULL, DEPT = 1], ReadSchema:")
    checkAnswer(df8, Seq(Row(1, "amy", 10000.00, 1000.0, true)))

    val df9 = spark.read
      .option("partitionColumn", "dept")
      .option("lowerBound", "0")
      .option("upperBound", "2")
      .option("numPartitions", "2")
      .table("h2.test.employee")
      .filter($"dept" > 1)
      .limit(2)
      .offset(1)
    checkLimitRemoved(df9, false)
    checkOffsetRemoved(df9, false)
    checkPushedInfo(df9,
      "PushedFilters: [DEPT IS NOT NULL, DEPT > 1], PushedLimit: LIMIT 2, ReadSchema:")
    checkAnswer(df9, Seq(Row(2, "david", 10000.00, 1300.0, true)))

    val df10 = spark.read
      .table("h2.test.employee")
      .groupBy("DEPT").sum("SALARY")
      .limit(2)
      .offset(1)
    checkLimitRemoved(df10, false)
    checkOffsetRemoved(df10, false)
    checkPushedInfo(df10,
      "PushedAggregates: [SUM(SALARY)], PushedFilters: [], PushedGroupByExpressions: [DEPT], ")
    checkAnswer(df10, Seq(Row(2, 22000.00)))

    val name = udf { (x: String) => x.matches("cat|dav|amy") }
    val sub = udf { (x: String) => x.substring(0, 3) }
    val df11 = spark.read
      .table("h2.test.employee")
      .select($"SALARY", $"BONUS", sub($"NAME").as("shortName"))
      .filter(name($"shortName"))
      .limit(2)
      .offset(1)
    checkLimitRemoved(df11, false)
    checkOffsetRemoved(df11, false)
    checkPushedInfo(df11, "PushedFilters: [], ")
    checkAnswer(df11, Seq(Row(9000.00, 1200.0, "cat")))
  }

  test("simple scan with OFFSET and LIMIT") {
    val df1 = spark.read
      .table("h2.test.employee")
      .where($"dept" === 1)
      .offset(1)
      .limit(1)
    checkLimitRemoved(df1)
    checkOffsetRemoved(df1)
    checkPushedInfo(df1,
      "[DEPT IS NOT NULL, DEPT = 1], PushedLimit: LIMIT 2, PushedOffset: OFFSET 1,")
    checkAnswer(df1, Seq(Row(1, "cathy", 9000.00, 1200.0, false)))

    val df2 = spark.read
      .option("pushDownOffset", "false")
      .table("h2.test.employee")
      .where($"dept" === 1)
      .offset(1)
      .limit(1)
    checkLimitRemoved(df2)
    checkOffsetRemoved(df2, false)
    checkPushedInfo(df2,
      "[DEPT IS NOT NULL, DEPT = 1], PushedLimit: LIMIT 2, ReadSchema:")
    checkAnswer(df2, Seq(Row(1, "cathy", 9000.00, 1200.0, false)))

    val df3 = spark.read
      .option("pushDownLimit", "false")
      .table("h2.test.employee")
      .where($"dept" === 1)
      .offset(1)
      .limit(1)
    checkLimitRemoved(df3, false)
    checkOffsetRemoved(df3)
    checkPushedInfo(df3,
      "[DEPT IS NOT NULL, DEPT = 1], PushedOffset: OFFSET 1, ReadSchema:")
    checkAnswer(df3, Seq(Row(1, "cathy", 9000.00, 1200.0, false)))

    val df4 = spark.read
      .option("pushDownOffset", "false")
      .option("pushDownLimit", "false")
      .table("h2.test.employee")
      .where($"dept" === 1)
      .offset(1)
      .limit(1)
    checkLimitRemoved(df4, false)
    checkOffsetRemoved(df4, false)
    checkPushedInfo(df4,
      "[DEPT IS NOT NULL, DEPT = 1], ReadSchema:")
    checkAnswer(df4, Seq(Row(1, "cathy", 9000.00, 1200.0, false)))

    val df5 = spark.read
      .table("h2.test.employee")
      .where($"dept" === 1)
      .sort($"salary")
      .offset(1)
      .limit(1)
    checkLimitRemoved(df5)
    checkOffsetRemoved(df5)
    checkPushedInfo(df5, "PushedFilters: [DEPT IS NOT NULL, DEPT = 1], " +
      "PushedOffset: OFFSET 1, PushedTopN: ORDER BY [SALARY ASC NULLS FIRST] LIMIT 2, ReadSchema:")
    checkAnswer(df5, Seq(Row(1, "amy", 10000.00, 1000.0, true)))

    val df6 = spark.read
      .option("pushDownOffset", "false")
      .table("h2.test.employee")
      .where($"dept" === 1)
      .sort($"salary")
      .offset(1)
      .limit(1)
    checkLimitRemoved(df6)
    checkOffsetRemoved(df6, false)
    checkPushedInfo(df6, "[DEPT IS NOT NULL, DEPT = 1]," +
      " PushedTopN: ORDER BY [SALARY ASC NULLS FIRST] LIMIT 2, ReadSchema:")
    checkAnswer(df6, Seq(Row(1, "amy", 10000.00, 1000.0, true)))

    val df7 = spark.read
      .option("pushDownLimit", "false")
      .table("h2.test.employee")
      .where($"dept" === 1)
      .sort($"salary")
      .offset(1)
      .limit(1)
    checkLimitRemoved(df7, false)
    checkOffsetRemoved(df7, false)
    checkPushedInfo(df7, "PushedFilters: [DEPT IS NOT NULL, DEPT = 1], ReadSchema:")
    checkAnswer(df7, Seq(Row(1, "amy", 10000.00, 1000.0, true)))

    val df8 = spark.read
      .option("pushDownOffset", "false")
      .option("pushDownLimit", "false")
      .table("h2.test.employee")
      .where($"dept" === 1)
      .sort($"salary")
      .offset(1)
      .limit(1)
    checkLimitRemoved(df8, false)
    checkOffsetRemoved(df8, false)
    checkPushedInfo(df8, "PushedFilters: [DEPT IS NOT NULL, DEPT = 1], ReadSchema:")
    checkAnswer(df8, Seq(Row(1, "amy", 10000.00, 1000.0, true)))

    val df9 = spark.read
      .option("partitionColumn", "dept")
      .option("lowerBound", "0")
      .option("upperBound", "2")
      .option("numPartitions", "2")
      .table("h2.test.employee")
      .filter($"dept" > 1)
      .offset(1)
      .limit(1)
    checkLimitRemoved(df9, false)
    checkOffsetRemoved(df9, false)
    checkPushedInfo(df9,
      "PushedFilters: [DEPT IS NOT NULL, DEPT > 1], PushedLimit: LIMIT 2, ReadSchema:")
    checkAnswer(df9, Seq(Row(2, "david", 10000.00, 1300.0, true)))

    val df10 = sql("SELECT dept, sum(salary) FROM h2.test.employee group by dept LIMIT 1 OFFSET 1")
    checkLimitRemoved(df10, false)
    checkOffsetRemoved(df10, false)
    checkPushedInfo(df10,
      "PushedAggregates: [SUM(SALARY)], PushedFilters: [], PushedGroupByExpressions: [DEPT], ")
    checkAnswer(df10, Seq(Row(2, 22000.00)))

    val name = udf { (x: String) => x.matches("cat|dav|amy") }
    val sub = udf { (x: String) => x.substring(0, 3) }
    val df11 = spark.read
      .table("h2.test.employee")
      .select($"SALARY", $"BONUS", sub($"NAME").as("shortName"))
      .filter(name($"shortName"))
      .offset(1)
      .limit(1)
    checkLimitRemoved(df11, false)
    checkOffsetRemoved(df11, false)
    checkPushedInfo(df11, "PushedFilters: [], ")
    checkAnswer(df11, Seq(Row(9000.00, 1200.0, "cat")))
  }

  private def checkSortRemoved(df: DataFrame, removed: Boolean = true): Unit = {
    val sorts = df.queryExecution.optimizedPlan.collect {
      case s: Sort => s
    }
    if (removed) {
      assert(sorts.isEmpty)
    } else {
      assert(sorts.nonEmpty)
    }
  }

  test("simple scan with top N") {
    val df1 = spark.read
      .table("h2.test.employee")
      .sort("salary")
      .limit(1)
    checkSortRemoved(df1)
    checkLimitRemoved(df1)
    checkPushedInfo(df1,
      "PushedFilters: [], PushedTopN: ORDER BY [SALARY ASC NULLS FIRST] LIMIT 1, ")
    checkAnswer(df1, Seq(Row(1, "cathy", 9000.00, 1200.0, false)))

    val df2 = spark.read
      .option("partitionColumn", "dept")
      .option("lowerBound", "0")
      .option("upperBound", "2")
      .option("numPartitions", "1")
      .table("h2.test.employee")
      .where($"dept" === 1)
      .orderBy($"salary")
      .limit(1)
    checkSortRemoved(df2)
    checkLimitRemoved(df2)
    checkPushedInfo(df2, "PushedFilters: [DEPT IS NOT NULL, DEPT = 1], " +
      "PushedTopN: ORDER BY [SALARY ASC NULLS FIRST] LIMIT 1, ")
    checkAnswer(df2, Seq(Row(1, "cathy", 9000.00, 1200.0, false)))

    val df3 = spark.read
      .option("partitionColumn", "dept")
      .option("lowerBound", "0")
      .option("upperBound", "2")
      .option("numPartitions", "2")
      .table("h2.test.employee")
      .filter($"dept" > 1)
      .orderBy($"salary".desc)
      .limit(1)
    checkSortRemoved(df3, false)
    checkLimitRemoved(df3, false)
    checkPushedInfo(df3, "PushedFilters: [DEPT IS NOT NULL, DEPT > 1], " +
      "PushedTopN: ORDER BY [SALARY DESC NULLS LAST] LIMIT 1, ")
    checkAnswer(df3, Seq(Row(2, "alex", 12000.00, 1200.0, false)))

    val df4 =
      sql("SELECT name FROM h2.test.employee WHERE dept > 1 ORDER BY salary NULLS LAST LIMIT 1")
    checkSchemaNames(df4, Seq("NAME"))
    checkSortRemoved(df4)
    checkLimitRemoved(df4)
    checkPushedInfo(df4, "PushedFilters: [DEPT IS NOT NULL, DEPT > 1], " +
      "PushedTopN: ORDER BY [SALARY ASC NULLS LAST] LIMIT 1, ")
    checkAnswer(df4, Seq(Row("david")))

    val df5 = spark.read.table("h2.test.employee")
      .where($"dept" === 1).orderBy($"salary")
    checkSortRemoved(df5, false)
    checkPushedInfo(df5, "PushedFilters: [DEPT IS NOT NULL, DEPT = 1], ")
    checkAnswer(df5,
      Seq(Row(1, "cathy", 9000.00, 1200.0, false), Row(1, "amy", 10000.00, 1000.0, true)))

    val df6 = spark.read
      .table("h2.test.employee")
      .groupBy("DEPT").sum("SALARY")
      .orderBy("DEPT")
      .limit(1)
    checkSortRemoved(df6, false)
    checkLimitRemoved(df6, false)
    checkPushedInfo(df6, "PushedAggregates: [SUM(SALARY)]," +
      " PushedFilters: [], PushedGroupByExpressions: [DEPT], ")
    checkAnswer(df6, Seq(Row(1, 19000.00)))

    val name = udf { (x: String) => x.matches("cat|dav|amy") }
    val sub = udf { (x: String) => x.substring(0, 3) }
    val df7 = spark.read
      .table("h2.test.employee")
      .select($"SALARY", $"BONUS", sub($"NAME").as("shortName"))
      .filter(name($"shortName"))
      .sort($"SALARY".desc)
      .limit(1)
    // LIMIT is pushed down only if all the filters are pushed down
    checkSortRemoved(df7, false)
    checkLimitRemoved(df7, false)
    checkPushedInfo(df7, "PushedFilters: [], ")
    checkAnswer(df7, Seq(Row(10000.00, 1000.0, "amy")))

    val df8 = spark.read
      .table("h2.test.employee")
      .sort(sub($"NAME"))
      .limit(1)
    checkSortRemoved(df8, false)
    checkLimitRemoved(df8, false)
    checkPushedInfo(df8, "PushedFilters: [], ")
    checkAnswer(df8, Seq(Row(2, "alex", 12000.00, 1200.0, false)))

    val df9 = spark.read
      .table("h2.test.employee")
      .select($"DEPT", $"name", $"SALARY",
        when(($"SALARY" > 8000).and($"SALARY" < 10000), $"salary").otherwise(0).as("key"))
      .sort("key", "dept", "SALARY")
      .limit(3)
    checkSortRemoved(df9)
    checkLimitRemoved(df9)
    checkPushedInfo(df9, "PushedFilters: [], " +
      "PushedTopN: " +
      "ORDER BY [CASE WHEN (SALARY > 8000.00) AND (SALARY < 10000.00) THEN SALARY ELSE 0.00 END " +
      "ASC NULLS FIRST, DEPT ASC NULLS FIRST, SALARY ASC NULLS FIRST] LIMIT 3,")
    checkAnswer(df9,
      Seq(Row(1, "amy", 10000, 0), Row(2, "david", 10000, 0), Row(2, "alex", 12000, 0)))

    val df10 = spark.read
      .option("partitionColumn", "dept")
      .option("lowerBound", "0")
      .option("upperBound", "2")
      .option("numPartitions", "2")
      .table("h2.test.employee")
      .select($"DEPT", $"name", $"SALARY",
        when(($"SALARY" > 8000).and($"SALARY" < 10000), $"salary").otherwise(0).as("key"))
      .orderBy($"key", $"dept", $"SALARY")
      .limit(3)
    checkSortRemoved(df10, false)
    checkLimitRemoved(df10, false)
    checkPushedInfo(df10, "PushedFilters: [], " +
      "PushedTopN: " +
      "ORDER BY [CASE WHEN (SALARY > 8000.00) AND (SALARY < 10000.00) THEN SALARY ELSE 0.00 END " +
      "ASC NULLS FIRST, DEPT ASC NULLS FIRST, SALARY ASC NULLS FIRST] LIMIT 3,")
    checkAnswer(df10,
      Seq(Row(1, "amy", 10000, 0), Row(2, "david", 10000, 0), Row(2, "alex", 12000, 0)))
  }

  test("simple scan with top N: order by with alias") {
    val df1 = spark.read
      .table("h2.test.employee")
      .select($"NAME", $"SALARY".as("mySalary"))
      .sort("mySalary")
      .limit(1)
    checkSortRemoved(df1)
    checkPushedInfo(df1,
      "PushedFilters: [], PushedTopN: ORDER BY [SALARY ASC NULLS FIRST] LIMIT 1, ")
    checkAnswer(df1, Seq(Row("cathy", 9000.00)))

    val df2 = spark.read
      .table("h2.test.employee")
      .select($"DEPT", $"NAME", $"SALARY".as("mySalary"))
      .filter($"DEPT" > 1)
      .sort("mySalary")
      .limit(1)
    checkSortRemoved(df2)
    checkPushedInfo(df2,
      "PushedFilters: [DEPT IS NOT NULL, DEPT > 1], " +
        "PushedTopN: ORDER BY [SALARY ASC NULLS FIRST] LIMIT 1, ")
    checkAnswer(df2, Seq(Row(2, "david", 10000.00)))
  }

  test("scan with filter push-down") {
    val df = spark.table("h2.test.people").filter($"id" > 1)
    checkFiltersRemoved(df)
    checkPushedInfo(df, "PushedFilters: [ID IS NOT NULL, ID > 1], ")
    checkAnswer(df, Row("mary", 2))

    val df2 = spark.table("h2.test.employee").filter($"name".isin("amy", "cathy"))
    checkFiltersRemoved(df2)
    checkPushedInfo(df2, "PushedFilters: [NAME IN ('amy', 'cathy')]")
    checkAnswer(df2, Seq(Row(1, "amy", 10000, 1000, true), Row(1, "cathy", 9000, 1200, false)))

    val df3 = spark.table("h2.test.employee").filter($"name".startsWith("a"))
    checkFiltersRemoved(df3)
    checkPushedInfo(df3, "PushedFilters: [NAME IS NOT NULL, NAME LIKE 'a%']")
    checkAnswer(df3, Seq(Row(1, "amy", 10000, 1000, true), Row(2, "alex", 12000, 1200, false)))

    val df4 = spark.table("h2.test.employee").filter($"is_manager")
    checkFiltersRemoved(df4)
    checkPushedInfo(df4, "PushedFilters: [IS_MANAGER IS NOT NULL, IS_MANAGER = true]")
    checkAnswer(df4, Seq(Row(1, "amy", 10000, 1000, true), Row(2, "david", 10000, 1300, true),
      Row(6, "jen", 12000, 1200, true)))

    val df5 = spark.table("h2.test.employee").filter($"is_manager".and($"salary" > 10000))
    checkFiltersRemoved(df5)
    checkPushedInfo(df5, "PushedFilters: [IS_MANAGER IS NOT NULL, SALARY IS NOT NULL, " +
      "IS_MANAGER = true, SALARY > 10000.00]")
    checkAnswer(df5, Seq(Row(6, "jen", 12000, 1200, true)))

    val df6 = spark.table("h2.test.employee").filter($"is_manager".or($"salary" > 10000))
    checkFiltersRemoved(df6)
    checkPushedInfo(df6, "PushedFilters: [(IS_MANAGER = true) OR (SALARY > 10000.00)], ")
    checkAnswer(df6, Seq(Row(1, "amy", 10000, 1000, true), Row(2, "alex", 12000, 1200, false),
      Row(2, "david", 10000, 1300, true), Row(6, "jen", 12000, 1200, true)))

    val df7 = spark.table("h2.test.employee").filter(not($"is_manager") === true)
    checkFiltersRemoved(df7)
    checkPushedInfo(df7, "PushedFilters: [IS_MANAGER IS NOT NULL, NOT (IS_MANAGER = true)], ")
    checkAnswer(df7, Seq(Row(1, "cathy", 9000, 1200, false), Row(2, "alex", 12000, 1200, false)))

    val df8 = spark.table("h2.test.employee").filter($"is_manager" === true)
    checkFiltersRemoved(df8)
    checkPushedInfo(df8, "PushedFilters: [IS_MANAGER IS NOT NULL, IS_MANAGER = true], ")
    checkAnswer(df8, Seq(Row(1, "amy", 10000, 1000, true),
      Row(2, "david", 10000, 1300, true), Row(6, "jen", 12000, 1200, true)))

    val df9 = spark.table("h2.test.employee")
      .filter(when($"dept" > 1, true).when($"is_manager", false).otherwise($"dept" > 3))
    checkFiltersRemoved(df9)
    checkPushedInfo(df9, "PushedFilters: [CASE WHEN DEPT > 1 THEN TRUE " +
      "WHEN IS_MANAGER = true THEN FALSE ELSE DEPT > 3 END], ")
    checkAnswer(df9, Seq(Row(2, "alex", 12000, 1200, false),
      Row(2, "david", 10000, 1300, true), Row(6, "jen", 12000, 1200, true)))

    val df10 = spark.table("h2.test.people")
      .select($"NAME".as("myName"), $"ID".as("myID"))
      .filter($"myID" > 1)
    checkFiltersRemoved(df10)
    checkPushedInfo(df10, "PushedFilters: [ID IS NOT NULL, ID > 1], ")
    checkAnswer(df10, Row("mary", 2))

    val df11 = sql(
      """
        |SELECT * FROM h2.test.employee
        |WHERE GREATEST(bonus, 1100) > 1200 AND RAND(1) < bonus
        |""".stripMargin)
    checkFiltersRemoved(df11)
    checkPushedInfo(df11, "PushedFilters: " +
      "[BONUS IS NOT NULL, (GREATEST(BONUS, 1100.0)) > 1200.0, RAND(1) < BONUS]")
    checkAnswer(df11, Row(2, "david", 10000, 1300, true))

    val df12 = sql(
      """
        |SELECT * FROM h2.test.employee
        |WHERE IF(SALARY > 10000, SALARY, LEAST(SALARY, 1000)) > 1200
        |""".stripMargin)
    checkFiltersRemoved(df12)
    checkPushedInfo(df12, "PushedFilters: " +
      "[(CASE WHEN SALARY > 10000.00 THEN SALARY ELSE LEAST(SALARY, 1000.00) END) > 1200.00]")
    checkAnswer(df12, Seq(Row(2, "alex", 12000, 1200, false), Row(6, "jen", 12000, 1200, true)))

    val df13 = spark.table("h2.test.employee")
      .filter(logarithm($"bonus") > 7)
      .filter(exp($"bonus") > 0)
      .filter(pow($"bonus", 2) === 1440000)
      .filter(sqrt($"bonus") > 34)
      .filter(floor($"bonus") === 1200)
      .filter(ceil($"bonus") === 1200)
    checkFiltersRemoved(df13)
    checkPushedInfo(df13, "PushedFilters: " +
      "[BONUS IS NOT NULL, LN(BONUS) > 7.0, EXP(BONUS) > 0.0, (POWER(BONUS, 2.0)) = 1440000.0, " +
      "SQRT(BONUS) > 34.0, FLOOR(BONUS) = 1200, CEIL(BONUS) = 1200],")
    checkAnswer(df13, Seq(Row(1, "cathy", 9000, 1200, false),
      Row(2, "alex", 12000, 1200, false), Row(6, "jen", 12000, 1200, true)))

    // H2 does not support width_bucket
    val df14 = sql(
      """
        |SELECT * FROM h2.test.employee
        |WHERE width_bucket(bonus, 1, 6, 3) > 4
        |""".stripMargin)
    checkFiltersRemoved(df14, false)
    checkPushedInfo(df14, "PushedFilters: [BONUS IS NOT NULL]")
    checkAnswer(df14, Seq.empty[Row])

    val df15 = spark.table("h2.test.employee")
      .filter(logarithm(2, $"bonus") > 10)
      .filter(log10($"bonus") > 3)
      .filter(round($"bonus") === 1200)
      .filter(degrees($"bonus") > 68754)
      .filter(radians($"bonus") > 20)
      .filter(signum($"bonus") === 1)
    checkFiltersRemoved(df15)
    checkPushedInfo(df15, "PushedFilters: " +
      "[BONUS IS NOT NULL, (LOG(2.0, BONUS)) > 10.0, LOG10(BONUS) > 3.0, " +
      "(ROUND(BONUS, 0)) = 1200.0, DEGREES(BONUS) > 68754.0, RADIANS(BONUS) > 20.0, " +
      "SIGN(BONUS) = 1.0],")
    checkAnswer(df15, Seq(Row(1, "cathy", 9000, 1200, false),
      Row(2, "alex", 12000, 1200, false), Row(6, "jen", 12000, 1200, true)))

    val df16 = spark.table("h2.test.employee")
      .filter(sin($"bonus") < -0.08)
      .filter(sinh($"bonus") > 200)
      .filter(cos($"bonus") > 0.9)
      .filter(cosh($"bonus") > 200)
      .filter(tan($"bonus") < -0.08)
      .filter(tanh($"bonus") === 1)
      .filter(cot($"bonus") < -11)
      .filter(asin($"bonus") > 0.1)
      .filter(acos($"bonus") > 1.4)
      .filter(atan($"bonus") > 1.4)
      .filter(atan2($"bonus", $"bonus") > 0.7)
    checkFiltersRemoved(df16)
    checkPushedInfo(df16, "PushedFilters: [" +
      "BONUS IS NOT NULL, SIN(BONUS) < -0.08, SINH(BONUS) > 200.0, COS(BONUS) > 0.9, " +
      "COSH(BONUS) > 200.0, TAN(BONUS) < -0.08, TANH(BONUS) = 1.0, COT(BONUS) < -11.0, " +
      "ASIN(BONUS) > 0.1, ACOS(BONUS) > 1.4, ATAN(BONUS) > 1.4, (ATAN2(BONUS, BONUS)) > 0.7],")
    checkAnswer(df16, Seq(Row(1, "cathy", 9000, 1200, false),
      Row(2, "alex", 12000, 1200, false), Row(6, "jen", 12000, 1200, true)))

    // H2 does not support log2, asinh, acosh, atanh, cbrt
    val df17 = sql(
      """
        |SELECT * FROM h2.test.employee
        |WHERE log2(dept) > 2.5
        |AND asinh(bonus / salary) > 0.09
        |AND acosh(dept) > 2.4
        |AND atanh(bonus / salary) > 0.1
        |AND cbrt(dept) > 1.8
        |""".stripMargin)
    checkFiltersRemoved(df17, false)
    checkPushedInfo(df17,
      "PushedFilters: [DEPT IS NOT NULL, BONUS IS NOT NULL, SALARY IS NOT NULL]")
    checkAnswer(df17, Seq(Row(6, "jen", 12000, 1200, true)))
  }

  test("scan with filter push-down with ansi mode") {
    Seq(false, true).foreach { ansiMode =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiMode.toString) {
        val df = spark.table("h2.test.people").filter($"id" + 1 > 1)
        checkFiltersRemoved(df, ansiMode)
        val expectedPlanFragment = if (ansiMode) {
          "PushedFilters: [ID IS NOT NULL, (ID + 1) > 1]"
        } else {
          "PushedFilters: [ID IS NOT NULL]"
        }
        checkPushedInfo(df, expectedPlanFragment)
        checkAnswer(df, Seq(Row("fred", 1), Row("mary", 2)))

        val df2 = spark.table("h2.test.people").filter($"id" + Int.MaxValue > 1)
        checkFiltersRemoved(df2, ansiMode)
        val expectedPlanFragment2 = if (ansiMode) {
          "PushedFilters: [ID IS NOT NULL, (ID + 2147483647) > 1], "
        } else {
          "PushedFilters: [ID IS NOT NULL], "
        }
        checkPushedInfo(df2, expectedPlanFragment2)
        if (ansiMode) {
          val e = intercept[SparkException] {
            checkAnswer(df2, Seq.empty)
          }
          assert(e.getMessage.contains(
            "org.h2.jdbc.JdbcSQLDataException: Numeric value out of range: \"2147483648\""))
        } else {
          checkAnswer(df2, Seq.empty)
        }

        val df3 = sql(
          """
            |SELECT * FROM h2.test.employee
            |WHERE (CASE WHEN SALARY > 10000 THEN BONUS ELSE BONUS + 200 END) > 1200
            |""".stripMargin)

        checkFiltersRemoved(df3, ansiMode)
        val expectedPlanFragment3 = if (ansiMode) {
          "PushedFilters: [(CASE WHEN SALARY > 10000.00 THEN BONUS" +
            " ELSE BONUS + 200.0 END) > 1200.0]"
        } else {
          "PushedFilters: []"
        }
        checkPushedInfo(df3, expectedPlanFragment3)
        checkAnswer(df3,
          Seq(Row(1, "cathy", 9000, 1200, false), Row(2, "david", 10000, 1300, true)))

        val df4 = spark.table("h2.test.employee")
          .filter(($"salary" > 1000d).and($"salary" < 12000d))
        checkFiltersRemoved(df4, ansiMode)
        val expectedPlanFragment4 = if (ansiMode) {
          "PushedFilters: [SALARY IS NOT NULL, " +
            "CAST(SALARY AS double) > 1000.0, CAST(SALARY AS double) < 12000.0], "
        } else {
          "PushedFilters: [SALARY IS NOT NULL], "
        }
        checkPushedInfo(df4, expectedPlanFragment4)
        checkAnswer(df4, Seq(Row(1, "amy", 10000, 1000, true),
          Row(1, "cathy", 9000, 1200, false), Row(2, "david", 10000, 1300, true)))

        val df5 = spark.table("h2.test.employee")
          .filter(abs($"dept" - 3) > 1)
          .filter(coalesce($"salary", $"bonus") > 2000)
        checkFiltersRemoved(df5, ansiMode)
        val expectedPlanFragment5 = if (ansiMode) {
          "PushedFilters: [DEPT IS NOT NULL, ABS(DEPT - 3) > 1, " +
            "(COALESCE(CAST(SALARY AS double), BONUS)) > 2000.0]"
        } else {
          "PushedFilters: [DEPT IS NOT NULL]"
        }
        checkPushedInfo(df5, expectedPlanFragment5)
        checkAnswer(df5, Seq(Row(1, "amy", 10000, 1000, true),
          Row(1, "cathy", 9000, 1200, false), Row(6, "jen", 12000, 1200, true)))

        val df6 = sql(
          """
            |SELECT * FROM h2.test.employee
            |WHERE cast(bonus as string) like '%30%'
            |AND cast(dept as byte) > 1
            |AND cast(dept as short) > 1
            |AND cast(bonus as decimal(20, 2)) > 1200""".stripMargin)
        checkFiltersRemoved(df6, ansiMode)
        val expectedPlanFragment6 = if (ansiMode) {
          "PushedFilters: [BONUS IS NOT NULL, DEPT IS NOT NULL, " +
            "CAST(BONUS AS string) LIKE '%30%', CAST(DEPT AS byte) > 1, " +
            "CAST(DEPT AS short) > 1, CAST(BONUS AS decimal(20,2)) > 1200.00]"
        } else {
          "PushedFilters: [BONUS IS NOT NULL, DEPT IS NOT NULL],"
        }
        checkPushedInfo(df6, expectedPlanFragment6)
        checkAnswer(df6, Seq(Row(2, "david", 10000, 1300, true)))
      }
    }
  }

  test("scan with filter push-down with date time functions") {
    val df1 = sql("SELECT name FROM h2.test.datetime WHERE " +
      "dayofyear(date1) > 100 AND dayofmonth(date1) > 10 ")
    checkFiltersRemoved(df1)
    val expectedPlanFragment1 =
      "PushedFilters: [DATE1 IS NOT NULL, EXTRACT(DAY_OF_YEAR FROM DATE1) > 100, " +
        "EXTRACT(DAY FROM DATE1) > 10]"
    checkPushedInfo(df1, expectedPlanFragment1)
    checkAnswer(df1, Seq(Row("amy"), Row("alex")))

    val df2 = sql("SELECT name FROM h2.test.datetime WHERE " +
      "year(date1) = 2022 AND quarter(date1) = 2")
    checkFiltersRemoved(df2)
    val expectedPlanFragment2 =
      "[DATE1 IS NOT NULL, EXTRACT(YEAR FROM DATE1) = 2022, " +
        "EXTRACT(QUARTER FROM DATE1) = 2]"
    checkPushedInfo(df2, expectedPlanFragment2)
    checkAnswer(df2, Seq(Row("amy"), Row("alex")))

    val df3 = sql("SELECT name FROM h2.test.datetime WHERE " +
      "second(time1) = 0 AND month(date1) = 5")
    checkFiltersRemoved(df3)
    val expectedPlanFragment3 =
      "PushedFilters: [TIME1 IS NOT NULL, DATE1 IS NOT NULL, " +
        "EXTRACT(SECOND FROM TIME1) = 0, EXTRACT(MONTH FROM DATE1) = 5]"
    checkPushedInfo(df3, expectedPlanFragment3)
    checkAnswer(df3, Seq(Row("amy"), Row("alex")))

    val df4 = sql("SELECT name FROM h2.test.datetime WHERE " +
      "hour(time1) = 0 AND minute(time1) = 0")
    checkFiltersRemoved(df4)
    val expectedPlanFragment4 =
      "PushedFilters: [TIME1 IS NOT NULL, EXTRACT(HOUR FROM TIME1) = 0, " +
        "EXTRACT(MINUTE FROM TIME1) = 0]"
    checkPushedInfo(df4, expectedPlanFragment4)
    checkAnswer(df4, Seq(Row("amy"), Row("alex")))

    val df5 = sql("SELECT name FROM h2.test.datetime WHERE " +
      "extract(WEEk from date1) > 10 AND extract(YEAROFWEEK from date1) = 2022")
    checkFiltersRemoved(df5)
    val expectedPlanFragment5 =
      "PushedFilters: [DATE1 IS NOT NULL, EXTRACT(WEEK FROM DATE1) > 10, " +
        "EXTRACT(YEAR_OF_WEEK FROM DATE1) = 2022]"
    checkPushedInfo(df5, expectedPlanFragment5)
    checkAnswer(df5, Seq(Row("alex"), Row("amy")))

    // H2 does not support
    val df6 = sql("SELECT name FROM h2.test.datetime WHERE " +
      "trunc(date1, 'week') = date'2022-05-16' AND date_add(date1, 1) = date'2022-05-20' " +
      "AND datediff(date1, '2022-05-10') > 0")
    checkFiltersRemoved(df6, false)
    val expectedPlanFragment6 =
      "PushedFilters: [DATE1 IS NOT NULL]"
    checkPushedInfo(df6, expectedPlanFragment6)
    checkAnswer(df6, Seq(Row("amy")))

    val df7 = sql("SELECT name FROM h2.test.datetime WHERE " +
      "weekday(date1) = 2")
    checkFiltersRemoved(df7)
    val expectedPlanFragment7 =
      "PushedFilters: [DATE1 IS NOT NULL, (EXTRACT(DAY_OF_WEEK FROM DATE1) - 1) = 2]"
    checkPushedInfo(df7, expectedPlanFragment7)
    checkAnswer(df7, Seq(Row("alex")))

    val df8 = sql("SELECT name FROM h2.test.datetime WHERE " +
      "dayofweek(date1) = 4")
    checkFiltersRemoved(df8)
    val expectedPlanFragment8 =
      "PushedFilters: [DATE1 IS NOT NULL, ((EXTRACT(DAY_OF_WEEK FROM DATE1) % 7) + 1) = 4]"
    checkPushedInfo(df8, expectedPlanFragment8)
    checkAnswer(df8, Seq(Row("alex")))
  }

  test("scan with filter push-down with UDF") {
    JdbcDialects.unregisterDialect(H2Dialect)
    try {
      JdbcDialects.registerDialect(testH2Dialect)
      val df1 = sql("SELECT * FROM h2.test.people where h2.my_strlen(name) > 2")
      checkFiltersRemoved(df1)
      checkPushedInfo(df1, "PushedFilters: [CHAR_LENGTH(NAME) > 2],")
      checkAnswer(df1, Seq(Row("fred", 1), Row("mary", 2)))

      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        val df2 = sql(
          """
            |SELECT *
            |FROM h2.test.people
            |WHERE h2.my_strlen(CASE WHEN NAME = 'fred' THEN NAME ELSE "abc" END) > 2
          """.stripMargin)
        checkFiltersRemoved(df2)
        checkPushedInfo(df2,
          "PushedFilters: [CHAR_LENGTH(CASE WHEN NAME = 'fred' THEN NAME ELSE 'abc' END) > 2],")
        checkAnswer(df2, Seq(Row("fred", 1), Row("mary", 2)))
      }
    } finally {
      JdbcDialects.unregisterDialect(testH2Dialect)
      JdbcDialects.registerDialect(H2Dialect)
    }
  }

  test("scan with column pruning") {
    val df = spark.table("h2.test.people").select("id")
    checkSchemaNames(df, Seq("ID"))
    checkAnswer(df, Seq(Row(1), Row(2)))
  }

  test("scan with filter push-down and column pruning") {
    val df = spark.table("h2.test.people").filter($"id" > 1).select("name")
    checkFiltersRemoved(df)
    checkSchemaNames(df, Seq("NAME"))
    checkAnswer(df, Row("mary"))
  }

  test("read/write with partition info") {
    withTable("h2.test.abc") {
      sql("CREATE TABLE h2.test.abc AS SELECT * FROM h2.test.people")
      val df1 = Seq(("evan", 3), ("cathy", 4), ("alex", 5)).toDF("NAME", "ID")
      val e = intercept[IllegalArgumentException] {
        df1.write
          .option("partitionColumn", "id")
          .option("lowerBound", "0")
          .option("upperBound", "3")
          .option("numPartitions", "0")
          .insertInto("h2.test.abc")
      }.getMessage
      assert(e.contains("Invalid value `0` for parameter `numPartitions` in table writing " +
        "via JDBC. The minimum value is 1."))

      df1.write
        .option("partitionColumn", "id")
        .option("lowerBound", "0")
        .option("upperBound", "3")
        .option("numPartitions", "3")
        .insertInto("h2.test.abc")

      val df2 = spark.read
        .option("partitionColumn", "id")
        .option("lowerBound", "0")
        .option("upperBound", "3")
        .option("numPartitions", "2")
        .table("h2.test.abc")

      assert(df2.rdd.getNumPartitions === 2)
      assert(df2.count() === 5)
    }
  }

  test("show tables") {
    checkAnswer(sql("SHOW TABLES IN h2.test"),
      Seq(Row("test", "people", false), Row("test", "empty_table", false),
        Row("test", "employee", false), Row("test", "item", false), Row("test", "dept", false),
        Row("test", "person", false), Row("test", "view1", false), Row("test", "view2", false),
        Row("test", "datetime", false)))
  }

  test("SQL API: create table as select") {
    withTable("h2.test.abc") {
      sql("CREATE TABLE h2.test.abc AS SELECT * FROM h2.test.people")
      checkAnswer(sql("SELECT name, id FROM h2.test.abc"), Seq(Row("fred", 1), Row("mary", 2)))
    }
  }

  test("DataFrameWriterV2: create table as select") {
    withTable("h2.test.abc") {
      spark.table("h2.test.people").writeTo("h2.test.abc").create()
      checkAnswer(sql("SELECT name, id FROM h2.test.abc"), Seq(Row("fred", 1), Row("mary", 2)))
    }
  }

  test("SQL API: replace table as select") {
    withTable("h2.test.abc") {
      intercept[CannotReplaceMissingTableException] {
        sql("REPLACE TABLE h2.test.abc AS SELECT 1 as col")
      }
      sql("CREATE OR REPLACE TABLE h2.test.abc AS SELECT 1 as col")
      checkAnswer(sql("SELECT col FROM h2.test.abc"), Row(1))
      sql("REPLACE TABLE h2.test.abc AS SELECT * FROM h2.test.people")
      checkAnswer(sql("SELECT name, id FROM h2.test.abc"), Seq(Row("fred", 1), Row("mary", 2)))
    }
  }

  test("DataFrameWriterV2: replace table as select") {
    withTable("h2.test.abc") {
      intercept[CannotReplaceMissingTableException] {
        sql("SELECT 1 AS col").writeTo("h2.test.abc").replace()
      }
      sql("SELECT 1 AS col").writeTo("h2.test.abc").createOrReplace()
      checkAnswer(sql("SELECT col FROM h2.test.abc"), Row(1))
      spark.table("h2.test.people").writeTo("h2.test.abc").replace()
      checkAnswer(sql("SELECT name, id FROM h2.test.abc"), Seq(Row("fred", 1), Row("mary", 2)))
    }
  }

  test("SQL API: insert and overwrite") {
    withTable("h2.test.abc") {
      sql("CREATE TABLE h2.test.abc AS SELECT * FROM h2.test.people")

      sql("INSERT INTO h2.test.abc SELECT 'lucy', 3")
      checkAnswer(
        sql("SELECT name, id FROM h2.test.abc"),
        Seq(Row("fred", 1), Row("mary", 2), Row("lucy", 3)))

      sql("INSERT OVERWRITE h2.test.abc SELECT 'bob', 4")
      checkAnswer(sql("SELECT name, id FROM h2.test.abc"), Row("bob", 4))
    }
  }

  test("DataFrameWriterV2: insert and overwrite") {
    withTable("h2.test.abc") {
      sql("CREATE TABLE h2.test.abc AS SELECT * FROM h2.test.people")

      // `DataFrameWriterV2` is by-name.
      sql("SELECT 3 AS ID, 'lucy' AS NAME").writeTo("h2.test.abc").append()
      checkAnswer(
        sql("SELECT name, id FROM h2.test.abc"),
        Seq(Row("fred", 1), Row("mary", 2), Row("lucy", 3)))

      sql("SELECT 'bob' AS NAME, 4 AS ID").writeTo("h2.test.abc").overwrite(lit(true))
      checkAnswer(sql("SELECT name, id FROM h2.test.abc"), Row("bob", 4))
    }
  }

  private def checkAggregateRemoved(df: DataFrame, removed: Boolean = true): Unit = {
    val aggregates = df.queryExecution.optimizedPlan.collect {
      case agg: Aggregate => agg
    }
    if (removed) {
      assert(aggregates.isEmpty)
    } else {
      assert(aggregates.nonEmpty)
    }
  }

  test("scan with filter push-down with string functions") {
    val df1 = sql("SELECT * FROM h2.test.employee WHERE " +
      "substr(name, 2, 1) = 'e'" +
      " AND upper(name) = 'JEN' AND lower(name) = 'jen' ")
    checkFiltersRemoved(df1)
    val expectedPlanFragment1 =
      "PushedFilters: [NAME IS NOT NULL, (SUBSTRING(NAME, 2, 1)) = 'e', " +
      "UPPER(NAME) = 'JEN', LOWER(NAME) = 'jen']"
    checkPushedInfo(df1, expectedPlanFragment1)
    checkAnswer(df1, Seq(Row(6, "jen", 12000, 1200, true)))

    val df2 = sql("SELECT * FROM h2.test.employee WHERE " +
      "trim(name) = 'jen' AND trim('j', name) = 'en'" +
      "AND translate(name, 'e', 1) = 'j1n'")
    checkFiltersRemoved(df2)
    val expectedPlanFragment2 =
      "PushedFilters: [NAME IS NOT NULL, TRIM(BOTH FROM NAME) = 'jen', " +
      "(TRIM(BOTH 'j' FROM NAME)) = 'en', (TRANSLATE(NAME, 'e', '1')) = 'j1n']"
    checkPushedInfo(df2, expectedPlanFragment2)
    checkAnswer(df2, Seq(Row(6, "jen", 12000, 1200, true)))

    val df3 = sql("SELECT * FROM h2.test.employee WHERE " +
      "ltrim(name) = 'jen' AND ltrim('j', name) = 'en'")
    checkFiltersRemoved(df3)
    val expectedPlanFragment3 =
      "PushedFilters: [TRIM(LEADING FROM NAME) = 'jen', " +
      "(TRIM(LEADING 'j' FROM NAME)) = 'en']"
    checkPushedInfo(df3, expectedPlanFragment3)
    checkAnswer(df3, Seq(Row(6, "jen", 12000, 1200, true)))

    val df4 = sql("SELECT * FROM h2.test.employee WHERE " +
      "rtrim(name) = 'jen' AND rtrim('n', name) = 'je'")
    checkFiltersRemoved(df4)
    val expectedPlanFragment4 =
      "PushedFilters: [TRIM(TRAILING FROM NAME) = 'jen', " +
      "(TRIM(TRAILING 'n' FROM NAME)) = 'je']"
    checkPushedInfo(df4, expectedPlanFragment4)
    checkAnswer(df4, Seq(Row(6, "jen", 12000, 1200, true)))

    // H2 does not support OVERLAY
    val df5 = sql("SELECT * FROM h2.test.employee WHERE OVERLAY(NAME, '1', 2, 1) = 'j1n'")
    checkFiltersRemoved(df5, false)
    val expectedPlanFragment5 =
      "PushedFilters: [NAME IS NOT NULL]"
    checkPushedInfo(df5, expectedPlanFragment5)
    checkAnswer(df5, Seq(Row(6, "jen", 12000, 1200, true)))
  }

  test("scan with aggregate push-down: MAX AVG with filter and group by") {
    val df = sql("SELECT MAX(SaLaRY), AVG(BONUS) FROM h2.test.employee WHERE dept > 0" +
      " GROUP BY DePt")
    checkFiltersRemoved(df)
    checkAggregateRemoved(df)
    checkPushedInfo(df, "PushedAggregates: [MAX(SALARY), AVG(BONUS)], " +
      "PushedFilters: [DEPT IS NOT NULL, DEPT > 0], " +
      "PushedGroupByExpressions: [DEPT], ")
    checkAnswer(df, Seq(Row(10000, 1100.0), Row(12000, 1250.0), Row(12000, 1200.0)))
  }

  private def checkFiltersRemoved(df: DataFrame, removed: Boolean = true): Unit = {
    val filters = df.queryExecution.optimizedPlan.collect {
      case f: Filter => f
    }
    if (removed) {
      assert(filters.isEmpty)
    } else {
      assert(filters.nonEmpty)
    }
  }

  test("scan with aggregate push-down: MAX AVG with filter without group by") {
    val df = sql("SELECT MAX(ID), AVG(ID) FROM h2.test.people WHERE id > 0")
    checkFiltersRemoved(df)
    checkAggregateRemoved(df)
    checkPushedInfo(df, "PushedAggregates: [MAX(ID), AVG(ID)], " +
      "PushedFilters: [ID IS NOT NULL, ID > 0], " +
      "PushedGroupByExpressions: [], ")
    checkAnswer(df, Seq(Row(2, 1.5)))
  }

  test("partitioned scan with aggregate push-down: complete push-down only") {
    withTempView("v") {
      spark.read
        .option("partitionColumn", "dept")
        .option("lowerBound", "0")
        .option("upperBound", "2")
        .option("numPartitions", "2")
        .table("h2.test.employee")
        .createTempView("v")
      val df = sql("select AVG(SALARY) FROM v GROUP BY name")
      // Partitioned JDBC Scan doesn't support complete aggregate push-down, and AVG requires
      // complete push-down so aggregate is not pushed at the end.
      checkAggregateRemoved(df, removed = false)
      checkAnswer(df, Seq(Row(9000.0), Row(10000.0), Row(10000.0), Row(12000.0), Row(12000.0)))
    }
  }

  test("scan with aggregate push-down: aggregate + number") {
    val df = sql("SELECT MAX(SALARY) + 1 FROM h2.test.employee")
    checkAggregateRemoved(df)
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [MAX(SALARY)]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
    checkPushedInfo(df, "PushedAggregates: [MAX(SALARY)]")
    checkAnswer(df, Seq(Row(12001)))
  }

  test("scan with aggregate push-down: COUNT(*)") {
    val df = sql("SELECT COUNT(*) FROM h2.test.employee")
    checkAggregateRemoved(df)
    checkPushedInfo(df, "PushedAggregates: [COUNT(*)]")
    checkAnswer(df, Seq(Row(5)))
  }

  test("scan with aggregate push-down: GROUP BY without aggregate functions") {
    val df = sql("SELECT name FROM h2.test.employee GROUP BY name")
    checkAggregateRemoved(df)
    checkPushedInfo(df,
      "PushedAggregates: [], PushedFilters: [], PushedGroupByExpressions: [NAME],")
    checkAnswer(df, Seq(Row("alex"), Row("amy"), Row("cathy"), Row("david"), Row("jen")))

    val df2 = spark.read
      .option("partitionColumn", "dept")
      .option("lowerBound", "0")
      .option("upperBound", "2")
      .option("numPartitions", "2")
      .table("h2.test.employee")
      .groupBy($"name")
      .agg(Map.empty[String, String])
    checkAggregateRemoved(df2, false)
    checkPushedInfo(df2,
      "PushedAggregates: [], PushedFilters: [], PushedGroupByExpressions: [NAME],")
    checkAnswer(df2, Seq(Row("alex"), Row("amy"), Row("cathy"), Row("david"), Row("jen")))

    val df3 = sql("SELECT CASE WHEN SALARY > 8000 AND SALARY < 10000 THEN SALARY ELSE 0 END as" +
      " key FROM h2.test.employee GROUP BY key")
    checkAggregateRemoved(df3)
    checkPushedInfo(df3,
      """
        |PushedAggregates: [],
        |PushedFilters: [],
        |PushedGroupByExpressions:
        |[CASE WHEN (SALARY > 8000.00) AND (SALARY < 10000.00) THEN SALARY ELSE 0.00 END],
        |""".stripMargin.replaceAll("\n", " "))
    checkAnswer(df3, Seq(Row(0), Row(9000)))

    val df4 = spark.read
      .option("partitionColumn", "dept")
      .option("lowerBound", "0")
      .option("upperBound", "2")
      .option("numPartitions", "2")
      .table("h2.test.employee")
      .groupBy(when(($"SALARY" > 8000).and($"SALARY" < 10000), $"SALARY").otherwise(0).as("key"))
      .agg(Map.empty[String, String])
    checkAggregateRemoved(df4, false)
    checkPushedInfo(df4,
      """
        |PushedAggregates: [],
        |PushedFilters: [],
        |PushedGroupByExpressions:
        |[CASE WHEN (SALARY > 8000.00) AND (SALARY < 10000.00) THEN SALARY ELSE 0.00 END],
        |""".stripMargin.replaceAll("\n", " "))
    checkAnswer(df4, Seq(Row(0), Row(9000)))
  }

  test("scan with aggregate push-down: COUNT(col)") {
    val df = sql("SELECT COUNT(DEPT) FROM h2.test.employee")
    checkAggregateRemoved(df)
    checkPushedInfo(df, "PushedAggregates: [COUNT(DEPT)]")
    checkAnswer(df, Seq(Row(5)))
  }

  test("scan with aggregate push-down: COUNT(DISTINCT col)") {
    val df = sql("SELECT COUNT(DISTINCT DEPT) FROM h2.test.employee")
    checkAggregateRemoved(df)
    checkPushedInfo(df, "PushedAggregates: [COUNT(DISTINCT DEPT)]")
    checkAnswer(df, Seq(Row(3)))
  }

  test("scan with aggregate push-down: cannot partial push down COUNT(DISTINCT col)") {
    val df = spark.read
      .option("partitionColumn", "dept")
      .option("lowerBound", "0")
      .option("upperBound", "2")
      .option("numPartitions", "2")
      .table("h2.test.employee")
      .agg(count_distinct($"DEPT"))
    checkAggregateRemoved(df, false)
    checkAnswer(df, Seq(Row(3)))
  }

  test("scan with aggregate push-down: SUM without filer and group by") {
    val df = sql("SELECT SUM(SALARY) FROM h2.test.employee")
    checkAggregateRemoved(df)
    checkPushedInfo(df, "PushedAggregates: [SUM(SALARY)]")
    checkAnswer(df, Seq(Row(53000)))
  }

  test("scan with aggregate push-down: DISTINCT SUM without filer and group by") {
    val df = sql("SELECT SUM(DISTINCT SALARY) FROM h2.test.employee")
    checkAggregateRemoved(df)
    checkPushedInfo(df, "PushedAggregates: [SUM(DISTINCT SALARY)]")
    checkAnswer(df, Seq(Row(31000)))
  }

  test("scan with aggregate push-down: SUM with group by") {
    val df1 = sql("SELECT SUM(SALARY) FROM h2.test.employee GROUP BY DEPT")
    checkAggregateRemoved(df1)
    checkPushedInfo(df1, "PushedAggregates: [SUM(SALARY)], " +
      "PushedFilters: [], PushedGroupByExpressions: [DEPT], ")
    checkAnswer(df1, Seq(Row(19000), Row(22000), Row(12000)))

    val df2 = sql(
      """
        |SELECT CASE WHEN SALARY > 8000 AND SALARY < 10000 THEN SALARY ELSE 0 END as key,
        |  SUM(SALARY) FROM h2.test.employee GROUP BY key""".stripMargin)
    checkAggregateRemoved(df2)
    checkPushedInfo(df2,
      """
        |PushedAggregates: [SUM(SALARY)],
        |PushedFilters: [],
        |PushedGroupByExpressions:
        |[CASE WHEN (SALARY > 8000.00) AND (SALARY < 10000.00) THEN SALARY ELSE 0.00 END],
        |""".stripMargin.replaceAll("\n", " "))
    checkAnswer(df2, Seq(Row(0, 44000), Row(9000, 9000)))

    val df3 = spark.read
      .option("partitionColumn", "dept")
      .option("lowerBound", "0")
      .option("upperBound", "2")
      .option("numPartitions", "2")
      .table("h2.test.employee")
      .groupBy(when(($"SALARY" > 8000).and($"SALARY" < 10000), $"SALARY").otherwise(0).as("key"))
      .agg(sum($"SALARY"))
    checkAggregateRemoved(df3, false)
    checkPushedInfo(df3,
      """
        |PushedAggregates: [SUM(SALARY)],
        |PushedFilters: [],
        |PushedGroupByExpressions:
        |[CASE WHEN (SALARY > 8000.00) AND (SALARY < 10000.00) THEN SALARY ELSE 0.00 END],
        |""".stripMargin.replaceAll("\n", " "))
    checkAnswer(df3, Seq(Row(0, 44000), Row(9000, 9000)))

    val df4 = sql(
      """
        |SELECT DEPT, CASE WHEN SALARY > 8000 AND SALARY < 10000 THEN SALARY ELSE 0 END as key,
        |  SUM(SALARY) FROM h2.test.employee GROUP BY DEPT, key""".stripMargin)
    checkAggregateRemoved(df4)
    checkPushedInfo(df4,
      """
        |PushedAggregates: [SUM(SALARY)],
        |PushedFilters: [],
        |PushedGroupByExpressions:
        |[DEPT, CASE WHEN (SALARY > 8000.00) AND (SALARY < 10000.00) THEN SALARY ELSE 0.00 END],
        |""".stripMargin.replaceAll("\n", " "))
    checkAnswer(df4, Seq(Row(1, 0, 10000), Row(1, 9000, 9000), Row(2, 0, 22000), Row(6, 0, 12000)))

    val df5 = spark.read
      .option("partitionColumn", "dept")
      .option("lowerBound", "0")
      .option("upperBound", "2")
      .option("numPartitions", "2")
      .table("h2.test.employee")
      .groupBy($"DEPT",
        when(($"SALARY" > 8000).and($"SALARY" < 10000), $"SALARY").otherwise(0)
          .as("key"))
      .agg(sum($"SALARY"))
    checkAggregateRemoved(df5, false)
    checkPushedInfo(df5,
      """
        |PushedAggregates: [SUM(SALARY)],
        |PushedFilters: [],
        |PushedGroupByExpressions:
        |[DEPT, CASE WHEN (SALARY > 8000.00) AND (SALARY < 10000.00) THEN SALARY ELSE 0.00 END],
        |""".stripMargin.replaceAll("\n", " "))
    checkAnswer(df5, Seq(Row(1, 0, 10000), Row(1, 9000, 9000), Row(2, 0, 22000), Row(6, 0, 12000)))

    val df6 = sql(
      """
        |SELECT CASE WHEN SALARY > 8000 AND is_manager <> false THEN SALARY ELSE 0 END as key,
        |  SUM(SALARY) FROM h2.test.employee GROUP BY key""".stripMargin)
    checkAggregateRemoved(df6)
    checkPushedInfo(df6,
      """
        |PushedAggregates: [SUM(SALARY)],
        |PushedFilters: [],
        |PushedGroupByExpressions:
        |[CASE WHEN (SALARY > 8000.00) AND (IS_MANAGER = true) THEN SALARY ELSE 0.00 END],
        |""".stripMargin.replaceAll("\n", " "))
    checkAnswer(df6, Seq(Row(0, 21000), Row(10000, 20000), Row(12000, 12000)))

    val df7 = sql(
      """
        |SELECT CASE WHEN SALARY > 8000 OR is_manager <> false THEN SALARY ELSE 0 END as key,
        |  SUM(SALARY) FROM h2.test.employee GROUP BY key""".stripMargin)
    checkAggregateRemoved(df7)
    checkPushedInfo(df7,
      """
        |PushedAggregates: [SUM(SALARY)],
        |PushedFilters: [],
        |PushedGroupByExpressions:
        |[CASE WHEN (SALARY > 8000.00) OR (IS_MANAGER = true) THEN SALARY ELSE 0.00 END],
        |""".stripMargin.replaceAll("\n", " "))
    checkAnswer(df7, Seq(Row(10000, 20000), Row(12000, 24000), Row(9000, 9000)))

    val df8 = sql(
      """
        |SELECT CASE WHEN NOT(is_manager <> false) THEN SALARY ELSE 0 END as key,
        |  SUM(SALARY) FROM h2.test.employee GROUP BY key""".stripMargin)
    checkAggregateRemoved(df8)
    checkPushedInfo(df8,
      """
        |PushedAggregates: [SUM(SALARY)],
        |PushedFilters: [],
        |PushedGroupByExpressions:
        |[CASE WHEN NOT (IS_MANAGER = true) THEN SALARY ELSE 0.00 END],
        |""".stripMargin.replaceAll("\n", " "))
    checkAnswer(df8, Seq(Row(0, 32000), Row(12000, 12000), Row(9000, 9000)))
  }

  test("scan with aggregate push-down: DISTINCT SUM with group by") {
    val df = sql("SELECT SUM(DISTINCT SALARY) FROM h2.test.employee GROUP BY DEPT")
    checkAggregateRemoved(df)
    checkPushedInfo(df, "PushedAggregates: [SUM(DISTINCT SALARY)], " +
      "PushedFilters: [], PushedGroupByExpressions: [DEPT]")
    checkAnswer(df, Seq(Row(19000), Row(22000), Row(12000)))
  }

  test("scan with aggregate push-down: with multiple group by columns") {
    val df = sql("SELECT MAX(SALARY), MIN(BONUS) FROM h2.test.employee WHERE dept > 0" +
      " GROUP BY DEPT, NAME")
    checkFiltersRemoved(df)
    checkAggregateRemoved(df)
    checkPushedInfo(df, "PushedAggregates: [MAX(SALARY), MIN(BONUS)], " +
      "PushedFilters: [DEPT IS NOT NULL, DEPT > 0], PushedGroupByExpressions: [DEPT, NAME]")
    checkAnswer(df, Seq(Row(9000, 1200), Row(12000, 1200), Row(10000, 1300),
      Row(10000, 1000), Row(12000, 1200)))
  }

  test("scan with aggregate push-down: with concat multiple group key in project") {
    val df1 = sql("SELECT concat_ws('#', DEPT, NAME), MAX(SALARY) FROM h2.test.employee" +
      " WHERE dept > 0 GROUP BY DEPT, NAME")
    val filters1 = df1.queryExecution.optimizedPlan.collect {
      case f: Filter => f
    }
    assert(filters1.isEmpty)
    checkAggregateRemoved(df1)
    checkPushedInfo(df1, "PushedAggregates: [MAX(SALARY)], " +
      "PushedFilters: [DEPT IS NOT NULL, DEPT > 0], PushedGroupByExpressions: [DEPT, NAME]")
    checkAnswer(df1, Seq(Row("1#amy", 10000), Row("1#cathy", 9000), Row("2#alex", 12000),
      Row("2#david", 10000), Row("6#jen", 12000)))

    val df2 = sql("SELECT concat_ws('#', DEPT, NAME), MAX(SALARY) + MIN(BONUS)" +
      " FROM h2.test.employee WHERE dept > 0 GROUP BY DEPT, NAME")
    val filters2 = df2.queryExecution.optimizedPlan.collect {
      case f: Filter => f
    }
    assert(filters2.isEmpty)
    checkAggregateRemoved(df2)
    checkPushedInfo(df2, "PushedAggregates: [MAX(SALARY), MIN(BONUS)], " +
      "PushedFilters: [DEPT IS NOT NULL, DEPT > 0], PushedGroupByExpressions: [DEPT, NAME]")
    checkAnswer(df2, Seq(Row("1#amy", 11000), Row("1#cathy", 10200), Row("2#alex", 13200),
      Row("2#david", 11300), Row("6#jen", 13200)))

    val df3 = sql("SELECT concat_ws('#', DEPT, NAME), MAX(SALARY) + MIN(BONUS)" +
      " FROM h2.test.employee WHERE dept > 0 GROUP BY concat_ws('#', DEPT, NAME)")
    checkFiltersRemoved(df3)
    checkAggregateRemoved(df3, false)
    checkPushedInfo(df3, "PushedFilters: [DEPT IS NOT NULL, DEPT > 0], ")
    checkAnswer(df3, Seq(Row("1#amy", 11000), Row("1#cathy", 10200), Row("2#alex", 13200),
      Row("2#david", 11300), Row("6#jen", 13200)))
  }

  test("scan with aggregate push-down: with having clause") {
    val df = sql("SELECT MAX(SALARY), MIN(BONUS) FROM h2.test.employee WHERE dept > 0" +
      " GROUP BY DEPT having MIN(BONUS) > 1000")
    // filter over aggregate not push down
    checkFiltersRemoved(df, false)
    checkAggregateRemoved(df)
    checkPushedInfo(df, "PushedAggregates: [MAX(SALARY), MIN(BONUS)], " +
      "PushedFilters: [DEPT IS NOT NULL, DEPT > 0], PushedGroupByExpressions: [DEPT]")
    checkAnswer(df, Seq(Row(12000, 1200), Row(12000, 1200)))
  }

  test("scan with aggregate push-down: alias over aggregate") {
    val df = sql("SELECT * FROM h2.test.employee")
      .groupBy($"DEPT")
      .min("SALARY").as("total")
    checkAggregateRemoved(df)
    checkPushedInfo(df, "PushedAggregates: [MIN(SALARY)], " +
      "PushedFilters: [], PushedGroupByExpressions: [DEPT]")
    checkAnswer(df, Seq(Row(1, 9000), Row(2, 10000), Row(6, 12000)))
  }

  test("scan with aggregate push-down: order by alias over aggregate") {
    val df = spark.table("h2.test.employee")
    val query = df.select($"DEPT", $"SALARY")
      .filter($"DEPT" > 0)
      .groupBy($"DEPT")
      .agg(sum($"SALARY").as("total"))
      .filter($"total" > 1000)
      .orderBy($"total")
    checkFiltersRemoved(query, false)// filter over aggregate not pushed down
    checkAggregateRemoved(query)
    checkPushedInfo(query, "PushedAggregates: [SUM(SALARY)], " +
      "PushedFilters: [DEPT IS NOT NULL, DEPT > 0], PushedGroupByExpressions: [DEPT]")
    checkAnswer(query, Seq(Row(6, 12000), Row(1, 19000), Row(2, 22000)))
  }

  test("scan with aggregate push-down: udf over aggregate") {
    val df = spark.table("h2.test.employee")
    val decrease = udf { (x: Double, y: Double) => x - y }
    val query = df.select(decrease(sum($"SALARY"), sum($"BONUS")).as("value"))
    checkAggregateRemoved(query)
    checkPushedInfo(query, "PushedAggregates: [SUM(SALARY), SUM(BONUS)], ")
    checkAnswer(query, Seq(Row(47100.0)))
  }

  test("scan with aggregate push-down: partition columns are same as group by columns") {
    val df = spark.read
      .option("partitionColumn", "dept")
      .option("lowerBound", "0")
      .option("upperBound", "2")
      .option("numPartitions", "2")
      .table("h2.test.employee")
      .groupBy($"dept")
      .count()
    checkAggregateRemoved(df)
    checkAnswer(df, Seq(Row(1, 2), Row(2, 2), Row(6, 1)))
  }

  test("scan with aggregate push-down: VAR_POP VAR_SAMP with filter and group by") {
    val df = sql(
      """
        |SELECT
        |  VAR_POP(bonus),
        |  VAR_POP(DISTINCT bonus),
        |  VAR_SAMP(bonus),
        |  VAR_SAMP(DISTINCT bonus)
        |FROM h2.test.employee WHERE dept > 0 GROUP BY DePt""".stripMargin)
    checkFiltersRemoved(df)
    checkAggregateRemoved(df)
    checkPushedInfo(df,
     """
       |PushedAggregates: [VAR_POP(BONUS), VAR_POP(DISTINCT BONUS),
       |VAR_SAMP(BONUS), VAR_SAMP(DISTINCT BONUS)],
       |PushedFilters: [DEPT IS NOT NULL, DEPT > 0],
       |PushedGroupByExpressions: [DEPT],
       |""".stripMargin.replaceAll("\n", " "))
    checkAnswer(df, Seq(Row(10000d, 10000d, 20000d, 20000d),
      Row(2500d, 2500d, 5000d, 5000d), Row(0d, 0d, null, null)))
  }

  test("scan with aggregate push-down: STDDEV_POP STDDEV_SAMP with filter and group by") {
    val df = sql(
      """
        |SELECT
        |  STDDEV_POP(bonus),
        |  STDDEV_POP(DISTINCT bonus),
        |  STDDEV_SAMP(bonus),
        |  STDDEV_SAMP(DISTINCT bonus)
        |FROM h2.test.employee WHERE dept > 0 GROUP BY DePt""".stripMargin)
    checkFiltersRemoved(df)
    checkAggregateRemoved(df)
    checkPushedInfo(df,
      """
        |PushedAggregates: [STDDEV_POP(BONUS), STDDEV_POP(DISTINCT BONUS),
        |STDDEV_SAMP(BONUS), STDDEV_SAMP(DISTINCT BONUS)],
        |PushedFilters: [DEPT IS NOT NULL, DEPT > 0],
        |PushedGroupByExpressions: [DEPT],
        |""".stripMargin.replaceAll("\n", " "))
    checkAnswer(df, Seq(Row(100d, 100d, 141.4213562373095d, 141.4213562373095d),
      Row(50d, 50d, 70.71067811865476d, 70.71067811865476d), Row(0d, 0d, null, null)))
  }

  test("scan with aggregate push-down: COVAR_POP COVAR_SAMP with filter and group by") {
    val df1 = sql("SELECT COVAR_POP(bonus, bonus), COVAR_SAMP(bonus, bonus)" +
      " FROM h2.test.employee WHERE dept > 0 GROUP BY DePt")
    checkFiltersRemoved(df1)
    checkAggregateRemoved(df1)
    checkPushedInfo(df1, "PushedAggregates: [COVAR_POP(BONUS, BONUS), COVAR_SAMP(BONUS, BONUS)], " +
      "PushedFilters: [DEPT IS NOT NULL, DEPT > 0], PushedGroupByExpressions: [DEPT]")
    checkAnswer(df1, Seq(Row(10000d, 20000d), Row(2500d, 5000d), Row(0d, null)))

    val df2 = sql("SELECT COVAR_POP(DISTINCT bonus, bonus), COVAR_SAMP(DISTINCT bonus, bonus)" +
      " FROM h2.test.employee WHERE dept > 0 GROUP BY DePt")
    checkFiltersRemoved(df2)
    checkAggregateRemoved(df2, false)
    checkPushedInfo(df2, "PushedFilters: [DEPT IS NOT NULL, DEPT > 0]")
    checkAnswer(df2, Seq(Row(10000d, 20000d), Row(2500d, 5000d), Row(0d, null)))
  }

  test("scan with aggregate push-down: CORR with filter and group by") {
    val df1 = sql("SELECT CORR(bonus, bonus) FROM h2.test.employee WHERE dept > 0" +
      " GROUP BY DePt")
    checkFiltersRemoved(df1)
    checkAggregateRemoved(df1)
    checkPushedInfo(df1, "PushedAggregates: [CORR(BONUS, BONUS)], " +
      "PushedFilters: [DEPT IS NOT NULL, DEPT > 0], PushedGroupByExpressions: [DEPT]")
    checkAnswer(df1, Seq(Row(1d), Row(1d), Row(null)))

    val df2 = sql("SELECT CORR(DISTINCT bonus, bonus) FROM h2.test.employee WHERE dept > 0" +
      " GROUP BY DePt")
    checkFiltersRemoved(df2)
    checkAggregateRemoved(df2, false)
    checkPushedInfo(df2, "PushedFilters: [DEPT IS NOT NULL, DEPT > 0]")
    checkAnswer(df2, Seq(Row(1d), Row(1d), Row(null)))
  }

  test("scan with aggregate push-down: linear regression functions with filter and group by") {
    val df1 = sql(
      """
        |SELECT
        |  REGR_INTERCEPT(bonus, bonus),
        |  REGR_R2(bonus, bonus),
        |  REGR_SLOPE(bonus, bonus),
        |  REGR_SXY(bonus, bonus)
        |FROM h2.test.employee WHERE dept > 0 GROUP BY DePt""".stripMargin)
    checkFiltersRemoved(df1)
    checkAggregateRemoved(df1)
    checkPushedInfo(df1,
      """
        |PushedAggregates: [REGR_INTERCEPT(BONUS, BONUS), REGR_R2(BONUS, BONUS),
        |REGR_SLOPE(BONUS, BONUS), REGR_SXY(BONUS, BONUS)],
        |PushedFilters: [DEPT IS NOT NULL, DEPT > 0],
        |PushedGroupByExpressions: [DEPT],
        |""".stripMargin.replaceAll("\n", " "))
    checkAnswer(df1,
      Seq(Row(0.0, 1.0, 1.0, 20000.0), Row(0.0, 1.0, 1.0, 5000.0), Row(null, null, null, 0.0)))

    val df2 = sql(
      """
        |SELECT
        |  REGR_INTERCEPT(DISTINCT bonus, bonus),
        |  REGR_R2(DISTINCT bonus, bonus),
        |  REGR_SLOPE(DISTINCT bonus, bonus),
        |  REGR_SXY(DISTINCT bonus, bonus)
        |FROM h2.test.employee WHERE dept > 0 GROUP BY DePt""".stripMargin)
    checkFiltersRemoved(df2)
    checkAggregateRemoved(df2, false)
    checkPushedInfo(df2, "PushedFilters: [DEPT IS NOT NULL, DEPT > 0], ReadSchema:")
    checkAnswer(df2,
      Seq(Row(0.0, 1.0, 1.0, 20000.0), Row(0.0, 1.0, 1.0, 5000.0), Row(null, null, null, 0.0)))

    val df3 = sql(
      """
        |SELECT
        |  REGR_AVGX(bonus, bonus),
        |  REGR_AVGY(bonus, bonus)
        |FROM h2.test.employee WHERE dept > 0 GROUP BY DePt""".stripMargin)
    checkFiltersRemoved(df3)
    checkAggregateRemoved(df3)
    checkPushedInfo(df3,
      """
        |PushedAggregates: [AVG(CASE WHEN BONUS IS NOT NULL THEN BONUS ELSE null END)],
        |PushedFilters: [DEPT IS NOT NULL, DEPT > 0],
        |PushedGroupByExpressions: [DEPT],
        |""".stripMargin.replaceAll("\n", " "))
    checkAnswer(df3, Seq(Row(1100.0, 1100.0), Row(1200.0, 1200.0), Row(1250.0, 1250.0)))

    val df4 = sql(
      """
        |SELECT
        |  REGR_AVGX(DISTINCT bonus, bonus),
        |  REGR_AVGY(DISTINCT bonus, bonus)
        |FROM h2.test.employee WHERE dept > 0 GROUP BY DePt""".stripMargin)
    checkFiltersRemoved(df4)
    checkAggregateRemoved(df4)
    checkPushedInfo(df4,
      """
        |PushedAggregates: [AVG(DISTINCT CASE WHEN BONUS IS NOT NULL THEN BONUS ELSE null END)],
        |PushedFilters: [DEPT IS NOT NULL, DEPT > 0],
        |PushedGroupByExpressions: [DEPT],
        |""".stripMargin.replaceAll("\n", " "))
    checkAnswer(df4, Seq(Row(1100.0, 1100.0), Row(1200.0, 1200.0), Row(1250.0, 1250.0)))
  }

  test("scan with aggregate push-down: aggregate over alias push down") {
    val cols = Seq("a", "b", "c", "d", "e")
    val df1 = sql("SELECT * FROM h2.test.employee").toDF(cols: _*)
    val df2 = df1.groupBy().sum("c")
    checkAggregateRemoved(df2)
    df2.queryExecution.optimizedPlan.collect {
      case relation: DataSourceV2ScanRelation =>
        val expectedPlanFragment =
          "PushedAggregates: [SUM(SALARY)], PushedFilters: [], PushedGroupByExpressions: []"
        checkKeywordsExistsInExplain(df2, expectedPlanFragment)
        relation.scan match {
          case v1: V1ScanWrapper =>
            assert(v1.pushedDownOperators.aggregation.nonEmpty)
      }
    }
    checkAnswer(df2, Seq(Row(53000.00)))
  }

  test("scan with aggregate push-down: aggregate with partially pushed down filters" +
    "will NOT push down") {
    val df = spark.table("h2.test.employee")
    val name = udf { (x: String) => x.matches("cat|dav|amy") }
    val sub = udf { (x: String) => x.substring(0, 3) }
    val query = df.select($"SALARY", $"BONUS", sub($"NAME").as("shortName"))
      .filter("SALARY > 100")
      .filter(name($"shortName"))
      .agg(sum($"SALARY").as("sum_salary"))
    checkAggregateRemoved(query, false)
    query.queryExecution.optimizedPlan.collect {
      case relation: DataSourceV2ScanRelation => relation.scan match {
        case v1: V1ScanWrapper =>
          assert(v1.pushedDownOperators.aggregation.isEmpty)
      }
    }
    checkAnswer(query, Seq(Row(29000.0)))
  }

  test("scan with aggregate push-down: aggregate function with CASE WHEN") {
    val df = sql(
      """
        |SELECT
        |  COUNT(CASE WHEN SALARY > 8000 AND SALARY < 10000 THEN SALARY ELSE 0 END),
        |  COUNT(CASE WHEN SALARY > 8000 AND SALARY <= 13000 THEN SALARY ELSE 0 END),
        |  COUNT(CASE WHEN SALARY > 11000 OR SALARY < 10000 THEN SALARY ELSE 0 END),
        |  COUNT(CASE WHEN SALARY >= 12000 OR SALARY < 9000 THEN SALARY ELSE 0 END),
        |  COUNT(CASE WHEN SALARY >= 12000 OR NOT(SALARY >= 9000) THEN SALARY ELSE 0 END),
        |  MAX(CASE WHEN NOT(SALARY > 10000) AND SALARY >= 8000 THEN SALARY ELSE 0 END),
        |  MAX(CASE WHEN NOT(SALARY > 9000) OR SALARY > 10000 THEN SALARY ELSE 0 END),
        |  MAX(CASE WHEN NOT(SALARY > 10000) AND NOT(SALARY < 8000) THEN SALARY ELSE 0 END),
        |  MAX(CASE WHEN NOT(SALARY != 0) OR NOT(SALARY < 8000) THEN SALARY ELSE 0 END),
        |  MAX(CASE WHEN NOT(SALARY > 8000 AND SALARY < 10000) THEN 0 ELSE SALARY END),
        |  MIN(CASE WHEN NOT(SALARY > 8000 OR SALARY IS NULL) THEN SALARY ELSE 0 END),
        |  SUM(CASE WHEN SALARY > 10000 THEN 2 WHEN SALARY > 8000 THEN 1 END),
        |  AVG(CASE WHEN NOT(SALARY > 8000 OR SALARY IS NOT NULL) THEN SALARY ELSE 0 END)
        |FROM h2.test.employee GROUP BY DEPT
      """.stripMargin)
    checkAggregateRemoved(df)
    checkPushedInfo(df,
      "PushedAggregates: " +
        "[COUNT(CASE WHEN (SALARY > 8000.00) AND (SALARY < 10000.00) THEN SALARY ELSE 0.00 END), " +
        "COUNT(CASE WHEN (SALARY > 8000.00) AND (SALARY <= 13000.00) THEN SALARY ELSE 0.00 END), " +
        "COUNT(CASE WHEN (SALARY > 11000.00) OR (SALARY < 10000.00) THEN SALARY ELSE 0.00 END), " +
        "COUNT(CASE WHEN (SALARY >= 12000.00) OR (SALARY < 9000.00) THEN SALARY ELSE 0.00 END), " +
        "MAX(CASE WHEN (SALARY <= 10000.00) AND (SALARY >= 8000.00) THEN SALARY ELSE 0.00 END), " +
        "MAX(CASE WHEN (SALARY <= 9000.00) OR (SALARY > 10000.00) THEN SALARY ELSE 0.00 END), " +
        "MAX(CASE WHEN (SALARY = 0.00) OR (SALARY >= 8000.00) THEN SALARY ELSE 0.00 END), " +
        "MAX(CASE WHEN (SALARY <= 8000.00) OR (SALARY >= 10000.00) THEN 0.00 ELSE SALARY END), " +
        "MIN(CASE WHEN (SALARY <= 8000.00) AND (SALARY IS NOT NULL) THEN SALARY ELSE 0.00 END), " +
        "SUM(CASE WHEN SALARY > 10000.00 THEN 2 WHEN SALARY > 8000.00 THEN 1 END), " +
        "AVG(CASE WHEN (SALARY <= 8000.00) AND (SALARY IS NULL) THEN SALARY ELSE 0.00 END)], " +
        "PushedFilters: [], " +
        "PushedGroupByExpressions: [DEPT],")
    checkAnswer(df, Seq(Row(1, 1, 1, 1, 1, 0d, 12000d, 0d, 12000d, 0d, 0d, 2, 0d),
      Row(2, 2, 2, 2, 2, 10000d, 12000d, 10000d, 12000d, 0d, 0d, 3, 0d),
      Row(2, 2, 2, 2, 2, 10000d, 9000d, 10000d, 10000d, 9000d, 0d, 2, 0d)))
  }

  test("scan with aggregate push-down: aggregate function with binary arithmetic") {
    Seq(false, true).foreach { ansiMode =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiMode.toString) {
        val df = sql("SELECT SUM(2147483647 + DEPT) FROM h2.test.employee")
        checkAggregateRemoved(df, ansiMode)
        val expectedPlanFragment = if (ansiMode) {
          "PushedAggregates: [SUM(2147483647 + DEPT)], " +
            "PushedFilters: [], " +
            "PushedGroupByExpressions: []"
        } else {
          "PushedFilters: []"
        }
        checkPushedInfo(df, expectedPlanFragment)
        if (ansiMode) {
          val e = intercept[SparkException] {
            checkAnswer(df, Seq(Row(-10737418233L)))
          }
          assert(e.getMessage.contains(
            "org.h2.jdbc.JdbcSQLDataException: Numeric value out of range: \"2147483648\""))
        } else {
          checkAnswer(df, Seq(Row(-10737418233L)))
        }
      }
    }
  }

  test("scan with aggregate push-down: aggregate function with UDF") {
    val df = spark.table("h2.test.employee")
    val decrease = udf { (x: Double, y: Double) => x - y }
    val query = df.select(sum(decrease($"SALARY", $"BONUS")).as("value"))
    checkAggregateRemoved(query, false)
    checkPushedInfo(query, "PushedFilters: []")
    checkAnswer(query, Seq(Row(47100.0)))
  }

  test("scan with aggregate push-down: partition columns with multi group by columns") {
    val df = spark.read
      .option("partitionColumn", "dept")
      .option("lowerBound", "0")
      .option("upperBound", "2")
      .option("numPartitions", "2")
      .table("h2.test.employee")
      .groupBy($"dept", $"name")
      .count()
    checkAggregateRemoved(df, false)
    checkAnswer(df, Seq(Row(1, "amy", 1), Row(1, "cathy", 1),
      Row(2, "alex", 1), Row(2, "david", 1), Row(6, "jen", 1)))
  }

  test("scan with aggregate push-down: partition columns is different from group by columns") {
    val df = spark.read
      .option("partitionColumn", "dept")
      .option("lowerBound", "0")
      .option("upperBound", "2")
      .option("numPartitions", "2")
      .table("h2.test.employee")
      .groupBy($"name")
      .count()
    checkAggregateRemoved(df, false)
    checkAnswer(df,
      Seq(Row("alex", 1), Row("amy", 1), Row("cathy", 1), Row("david", 1), Row("jen", 1)))
  }

  test("column name with composite field") {
    checkAnswer(sql("SELECT `dept id`, `dept.id` FROM h2.test.dept"), Seq(Row(1, 1), Row(2, 1)))

    val df1 = sql("SELECT COUNT(`dept id`) FROM h2.test.dept")
    checkPushedInfo(df1, "PushedAggregates: [COUNT(`dept id`)]")
    checkAnswer(df1, Seq(Row(2)))

    val df2 = sql("SELECT `dept.id`, COUNT(`dept id`) FROM h2.test.dept GROUP BY `dept.id`")
    checkPushedInfo(df2,
      "PushedGroupByExpressions: [`dept.id`]", "PushedAggregates: [COUNT(`dept id`)]")
    checkAnswer(df2, Seq(Row(1, 2)))

    val df3 = sql("SELECT `dept id`, COUNT(`dept.id`) FROM h2.test.dept GROUP BY `dept id`")
    checkPushedInfo(df3,
      "PushedGroupByExpressions: [`dept id`]", "PushedAggregates: [COUNT(`dept.id`)]")
    checkAnswer(df3, Seq(Row(1, 1), Row(2, 1)))
  }

  test("column name with non-ascii") {
    // scalastyle:off
    checkAnswer(sql("SELECT `名` FROM h2.test.person"), Seq(Row(1), Row(2)))
    val df = sql("SELECT COUNT(`名`) FROM h2.test.person")
    checkAggregateRemoved(df)
    checkPushedInfo(df, "PushedAggregates: [COUNT(`名`)]")
    checkAnswer(df, Seq(Row(2)))
    // scalastyle:on
  }

  test("scan with aggregate push-down: complete push-down SUM, AVG, COUNT") {
    val df = spark.read
      .option("partitionColumn", "dept")
      .option("lowerBound", "0")
      .option("upperBound", "2")
      .option("numPartitions", "1")
      .table("h2.test.employee")
      .agg(sum($"SALARY").as("sum"), avg($"SALARY").as("avg"), count($"SALARY").as("count"))
    checkAggregateRemoved(df)
    checkPushedInfo(df, "PushedAggregates: [SUM(SALARY), AVG(SALARY), COUNT(SALARY)]")
    checkAnswer(df, Seq(Row(53000.00, 10600.000000, 5)))

    val df2 = spark.read
      .option("partitionColumn", "dept")
      .option("lowerBound", "0")
      .option("upperBound", "2")
      .option("numPartitions", "1")
      .table("h2.test.employee")
      .groupBy($"name")
      .agg(sum($"SALARY").as("sum"), avg($"SALARY").as("avg"), count($"SALARY").as("count"))
    checkAggregateRemoved(df)
    checkPushedInfo(df, "PushedAggregates: [SUM(SALARY), AVG(SALARY), COUNT(SALARY)]")
    checkAnswer(df2, Seq(
      Row("alex", 12000.00, 12000.000000, 1),
      Row("amy", 10000.00, 10000.000000, 1),
      Row("cathy", 9000.00, 9000.000000, 1),
      Row("david", 10000.00, 10000.000000, 1),
      Row("jen", 12000.00, 12000.000000, 1)))
  }

  test("scan with aggregate push-down: partial push-down SUM, AVG, COUNT") {
    val df = spark.read
      .option("partitionColumn", "dept")
      .option("lowerBound", "0")
      .option("upperBound", "2")
      .option("numPartitions", "2")
      .table("h2.test.employee")
      .agg(sum($"SALARY").as("sum"), avg($"SALARY").as("avg"), count($"SALARY").as("count"))
    checkAggregateRemoved(df, false)
    checkPushedInfo(df, "PushedAggregates: [SUM(SALARY), COUNT(SALARY)]")
    checkAnswer(df, Seq(Row(53000.00, 10600.000000, 5)))

    val df2 = spark.read
      .option("partitionColumn", "dept")
      .option("lowerBound", "0")
      .option("upperBound", "2")
      .option("numPartitions", "2")
      .table("h2.test.employee")
      .groupBy($"name")
      .agg(sum($"SALARY").as("sum"), avg($"SALARY").as("avg"), count($"SALARY").as("count"))
    checkAggregateRemoved(df, false)
    checkPushedInfo(df, "PushedAggregates: [SUM(SALARY), COUNT(SALARY)]")
    checkAnswer(df2, Seq(
      Row("alex", 12000.00, 12000.000000, 1),
      Row("amy", 10000.00, 10000.000000, 1),
      Row("cathy", 9000.00, 9000.000000, 1),
      Row("david", 10000.00, 10000.000000, 1),
      Row("jen", 12000.00, 12000.000000, 1)))
  }

  test("SPARK-37895: JDBC push down with delimited special identifiers") {
    val df = sql(
      """SELECT h2.test.view1.`|col1`, h2.test.view1.`|col2`, h2.test.view2.`|col3`
        |FROM h2.test.view1 LEFT JOIN h2.test.view2
        |ON h2.test.view1.`|col1` = h2.test.view2.`|col1`""".stripMargin)
    checkAnswer(df, Seq.empty[Row])
  }

  test("scan with aggregate push-down: complete push-down aggregate with alias") {
    val df = spark.table("h2.test.employee")
      .select($"DEPT", $"SALARY".as("mySalary"))
      .groupBy($"DEPT")
      .agg(sum($"mySalary").as("total"))
      .filter($"total" > 1000)
    checkAggregateRemoved(df)
    checkPushedInfo(df,
      "PushedAggregates: [SUM(SALARY)], PushedFilters: [], PushedGroupByExpressions: [DEPT]")
    checkAnswer(df, Seq(Row(1, 19000.00), Row(2, 22000.00), Row(6, 12000.00)))

    val df2 = spark.table("h2.test.employee")
      .select($"DEPT".as("myDept"), $"SALARY".as("mySalary"))
      .groupBy($"myDept")
      .agg(sum($"mySalary").as("total"))
      .filter($"total" > 1000)
    checkAggregateRemoved(df2)
    checkPushedInfo(df2,
      "PushedAggregates: [SUM(SALARY)], PushedFilters: [], PushedGroupByExpressions: [DEPT]")
    checkAnswer(df2, Seq(Row(1, 19000.00), Row(2, 22000.00), Row(6, 12000.00)))
  }

  test("scan with aggregate push-down: partial push-down aggregate with alias") {
    val df = spark.read
      .option("partitionColumn", "DEPT")
      .option("lowerBound", "0")
      .option("upperBound", "2")
      .option("numPartitions", "2")
      .table("h2.test.employee")
      .select($"NAME", $"SALARY".as("mySalary"))
      .groupBy($"NAME")
      .agg(sum($"mySalary").as("total"))
      .filter($"total" > 1000)
    checkAggregateRemoved(df, false)
    checkPushedInfo(df,
      "PushedAggregates: [SUM(SALARY)], PushedFilters: [], PushedGroupByExpressions: [NAME]")
    checkAnswer(df, Seq(Row("alex", 12000.00), Row("amy", 10000.00),
      Row("cathy", 9000.00), Row("david", 10000.00), Row("jen", 12000.00)))

    val df2 = spark.read
      .option("partitionColumn", "DEPT")
      .option("lowerBound", "0")
      .option("upperBound", "2")
      .option("numPartitions", "2")
      .table("h2.test.employee")
      .select($"NAME".as("myName"), $"SALARY".as("mySalary"))
      .groupBy($"myName")
      .agg(sum($"mySalary").as("total"))
      .filter($"total" > 1000)
    checkAggregateRemoved(df2, false)
    checkPushedInfo(df2,
      "PushedAggregates: [SUM(SALARY)], PushedFilters: [], PushedGroupByExpressions: [NAME]")
    checkAnswer(df2, Seq(Row("alex", 12000.00), Row("amy", 10000.00),
      Row("cathy", 9000.00), Row("david", 10000.00), Row("jen", 12000.00)))
  }

  test("scan with aggregate push-down: partial push-down AVG with overflow") {
    def createDataFrame: DataFrame = spark.read
      .option("partitionColumn", "id")
      .option("lowerBound", "0")
      .option("upperBound", "2")
      .option("numPartitions", "2")
      .table("h2.test.item")
      .agg(avg($"PRICE").as("avg"))

    Seq(true, false).foreach { ansiEnabled =>
      withSQLConf((SQLConf.ANSI_ENABLED.key, ansiEnabled.toString)) {
        val df = createDataFrame
        checkAggregateRemoved(df, false)
        df.queryExecution.optimizedPlan.collect {
          case _: DataSourceV2ScanRelation =>
            val expected_plan_fragment =
              "PushedAggregates: [COUNT(PRICE), SUM(PRICE)]"
            checkKeywordsExistsInExplain(df, expected_plan_fragment)
        }
        if (ansiEnabled) {
          val e = intercept[SparkException] {
            df.collect()
          }
          assert(e.getCause.isInstanceOf[ArithmeticException])
          assert(e.getCause.getMessage.contains("cannot be represented as Decimal") ||
            e.getCause.getMessage.contains("Overflow in sum of decimals"))
        } else {
          checkAnswer(df, Seq(Row(null)))
        }
      }
    }
  }

  test("register dialect specific functions") {
    JdbcDialects.unregisterDialect(H2Dialect)
    try {
      JdbcDialects.registerDialect(testH2Dialect)
      val df = sql("SELECT h2.my_avg(id) FROM h2.test.people")
      checkAggregateRemoved(df)
      checkAnswer(df, Row(1) :: Nil)
      val e1 = intercept[AnalysisException] {
        checkAnswer(sql("SELECT h2.test.my_avg2(id) FROM h2.test.people"), Seq.empty)
      }
      assert(e1.getMessage.contains("Undefined function: h2.test.my_avg2"))
      val e2 = intercept[AnalysisException] {
        checkAnswer(sql("SELECT h2.my_avg2(id) FROM h2.test.people"), Seq.empty)
      }
      assert(e2.getMessage.contains("Undefined function: h2.my_avg2"))
    } finally {
      JdbcDialects.unregisterDialect(testH2Dialect)
      JdbcDialects.registerDialect(H2Dialect)
    }
  }

  test("scan with aggregate push-down: complete push-down UDAF") {
    JdbcDialects.unregisterDialect(H2Dialect)
    try {
      JdbcDialects.registerDialect(testH2Dialect)
      val df1 = sql("SELECT h2.my_avg(id) FROM h2.test.people")
      checkAggregateRemoved(df1)
      checkPushedInfo(df1,
        "PushedAggregates: [iavg(ID)], PushedFilters: [], PushedGroupByExpressions: []")
      checkAnswer(df1, Seq(Row(1)))

      val df2 = sql("SELECT name, h2.my_avg(id) FROM h2.test.people group by name")
      checkAggregateRemoved(df2)
      checkPushedInfo(df2,
        "PushedAggregates: [iavg(ID)], PushedFilters: [], PushedGroupByExpressions: [NAME]")
      checkAnswer(df2, Seq(Row("fred", 1), Row("mary", 2)))
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        val df3 = sql(
          """
            |SELECT
            |  h2.my_avg(CASE WHEN NAME = 'fred' THEN id + 1 ELSE id END)
            |FROM h2.test.people
          """.stripMargin)
        checkAggregateRemoved(df3)
        checkPushedInfo(df3,
          "PushedAggregates: [iavg(CASE WHEN NAME = 'fred' THEN ID + 1 ELSE ID END)]," +
            " PushedFilters: [], PushedGroupByExpressions: []")
        checkAnswer(df3, Seq(Row(2)))

        val df4 = sql(
          """
            |SELECT
            |  name,
            |  h2.my_avg(CASE WHEN NAME = 'fred' THEN id + 1 ELSE id END)
            |FROM h2.test.people
            |GROUP BY name
          """.stripMargin)
        checkAggregateRemoved(df4)
        checkPushedInfo(df4,
          "PushedAggregates: [iavg(CASE WHEN NAME = 'fred' THEN ID + 1 ELSE ID END)]," +
            " PushedFilters: [], PushedGroupByExpressions: [NAME]")
        checkAnswer(df4, Seq(Row("fred", 2), Row("mary", 2)))
      }
    } finally {
      JdbcDialects.unregisterDialect(testH2Dialect)
      JdbcDialects.registerDialect(H2Dialect)
    }
  }

  test("Test INDEX Using SQL") {
    val loaded = Catalogs.load("h2", conf)
    val jdbcTable = loaded.asInstanceOf[TableCatalog]
      .loadTable(Identifier.of(Array("test"), "people"))
      .asInstanceOf[SupportsIndex]
    assert(jdbcTable != null)
    assert(jdbcTable.indexExists("people_index") == false)
    val indexes1 = jdbcTable.listIndexes()
    assert(indexes1.isEmpty)

    sql(s"CREATE INDEX people_index ON TABLE h2.test.people (id)")
    assert(jdbcTable.indexExists("people_index"))
    val indexes2 = jdbcTable.listIndexes()
    assert(!indexes2.isEmpty)
    assert(indexes2.size == 1)
    val tableIndex = indexes2.head
    assert(tableIndex.indexName() == "people_index")

    sql(s"DROP INDEX people_index ON TABLE h2.test.people")
    assert(jdbcTable.indexExists("people_index") == false)
    val indexes3 = jdbcTable.listIndexes()
    assert(indexes3.isEmpty)
  }
}
