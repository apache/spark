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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, ExplainSuiteHelper, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.CannotReplaceMissingTableException
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Sort}
import org.apache.spark.sql.connector.expressions.{FieldReference, NullOrdering, SortDirection, SortValue}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2ScanRelation, V1ScanWrapper}
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.functions.{avg, count, lit, sum, udf}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class JDBCV2Suite extends QueryTest with SharedSparkSession with ExplainSuiteHelper {
  import testImplicits._

  val tempDir = Utils.createTempDir()
  val url = s"jdbc:h2:${tempDir.getCanonicalPath};user=testUser;password=testPass"
  var conn: java.sql.Connection = null

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.h2", classOf[JDBCTableCatalog].getName)
    .set("spark.sql.catalog.h2.url", url)
    .set("spark.sql.catalog.h2.driver", "org.h2.Driver")
    .set("spark.sql.catalog.h2.pushDownAggregate", "true")
    .set("spark.sql.catalog.h2.pushDownLimit", "true")

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
          " bonus DOUBLE)").executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"employee\" VALUES (1, 'amy', 10000, 1000)")
        .executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"employee\" VALUES (2, 'alex', 12000, 1200)")
        .executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"employee\" VALUES (1, 'cathy', 9000, 1200)")
        .executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"employee\" VALUES (2, 'david', 10000, 1300)")
        .executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"employee\" VALUES (6, 'jen', 12000, 1200)")
        .executeUpdate()
      conn.prepareStatement(
        "CREATE TABLE \"test\".\"dept\" (\"dept id\" INTEGER NOT NULL)").executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"dept\" VALUES (1)").executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"dept\" VALUES (2)").executeUpdate()

      // scalastyle:off
      conn.prepareStatement(
        "CREATE TABLE \"test\".\"person\" (\"名\" INTEGER NOT NULL)").executeUpdate()
      // scalastyle:on
      conn.prepareStatement("INSERT INTO \"test\".\"person\" VALUES (1)").executeUpdate()
      conn.prepareStatement("INSERT INTO \"test\".\"person\" VALUES (2)").executeUpdate()
    }
  }

  override def afterAll(): Unit = {
    Utils.deleteRecursively(tempDir)
    super.afterAll()
  }

  test("simple scan") {
    checkAnswer(sql("SELECT * FROM h2.test.empty_table"), Seq())
    checkAnswer(sql("SELECT * FROM h2.test.people"), Seq(Row("fred", 1), Row("mary", 2)))
    checkAnswer(sql("SELECT name, id FROM h2.test.people"), Seq(Row("fred", 1), Row("mary", 2)))
  }

  // TABLESAMPLE ({integer_expression | decimal_expression} PERCENT) and
  // TABLESAMPLE (BUCKET integer_expression OUT OF integer_expression)
  // are tested in JDBC dialect tests because TABLESAMPLE is not supported by all the DBMS
  test("TABLESAMPLE (integer_expression ROWS) is the same as LIMIT") {
    val df = sql("SELECT NAME FROM h2.test.employee TABLESAMPLE (3 ROWS)")
    checkSchemaNames(df, Seq("NAME"))
    checkPushedLimit(df, Some(3))
    checkAnswer(df, Seq(Row("amy"), Row("alex"), Row("cathy")))
  }

  private def checkSchemaNames(df: DataFrame, names: Seq[String]): Unit = {
    val scan = df.queryExecution.optimizedPlan.collectFirst {
      case s: DataSourceV2ScanRelation => s
    }.get
    assert(scan.schema.names.sameElements(names))
  }

  test("simple scan with LIMIT") {
    val df1 = spark.read.table("h2.test.employee")
      .where($"dept" === 1).limit(1)
    checkPushedLimit(df1, Some(1))
    checkAnswer(df1, Seq(Row(1, "amy", 10000.00, 1000.0)))

    val df2 = spark.read
      .option("partitionColumn", "dept")
      .option("lowerBound", "0")
      .option("upperBound", "2")
      .option("numPartitions", "2")
      .table("h2.test.employee")
      .filter($"dept" > 1)
      .limit(1)
    checkPushedLimit(df2, Some(1))
    checkAnswer(df2, Seq(Row(2, "alex", 12000.00, 1200.0)))

    val df3 = sql("SELECT name FROM h2.test.employee WHERE dept > 1 LIMIT 1")
    checkSchemaNames(df3, Seq("NAME"))
    checkPushedLimit(df3, Some(1))
    checkAnswer(df3, Seq(Row("alex")))

    val df4 = spark.read
      .table("h2.test.employee")
      .groupBy("DEPT").sum("SALARY")
      .limit(1)
    checkPushedLimit(df4, None)
    checkAnswer(df4, Seq(Row(1, 19000.00)))

    val name = udf { (x: String) => x.matches("cat|dav|amy") }
    val sub = udf { (x: String) => x.substring(0, 3) }
    val df5 = spark.read
      .table("h2.test.employee")
      .select($"SALARY", $"BONUS", sub($"NAME").as("shortName"))
      .filter(name($"shortName"))
      .limit(1)
    // LIMIT is pushed down only if all the filters are pushed down
    checkPushedLimit(df5, None)
    checkAnswer(df5, Seq(Row(10000.00, 1000.0, "amy")))
  }

  private def checkPushedLimit(df: DataFrame, limit: Option[Int] = None,
      sortValues: Seq[SortValue] = Nil): Unit = {
    df.queryExecution.optimizedPlan.collect {
      case relation: DataSourceV2ScanRelation => relation.scan match {
        case v1: V1ScanWrapper =>
          assert(v1.pushedDownOperators.limit === limit)
          assert(v1.pushedDownOperators.sortValues === sortValues)
      }
    }
    if (sortValues.nonEmpty) {
      val sorts = df.queryExecution.optimizedPlan.collect {
        case s: Sort => s
      }
      assert(sorts.isEmpty)
    }
  }

  test("simple scan with top N") {
    val df1 = spark.read
      .table("h2.test.employee")
      .sort("salary")
      .limit(1)
    checkPushedLimit(df1, Some(1), createSortValues())
    checkAnswer(df1, Seq(Row(1, "cathy", 9000.00, 1200.0)))

    val df2 = spark.read.table("h2.test.employee")
      .where($"dept" === 1).orderBy($"salary").limit(1)
    checkPushedLimit(df2, Some(1), createSortValues())
    checkAnswer(df2, Seq(Row(1, "cathy", 9000.00, 1200.0)))

    val df3 = spark.read
      .option("partitionColumn", "dept")
      .option("lowerBound", "0")
      .option("upperBound", "2")
      .option("numPartitions", "2")
      .table("h2.test.employee")
      .filter($"dept" > 1)
      .orderBy($"salary".desc)
      .limit(1)
    checkPushedLimit(
      df3, Some(1), createSortValues(SortDirection.DESCENDING, NullOrdering.NULLS_LAST))
    checkAnswer(df3, Seq(Row(2, "alex", 12000.00, 1200.0)))

    val df4 =
      sql("SELECT name FROM h2.test.employee WHERE dept > 1 ORDER BY salary NULLS LAST LIMIT 1")
    checkSchemaNames(df4, Seq("NAME"))
    checkPushedLimit(df4, Some(1), createSortValues(nullOrdering = NullOrdering.NULLS_LAST))
    checkAnswer(df4, Seq(Row("david")))

    val df5 = spark.read.table("h2.test.employee")
      .where($"dept" === 1).orderBy($"salary")
    checkPushedLimit(df5, None)
    checkAnswer(df5, Seq(Row(1, "cathy", 9000.00, 1200.0), Row(1, "amy", 10000.00, 1000.0)))

    val df6 = spark.read
      .table("h2.test.employee")
      .groupBy("DEPT").sum("SALARY")
      .orderBy("DEPT")
      .limit(1)
    checkPushedLimit(df6)
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
    checkPushedLimit(df7)
    checkAnswer(df7, Seq(Row(10000.00, 1000.0, "amy")))

    val df8 = spark.read
      .table("h2.test.employee")
      .sort(sub($"NAME"))
      .limit(1)
    checkPushedLimit(df8)
    checkAnswer(df8, Seq(Row(2, "alex", 12000.00, 1200.0)))
  }

  private def createSortValues(
      sortDirection: SortDirection = SortDirection.ASCENDING,
      nullOrdering: NullOrdering = NullOrdering.NULLS_FIRST): Seq[SortValue] = {
    Seq(SortValue(FieldReference("salary"), sortDirection, nullOrdering))
  }

  test("scan with filter push-down") {
    val df = spark.table("h2.test.people").filter($"id" > 1)

    checkFiltersRemoved(df)

    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedFilters: [IsNotNull(ID), GreaterThan(ID,1)]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }

    checkAnswer(df, Row("mary", 2))
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
        Row("test", "employee", false), Row("test", "dept", false), Row("test", "person", false)))
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

  test("scan with aggregate push-down: MAX AVG with filter and group by") {
    val df = sql("select MAX(SaLaRY), AVG(BONUS) FROM h2.test.employee where dept > 0" +
      " group by DePt")
    checkFiltersRemoved(df)
    checkAggregateRemoved(df)
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [MAX(SALARY), AVG(BONUS)], " +
            "PushedFilters: [IsNotNull(DEPT), GreaterThan(DEPT,0)], " +
            "PushedGroupByColumns: [DEPT]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
    checkAnswer(df, Seq(Row(10000, 1100.0), Row(12000, 1250.0), Row(12000, 1200.0)))
  }

  private def checkFiltersRemoved(df: DataFrame): Unit = {
    val filters = df.queryExecution.optimizedPlan.collect {
      case f: Filter => f
    }
    assert(filters.isEmpty)
  }

  test("scan with aggregate push-down: MAX AVG with filter without group by") {
    val df = sql("select MAX(ID), AVG(ID) FROM h2.test.people where id > 0")
    checkFiltersRemoved(df)
    checkAggregateRemoved(df)
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [MAX(ID), AVG(ID)], " +
            "PushedFilters: [IsNotNull(ID), GreaterThan(ID,0)], " +
            "PushedGroupByColumns: []"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
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
    val df = sql("select MAX(SALARY) + 1 FROM h2.test.employee")
    checkAggregateRemoved(df)
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [MAX(SALARY)]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
    checkAnswer(df, Seq(Row(12001)))
  }

  test("scan with aggregate push-down: COUNT(*)") {
    val df = sql("select COUNT(*) FROM h2.test.employee")
    checkAggregateRemoved(df)
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [COUNT(*)]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
    checkAnswer(df, Seq(Row(5)))
  }

  test("scan with aggregate push-down: COUNT(col)") {
    val df = sql("select COUNT(DEPT) FROM h2.test.employee")
    checkAggregateRemoved(df)
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [COUNT(DEPT)]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
    checkAnswer(df, Seq(Row(5)))
  }

  test("scan with aggregate push-down: COUNT(DISTINCT col)") {
    val df = sql("select COUNT(DISTINCT DEPT) FROM h2.test.employee")
    checkAggregateRemoved(df)
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [COUNT(DISTINCT DEPT)]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
    checkAnswer(df, Seq(Row(3)))
  }

  test("scan with aggregate push-down: SUM without filer and group by") {
    val df = sql("SELECT SUM(SALARY) FROM h2.test.employee")
    checkAggregateRemoved(df)
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [SUM(SALARY)]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
    checkAnswer(df, Seq(Row(53000)))
  }

  test("scan with aggregate push-down: DISTINCT SUM without filer and group by") {
    val df = sql("SELECT SUM(DISTINCT SALARY) FROM h2.test.employee")
    checkAggregateRemoved(df)
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [SUM(DISTINCT SALARY)]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
    checkAnswer(df, Seq(Row(31000)))
  }

  test("scan with aggregate push-down: SUM with group by") {
    val df = sql("SELECT SUM(SALARY) FROM h2.test.employee GROUP BY DEPT")
    checkAggregateRemoved(df)
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [SUM(SALARY)], " +
            "PushedFilters: [], " +
            "PushedGroupByColumns: [DEPT]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
    checkAnswer(df, Seq(Row(19000), Row(22000), Row(12000)))
  }

  test("scan with aggregate push-down: DISTINCT SUM with group by") {
    val df = sql("SELECT SUM(DISTINCT SALARY) FROM h2.test.employee GROUP BY DEPT")
    checkAggregateRemoved(df)
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [SUM(DISTINCT SALARY)], " +
            "PushedFilters: [], " +
            "PushedGroupByColumns: [DEPT]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
    checkAnswer(df, Seq(Row(19000), Row(22000), Row(12000)))
  }

  test("scan with aggregate push-down: with multiple group by columns") {
    val df = sql("select MAX(SALARY), MIN(BONUS) FROM h2.test.employee where dept > 0" +
      " group by DEPT, NAME")
    checkFiltersRemoved(df)
    checkAggregateRemoved(df)
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [MAX(SALARY), MIN(BONUS)], " +
            "PushedFilters: [IsNotNull(DEPT), GreaterThan(DEPT,0)], " +
            "PushedGroupByColumns: [DEPT, NAME]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
    checkAnswer(df, Seq(Row(9000, 1200), Row(12000, 1200), Row(10000, 1300),
      Row(10000, 1000), Row(12000, 1200)))
  }

  test("scan with aggregate push-down: with concat multiple group key in project") {
    val df1 = sql("select concat_ws('#', DEPT, NAME), MAX(SALARY) FROM h2.test.employee" +
      " where dept > 0 group by DEPT, NAME")
    val filters1 = df1.queryExecution.optimizedPlan.collect {
      case f: Filter => f
    }
    assert(filters1.isEmpty)
    checkAggregateRemoved(df1)
    df1.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [MAX(SALARY)], " +
            "PushedFilters: [IsNotNull(DEPT), GreaterThan(DEPT,0)], " +
            "PushedGroupByColumns: [DEPT, NAME]"
        checkKeywordsExistsInExplain(df1, expected_plan_fragment)
    }
    checkAnswer(df1, Seq(Row("1#amy", 10000), Row("1#cathy", 9000), Row("2#alex", 12000),
      Row("2#david", 10000), Row("6#jen", 12000)))

    val df2 = sql("select concat_ws('#', DEPT, NAME), MAX(SALARY) + MIN(BONUS)" +
      " FROM h2.test.employee where dept > 0 group by DEPT, NAME")
    val filters2 = df2.queryExecution.optimizedPlan.collect {
      case f: Filter => f
    }
    assert(filters2.isEmpty)
    checkAggregateRemoved(df2)
    df2.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [MAX(SALARY), MIN(BONUS)], " +
            "PushedFilters: [IsNotNull(DEPT), GreaterThan(DEPT,0)], " +
            "PushedGroupByColumns: [DEPT, NAME]"
        checkKeywordsExistsInExplain(df2, expected_plan_fragment)
    }
    checkAnswer(df2, Seq(Row("1#amy", 11000), Row("1#cathy", 10200), Row("2#alex", 13200),
      Row("2#david", 11300), Row("6#jen", 13200)))

    val df3 = sql("select concat_ws('#', DEPT, NAME), MAX(SALARY) + MIN(BONUS)" +
      " FROM h2.test.employee where dept > 0 group by concat_ws('#', DEPT, NAME)")
    val filters3 = df3.queryExecution.optimizedPlan.collect {
      case f: Filter => f
    }
    assert(filters3.isEmpty)
    checkAggregateRemoved(df3, false)
    df3.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedFilters: [IsNotNull(DEPT), GreaterThan(DEPT,0)], "
        checkKeywordsExistsInExplain(df3, expected_plan_fragment)
    }
    checkAnswer(df3, Seq(Row("1#amy", 11000), Row("1#cathy", 10200), Row("2#alex", 13200),
      Row("2#david", 11300), Row("6#jen", 13200)))
  }

  test("scan with aggregate push-down: with having clause") {
    val df = sql("select MAX(SALARY), MIN(BONUS) FROM h2.test.employee where dept > 0" +
      " group by DEPT having MIN(BONUS) > 1000")
    val filters = df.queryExecution.optimizedPlan.collect {
      case f: Filter => f  // filter over aggregate not push down
    }
    assert(filters.nonEmpty)
    checkAggregateRemoved(df)
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [MAX(SALARY), MIN(BONUS)], " +
          "PushedFilters: [IsNotNull(DEPT), GreaterThan(DEPT,0)], " +
          "PushedGroupByColumns: [DEPT]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
    checkAnswer(df, Seq(Row(12000, 1200), Row(12000, 1200)))
  }

  test("scan with aggregate push-down: alias over aggregate") {
    val df = sql("select * from h2.test.employee")
      .groupBy($"DEPT")
      .min("SALARY").as("total")
    checkAggregateRemoved(df)
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [MIN(SALARY)], " +
            "PushedFilters: [], " +
            "PushedGroupByColumns: [DEPT]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
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
    val filters = query.queryExecution.optimizedPlan.collect {
      case f: Filter => f
    }
    assert(filters.nonEmpty) // filter over aggregate not pushed down
    checkAggregateRemoved(df)
    query.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [SUM(SALARY)], " +
            "PushedFilters: [IsNotNull(DEPT), GreaterThan(DEPT,0)], " +
            "PushedGroupByColumns: [DEPT]"
        checkKeywordsExistsInExplain(query, expected_plan_fragment)
    }
    checkAnswer(query, Seq(Row(6, 12000), Row(1, 19000), Row(2, 22000)))
  }

  test("scan with aggregate push-down: udf over aggregate") {
    val df = spark.table("h2.test.employee")
    val decrease = udf { (x: Double, y: Double) => x - y }
    val query = df.select(decrease(sum($"SALARY"), sum($"BONUS")).as("value"))
    checkAggregateRemoved(query)
    query.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [SUM(SALARY), SUM(BONUS)]"
        checkKeywordsExistsInExplain(query, expected_plan_fragment)
    }
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
    val df = sql("select VAR_POP(bonus), VAR_SAMP(bonus) FROM h2.test.employee where dept > 0" +
      " group by DePt")
    checkFiltersRemoved(df)
    checkAggregateRemoved(df)
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [VAR_POP(BONUS), VAR_SAMP(BONUS)], " +
            "PushedFilters: [IsNotNull(DEPT), GreaterThan(DEPT,0)], " +
            "PushedGroupByColumns: [DEPT]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
    checkAnswer(df, Seq(Row(10000d, 20000d), Row(2500d, 5000d), Row(0d, null)))
  }

  test("scan with aggregate push-down: STDDEV_POP STDDEV_SAMP with filter and group by") {
    val df = sql("select STDDEV_POP(bonus), STDDEV_SAMP(bonus) FROM h2.test.employee" +
      " where dept > 0 group by DePt")
    checkFiltersRemoved(df)
    checkAggregateRemoved(df)
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [STDDEV_POP(BONUS), STDDEV_SAMP(BONUS)], " +
            "PushedFilters: [IsNotNull(DEPT), GreaterThan(DEPT,0)], " +
            "PushedGroupByColumns: [DEPT]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
    checkAnswer(df, Seq(Row(100d, 141.4213562373095d), Row(50d, 70.71067811865476d), Row(0d, null)))
  }

  test("scan with aggregate push-down: COVAR_POP COVAR_SAMP with filter and group by") {
    val df = sql("select COVAR_POP(bonus, bonus), COVAR_SAMP(bonus, bonus)" +
      " FROM h2.test.employee where dept > 0 group by DePt")
    checkFiltersRemoved(df)
    checkAggregateRemoved(df)
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [COVAR_POP(BONUS, BONUS), COVAR_SAMP(BONUS, BONUS)], " +
            "PushedFilters: [IsNotNull(DEPT), GreaterThan(DEPT,0)], " +
            "PushedGroupByColumns: [DEPT]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
    checkAnswer(df, Seq(Row(10000d, 20000d), Row(2500d, 5000d), Row(0d, null)))
  }

  test("scan with aggregate push-down: CORR with filter and group by") {
    val df = sql("select CORR(bonus, bonus) FROM h2.test.employee where dept > 0" +
      " group by DePt")
    checkFiltersRemoved(df)
    checkAggregateRemoved(df)
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [CORR(BONUS, BONUS)], " +
            "PushedFilters: [IsNotNull(DEPT), GreaterThan(DEPT,0)], " +
            "PushedGroupByColumns: [DEPT]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
    checkAnswer(df, Seq(Row(1d), Row(1d), Row(null)))
  }

  test("scan with aggregate push-down: aggregate over alias NOT push down") {
    val cols = Seq("a", "b", "c", "d")
    val df1 = sql("select * from h2.test.employee").toDF(cols: _*)
    val df2 = df1.groupBy().sum("c")
    checkAggregateRemoved(df2, false)
    df2.queryExecution.optimizedPlan.collect {
      case relation: DataSourceV2ScanRelation => relation.scan match {
        case v1: V1ScanWrapper =>
          assert(v1.pushedDownOperators.aggregation.isEmpty)
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

  test("scan with aggregate push-down: SUM(CASE WHEN) with group by") {
    val df =
      sql("SELECT SUM(CASE WHEN SALARY > 0 THEN 1 ELSE 0 END) FROM h2.test.employee GROUP BY DEPT")
    checkAggregateRemoved(df, false)
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedFilters: [], "
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
    checkAnswer(df, Seq(Row(1), Row(2), Row(2)))
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
    checkAnswer(sql("SELECT `dept id` FROM h2.test.dept"), Seq(Row(1), Row(2)))
    val df = sql("SELECT COUNT(`dept id`) FROM h2.test.dept")
    checkAggregateRemoved(df)
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [COUNT(`dept id`)]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
    checkAnswer(df, Seq(Row(2)))
  }

  test("column name with non-ascii") {
    // scalastyle:off
    checkAnswer(sql("SELECT `名` FROM h2.test.person"), Seq(Row(1), Row(2)))
    val df = sql("SELECT COUNT(`名`) FROM h2.test.person")
    checkAggregateRemoved(df)
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [COUNT(`名`)]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
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
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [SUM(SALARY), AVG(SALARY), COUNT(SALARY)]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
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
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [SUM(SALARY), AVG(SALARY), COUNT(SALARY)]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
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
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [SUM(SALARY), COUNT(SALARY)]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
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
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [SUM(SALARY), COUNT(SALARY)]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
    checkAnswer(df2, Seq(
      Row("alex", 12000.00, 12000.000000, 1),
      Row("amy", 10000.00, 10000.000000, 1),
      Row("cathy", 9000.00, 9000.000000, 1),
      Row("david", 10000.00, 10000.000000, 1),
      Row("jen", 12000.00, 12000.000000, 1)))
  }
}
