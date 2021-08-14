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
import org.apache.spark.sql.{ExplainSuiteHelper, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.CannotReplaceMissingTableException
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.functions.{lit, sum, udf}
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

  test("scan with filter push-down") {
    val df = spark.table("h2.test.people").filter($"id" > 1)
    val filters = df.queryExecution.optimizedPlan.collect {
      case f: Filter => f
    }
    assert(filters.isEmpty)

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
    val scan = df.queryExecution.optimizedPlan.collectFirst {
      case s: DataSourceV2ScanRelation => s
    }.get
    assert(scan.schema.names.sameElements(Seq("ID")))
    checkAnswer(df, Seq(Row(1), Row(2)))
  }

  test("scan with filter push-down and column pruning") {
    val df = spark.table("h2.test.people").filter($"id" > 1).select("name")
    val filters = df.queryExecution.optimizedPlan.collect {
      case f: Filter => f
    }
    assert(filters.isEmpty)
    val scan = df.queryExecution.optimizedPlan.collectFirst {
      case s: DataSourceV2ScanRelation => s
    }.get
    assert(scan.schema.names.sameElements(Seq("NAME")))
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
        Row("test", "employee", false)))
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

  test("scan with aggregate push-down: MAX MIN with filter and group by") {
    val df = sql("select MAX(SALARY), MIN(BONUS) FROM h2.test.employee where dept > 0" +
      " group by DEPT")
    val filters = df.queryExecution.optimizedPlan.collect {
      case f: Filter => f
    }
    assert(filters.isEmpty)
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [MAX(SALARY), MIN(BONUS)], " +
            "PushedFilters: [IsNotNull(DEPT), GreaterThan(DEPT,0)], " +
            "PushedGroupby: [DEPT]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
    checkAnswer(df, Seq(Row(10000, 1000), Row(12000, 1200), Row(12000, 1200)))
  }

  test("scan with aggregate push-down: MAX MIN with filter without group by") {
    val df = sql("select MAX(ID), MIN(ID) FROM h2.test.people where id > 0")
    val filters = df.queryExecution.optimizedPlan.collect {
      case f: Filter => f
    }
    assert(filters.isEmpty)
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [MAX(ID), MIN(ID)], " +
            "PushedFilters: [IsNotNull(ID), GreaterThan(ID,0)], " +
            "PushedGroupby: []"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
    checkAnswer(df, Seq(Row(2, 1)))
  }

  test("scan with aggregate push-down: aggregate + number") {
    val df = sql("select MAX(SALARY) + 1 FROM h2.test.employee")
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
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [SUM(SALARY)], " +
            "PushedFilters: [], " +
            "PushedGroupby: [DEPT]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
    checkAnswer(df, Seq(Row(19000), Row(22000), Row(12000)))
  }

  test("scan with aggregate push-down: DISTINCT SUM with group by") {
    val df = sql("SELECT SUM(DISTINCT SALARY) FROM h2.test.employee GROUP BY DEPT")
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [SUM(DISTINCT SALARY)], " +
            "PushedFilters: [], " +
            "PushedGroupby: [DEPT]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
    checkAnswer(df, Seq(Row(19000), Row(22000), Row(12000)))
  }

  test("scan with aggregate push-down: with multiple group by columns") {
    val df = sql("select MAX(SALARY), MIN(BONUS) FROM h2.test.employee where dept > 0" +
      " group by DEPT, NAME")
    val filters11 = df.queryExecution.optimizedPlan.collect {
      case f: Filter => f
    }
    assert(filters11.isEmpty)
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [MAX(SALARY), MIN(BONUS)], " +
            "PushedFilters: [IsNotNull(DEPT), GreaterThan(DEPT,0)], " +
            "PushedGroupby: [DEPT, NAME]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
    checkAnswer(df, Seq(Row(9000, 1200), Row(12000, 1200), Row(10000, 1300),
      Row(10000, 1000), Row(12000, 1200)))
  }

  test("scan with aggregate push-down: with having clause") {
    val df = sql("select MAX(SALARY), MIN(BONUS) FROM h2.test.employee where dept > 0" +
      " group by DEPT having MIN(BONUS) > 1000")
    val filters = df.queryExecution.optimizedPlan.collect {
      case f: Filter => f  // filter over aggregate not push down
    }
    assert(filters.nonEmpty)
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [MAX(SALARY), MIN(BONUS)], " +
          "PushedFilters: [IsNotNull(DEPT), GreaterThan(DEPT,0)], " +
          "PushedGroupby: [DEPT]"
        checkKeywordsExistsInExplain(df, expected_plan_fragment)
    }
    checkAnswer(df, Seq(Row(12000, 1200), Row(12000, 1200)))
  }

  test("scan with aggregate push-down: alias over aggregate") {
    val df = sql("select * from h2.test.employee")
      .groupBy($"DEPT")
      .min("SALARY").as("total")
    df.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [MIN(SALARY)], " +
            "PushedFilters: [], " +
            "PushedGroupby: [DEPT]"
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
    query.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [SUM(SALARY)], " +
            "PushedFilters: [IsNotNull(DEPT), GreaterThan(DEPT,0)], " +
            "PushedGroupby: [DEPT]"
        checkKeywordsExistsInExplain(query, expected_plan_fragment)
    }
    checkAnswer(query, Seq(Row(6, 12000), Row(1, 19000), Row(2, 22000)))
  }

  test("scan with aggregate push-down: udf over aggregate") {
    val df = spark.table("h2.test.employee")
    val decrease = udf { (x: Double, y: Double) => x - y }
    val query = df.select(decrease(sum($"SALARY"), sum($"BONUS")).as("value"))
    query.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: [SUM(SALARY), SUM(BONUS)"
        checkKeywordsExistsInExplain(query, expected_plan_fragment)
    }
    checkAnswer(query, Seq(Row(47100.0)))
  }

  test("scan with aggregate push-down: aggregate over alias NOT push down") {
    val cols = Seq("a", "b", "c", "d")
    val df1 = sql("select * from h2.test.employee").toDF(cols: _*)
    val df2 = df1.groupBy().sum("c")
    df2.queryExecution.optimizedPlan.collect {
      case _: DataSourceV2ScanRelation =>
        val expected_plan_fragment =
          "PushedAggregates: []"  // aggregate over alias not push down
        checkKeywordsExistsInExplain(df2, expected_plan_fragment)
    }
    checkAnswer(df2, Seq(Row(53000.00)))
  }
}
