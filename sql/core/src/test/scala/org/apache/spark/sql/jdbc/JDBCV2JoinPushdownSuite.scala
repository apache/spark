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
import org.apache.spark.sql.{DataFrame, ExplainSuiteHelper, QueryTest}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, GlobalLimit, Join, Sort}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class JDBCV2JoinPushdownSuite extends QueryTest with SharedSparkSession with ExplainSuiteHelper {
  val tempDir = Utils.createTempDir()
  val url = s"jdbc:h2:${tempDir.getCanonicalPath};user=testUser;password=testPass"

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.h2", classOf[JDBCTableCatalog].getName)
    .set("spark.sql.catalog.h2.url", url)
    .set("spark.sql.catalog.h2.driver", "org.h2.Driver")
    .set("spark.sql.catalog.h2.pushDownAggregate", "true")
    .set("spark.sql.catalog.h2.pushDownLimit", "true")
    .set("spark.sql.catalog.h2.pushDownOffset", "true")
    .set("spark.sql.catalog.h2.pushDownJoin", "true")

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
    }
  }

  override def afterAll(): Unit = {
    Utils.deleteRecursively(tempDir)
    super.afterAll()
  }

  private def checkPushedInfo(df: DataFrame, expectedPlanFragment: String*): Unit = {
    withSQLConf(SQLConf.MAX_METADATA_STRING_LENGTH.key -> "1000") {
      df.queryExecution.optimizedPlan.collect {
        case _: DataSourceV2ScanRelation =>
          checkKeywordsExistsInExplain(df, expectedPlanFragment: _*)
      }
    }
  }

  // Conditionless joins are not supported in join pushdown
  test("Test that 2-way join without condition should not have join pushed down") {
    val sqlQuery = "SELECT * FROM h2.test.employee a, h2.test.employee b"
    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)
      val joinNodes = df.queryExecution.optimizedPlan.collect {
        case j: Join => j
      }

      assert(joinNodes.nonEmpty)
      checkAnswer(df, rows)
    }
  }

  // Conditionless joins are not supported in join pushdown
  test("Test that multi-way join without condition should not have join pushed down") {
    val sqlQuery = """
      |SELECT * FROM
      |h2.test.employee a,
      |h2.test.employee b,
      |h2.test.employee c
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)

      val joinNodes = df.queryExecution.optimizedPlan.collect {
        case j: Join => j
      }

      assert(joinNodes.nonEmpty)
      checkAnswer(df, rows)
    }
  }

  test("Test self join with condition") {
    val sqlQuery = "SELECT * FROM h2.test.employee a JOIN h2.test.employee b ON a.dept = b.dept + 1"

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)

      val joinNodes = df.queryExecution.optimizedPlan.collect {
        case j: Join => j
      }

      assert(joinNodes.isEmpty)
      checkPushedInfo(df, "PushedJoins: [h2.test.employee, h2.test.employee]")
      checkAnswer(df, rows)
    }
  }

  test("Test multi-way self join with conditions") {
    val sqlQuery = """
      |SELECT * FROM
      |h2.test.employee a
      |JOIN h2.test.employee b ON b.dept = a.dept + 1
      |JOIN h2.test.employee c ON c.dept = b.dept - 1
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    assert(!rows.isEmpty)

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)
      val joinNodes = df.queryExecution.optimizedPlan.collect {
        case j: Join => j
      }

      assert(joinNodes.isEmpty)
      checkPushedInfo(df, "PushedJoins: [h2.test.employee, h2.test.employee, h2.test.employee]")
      checkAnswer(df, rows)
    }
  }

  test("Test self join with column pruning") {
    val sqlQuery = """
      |SELECT a.dept + 2, b.dept, b.salary FROM
      |h2.test.employee a JOIN h2.test.employee b
      |ON a.dept = b.dept + 1
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)

      val joinNodes = df.queryExecution.optimizedPlan.collect {
        case j: Join => j
      }

      assert(joinNodes.isEmpty)
      checkPushedInfo(df, "PushedJoins: [h2.test.employee, h2.test.employee]")
      checkAnswer(df, rows)
    }
  }

  test("Test 2-way join with column pruning - different tables") {
    val sqlQuery = """
      |SELECT * FROM
      |h2.test.employee a JOIN h2.test.people b
      |ON a.dept = b.id
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)

      val joinNodes = df.queryExecution.optimizedPlan.collect {
        case j: Join => j
      }

      assert(joinNodes.isEmpty)
      checkPushedInfo(df, "PushedJoins: [h2.test.employee, h2.test.people]")
      checkPushedInfo(df,
        "PushedFilters: [DEPT IS NOT NULL, ID IS NOT NULL, DEPT = ID]")
      checkAnswer(df, rows)
    }
  }

  test("Test multi-way self join with column pruning") {
    val sqlQuery = """
      |SELECT a.dept, b.*, c.dept, c.salary + a.salary
      |FROM h2.test.employee a
      |JOIN h2.test.employee b ON b.dept = a.dept + 1
      |JOIN h2.test.employee c ON c.dept = b.dept - 1
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)

      val joinNodes = df.queryExecution.optimizedPlan.collect {
        case j: Join => j
      }

      assert(joinNodes.isEmpty)
      checkPushedInfo(df, "PushedJoins: [h2.test.employee, h2.test.employee, h2.test.employee]")
      checkAnswer(df, rows)
    }
  }

  test("Test aliases not supported in join pushdown") {
    val sqlQuery = """
      |SELECT a.dept, bc.*
      |FROM h2.test.employee a
      |JOIN (
      |  SELECT b.*, c.dept AS c_dept, c.salary AS c_salary
      |  FROM h2.test.employee b
      |  JOIN h2.test.employee c ON c.dept = b.dept - 1
      |) bc ON bc.dept = a.dept + 1
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)
      val joinNodes = df.queryExecution.optimizedPlan.collect {
        case j: Join => j
      }

      assert(joinNodes.nonEmpty)
      checkAnswer(df, rows)
    }
  }

  test("Test join with dataframe with duplicated columns") {
    val df1 = sql("SELECT dept FROM h2.test.employee")
    val df2 = sql("SELECT dept, dept FROM h2.test.employee")

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      df1.join(df2, "dept").collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val joinDf = df1.join(df2, "dept")
      val joinNodes = joinDf.queryExecution.optimizedPlan.collect {
        case j: Join => j
      }

      assert(joinNodes.isEmpty)
      checkPushedInfo(joinDf, "PushedJoins: [h2.test.employee, h2.test.employee]")
      checkAnswer(joinDf, rows)
    }
  }

  test("Test aggregate on top of 2-way self join") {
    val sqlQuery = """
      |SELECT min(a.dept + b.dept), min(a.dept)
      |FROM h2.test.employee a
      |JOIN h2.test.employee b ON a.dept = b.dept + 1
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)
      val joinNodes = df.queryExecution.optimizedPlan.collect {
        case j: Join => j
      }

      val aggNodes = df.queryExecution.optimizedPlan.collect {
        case a: Aggregate => a
      }

      assert(joinNodes.isEmpty)
      assert(aggNodes.isEmpty)
      checkPushedInfo(df, "PushedJoins: [h2.test.employee, h2.test.employee]")
      checkAnswer(df, rows)
    }
  }

  test("Test aggregate on top of multi-way self join") {
    val sqlQuery = """
      |SELECT min(a.dept + b.dept), min(a.dept), min(c.dept - 2)
      |FROM h2.test.employee a
      |JOIN h2.test.employee b ON b.dept = a.dept + 1
      |JOIN h2.test.employee c ON c.dept = b.dept - 1
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)
      val joinNodes = df.queryExecution.optimizedPlan.collect {
        case j: Join => j
      }

      val aggNodes = df.queryExecution.optimizedPlan.collect {
        case a: Aggregate => a
      }

      assert(joinNodes.isEmpty)
      assert(aggNodes.isEmpty)
      checkPushedInfo(df, "PushedJoins: [h2.test.employee, h2.test.employee, h2.test.employee]")
      checkAnswer(df, rows)
    }
  }

  test("Test sort limit on top of join is pushed down") {
    val sqlQuery = """
      |SELECT min(a.dept + b.dept), a.dept, b.dept
      |FROM h2.test.employee a
      |JOIN h2.test.employee b ON b.dept = a.dept + 1
      |GROUP BY a.dept, b.dept
      |ORDER BY a.dept
      |LIMIT 1
      |""".stripMargin

    val rows = withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "false") {
      sql(sqlQuery).collect().toSeq
    }

    withSQLConf(
      SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      val df = sql(sqlQuery)
      val joinNodes = df.queryExecution.optimizedPlan.collect {
        case j: Join => j
      }

      val sortNodes = df.queryExecution.optimizedPlan.collect {
        case s: Sort => s
      }

      val limitNodes = df.queryExecution.optimizedPlan.collect {
        case l: GlobalLimit => l
      }

      assert(joinNodes.isEmpty)
      assert(sortNodes.isEmpty)
      assert(limitNodes.isEmpty)
      checkPushedInfo(df, "PushedJoins: [h2.test.employee, h2.test.employee]")
      checkAnswer(df, rows)
    }
  }
}
