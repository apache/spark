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

package org.apache.spark.sql.execution

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.connector.SimpleWritableDataSource
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, DisableAdaptiveExecutionSuite, EnableAdaptiveExecutionSuite}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

abstract class RemoveRedundantProjectsSuiteBase
  extends QueryTest
    with SharedSparkSession
    with AdaptiveSparkPlanHelper {
  import testImplicits._

  private def assertProjectExecCount(df: DataFrame, expected: Int): Unit = {
    withClue(df.queryExecution) {
      val plan = df.queryExecution.executedPlan
      val actual = collectWithSubqueries(plan) { case p: ProjectExec => p }.size
      assert(actual == expected)
    }
  }

  private def assertProjectExec(query: String, enabled: Int, disabled: Int): Unit = {
    val df = sql(query)
    assertProjectExecCount(df, enabled)
    val result = df.collect()
    withSQLConf(SQLConf.REMOVE_REDUNDANT_PROJECTS_ENABLED.key -> "false") {
      val df2 = sql(query)
      assertProjectExecCount(df2, disabled)
      checkAnswer(df2, result)
    }
  }

  private val tmpPath = Utils.createTempDir()

  override def beforeAll(): Unit = {
    super.beforeAll()
    tmpPath.delete()
    val path = tmpPath.getAbsolutePath
    spark.range(100).selectExpr("id % 10 as key", "cast(id * 2 as int) as a",
      "cast(id * 3 as int) as b", "cast(id as string) as c", "array(id, id + 1, id + 3) as d")
      .write.partitionBy("key").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("testView")
  }

  override def afterAll(): Unit = {
    Utils.deleteRecursively(tmpPath)
    super.afterAll()
  }

  test("project") {
    val query = "select * from testView"
    assertProjectExec(query, 0, 0)
  }

  test("project with filter") {
    val query = "select * from testView where a > 5"
    assertProjectExec(query, 0, 1)
  }

  test("project with specific column ordering") {
    val query = "select key, a, b, c from testView"
    assertProjectExec(query, 1, 1)
  }

  test("project with extra columns") {
    val query = "select a, b, c, key, a from testView"
    assertProjectExec(query, 1, 1)
  }

  test("project with fewer columns") {
    val query = "select a from testView where a > 3"
    assertProjectExec(query, 1, 1)
  }

  test("aggregate without ordering requirement") {
    val query = "select sum(a) as sum_a, key, last(b) as last_b " +
      "from (select key, a, b from testView where a > 100) group by key"
    assertProjectExec(query, 0, 1)
  }

  test("aggregate with ordering requirement") {
    val query = "select a, sum(b) as sum_b from testView group by a"
    assertProjectExec(query, 1, 1)
  }

  test("join without ordering requirement") {
    val query = "select t1.key, t2.key, t1.a, t2.b from (select key, a, b, c from testView)" +
      " as t1 join (select key, a, b, c from testView) as t2 on t1.c > t2.c and t1.key > 10"
    assertProjectExec(query, 1, 3)
  }

  test("join with ordering requirement") {
    val query = "select * from (select key, a, c, b from testView) as t1 join " +
      "(select key, a, b, c from testView) as t2 on t1.key = t2.key where t2.a > 50"
    assertProjectExec(query, 2, 2)
  }

  test("window function") {
    val query = "select key, b, avg(a) over (partition by key order by a " +
      "rows between 1 preceding and 1 following) as avg from testView"
    assertProjectExec(query, 1, 2)
  }

  test("generate should require column ordering") {
    withTempView("testData") {
      spark.range(0, 10, 1)
        .selectExpr("id as key", "id * 2 as a", "id * 3 as b")
        .createOrReplaceTempView("testData")

      val data = sql("select key, a, b, count(*) from testData group by key, a, b limit 2")
      val df = data.selectExpr("a", "b", "key", "explode(array(key, a, b)) as d").filter("d > 0")
      df.collect()
      val plan = df.queryExecution.executedPlan
      val numProjects = collectWithSubqueries(plan) { case p: ProjectExec => p }.length

      // Create a new plan that reverse the GenerateExec output and add a new ProjectExec between
      // GenerateExec and its child. This is to test if the ProjectExec is removed, the output of
      // the query will be incorrect.
      val newPlan = stripAQEPlan(plan) transform {
        case g @ GenerateExec(_, requiredChildOutput, _, _, child) =>
          g.copy(requiredChildOutput = requiredChildOutput.reverse,
            child = ProjectExec(requiredChildOutput.reverse, child))
      }

      // Re-apply remove redundant project rule.
      val rule = RemoveRedundantProjects
      val newExecutedPlan = rule.apply(newPlan)
      // The manually added ProjectExec node shouldn't be removed.
      assert(collectWithSubqueries(newExecutedPlan) {
        case p: ProjectExec => p
      }.size == numProjects + 1)

      // Check the original plan's output and the new plan's output are the same.
      val expectedRows = plan.executeCollect()
      val actualRows = newExecutedPlan.executeCollect()
      assert(expectedRows.length == actualRows.length)
      expectedRows.zip(actualRows).foreach { case (expected, actual) => assert(expected == actual) }
    }
  }

  test("subquery") {
    withTempView("testData") {
      val data = spark.sparkContext.parallelize((1 to 100).map(i => Row(i, i.toString)))
      val schema = new StructType().add("key", "int").add("value", "string")
      spark.createDataFrame(data, schema).createOrReplaceTempView("testData")
      val query = "select key, value from testData where key in " +
        "(select sum(a) from testView where a > 5 group by key)"
      assertProjectExec(query, 0, 1)
    }
  }

  test("SPARK-33697: UnionExec should require column ordering") {
    withTable("t1", "t2") {
      spark.range(-10, 20)
        .selectExpr(
          "id",
          "date_add(date '1950-01-01', cast(id as int)) as datecol",
          "cast(id as string) strcol")
        .write.mode("overwrite").format("parquet").saveAsTable("t1")
      spark.range(-10, 20)
        .selectExpr(
          "cast(id as string) strcol",
          "id",
          "date_add(date '1950-01-01', cast(id as int)) as datecol")
        .write.mode("overwrite").format("parquet").saveAsTable("t2")

      val queryTemplate =
        """
          |SELECT DISTINCT datecol, strcol FROM
          |(
          |(SELECT datecol, id, strcol from t1)
          | %s
          |(SELECT datecol, id, strcol from t2)
          |)
          |""".stripMargin

      Seq(("UNION", 2, 2), ("UNION ALL", 1, 2)).foreach { case (setOperation, enabled, disabled) =>
        val query = queryTemplate.format(setOperation)
        assertProjectExec(query, enabled = enabled, disabled = disabled)
      }
    }
  }

  test("SPARK-33697: remove redundant projects under expand") {
    val query =
      """
        |SELECT t1.key, t2.key, sum(t1.a) AS s1, sum(t2.b) AS s2 FROM
        |(SELECT a, key FROM testView) t1
        |JOIN
        |(SELECT b, key FROM testView) t2
        |ON t1.key = t2.key
        |GROUP BY t1.key, t2.key GROUPING SETS(t1.key, t2.key)
        |ORDER BY t1.key, t2.key, s1, s2
        |LIMIT 10
        |""".stripMargin
    // The Project above the Expand is not removed due to SPARK-36020.
    assertProjectExec(query, 1, 3)
  }

  test("SPARK-36020: Project should not be removed when child's logical link is different") {
    val query =
      """
        |WITH t AS (
        | SELECT key, a, b, c, explode(d) AS d FROM testView
        |)
        |SELECT t1.key, t1.d, t2.key
        |FROM (SELECT d, key FROM t) t1
        |JOIN testView t2 ON t1.key = t2.key
        |""".stripMargin
    // The ProjectExec above the GenerateExec should not be removed because
    // they have different logical links.
    assertProjectExec(query, enabled = 2, disabled = 3)
  }

  Seq("true", "false").foreach { codegenEnabled =>
    test("SPARK-35287: project generating unsafe row for DataSourceV2ScanRelation " +
      s"should not be removed (codegen=$codegenEnabled)") {
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegenEnabled) {
        withTempPath { path =>
          val format = classOf[SimpleWritableDataSource].getName
          spark.range(3).select($"id" as "i", $"id" as "j")
            .write.format(format).mode("overwrite").save(path.getCanonicalPath)

          val df =
            spark.read.format(format).load(path.getCanonicalPath).filter($"i" > 0).orderBy($"i")
          assert(df.collect === Array(Row(1, 1), Row(2, 2)))
        }
      }
    }
  }
}

class RemoveRedundantProjectsSuite extends RemoveRedundantProjectsSuiteBase
  with DisableAdaptiveExecutionSuite

class RemoveRedundantProjectsSuiteAE extends RemoveRedundantProjectsSuiteBase
  with EnableAdaptiveExecutionSuite
