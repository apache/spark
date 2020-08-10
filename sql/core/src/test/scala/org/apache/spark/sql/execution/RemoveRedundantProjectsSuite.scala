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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

class RemoveRedundantProjectsSuite extends QueryTest with SharedSparkSession with SQLTestUtils {

  private def assertProjectExecCount(df: DataFrame, expected: Int): Unit = {
    withClue(df.queryExecution) {
      val plan = df.queryExecution.executedPlan
      val actual = plan.collectWithSubqueries { case p: ProjectExec => p }.size
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

  test("generate") {
    val query = "select a, key, explode(d) from testView where a > 10"
    assertProjectExec(query, 0, 1)
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
}
