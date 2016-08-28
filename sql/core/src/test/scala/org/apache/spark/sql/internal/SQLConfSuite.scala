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

package org.apache.spark.sql.internal

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{QueryTest, Row, SparkSession, SQLContext}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.test.{SharedSQLContext, TestSQLContext}

class SQLConfSuite extends QueryTest with SharedSQLContext {
  private val testKey = "test.key.0"
  private val testVal = "test.val.0"

  test("propagate from spark conf") {
    // We create a new context here to avoid order dependence with other tests that might call
    // clear().
    val newContext = new SQLContext(SparkSession.builder().sparkContext(sparkContext).getOrCreate())
    assert(newContext.getConf("spark.sql.testkey", "false") === "true")
  }

  test("programmatic ways of basic setting and getting") {
    // Set a conf first.
    spark.conf.set(testKey, testVal)
    // Clear the conf.
    spark.sessionState.conf.clear()
    // After clear, only overrideConfs used by unit test should be in the SQLConf.
    assert(spark.conf.getAll === TestSQLContext.overrideConfs)

    spark.conf.set(testKey, testVal)
    assert(spark.conf.get(testKey) === testVal)
    assert(spark.conf.get(testKey, testVal + "_") === testVal)
    assert(spark.conf.getAll.contains(testKey))

    // Tests SQLConf as accessed from a SQLContext is mutable after
    // the latter is initialized, unlike SparkConf inside a SparkContext.
    assert(spark.conf.get(testKey) === testVal)
    assert(spark.conf.get(testKey, testVal + "_") === testVal)
    assert(spark.conf.getAll.contains(testKey))

    spark.sessionState.conf.clear()
  }

  test("parse SQL set commands") {
    spark.sessionState.conf.clear()
    sql(s"set $testKey=$testVal")
    assert(spark.conf.get(testKey, testVal + "_") === testVal)
    assert(spark.conf.get(testKey, testVal + "_") === testVal)

    sql("set some.property=20")
    assert(spark.conf.get("some.property", "0") === "20")
    sql("set some.property = 40")
    assert(spark.conf.get("some.property", "0") === "40")

    val key = "spark.sql.key"
    val vs = "val0,val_1,val2.3,my_table"
    sql(s"set $key=$vs")
    assert(spark.conf.get(key, "0") === vs)

    sql(s"set $key=")
    assert(spark.conf.get(key, "0") === "")

    spark.sessionState.conf.clear()
  }

  test("set command for display") {
    spark.sessionState.conf.clear()
    checkAnswer(
      sql("SET").where("key = 'spark.sql.groupByOrdinal'").select("key", "value"),
      Nil)

    checkAnswer(
      sql("SET -v").where("key = 'spark.sql.groupByOrdinal'").select("key", "value"),
      Row("spark.sql.groupByOrdinal", "true"))

    sql("SET spark.sql.groupByOrdinal=false")

    checkAnswer(
      sql("SET").where("key = 'spark.sql.groupByOrdinal'").select("key", "value"),
      Row("spark.sql.groupByOrdinal", "false"))

    checkAnswer(
      sql("SET -v").where("key = 'spark.sql.groupByOrdinal'").select("key", "value"),
      Row("spark.sql.groupByOrdinal", "false"))
  }

  test("deprecated property") {
    spark.sessionState.conf.clear()
    val original = spark.conf.get(SQLConf.SHUFFLE_PARTITIONS)
    try {
      sql(s"set ${SQLConf.Deprecated.MAPRED_REDUCE_TASKS}=10")
      assert(spark.conf.get(SQLConf.SHUFFLE_PARTITIONS) === 10)
    } finally {
      sql(s"set ${SQLConf.SHUFFLE_PARTITIONS}=$original")
    }
  }

  test("reset - public conf") {
    spark.sessionState.conf.clear()
    val original = spark.conf.get(SQLConf.GROUP_BY_ORDINAL)
    try {
      assert(spark.conf.get(SQLConf.GROUP_BY_ORDINAL) === true)
      sql(s"set ${SQLConf.GROUP_BY_ORDINAL.key}=false")
      assert(spark.conf.get(SQLConf.GROUP_BY_ORDINAL) === false)
      assert(sql(s"set").where(s"key = '${SQLConf.GROUP_BY_ORDINAL.key}'").count() == 1)
      sql(s"reset")
      assert(spark.conf.get(SQLConf.GROUP_BY_ORDINAL) === true)
      assert(sql(s"set").where(s"key = '${SQLConf.GROUP_BY_ORDINAL.key}'").count() == 0)
    } finally {
      sql(s"set ${SQLConf.GROUP_BY_ORDINAL}=$original")
    }
  }

  test("reset - internal conf") {
    spark.sessionState.conf.clear()
    val original = spark.conf.get(SQLConf.OPTIMIZER_MAX_ITERATIONS)
    try {
      assert(spark.conf.get(SQLConf.OPTIMIZER_MAX_ITERATIONS) === 100)
      sql(s"set ${SQLConf.OPTIMIZER_MAX_ITERATIONS.key}=10")
      assert(spark.conf.get(SQLConf.OPTIMIZER_MAX_ITERATIONS) === 10)
      assert(sql(s"set").where(s"key = '${SQLConf.OPTIMIZER_MAX_ITERATIONS.key}'").count() == 1)
      sql(s"reset")
      assert(spark.conf.get(SQLConf.OPTIMIZER_MAX_ITERATIONS) === 100)
      assert(sql(s"set").where(s"key = '${SQLConf.OPTIMIZER_MAX_ITERATIONS.key}'").count() == 0)
    } finally {
      sql(s"set ${SQLConf.OPTIMIZER_MAX_ITERATIONS}=$original")
    }
  }

  test("reset - user-defined conf") {
    spark.sessionState.conf.clear()
    val userDefinedConf = "x.y.z.reset"
    try {
      assert(spark.conf.getOption(userDefinedConf).isEmpty)
      sql(s"set $userDefinedConf=false")
      assert(spark.conf.get(userDefinedConf) === "false")
      assert(sql(s"set").where(s"key = '$userDefinedConf'").count() == 1)
      sql(s"reset")
      assert(spark.conf.getOption(userDefinedConf).isEmpty)
    } finally {
      spark.conf.unset(userDefinedConf)
    }
  }

  test("invalid conf value") {
    spark.sessionState.conf.clear()
    val e = intercept[IllegalArgumentException] {
      sql(s"set ${SQLConf.CASE_SENSITIVE.key}=10")
    }
    assert(e.getMessage === s"${SQLConf.CASE_SENSITIVE.key} should be boolean, but was 10")
  }

  test("Test SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE's method") {
    spark.sessionState.conf.clear()

    spark.conf.set(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE.key, "100")
    assert(spark.conf.get(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE) === 100)

    spark.conf.set(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE.key, "1k")
    assert(spark.conf.get(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE) === 1024)

    spark.conf.set(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE.key, "1M")
    assert(spark.conf.get(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE) === 1048576)

    spark.conf.set(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE.key, "1g")
    assert(spark.conf.get(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE) === 1073741824)

    spark.conf.set(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE.key, "-1")
    assert(spark.conf.get(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE) === -1)

    // Test overflow exception
    intercept[IllegalArgumentException] {
      // This value exceeds Long.MaxValue
      spark.conf.set(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE.key, "90000000000g")
    }

    intercept[IllegalArgumentException] {
      // This value less than Long.MinValue
      spark.conf.set(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE.key, "-90000000000g")
    }

    spark.sessionState.conf.clear()
  }

  test("SparkSession can access configs set in SparkConf") {
    try {
      sparkContext.conf.set("spark.to.be.or.not.to.be", "my love")
      sparkContext.conf.set("spark.sql.with.or.without.you", "my love")
      val spark = new SparkSession(sparkContext)
      assert(spark.conf.get("spark.to.be.or.not.to.be") == "my love")
      assert(spark.conf.get("spark.sql.with.or.without.you") == "my love")
    } finally {
      sparkContext.conf.remove("spark.to.be.or.not.to.be")
      sparkContext.conf.remove("spark.sql.with.or.without.you")
    }
  }

  test("default value of WAREHOUSE_PATH") {
    val original = spark.conf.get(SQLConf.WAREHOUSE_PATH)
    try {
      // to get the default value, always unset it
      spark.conf.unset(SQLConf.WAREHOUSE_PATH.key)
      assert(spark.sessionState.conf.warehousePath
        === new Path(s"${System.getProperty("user.dir")}/spark-warehouse").toString)
    } finally {
      sql(s"set ${SQLConf.WAREHOUSE_PATH}=$original")
    }
  }

  test("MAX_CASES_BRANCHES") {
    withTable("tab1") {
      spark.range(10).write.saveAsTable("tab1")
      val sql_one_branch_caseWhen = "SELECT CASE WHEN id = 1 THEN 1 END FROM tab1"
      val sql_two_branch_caseWhen = "SELECT CASE WHEN id = 1 THEN 1 ELSE 0 END FROM tab1"

      withSQLConf(SQLConf.MAX_CASES_BRANCHES.key -> "0") {
        assert(!sql(sql_one_branch_caseWhen)
          .queryExecution.executedPlan.isInstanceOf[WholeStageCodegenExec])
        assert(!sql(sql_two_branch_caseWhen)
          .queryExecution.executedPlan.isInstanceOf[WholeStageCodegenExec])
      }

      withSQLConf(SQLConf.MAX_CASES_BRANCHES.key -> "1") {
        assert(sql(sql_one_branch_caseWhen)
          .queryExecution.executedPlan.isInstanceOf[WholeStageCodegenExec])
        assert(!sql(sql_two_branch_caseWhen)
          .queryExecution.executedPlan.isInstanceOf[WholeStageCodegenExec])
      }

      withSQLConf(SQLConf.MAX_CASES_BRANCHES.key -> "2") {
        assert(sql(sql_one_branch_caseWhen)
          .queryExecution.executedPlan.isInstanceOf[WholeStageCodegenExec])
        assert(sql(sql_two_branch_caseWhen)
          .queryExecution.executedPlan.isInstanceOf[WholeStageCodegenExec])
      }
    }
  }
}
