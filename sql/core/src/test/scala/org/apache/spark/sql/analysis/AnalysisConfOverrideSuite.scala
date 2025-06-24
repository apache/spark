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

package org.apache.spark.sql.analysis

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.test.SharedSparkSession

class AnalysisConfOverrideSuite extends SharedSparkSession {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.extensions", "com.databricks.sql.ConfOverrideValidationExtensions")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("SELECT id as a FROM range(10)").createTempView("table")
    spark.sql("SELECT id as a, (id + 1) as b FROM range(10)").createTempView("table2")
  }

  override def afterAll(): Unit = {
    spark.catalog.dropTempView("table")
    spark.catalog.dropTempView("table2")
    super.afterAll()
  }

  private def testOverride(testName: String)(f: (String, String) => Unit): Unit = {
    test(testName) {
      val key = "spark.sql.catalog.x.y"
      val value = "true"
      withSQLConf(key -> value) {
        f
      }
    }
  }

  testOverride("simple plan") { case (key, value) =>
    ValidateConfOverrideRule.withConfValidationEnabled(key, value) {
      spark.sql("SELECT * FROM TaBlE")
    }
  }

  testOverride("CTE") { case (key, value) =>
    ValidateConfOverrideRule.withConfValidationEnabled(key, value) {
      spark.sql(
        """WITH cte AS (SELECT * FROM TaBlE)
          |SELECT * FROM cte
          |""".stripMargin)
    }
  }

  testOverride("Subquery") { case (key, value) =>
    ValidateConfOverrideRule.withConfValidationEnabled(key, value) {
      spark.sql(
        """
          |SELECT * FROM TaBlE WHERE a in (SELECT a FROM table2)
          |""".stripMargin)
    }
  }

  testOverride("View") { case (key, value) =>
    withTable("test_table", "test_table2") {
      spark.sql("CREATE TABLE test_table AS SELECT id as a FROM range(10)")
      spark.sql("CREATE TABLE test_table2 AS SELECT id as a, (id + 1) as b FROM range(10)")
      withView("test_view") {
        spark.sql("CREATE VIEW test_view AS " +
          "SELECT * FROM test_table WHERE a in (SELECT a FROM test_table2)")

        ValidateConfOverrideRule.withConfValidationEnabled(key, value) {
          spark.sql("SELECT * FROM test_view")
        }
      }
    }
  }

  testOverride("user defined SQL functions") { case (key, value) =>
    withTable("test_table", "test_table2") {
      spark.sql("CREATE TABLE test_table AS SELECT id as a FROM range(10)")
      spark.sql("CREATE TABLE test_table2 AS SELECT id as a, (id + 1) as b FROM range(10)")
      withUserDefinedFunction("f1" -> true, "f2" -> false, "f3" -> false) {
        spark.sql(
          """CREATE OR REPLACE TEMPORARY FUNCTION f1() RETURNS TABLE (a bigint)
            |RETURN SELECT * FROM test_table WHERE a in (SELECT a FROM test_table2)
            |""".stripMargin
        )
        spark.sql(
          """CREATE OR REPLACE FUNCTION f2() RETURNS TABLE (a bigint)
            |RETURN SELECT * FROM test_table WHERE a in (SELECT a FROM test_table2)
            |""".stripMargin
        )
        spark.sql(
          """CREATE OR REPLACE FUNCTION f3(in bigint) RETURNS (out bigint)
            |RETURN in + 1
            |""".stripMargin
        )
        ValidateConfOverrideRule.withConfValidationEnabled(key, value) {
          spark.sql("SELECT * FROM f1()")
        }
        ValidateConfOverrideRule.withConfValidationEnabled(key, value) {
          spark.sql("SELECT * FROM f2()")
        }
        ValidateConfOverrideRule.withConfValidationEnabled(key, value) {
          spark.sql("SELECT f3(1)")
        }
      }
    }
  }

  testOverride("user defined SQL functions - test conf disabled") { case (key, value) =>
    withTable("test_table", "test_table2") {
      spark.sql("CREATE TABLE test_table AS SELECT id as a FROM range(10)")
      spark.sql("CREATE TABLE test_table2 AS SELECT id as a, (id + 1) as b FROM range(10)")
      // turn the flag off to maintain former behavior
      withSQLConf("spark.sql.analyzer.sqlFunctionResolution.applyConfOverrides" -> "false") {
        withUserDefinedFunction("f1" -> true, "f2" -> false, "f3" -> false) {
          spark.sql(
            """CREATE OR REPLACE TEMPORARY FUNCTION f1() RETURNS TABLE (a bigint)
              |RETURN SELECT * FROM test_table WHERE a in (SELECT a FROM test_table2)
              |""".stripMargin
          )
          spark.sql(
            """CREATE OR REPLACE FUNCTION f2() RETURNS TABLE (a bigint)
              |RETURN SELECT * FROM test_table WHERE a in (SELECT a FROM test_table2)
              |""".stripMargin
          )
          spark.sql(
            """CREATE OR REPLACE FUNCTION f3(in bigint) RETURNS (out bigint)
              |RETURN in + 1
              |""".stripMargin
          )
          intercept[AssertionError] {
            ValidateConfOverrideRule.withConfValidationEnabled(key, value) {
              spark.sql("SELECT * FROM f1()")
            }
          }
          intercept[AssertionError] {
            ValidateConfOverrideRule.withConfValidationEnabled(key, value) {
              spark.sql("SELECT * FROM f2()")
            }
          }
          intercept[AssertionError] {
            ValidateConfOverrideRule.withConfValidationEnabled(key, value) {
              spark.sql("SELECT f3(1)")
            }
          }
        }
      }
    }
  }
}

/** Utility singleton object to orchestrate the test. */
object ValidateConfOverrideRule {
  private var confToCheck: Option[(String, String)] = None
  private var isCalled: Boolean = false

  def withConfValidationEnabled(key: String, value: String)(f: => Unit): Unit = {
    try {
      confToCheck = Some(key -> value)
      f
      assert(isCalled, "The rule was enabled, but not called. This is a test setup error.")
    } finally {
      isCalled = false
      confToCheck = None
    }
  }
}

class ValidateConfOverrideRule extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    ValidateConfOverrideRule.confToCheck.foreach { case (k, v) =>
      assert(conf.getConfString(k) == v,
        s"The feature wasn't enabled within plan:\n$plan")
      ValidateConfOverrideRule.isCalled = true
    }
    plan
  }
}

class ConfOverrideValidationExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectResolutionRule(_ => new ValidateConfOverrideRule)
  }
}
