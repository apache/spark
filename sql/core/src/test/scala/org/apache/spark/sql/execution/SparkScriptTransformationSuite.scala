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

import org.apache.spark.TestUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class SparkScriptTransformationSuite extends BaseScriptTransformationSuite with SharedSparkSession {
  import testImplicits._

  override protected def defaultSerDe(): String = "row-format-delimited"

  override def createScriptTransformationExec(
      script: String,
      output: Seq[Attribute],
      child: SparkPlan,
      ioschema: ScriptTransformationIOSchema): BaseScriptTransformationExec = {
    SparkScriptTransformationExec(
      script = script,
      output = output,
      child = child,
      ioschema = ioschema
    )
  }

  // Nested complex TRANSFORM output is JSON without SerDe. Hive sessions default to
  // LazySimpleSerDe, so keep these no-SerDe JSON cases in the Spark suite.
  test("SPARK-60090: TRANSFORM nested CHAR/VARCHAR assignment via Project") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))
    withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
      checkAnswer(
        sql(
          """
            |SELECT TRANSFORM(value)
            |USING 'cat' AS (value ARRAY<CHAR(4)>)
            |FROM VALUES ('["ab"]') t(value)
            |""".stripMargin),
        Row(Seq("ab  ")))
      checkAnswer(
        sql(
          """
            |SELECT TRANSFORM(value)
            |USING 'cat' AS (value ARRAY<VARCHAR(4)>)
            |FROM VALUES ('["xy"]') t(value)
            |""".stripMargin),
        Row(Seq("xy")))
      checkAnswer(
        sql(
          """
            |SELECT TRANSFORM(value)
            |USING 'cat' AS (value STRUCT<value: CHAR(5)>)
            |FROM VALUES ('{"value":"xy"}') t(value)
            |""".stripMargin),
        Row(Row("xy   ")))
      checkAnswer(
        sql(
          """
            |SELECT TRANSFORM(value)
            |USING 'cat' AS (value MAP<STRING, CHAR(4)>)
            |FROM VALUES ('{"k":"ab"}') t(value)
            |""".stripMargin),
        Row(Map("k" -> "ab  ")))
      checkAnswer(
        sql(
          """
            |SELECT TRANSFORM(value)
            |USING 'cat' AS (value MAP<CHAR(4), INT>)
            |FROM VALUES ('{"ab":1}') t(value)
            |""".stripMargin),
        Row(Map("ab  " -> 1)))
      checkAnswer(
        sql(
          """
            |SELECT TRANSFORM(value)
            |USING 'cat' AS (value ARRAY<CHAR(4)>)
            |FROM VALUES ('[null]') t(value)
            |""".stripMargin),
        Row(Seq(null)))
    }
    assert(uncaughtExceptionHandler.exception.isEmpty)
  }

  test("SPARK-60090: TRANSFORM nested CHAR/VARCHAR overflow via Project") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))
    withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
      Seq(
        """
          |SELECT TRANSFORM(value)
          |USING 'cat' AS (value ARRAY<CHAR(4)>)
          |FROM VALUES ('["abcdef"]') t(value)
          |""".stripMargin,
        """
          |SELECT TRANSFORM(value)
          |USING 'cat' AS (value STRUCT<value: VARCHAR(4)>)
          |FROM VALUES ('{"value":"abcdef"}') t(value)
          |""".stripMargin,
        """
          |SELECT TRANSFORM(value)
          |USING 'cat' AS (value MAP<STRING, CHAR(4)>)
          |FROM VALUES ('{"k":"abcdef"}') t(value)
          |""".stripMargin).foreach { query =>
        checkExceedLimitLength(intercept[Exception](sql(query).collect()), "4")
      }
    }
  }

  test("SPARK-60090: TRANSFORM view keeps CHAR/VARCHAR Project assignment") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))
    // The Project with stringLengthCheck is baked into the parsed view plan, so a later
    // session conf change cannot drop pad / EXCEED_LIMIT_LENGTH.
    withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
      withView("v") {
        sql(
          """CREATE VIEW v AS
            |SELECT TRANSFORM('ab') USING 'cat' AS (c CHAR(4))
            |FROM VALUES (1) input(dummy)""".stripMargin)
        withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "false") {
          checkAnswer(sql("SELECT * FROM v"), Row("ab  "))
        }
      }
    }
    withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
      withView("v_overflow") {
        sql(
          """CREATE VIEW v_overflow AS
            |SELECT TRANSFORM('abcdef') USING 'cat' AS (c CHAR(4))
            |FROM VALUES (1) input(dummy)""".stripMargin)
        withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "false") {
          checkExceedLimitLength(
            intercept[Exception](sql("SELECT * FROM v_overflow").collect()),
            "4")
        }
      }
    }
    assert(uncaughtExceptionHandler.exception.isEmpty)
  }

  test("SPARK-32106: TRANSFORM with serde without hive should throw exception") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))
    withTempView("v") {
      val df = Seq("a", "b", "c").map(Tuple1.apply).toDF("a")
      df.createTempView("v")

      val sqlText =
        """SELECT TRANSFORM (a)
          |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
          |USING 'cat' AS (a)
          |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
          |FROM v""".stripMargin
      checkError(
        exception = intercept[ParseException](sql(sqlText)),
        condition = "UNSUPPORTED_FEATURE.TRANSFORM_NON_HIVE",
        parameters = Map.empty,
        context = ExpectedContext(sqlText, 0, 185))
    }
  }
}
