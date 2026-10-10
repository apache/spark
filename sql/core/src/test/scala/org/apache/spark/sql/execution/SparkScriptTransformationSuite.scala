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

  test("SPARK-59683: TRANSFORM view keeps bound CHAR/VARCHAR mode") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))
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
          val exception = intercept[Exception] {
            sql("SELECT * FROM v_overflow").collect()
          }
          val runtimeException = exception match {
            case s: org.apache.spark.SparkRuntimeException => s
            case other =>
              other.getCause.asInstanceOf[org.apache.spark.SparkRuntimeException]
          }
          checkError(
            exception = runtimeException,
            condition = "EXCEED_LIMIT_LENGTH",
            parameters = Map("limit" -> "4"))
        }
      }
    }
    withSQLConf(
        SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> "true",
        SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "false") {
      withView("v_disabled") {
        sql(
          """CREATE VIEW v_disabled AS
            |SELECT TRANSFORM('ab') USING 'cat' AS (c CHAR(4))
            |FROM VALUES (1) input(dummy)""".stripMargin)
        withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
          val exception = intercept[Exception] {
            sql("SELECT * FROM v_disabled").collect()
          }
          checkTransformWithoutSerdeUnsupportedType(exception, "\"CHAR(4)\"")
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
