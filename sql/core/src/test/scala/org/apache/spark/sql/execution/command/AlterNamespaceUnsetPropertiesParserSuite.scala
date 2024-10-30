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
package org.apache.spark.sql.execution.command

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedNamespace}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.test.SharedSparkSession

class AlterNamespaceUnsetPropertiesParserSuite extends AnalysisTest with SharedSparkSession {

  private lazy val parser = new SparkSqlParser()

  private def parseException(sqlText: String): SparkThrowable = {
    intercept[ParseException](sql(sqlText).collect())
  }

  private def parsePlan(sqlText: String): LogicalPlan = {
    parser.parsePlan(sqlText)
  }

  test("unset namespace properties") {
    Seq("DATABASE", "SCHEMA", "NAMESPACE").foreach { nsToken =>
      Seq("PROPERTIES", "DBPROPERTIES").foreach { propToken =>
        comparePlans(
          parsePlan(s"ALTER $nsToken a.b.c UNSET $propToken ('a', 'b', 'c')"),
          UnsetNamespacePropertiesCommand(
            UnresolvedNamespace(Seq("a", "b", "c")), Seq("a", "b", "c")))

        comparePlans(
          parsePlan(s"ALTER $nsToken a.b.c UNSET $propToken ('a')"),
          UnsetNamespacePropertiesCommand(UnresolvedNamespace(Seq("a", "b", "c")), Seq("a")))
      }
    }
  }

  test("property values must not be set") {
    val sql = "ALTER NAMESPACE my_db UNSET PROPERTIES('key_without_value', 'key_with_value'='x')"
    checkError(
      exception = parseException(sql),
      condition = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> "Values should not be specified for key(s): [key_with_value]"),
      context = ExpectedContext(
        fragment = sql,
        start = 0,
        stop = 80))
  }

  test("not support clause - IF EXISTS") {
    Seq("DATABASE", "SCHEMA", "NAMESPACE").foreach { nsToken =>
      Seq("PROPERTIES", "DBPROPERTIES").foreach { propToken =>
        val sql = s"ALTER $nsToken a.b.c UNSET $propToken IF EXISTS ('a', 'b', 'c')"
        checkError(
          exception = parseException(sql),
          condition = "PARSE_SYNTAX_ERROR",
          parameters = Map("error" -> "'IF'", "hint" -> ": missing '('")
        )
      }
    }
  }
}
