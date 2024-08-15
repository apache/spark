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

package org.apache.spark.sql.execution.command.v2

import java.util.Locale

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.internal.SQLConf

/**
 * The class contains tests for the `SHOW COLUMNS ...` command to check V2 table catalogs.
 */
class ShowColumnsSuite extends command.ShowColumnsSuiteBase with CommandSuiteBase {

  test("the namespace of the table conflicts with the specified namespace") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t(col1 int, col2 string) $defaultUsing")

      val sqlText1 = s"SHOW COLUMNS IN $t IN ns1"
      val sqlText2 = s"SHOW COLUMNS IN $t FROM ${"ns".toUpperCase(Locale.ROOT)}"
      val expected = Seq(Row("col1"), Row("col2"))

      Seq(true, false).foreach { noCheck =>
        withSQLConf(SQLConf.LEGACY_SHOW_COLUMNS_NO_CHECK.key -> noCheck.toString) {
          if (noCheck) {
            checkAnswer(sql(sqlText1), expected)
            checkAnswer(sql(sqlText2), expected)
          } else {
            checkError(
              exception = intercept[AnalysisException] {
                sql(sqlText1)
              },
              errorClass = "SHOW_COLUMNS_WITH_CONFLICT_NAMESPACE",
              parameters = Map(
                "namespaceA" -> s"`ns1`",
                "namespaceB" -> s"`ns`"
              )
            )
            // When case sensitivity is true, the user supplied namespace name in table identifier
            // should match the supplied namespace name in case-sensitive way.
            withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
              checkError(
                exception = intercept[AnalysisException] {
                  sql(sqlText2)
                },
                errorClass = "SHOW_COLUMNS_WITH_CONFLICT_NAMESPACE",
                parameters = Map(
                  "namespaceA" -> s"`${"ns".toUpperCase(Locale.ROOT)}`",
                  "namespaceB" -> "`ns`"
                )
              )
            }
          }
        }
      }
    }
  }
}
