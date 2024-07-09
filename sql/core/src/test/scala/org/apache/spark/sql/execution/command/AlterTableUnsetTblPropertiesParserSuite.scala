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
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedTable}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.UnsetTableProperties
import org.apache.spark.sql.test.SharedSparkSession

class AlterTableUnsetTblPropertiesParserSuite extends AnalysisTest with SharedSparkSession {

  private def parseException(sqlText: String): SparkThrowable = {
    intercept[ParseException](sql(sqlText).collect())
  }

  // ALTER TABLE table_name UNSET TBLPROPERTIES [IF EXISTS] ('comment', 'key');
  test("alter table: alter table properties") {
    val sql1 = "ALTER TABLE table_name UNSET TBLPROPERTIES ('comment', 'test')"
    val sql2 = "ALTER TABLE table_name UNSET TBLPROPERTIES IF EXISTS ('comment', 'test')"

    comparePlans(
      parsePlan(sql1),
      UnsetTableProperties(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... UNSET TBLPROPERTIES",
          suggestAlternative = true),
        Seq("comment", "test"),
        ifExists = false))
    comparePlans(
      parsePlan(sql2),
      UnsetTableProperties(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... UNSET TBLPROPERTIES",
          suggestAlternative = true),
        Seq("comment", "test"),
        ifExists = true))
  }

  test("alter table unset properties - property values must NOT be set") {
    val sql = "ALTER TABLE my_tab UNSET TBLPROPERTIES('key_without_value', 'key_with_value'='x')"
    checkError(
      exception = parseException(sql),
      errorClass = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> "Values should not be specified for key(s): [key_with_value]"),
      context = ExpectedContext(
        fragment = sql,
        start = 0,
        stop = 80))
  }
}
