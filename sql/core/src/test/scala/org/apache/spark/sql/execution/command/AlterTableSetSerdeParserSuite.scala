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

import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedTable}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.plans.logical.SetTableSerDeProperties
import org.apache.spark.sql.test.SharedSparkSession

class AlterTableSetSerdeParserSuite extends AnalysisTest with SharedSparkSession {

  private val HINT = Some("Please use ALTER VIEW instead.")

  test("SerDe property values must be set") {
    val sql = "ALTER TABLE table_name SET SERDE 'serde' " +
      "WITH SERDEPROPERTIES('key_without_value', 'key_with_value'='x')"
    checkError(
      exception = parseException(parsePlan)(sql),
      errorClass = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> "Values must be specified for key(s): [key_without_value]"),
      context = ExpectedContext(
        fragment = sql,
        start = 0,
        stop = 103))
  }

  test("alter table SerDe properties by 'SET SERDE'") {
    val sql = "ALTER TABLE table_name SET SERDE 'org.apache.class'"
    val parsed = parsePlan(sql)
    val expected = SetTableSerDeProperties(
      UnresolvedTable(Seq("table_name"), "ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]", HINT),
      Some("org.apache.class"),
      None,
      None)
    comparePlans(parsed, expected)
  }

  test("alter table SerDe properties by 'SET SERDE ... WITH SERDEPROPERTIES'") {
    val sql =
      """
        |ALTER TABLE table_name SET SERDE 'org.apache.class'
        |WITH SERDEPROPERTIES ('columns'='foo,bar', 'field.delim' = ',')
      """.stripMargin
    val parsed = parsePlan(sql)
    val expected = SetTableSerDeProperties(
      UnresolvedTable(Seq("table_name"), "ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]", HINT),
      Some("org.apache.class"),
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      None)
    comparePlans(parsed, expected)
  }

  test("alter table SerDe properties by 'SET SERDEPROPERTIES'") {
    val sql =
      """
        |ALTER TABLE table_name
        |SET SERDEPROPERTIES ('columns'='foo,bar', 'field.delim' = ',')
      """.stripMargin
    val parsed = parsePlan(sql)
    val expected = SetTableSerDeProperties(
      UnresolvedTable(Seq("table_name"), "ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]", HINT),
      None,
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      None)
    comparePlans(parsed, expected)
  }

  test("alter parition SerDe properties by 'SET SERDE ... WITH SERDEPROPERTIES'") {
    val sql =
      """
        |ALTER TABLE table_name PARTITION (test=1, dt='2008-08-08', country='us')
        |SET SERDE 'org.apache.class'
        |WITH SERDEPROPERTIES ('columns'='foo,bar', 'field.delim' = ',')
      """.stripMargin
    val parsed = parsePlan(sql)
    val expected = SetTableSerDeProperties(
      UnresolvedTable(Seq("table_name"), "ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]", HINT),
      Some("org.apache.class"),
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      Some(Map("test" -> "1", "dt" -> "2008-08-08", "country" -> "us")))
    comparePlans(parsed, expected)
  }

  test("alter parition SerDe properties by 'SET SERDEPROPERTIES'") {
    val sql =
      """
        |ALTER TABLE table_name PARTITION (test=1, dt='2008-08-08', country='us')
        |SET SERDEPROPERTIES ('columns'='foo,bar', 'field.delim' = ',')
      """.stripMargin
    val parsed = parsePlan(sql)
    val expected = SetTableSerDeProperties(
      UnresolvedTable(Seq("table_name"), "ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]", HINT),
      None,
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      Some(Map("test" -> "1", "dt" -> "2008-08-08", "country" -> "us")))
    comparePlans(parsed, expected)
  }

  test("table with multi-part identifier: " +
    "alter table SerDe properties by 'SET SERDE ... WITH SERDEPROPERTIES'") {
    val sql =
      """
        |ALTER TABLE a.b.c SET SERDE 'org.apache.class'
        |WITH SERDEPROPERTIES ('columns'='foo,bar', 'field.delim' = ',')
      """.stripMargin
    val parsed = parsePlan(sql)
    val expected = SetTableSerDeProperties(
      UnresolvedTable(Seq("a", "b", "c"), "ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]", HINT),
      Some("org.apache.class"),
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      None)
    comparePlans(parsed, expected)
  }

  test("table with multi-part identifier: " +
    "alter parition SerDe properties by 'SET SERDE ... WITH SERDEPROPERTIES'") {
    val sql =
      """
        |ALTER TABLE a.b.c PARTITION (test=1, dt='2008-08-08', country='us')
        |SET SERDEPROPERTIES ('columns'='foo,bar', 'field.delim' = ',')
      """.stripMargin
    val parsed = parsePlan(sql)
    val expected = SetTableSerDeProperties(
      UnresolvedTable(Seq("a", "b", "c"), "ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]", HINT),
      None,
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      Some(Map("test" -> "1", "dt" -> "2008-08-08", "country" -> "us")))
    comparePlans(parsed, expected)
  }
}
