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
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.SetTableSerDeProperties
import org.apache.spark.sql.test.SharedSparkSession

class AlterTableSetSerdeParserSuite extends AnalysisTest with SharedSparkSession {

  test("alter table - SerDe property values must be set") {
    val sql = "ALTER TABLE my_tab SET SERDE 'serde' " +
      "WITH SERDEPROPERTIES('key_without_value', 'key_with_value'='x')"
    val errMsg = intercept[ParseException] {
      parsePlan(sql)
    }.getMessage
    assert(errMsg.contains("Operation not allowed"))
    assert(errMsg.contains("key_without_value"))
  }

  test("alter table: SerDe properties") {
    val sql1 = "ALTER TABLE table_name SET SERDE 'org.apache.class'"
    val hint = Some("Please use ALTER VIEW instead.")
    val parsed1 = parsePlan(sql1)
    val expected1 = SetTableSerDeProperties(
      UnresolvedTable(Seq("table_name"), "ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]", hint),
      Some("org.apache.class"),
      None,
      None)
    comparePlans(parsed1, expected1)

    val sql2 =
      """
        |ALTER TABLE table_name SET SERDE 'org.apache.class'
        |WITH SERDEPROPERTIES ('columns'='foo,bar', 'field.delim' = ',')
      """.stripMargin
    val parsed2 = parsePlan(sql2)
    val expected2 = SetTableSerDeProperties(
      UnresolvedTable(Seq("table_name"), "ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]", hint),
      Some("org.apache.class"),
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      None)
    comparePlans(parsed2, expected2)

    val sql3 =
      """
        |ALTER TABLE table_name
        |SET SERDEPROPERTIES ('columns'='foo,bar', 'field.delim' = ',')
      """.stripMargin
    val parsed3 = parsePlan(sql3)
    val expected3 = SetTableSerDeProperties(
      UnresolvedTable(Seq("table_name"), "ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]", hint),
      None,
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      None)
    comparePlans(parsed3, expected3)

    val sql4 =
      """
        |ALTER TABLE table_name PARTITION (test=1, dt='2008-08-08', country='us')
        |SET SERDE 'org.apache.class'
        |WITH SERDEPROPERTIES ('columns'='foo,bar', 'field.delim' = ',')
      """.stripMargin
    val parsed4 = parsePlan(sql4)
    val expected4 = SetTableSerDeProperties(
      UnresolvedTable(Seq("table_name"), "ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]", hint),
      Some("org.apache.class"),
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      Some(Map("test" -> "1", "dt" -> "2008-08-08", "country" -> "us")))
    comparePlans(parsed4, expected4)

    val sql5 =
      """
        |ALTER TABLE table_name PARTITION (test=1, dt='2008-08-08', country='us')
        |SET SERDEPROPERTIES ('columns'='foo,bar', 'field.delim' = ',')
      """.stripMargin
    val parsed5 = parsePlan(sql5)
    val expected5 = SetTableSerDeProperties(
      UnresolvedTable(Seq("table_name"), "ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]", hint),
      None,
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      Some(Map("test" -> "1", "dt" -> "2008-08-08", "country" -> "us")))
    comparePlans(parsed5, expected5)

    val sql6 =
      """
        |ALTER TABLE a.b.c SET SERDE 'org.apache.class'
        |WITH SERDEPROPERTIES ('columns'='foo,bar', 'field.delim' = ',')
      """.stripMargin
    val parsed6 = parsePlan(sql6)
    val expected6 = SetTableSerDeProperties(
      UnresolvedTable(Seq("a", "b", "c"), "ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]", hint),
      Some("org.apache.class"),
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      None)
    comparePlans(parsed6, expected6)

    val sql7 =
      """
        |ALTER TABLE a.b.c PARTITION (test=1, dt='2008-08-08', country='us')
        |SET SERDEPROPERTIES ('columns'='foo,bar', 'field.delim' = ',')
      """.stripMargin
    val parsed7 = parsePlan(sql7)
    val expected7 = SetTableSerDeProperties(
      UnresolvedTable(Seq("a", "b", "c"), "ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]", hint),
      None,
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      Some(Map("test" -> "1", "dt" -> "2008-08-08", "country" -> "us")))
    comparePlans(parsed7, expected7)
  }
}
