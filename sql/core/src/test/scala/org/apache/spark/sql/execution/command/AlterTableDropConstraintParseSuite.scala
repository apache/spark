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
import org.apache.spark.sql.catalyst.plans.logical.DropConstraint
import org.apache.spark.sql.test.SharedSparkSession

class AlterTableDropConstraintParseSuite extends AnalysisTest with SharedSparkSession {

  test("Drop constraint") {
    val sql = "ALTER TABLE a.b.c DROP CONSTRAINT c1"
    val parsed = parsePlan(sql)
    val expected = DropConstraint(
      UnresolvedTable(
        Seq("a", "b", "c"),
        "ALTER TABLE ... DROP CONSTRAINT"),
      "c1",
      ifExists = false,
      cascade = false)
    comparePlans(parsed, expected)
  }

  test("Drop constraint if exists") {
    val sql = "ALTER TABLE a.b.c DROP CONSTRAINT IF EXISTS c1"
    val parsed = parsePlan(sql)
    val expected = DropConstraint(
      UnresolvedTable(
        Seq("a", "b", "c"),
        "ALTER TABLE ... DROP CONSTRAINT"),
      "c1",
      ifExists = true,
      cascade = false)
    comparePlans(parsed, expected)
  }

  test("Drop constraint cascade") {
    val sql = "ALTER TABLE a.b.c DROP CONSTRAINT c1 CASCADE"
    val parsed = parsePlan(sql)
    val expected = DropConstraint(
      UnresolvedTable(
        Seq("a", "b", "c"),
        "ALTER TABLE ... DROP CONSTRAINT"),
      "c1",
      ifExists = false,
      cascade = true)
    comparePlans(parsed, expected)
  }

  test("Drop constraint restrict") {
    val sql = "ALTER TABLE a.b.c DROP CONSTRAINT c1 RESTRICT"
    val parsed = parsePlan(sql)
    val expected = DropConstraint(
      UnresolvedTable(
        Seq("a", "b", "c"),
        "ALTER TABLE ... DROP CONSTRAINT"),
      "c1",
      ifExists = false,
      cascade = false)
    comparePlans(parsed, expected)
  }

  test("Drop constraint with invalid name") {
    val sql = "ALTER TABLE a.b.c DROP CONSTRAINT c1-c3 ENFORCE"
    val msg = intercept[ParseException] {
      parsePlan(sql)
    }.getMessage
    assert(msg.contains("Syntax error at or near '-'"))
  }

  test("Drop constraint with invalid mode") {
    val sql = "ALTER TABLE a.b.c DROP CONSTRAINT c1 ENFORCE"
    val msg = intercept[ParseException] {
      parsePlan(sql)
    }.getMessage
    assert(msg.contains("Syntax error at or near 'ENFORCE': extra input 'ENFORCE'."))
  }
}
