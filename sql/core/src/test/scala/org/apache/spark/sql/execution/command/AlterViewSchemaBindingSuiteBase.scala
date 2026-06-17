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

import org.apache.spark.sql.QueryTest

/**
 * Unified tests for `ALTER VIEW ... WITH SCHEMA` against V1 (session) and V2 view catalogs.
 */
trait AlterViewSchemaBindingSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command: String = "ALTER VIEW ... WITH SCHEMA"

  protected def namespace: String = "default"

  protected def createView(view: String): Unit = {
    sql(s"CREATE VIEW $view AS SELECT 1 AS x")
  }

  protected def schemaModeOf(view: String): String = {
    val rows = sql(s"SHOW CREATE TABLE $view").collect()
    val ddl = rows.head.getString(0)
    // Extract the WITH SCHEMA <mode> clause if present.
    val pattern = """WITH SCHEMA\s+(BINDING|COMPENSATION|EVOLUTION|TYPE EVOLUTION)""".r
    pattern.findFirstMatchIn(ddl).map(_.group(1)).getOrElse("BINDING")
  }

  test("set EVOLUTION") {
    val view = s"$catalog.$namespace.v_schema_evolve"
    createView(view)
    sql(s"ALTER VIEW $view WITH SCHEMA EVOLUTION")
    assert(schemaModeOf(view) == "EVOLUTION")
  }

  test("set COMPENSATION") {
    val view = s"$catalog.$namespace.v_schema_compensate"
    createView(view)
    sql(s"ALTER VIEW $view WITH SCHEMA COMPENSATION")
    assert(schemaModeOf(view) == "COMPENSATION")
  }

  test("set BINDING (default)") {
    val view = s"$catalog.$namespace.v_schema_binding"
    createView(view)
    sql(s"ALTER VIEW $view WITH SCHEMA EVOLUTION")
    sql(s"ALTER VIEW $view WITH SCHEMA BINDING")
    assert(schemaModeOf(view) == "BINDING")
  }

  test("WITH SCHEMA does not change the view body") {
    val view = s"$catalog.$namespace.v_schema_body_intact"
    sql(s"CREATE VIEW $view AS SELECT 7 AS x")
    sql(s"ALTER VIEW $view WITH SCHEMA EVOLUTION")
    checkAnswer(sql(s"SELECT * FROM $view"), org.apache.spark.sql.Row(7))
  }
}
