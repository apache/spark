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
 * Unified tests for `ALTER VIEW ... UNSET TBLPROPERTIES` against V1 (session) and V2 view
 * catalogs.
 */
trait AlterViewUnsetTblPropertiesSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command: String = "ALTER VIEW ... UNSET TBLPROPERTIES"

  protected def namespace: String = "default"

  protected def createViewWithProps(view: String, propPairs: (String, String)*): Unit = {
    sql(s"CREATE VIEW $view AS SELECT 1 AS x")
    if (propPairs.nonEmpty) {
      val props = propPairs.map { case (k, v) => s"'$k' = '$v'" }.mkString(", ")
      sql(s"ALTER VIEW $view SET TBLPROPERTIES ($props)")
    }
  }

  protected def lookupProperty(view: String, key: String): Option[String] = {
    sql(s"SHOW TBLPROPERTIES $view").collect()
      .find(_.getString(0) == key)
      .map(_.getString(1))
  }

  test("unset a single property") {
    val view = s"$catalog.$namespace.v_unset_one"
    createViewWithProps(view, "k" -> "v")
    sql(s"ALTER VIEW $view UNSET TBLPROPERTIES ('k')")
    assert(lookupProperty(view, "k").isEmpty)
  }

  test("unset preserves other properties") {
    val view = s"$catalog.$namespace.v_unset_keeps_others"
    createViewWithProps(view, "k" -> "v", "other" -> "stay")
    sql(s"ALTER VIEW $view UNSET TBLPROPERTIES ('k')")
    assert(lookupProperty(view, "k").isEmpty)
    assert(lookupProperty(view, "other").contains("stay"))
  }

  test("unset multiple keys at once") {
    val view = s"$catalog.$namespace.v_unset_many"
    createViewWithProps(view, "a" -> "1", "b" -> "2", "c" -> "3")
    sql(s"ALTER VIEW $view UNSET TBLPROPERTIES ('a', 'b')")
    assert(lookupProperty(view, "a").isEmpty)
    assert(lookupProperty(view, "b").isEmpty)
    assert(lookupProperty(view, "c").contains("3"))
  }

  test("unset with IF EXISTS on a missing key is a no-op") {
    val view = s"$catalog.$namespace.v_unset_if_exists"
    createViewWithProps(view, "k" -> "v")
    sql(s"ALTER VIEW $view UNSET TBLPROPERTIES IF EXISTS ('not_there')")
    // Existing property remains.
    assert(lookupProperty(view, "k").contains("v"))
  }
}
