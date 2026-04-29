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

import org.apache.spark.sql.{AnalysisException, QueryTest}

/**
 * Unified tests for `DROP VIEW` against V1 (session) and V2 view catalogs.
 */
trait DropViewSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command: String = "DROP VIEW"

  protected def namespace: String = "default"

  protected def viewExists(qualified: String): Boolean = {
    val parts = qualified.split('.').toSeq
    val nsAndView = parts.tail
    val ns = nsAndView.init.mkString(".")
    val name = nsAndView.last
    sql(s"SHOW VIEWS IN $catalog.$ns").collect().exists(_.getString(1) == name)
  }

  test("drop existing view") {
    val view = s"$catalog.$namespace.v_drop_basic"
    sql(s"CREATE VIEW $view AS SELECT 1 AS x")
    assert(viewExists(view))
    sql(s"DROP VIEW $view")
    assert(!viewExists(view))
  }

  test("drop missing view fails without IF EXISTS") {
    val view = s"$catalog.$namespace.v_drop_missing"
    intercept[AnalysisException] {
      sql(s"DROP VIEW $view")
    }
  }

  test("drop with IF EXISTS is a no-op when missing") {
    sql(s"DROP VIEW IF EXISTS $catalog.$namespace.v_drop_never_existed")
  }
}
