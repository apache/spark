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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

class HiveDDLSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {

  test("drop tables") {
    withTable("tab1") {
      sql("CREATE TABLE tab1(c1 int)")
      sql("DROP TABLE tab1")
      sql("DROP TABLE IF EXISTS tab1")
      sql("DROP VIEW IF EXISTS tab1")
    }
  }

  test("drop views") {
    withTable("tab1") {
      sqlContext.range(10).write.saveAsTable("tab1")
      withView("view1") {
        sql("CREATE VIEW view1 AS SELECT * FROM tab1")
        sql("DROP VIEW view1")
        sql("DROP VIEW IF EXISTS view1")
      }
    }
  }

  test("drop table using drop view") {
    withTable("tab1") {
      sql("CREATE TABLE tab1(c1 int)")
      val message = intercept[AnalysisException] {
        sql("DROP VIEW tab1")
      }.getMessage
      assert(message.contains("Cannot drop a table with DROP VIEW"))
    }
  }

  test("drop view using drop table") {
    withTable("tab1") {
      sqlContext.range(10).write.saveAsTable("tab1")
      withView("view1") {
        sql("CREATE VIEW view1 AS SELECT * FROM tab1")
        val message = intercept[AnalysisException] {
          sql("DROP TABLE view1")
        }.getMessage
        assert(message.contains("Cannot drop a view with DROP TABLE"))
      }
    }
  }
}
