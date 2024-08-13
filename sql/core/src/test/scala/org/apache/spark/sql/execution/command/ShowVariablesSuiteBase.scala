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

import org.apache.spark.sql.{QueryTest, Row}

/**
 * This base suite contains unified tests for the `SHOW VARIABLES` command that check V1 and V2
 * table catalogs. The tests that cannot run for all supported catalogs are located in more
 * specific test suites:
 *
 *   - V2 table catalog tests: `org.apache.spark.sql.execution.command.v2.ShowVariablesSuite`
 *   - V1 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v1.ShowVariablesSuiteBase`
 *     - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.ShowVariablesSuite`
 *     - V1 Hive External catalog:
 *        `org.apache.spark.sql.hive.execution.command.ShowVariablesSuite`
 */
trait ShowVariablesSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "SHOW VARIABLES LIKE ..."

  test("SHOW VARIABLE BASIC") {
    withVariable("name1", "name2", "name_3") {
      sql("DECLARE VARIABLE name1 INT DEFAULT 1")
      sql("SET VARIABLE name1=2")
      checkAnswer(sql("SHOW VARIABLES"), Seq(Row("name1", "INT", "1", "2")))

      sql("DECLARE VARIABLE name2 = 'spark'")
      sql("SET VARIABLE name2='baidu'")
      checkAnswer(sql("SHOW VARIABLES"),
        Seq(Row("name1", "INT", "1", "2"), Row("name2", "STRING", "'spark'", "baidu")))

      sql("DROP TEMPORARY VARIABLE IF EXISTS name2")
      checkAnswer(sql("SHOW VARIABLES"), Seq(Row("name1", "INT", "1", "2")))

      sql("DECLARE VARIABLE name2 STRING")
      sql("SET VARIABLE name2='databricks'")
      checkAnswer(sql("SHOW VARIABLES"),
        Seq(Row("name1", "INT", "1", "2"), Row("name2", "STRING", "null", "databricks")))

      sql("DECLARE VARIABLE name_3 STRING")
      sql("SET VARIABLE name_3='spark'")
      checkAnswer(sql("SHOW VARIABLES"),
        Seq(
          Row("name1", "INT", "1", "2"),
          Row("name2", "STRING", "null", "databricks"),
          Row("name_3", "STRING", "null", "spark")))

      checkAnswer(sql("SHOW VARIABLES LIKE 'name_*'"),
        Seq(Row("name_3", "STRING", "null", "spark")))
    }
  }
}
