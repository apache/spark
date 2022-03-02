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

package org.apache.spark.sql.execution.command.v1

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.command

/**
 * This base suite contains unified tests for the `SHOW TBLPROPERTIES` command that checks V1
 * table catalogs. The tests that cannot run for all V1 catalogs are located in more
 * specific test suites:
 *
 *   - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.ShowTblPropertiesSuite`
 *   - V1 Hive External catalog:
 *     `org.apache.spark.sql.hive.execution.command.ShowTblPropertiesSuite`
 */
trait ShowTblPropertiesSuiteBase extends command.ShowTblPropertiesSuiteBase
    with command.TestsV1AndV2Commands {

  test("SHOW TBLPROPERTIES FOR VIEW") {
    val v = "testview"
    withView(v) {
      spark.sql(s"CREATE VIEW $v TBLPROPERTIES('p1'='v1', 'p2'='v2') AS SELECT 1 AS c1")
      checkAnswer(sql(s"SHOW TBLPROPERTIES $v").filter("key != 'transient_lastDdlTime'"),
        Seq(Row("p1", "v1"), Row("p2", "v2")))
      checkAnswer(sql(s"SHOW TBLPROPERTIES $v('p1')"), Row("p1", "v1"))
      checkAnswer(sql(s"SHOW TBLPROPERTIES $v('p3')"),
        Row("p3", s"Table default.$v does not have property: p3"))
    }
  }

  test("SHOW TBLPROPERTIES FOR TEMPORARY VIEW") {
    val v = "testview"
    withView(v) {
      spark.sql(s"CREATE TEMPORARY VIEW $v AS SELECT 1 AS c1;")
      checkAnswer(sql(s"SHOW TBLPROPERTIES $v"), Seq.empty)
    }
  }
}

/**
 * The class contains tests for the `SHOW TBLPROPERTIES` command to check V1 In-Memory
 * table catalog.
 */
class ShowTblPropertiesSuite extends ShowTblPropertiesSuiteBase with CommandSuiteBase {
  override def commandVersion: String = super[ShowTblPropertiesSuiteBase].commandVersion
}
