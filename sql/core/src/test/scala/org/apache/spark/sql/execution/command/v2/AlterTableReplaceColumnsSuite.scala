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

package org.apache.spark.sql.execution.command.v2

import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.execution.command

/**
 * The class contains tests for the `ALTER TABLE .. REPLACE COLUMNS` command
 * to check V2 table catalogs.
 */
class AlterTableReplaceColumnsSuite
  extends command.AlterTableReplaceColumnsSuiteBase
  with CommandSuiteBase {

  test("UNSUPPORTED_DEFAULT_VALUE.WITHOUT_SUGGESTION: " +
    "Support for DEFAULT column values is not implemented yet") {
    val sql1 = "ALTER TABLE t1 REPLACE COLUMNS (ym INT default 1)"
    checkError(
      exception = intercept[ParseException] {
        sql(sql1)
      },
      condition = "UNSUPPORTED_DEFAULT_VALUE.WITHOUT_SUGGESTION",
      parameters = Map.empty,
      context = ExpectedContext(sql1, 0, 48)
    )
  }
}
