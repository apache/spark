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

import org.apache.spark.sql.catalyst.analysis.UnresolvedIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{CreateStreamingTable, TableSpec}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.command.v1.CommandSuiteBase

/**
 * The class contains tests for the `CREATE STREAMING TABLE ... AS ...` command
 */
class CreateStreamingTableParserSuite extends CommandSuiteBase {
  protected lazy val parser = new SparkSqlParser()

  test("CREATE STREAMING TABLE without subquery is parsed correctly") {
    comparePlans(
      parser
        .parsePlan("CREATE STREAMING TABLE st COMMENT 'populate with flow later'"),
      CreateStreamingTable(
        name = UnresolvedIdentifier(
          nameParts = Seq("st")
        ),
        columns = Seq.empty,
        partitioning = Seq.empty,
        tableSpec = TableSpec(
          properties = Map.empty,
          provider = None,
          options = Map.empty,
          location = None,
          comment = Option("populate with flow later"),
          collation = None,
          serde = None,
          external = false,
          constraints = Seq.empty
        ),
        ifNotExists = false
      ),
      checkAnalysis = false
    )
  }
}
