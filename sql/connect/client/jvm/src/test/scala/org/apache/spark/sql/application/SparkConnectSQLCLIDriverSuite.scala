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
package org.apache.spark.sql.application

import org.apache.spark.sql.Row
import org.apache.spark.sql.connect.test.ConnectFunSuite

/**
 * Unit tests for the pure argument/statement/output helpers of [[SparkConnectSQLCLIDriver]].
 * These do not require a live Spark Connect server.
 */
class SparkConnectSQLCLIDriverSuite extends ConnectFunSuite {

  test("SPARK-58227: parseArgs separates -e statements from Connect client args") {
    val cli = SparkConnectSQLCLIDriver.parseArgs(
      Array("--remote", "sc://localhost:15002", "-e", "SELECT 1"))
    assert(cli.statements == Seq("SELECT 1"))
    assert(cli.files.isEmpty)
    assert(cli.clientArgs.toSeq == Seq("--remote", "sc://localhost:15002"))
  }

  test("SPARK-58227: parseArgs collects multiple -e and -f in order") {
    val cli =
      SparkConnectSQLCLIDriver.parseArgs(Array("-e", "SELECT 1", "-f", "a.sql", "-e", "SELECT 2"))
    assert(cli.statements == Seq("SELECT 1", "SELECT 2"))
    assert(cli.files == Seq("a.sql"))
    assert(cli.clientArgs.isEmpty)
  }

  test("SPARK-58227: parseArgs forwards non-CLI flags to the Connect client parser") {
    val cli = SparkConnectSQLCLIDriver.parseArgs(Array("--host", "h", "--port", "15002"))
    assert(cli.clientArgs.toSeq == Seq("--host", "h", "--port", "15002"))
    assert(cli.statements.isEmpty)
    assert(cli.files.isEmpty)
  }

  test("SPARK-58227: splitStatements splits on ';' and drops blanks") {
    assert(
      SparkConnectSQLCLIDriver.splitStatements(" SELECT 1 ;\n SELECT 2 ;; ") ==
        Seq("SELECT 1", "SELECT 2"))
    assert(SparkConnectSQLCLIDriver.splitStatements("   ").isEmpty)
  }

  test("SPARK-58227: formatRow renders tab-separated values with NULL for nulls") {
    assert(SparkConnectSQLCLIDriver.formatRow(Row(1, "abc", null)) == "1\tabc\tNULL")
    assert(SparkConnectSQLCLIDriver.formatRow(Row()) == "")
  }
}
