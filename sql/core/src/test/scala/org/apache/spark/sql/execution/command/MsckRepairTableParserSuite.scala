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
import org.apache.spark.sql.catalyst.plans.logical.RepairTable

class MsckRepairTableParserSuite extends AnalysisTest {
  test("repair a table") {
    comparePlans(
      parsePlan("MSCK REPAIR TABLE a.b.c"),
      RepairTable(
        UnresolvedTable(Seq("a", "b", "c"), "MSCK REPAIR TABLE"),
        enableAddPartitions = true,
        enableDropPartitions = false))
  }

  test("repair a table without MSCK keyword") {
    comparePlans(
      parsePlan("REPAIR TABLE a.b.c"),
      RepairTable(
        UnresolvedTable(Seq("a", "b", "c"), "MSCK REPAIR TABLE"),
        enableAddPartitions = true,
        enableDropPartitions = false))
  }

  test("add partitions") {
    comparePlans(
      parsePlan("msck repair table ns.tbl add partitions"),
      RepairTable(
        UnresolvedTable(
          Seq("ns", "tbl"),
          "MSCK REPAIR TABLE ... ADD PARTITIONS"),
        enableAddPartitions = true,
        enableDropPartitions = false))
  }

  test("drop partitions") {
    comparePlans(
      parsePlan("MSCK repair table TBL Drop Partitions"),
      RepairTable(
        UnresolvedTable(
          Seq("TBL"),
          "MSCK REPAIR TABLE ... DROP PARTITIONS"),
        enableAddPartitions = false,
        enableDropPartitions = true))
  }

  test("sync partitions") {
    comparePlans(
      parsePlan("MSCK REPAIR TABLE spark_catalog.ns.tbl SYNC PARTITIONS"),
      RepairTable(
        UnresolvedTable(
          Seq("spark_catalog", "ns", "tbl"),
          "MSCK REPAIR TABLE ... SYNC PARTITIONS"),
        enableAddPartitions = true,
        enableDropPartitions = true))
  }
}
