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

import java.io.File

import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.hive.test.TestHive

/**
 * Runs the test cases that are included in the hive distribution with hash joins.
 */
class HashJoinCompatibilitySuite extends HiveCompatibilitySuite {
  override def beforeAll() {
    super.beforeAll()
    TestHive.setConf(SQLConf.SORTMERGE_JOIN, false)
  }

  override def afterAll() {
    TestHive.setConf(SQLConf.SORTMERGE_JOIN, true)
    super.afterAll()
  }

  override def whiteList = Seq(
    "auto_join0",
    "auto_join1",
    "auto_join10",
    "auto_join11",
    "auto_join12",
    "auto_join13",
    "auto_join14",
    "auto_join14_hadoop20",
    "auto_join15",
    "auto_join17",
    "auto_join18",
    "auto_join19",
    "auto_join2",
    "auto_join20",
    "auto_join21",
    "auto_join22",
    "auto_join23",
    "auto_join24",
    "auto_join25",
    "auto_join26",
    "auto_join27",
    "auto_join28",
    "auto_join3",
    "auto_join30",
    "auto_join31",
    "auto_join32",
    "auto_join4",
    "auto_join5",
    "auto_join6",
    "auto_join7",
    "auto_join8",
    "auto_join9",
    "auto_join_filters",
    "auto_join_nulls",
    "auto_join_reordering_values",
    "auto_smb_mapjoin_14",
    "auto_sortmerge_join_1",
    "auto_sortmerge_join_10",
    "auto_sortmerge_join_11",
    "auto_sortmerge_join_12",
    "auto_sortmerge_join_13",
    "auto_sortmerge_join_14",
    "auto_sortmerge_join_15",
    "auto_sortmerge_join_16",
    "auto_sortmerge_join_2",
    "auto_sortmerge_join_3",
    "auto_sortmerge_join_4",
    "auto_sortmerge_join_5",
    "auto_sortmerge_join_6",
    "auto_sortmerge_join_7",
    "auto_sortmerge_join_8",
    "auto_sortmerge_join_9",
    "correlationoptimizer1",
    "correlationoptimizer10",
    "correlationoptimizer11",
    "correlationoptimizer13",
    "correlationoptimizer14",
    "correlationoptimizer15",
    "correlationoptimizer2",
    "correlationoptimizer3",
    "correlationoptimizer4",
    "correlationoptimizer6",
    "correlationoptimizer7",
    "correlationoptimizer8",
    "correlationoptimizer9",
    "join0",
    "join1",
    "join10",
    "join11",
    "join12",
    "join13",
    "join14",
    "join14_hadoop20",
    "join15",
    "join16",
    "join17",
    "join18",
    "join19",
    "join2",
    "join20",
    "join21",
    "join22",
    "join23",
    "join24",
    "join25",
    "join26",
    "join27",
    "join28",
    "join29",
    "join3",
    "join30",
    "join31",
    "join32",
    "join32_lessSize",
    "join33",
    "join34",
    "join35",
    "join36",
    "join37",
    "join38",
    "join39",
    "join4",
    "join40",
    "join41",
    "join5",
    "join6",
    "join7",
    "join8",
    "join9",
    "join_1to1",
    "join_array",
    "join_casesensitive",
    "join_empty",
    "join_filters",
    "join_hive_626",
    "join_map_ppr",
    "join_nulls",
    "join_nullsafe",
    "join_rc",
    "join_reorder2",
    "join_reorder3",
    "join_reorder4",
    "join_star"
  )

  // Only run those query tests in the realWhileList (do not try other ignored query files).
  override def testCases: Seq[(String, File)] = super.testCases.filter {
    case (name, _) => realWhiteList.contains(name)
  }
}
