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

package org.apache.spark.sql

import org.apache.spark.tags.ExtendedSQLTest

@ExtendedSQLTest
class TPCDSIcebergModifiedPlanStabilitySuite extends TPCDSModifiedPlanStabilitySuite
  with TPCDSIcebergBase {
  // these queries are affected because push down of broadcast variable results in
  // reuse exchange getting eliminated
  val queriesAffectedByBroadcastVarPush = Set("q59")

  protected override lazy val baseResourcePath = {
    getWorkspaceFilePath("sql", "core", "src", "test", "resources", "iceberg",
      "tpcds-plan-stability").toFile
  }

  override protected def getAppropriateFileName(queryName: String, suggestedFileName: String):
  String = if (conf.pushBroadcastedJoinKeysASFilterToScan &&
      queriesAffectedByBroadcastVarPush.contains(queryName)) {
      s"broadcastvar_$suggestedFileName"
    } else {
      super.getAppropriateFileName(queryName, suggestedFileName)
    }
}

@ExtendedSQLTest
class TPCDSIcebergV1_4_PlanStabilitySuite extends TPCDSV1_4_PlanStabilitySuite
  with TPCDSIcebergBase {
  // these queries are failing due to call of estimation of stats before pushdown of filters &
  // columns
  override def excludedTpcdsQueries: Set[String] = super.excludedTpcdsQueries ++
    Set("q14a", "q14b", "q38", "q87")
  override lazy val baseResourcePath = {
    getWorkspaceFilePath("sql", "core", "src", "test", "resources", "iceberg",
      "tpcds-plan-stability").toFile
  }

  // these queries are affected because push down of broadcast variable results in
  // reuse exchange getting eliminated

  // TODO: Asif check q58 && q83 it has subquery broadcast which got eliminated, why?
  val queriesAffectedByBroadcastVarPush = Set("q2", "q23b", "q31", "q33", "q39a", "q39b", "q56",
  "q58", "q59", "q60", "q61", "q83")

  override protected def getAppropriateFileName(queryName: String, suggestedFileName: String):
  String = if (conf.pushBroadcastedJoinKeysASFilterToScan &&
    queriesAffectedByBroadcastVarPush.contains(queryName)) {
    s"broadcastvar_$suggestedFileName"
  } else {
    super.getAppropriateFileName(queryName, suggestedFileName)
  }
}

@ExtendedSQLTest
class TPCDSIcebergV2_7_PlanStabilitySuite extends TPCDSV2_7_PlanStabilitySuite
  with TPCDSIcebergBase {
  override lazy val baseResourcePath = {
    getWorkspaceFilePath("sql", "core", "src", "test", "resources", "iceberg",
      "tpcds-plan-stability").toFile
  }

  override def excludedTpcdsV2_7_0Queries: Set[String] = super.excludedTpcdsV2_7_0Queries ++
    Set("q14", "q14a")

  // these queries are affected because push down of broadcast variable results in
  // reuse exchange getting eliminated

  // TODO: Asif check q78 it has subquery broadcast which got eliminated, why?
  val queriesAffectedByBroadcastVarPush = Set("q18a", "q64", "q70a", "q75", "q78")

  override protected def getAppropriateFileName(queryName: String, suggestedFileName: String):
  String = if (conf.pushBroadcastedJoinKeysASFilterToScan &&
    queriesAffectedByBroadcastVarPush.contains(queryName)) {
    s"broadcastvar_$suggestedFileName"
  } else {
    super.getAppropriateFileName(queryName, suggestedFileName)
  }
}

@ExtendedSQLTest
class TPCHIcebergPlanStabilitySuite extends TPCHPlanStabilitySuite with TPCHIcebergBase {
  override def goldenFilePath: String = getWorkspaceFilePath(
    "sql", "core", "src", "test", "resources", "iceberg", "tpch-plan-stability").toFile
    .getAbsolutePath

  override def excludedTpchQueries: Set[String] =
    if (conf.pushBroadcastedJoinKeysASFilterToScan) {
      // this query is disabled because push down of broadcast variable results in one
      // reuse exchange getting eliminated
      Set("q11")
    } else {
      Set.empty
    }
}
