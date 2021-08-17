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

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

trait PlanSuiteBase extends SharedSparkSession {

  private val originalCBCEnabled = conf.cboEnabled
  private val originalPlanStatsEnabled = conf.planStatsEnabled
  private val originalJoinReorderEnabled = conf.joinReorderEnabled

  protected def injectStats: Boolean = false

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (injectStats) {
      // Sets configurations for enabling the optimization rules that
      // exploit data statistics.
      conf.setConf(SQLConf.CBO_ENABLED, true)
      conf.setConf(SQLConf.PLAN_STATS_ENABLED, true)
      conf.setConf(SQLConf.JOIN_REORDER_ENABLED, true)
    }
    createTables()
  }

  override def afterAll(): Unit = {
    conf.setConf(SQLConf.CBO_ENABLED, originalCBCEnabled)
    conf.setConf(SQLConf.PLAN_STATS_ENABLED, originalPlanStatsEnabled)
    conf.setConf(SQLConf.JOIN_REORDER_ENABLED, originalJoinReorderEnabled)
    dropTables()
    super.afterAll()
  }

  protected def createTables(): Unit

  protected def dropTables(): Unit
}
