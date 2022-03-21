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

package org.apache.spark.status.api.v1.sql

import javax.ws.rs._
import javax.ws.rs.core.MediaType

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.execution.ui.{SQLAppStatusStore, SQLExecutionUIData}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.status.api.v1.{BaseAppResource, NotFoundException}

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class SqlCompilerResource extends BaseAppResource with Logging {

  @GET
  def compilerStat(
      @DefaultValue("0") @QueryParam("offset") offset: Int,
      @DefaultValue("20") @QueryParam("length") length: Int): Seq[CompileData] = {
    withUI { ui =>
      val sqlStore = new SQLAppStatusStore(ui.store.store)
      val appid = ui.store.environmentInfo().sparkProperties.toMap.getOrElse("spark.app.id", "")
      sqlStore.executionsList(offset, length).map { exec =>
        val compileStats = sqlStore.getCompilerStats(exec.executionId)
        sqlStore
          .execution(exec.executionId)
          .map(prepareCompileData(_, compileStats, appid)).get
      }
    }
  }

  @GET
  @Path("{executionId:\\d+}")
  def compilerStat(
      @PathParam("executionId") execId: Long): CompileData = {
    withUI { ui =>
      val sqlStore = new SQLAppStatusStore(ui.store.store)
      val compileStats = sqlStore.getCompilerStats(execId)
      val appid = ui.store.environmentInfo().sparkProperties.toMap.getOrElse("spark.app.id", "")
      sqlStore
        .execution(execId)
        .map(prepareCompileData(_, compileStats, appid))
        .getOrElse(throw new NotFoundException("unknown query execution id: " + execId))
    }
  }

  private def prepareCompileData(
                                  exec: SQLExecutionUIData,
                                  compileStats: QueryPlanningTracker,
                                  appId: String): CompileData = {

    val phases = compileStats.phases.map{ case (phaseStr, phaseSummary) => Metric(phaseStr,
      Option(phaseSummary.durationMs.toString).getOrElse(""))}

    val rules = compileStats.topRulesByTime(SQLConf.get.uiRulesShow).map{
      case (strName, summary) => Rule(strName, (summary.totalTimeNs/1000000.0).toString,
        summary.numInvocations, summary.numEffectiveInvocations)
    }

    new CompileData(
      exec.executionId,
      appId,
      phases.toSeq,
      rules)
  }
}
