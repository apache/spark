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

import scala.collection.mutable.ListBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.ui.{SQLAppStatusStore, SQLExecutionUIData}
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
    compileStats: String,
    appId: String): CompileData = {

    val phaseString = compileStats.split("=== Spark Rule Timing Statistics ===")
    val phaseListStr = phaseString.head.split("=== Spark Phase Timing Statistics ===")(1).
      split("\\r?\\n")
    val phaseTimes = new ListBuffer[PhaseTime]()
    for(i <- 1 until phaseListStr.length by 2) {
      val phaseStr = phaseListStr(i).split(":")(1).trim
      val time = phaseListStr(i + 1).split(":")(1).trim
      phaseTimes += PhaseTime(phaseStr, time.toLong)
    }

    val rulesListStr = phaseString(1).trim().split("\\r?\\n")
    val rules = new ListBuffer[Rule]()
    for (i <- 0 until rulesListStr.length by 4) {
      val name = rulesListStr(i).split(":")(1).trim
      val time = rulesListStr(i + 1).split(":")(1).trim
      val invocation = rulesListStr(i + 2).split(": ")(1).trim
      val effective = rulesListStr(i + 3).split(": ")(1).trim
      rules += Rule(name, time.toDouble, invocation.toLong, effective.toLong)
    }

    new CompileData(
      exec.executionId,
      appId,
      phaseTimes.toSeq,
      rules.toSeq)
  }
}
