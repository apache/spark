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

import java.util.Date
import javax.ws.rs._

import org.apache.spark.sql.diagnostic._
import org.apache.spark.status.api.v1.{BaseAppResource, NotFoundException}

private[v1] class SQLDiagnosticResource extends BaseAppResource {

  @GET
  def sqlDiagnosticList(
      @DefaultValue("0") @QueryParam("offset") offset: Int,
      @DefaultValue("20") @QueryParam("length") length: Int): Seq[SQLDiagnosticData] = {
    withUI { ui =>
      ui.store.diskStore.map { kvStore =>
        val store = new DiagnosticStore(kvStore)
        store.diagnosticsList(offset, length)
          // Do not display the plan changes in the list
          .map(d => prepareSqlDiagnosticData(d, Seq.empty))
      }.getOrElse(Seq.empty)
    }
  }

  @GET
  @Path("{executionId:\\d+}")
  def sqlDiagnostic(
      @PathParam("executionId") execId: Long): SQLDiagnosticData = {
    withUI { ui =>
      ui.store.diskStore.flatMap { kvStore =>
        val store = new DiagnosticStore(kvStore)
        val updates = store.adaptiveExecutionUpdates(execId)
        store.diagnostic(execId)
          .map(d => prepareSqlDiagnosticData(d, updates))
      }.getOrElse(throw new NotFoundException("unknown query execution id: " + execId))
    }
  }

  private def prepareSqlDiagnosticData(
      diagnostic: ExecutionDiagnosticData,
      updates: Seq[AdaptiveExecutionUpdate]): SQLDiagnosticData = {
    new SQLDiagnosticData(
      diagnostic.executionId,
      diagnostic.physicalPlan,
      new Date(diagnostic.submissionTime),
      diagnostic.completionTime.map(t => new Date(t)),
      diagnostic.errorMessage,
      updates.map(u => AdaptivePlanChange(new Date(u.updateTime), u.physicalPlan)))
  }
}
