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
import javax.ws.rs.core.MediaType

import org.apache.spark.JobExecutionStatus
import org.apache.spark.sql.execution.ui.{SQLAppStatusStore, SQLExecutionUIData, SQLPlanMetric}
import org.apache.spark.status.api.v1.{BaseAppResource, NotFoundException}

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class SqlResource extends BaseAppResource {

  @GET
  def sqlList(
      @DefaultValue("false") @QueryParam("details") details: Boolean,
      @DefaultValue("0") @QueryParam("offset") offset: Int,
      @DefaultValue("20") @QueryParam("length") length: Int): Seq[ExecutionData] = {
    withUI { ui =>
      val sqlStore = new SQLAppStatusStore(ui.store.store)
      sqlStore.executionsList(offset, length).map(prepareExecutionData(_, details))
    }
  }

  @GET
  @Path("{executionId:\\d+}")
  def sql(
      @PathParam("executionId") execId: Long,
      @DefaultValue("false") @QueryParam("details") details: Boolean): ExecutionData = {
    withUI { ui =>
      val sqlStore = new SQLAppStatusStore(ui.store.store)
      sqlStore
        .execution(execId)
        .map(prepareExecutionData(_, details))
        .getOrElse(throw new NotFoundException("unknown id: " + execId))
    }
  }

  private def printableMetrics(
      metrics: Seq[SQLPlanMetric],
      metricValues: Map[Long, String]): Seq[Metrics] = {
    metrics.map(metric =>
      Metrics(metric.name, metricValues.get(metric.accumulatorId).getOrElse("")))
  }

  private def prepareExecutionData(exec: SQLExecutionUIData, details: Boolean): ExecutionData = {
    var running = Seq[Int]()
    var completed = Seq[Int]()
    var failed = Seq[Int]()

    exec.jobs.foreach {
      case (id, JobExecutionStatus.RUNNING) =>
        running = running :+ id
      case (id, JobExecutionStatus.SUCCEEDED) =>
        completed = completed :+ id
      case (id, JobExecutionStatus.FAILED) =>
        failed = failed :+ id
      case _ =>
    }

    val status = if (exec.jobs.size == completed.size) {
      "COMPLETED"
    } else if (failed.nonEmpty) {
      "FAILED"
    } else {
      "RUNNING"
    }

    val duration = exec.completionTime.getOrElse(new Date()).getTime - exec.submissionTime
    val planDetails = if (details) exec.physicalPlanDescription else ""
    val metrics = if (details) printableMetrics(exec.metrics, exec.metricValues) else Seq.empty
    new ExecutionData(
      exec.executionId,
      status,
      exec.description,
      planDetails,
      metrics,
      new Date(exec.submissionTime),
      duration,
      running,
      completed,
      failed)
  }
}
