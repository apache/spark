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

package org.apache.spark.status.api.v1

import java.util.Date
import javax.ws.rs.{GET, Produces}
import javax.ws.rs.core.MediaType

import org.apache.spark.JobExecutionStatus
import org.apache.spark.sql.execution.ui.SQLAppStatusStore
import org.apache.spark.ui.UIUtils

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class SqlListResource extends BaseAppResource {

  @GET
  def sqlList(): ExecutionSummary = {
    withUI { ui =>
      val sqlStore = new SQLAppStatusStore(ui.store.store)
      var executions = List[ExecutionData]()

      sqlStore.executionsList().foreach { exec =>
        val running = exec.jobs
          .filter {case(_, status) => status == JobExecutionStatus.RUNNING }
          .keys.toSeq
        val completed = exec.jobs
          .filter {case(_, status) => status == JobExecutionStatus.SUCCEEDED }
          .keys.toSeq
        val failed = exec.jobs
          .filter {case(_, status) => status == JobExecutionStatus.FAILED }
          .keys.toSeq
        val status = if (exec.jobs.size == completed.size) {
          "COMPLETED"
        } else if (failed.length > 0) {
          "FAILED"
        } else {
          "RUNNING"
        }
        val duration = UIUtils.formatDuration(
          exec.completionTime.getOrElse(new Date()).getTime - exec.submissionTime)
        executions = executions.+:(new ExecutionData(exec.executionId,
          status, exec.description, UIUtils.formatDate(exec.submissionTime),
          duration, running, completed, failed))
      }
      if (executions.size > 0) {
        executions = executions.sortBy(x => x.id)
      }
      new ExecutionSummary(executions)
    }
  }
}

class ExecutionData (val id : Long,
                     val status: String,
                     val description: String,
                     val submissionTime: String,
                     val duration: String,
                     val runningJobs: Seq[Int],
                     val successJobs: Seq[Int],
                     val failedJobs: Seq[Int])

class ExecutionSummary (val sql: Seq[ExecutionData])
