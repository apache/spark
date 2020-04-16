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
package org.apache.spark.ui.jobs

import org.apache.spark.status.api.v1.JobData
import org.apache.spark.ui.UIUtils

private[ui] object JobDataUtil {
  def getDuration(jobData: JobData): Option[Long] = {
    jobData.submissionTime.map { start =>
        val end = jobData.completionTime.map(_.getTime()).getOrElse(System.currentTimeMillis())
        end - start.getTime()
    }
  }

  def getFormattedDuration(jobData: JobData): String = {
    val duration = getDuration(jobData)
    duration.map(d => UIUtils.formatDuration(d)).getOrElse("Unknown")
  }

  def getFormattedSubmissionTime(jobData: JobData): String = {
    jobData.submissionTime.map(UIUtils.formatDate).getOrElse("Unknown")
  }
}
