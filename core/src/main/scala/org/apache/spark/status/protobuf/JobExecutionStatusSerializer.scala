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

package org.apache.spark.status.protobuf

import org.apache.spark.JobExecutionStatus
import org.apache.spark.status.protobuf.StoreTypes.{JobExecutionStatus => GJobExecutionStatus}

private[protobuf] object JobExecutionStatusSerializer {

  def serialize(input: JobExecutionStatus): GJobExecutionStatus = {
    input match {
      case JobExecutionStatus.RUNNING => GJobExecutionStatus.JOB_EXECUTION_STATUS_RUNNING
      case JobExecutionStatus.SUCCEEDED => GJobExecutionStatus.JOB_EXECUTION_STATUS_SUCCEEDED
      case JobExecutionStatus.FAILED => GJobExecutionStatus.JOB_EXECUTION_STATUS_FAILED
      case JobExecutionStatus.UNKNOWN => GJobExecutionStatus.JOB_EXECUTION_STATUS_UNKNOWN
    }
  }

  def deserialize(binary: GJobExecutionStatus): JobExecutionStatus = {
    binary match {
      case GJobExecutionStatus.JOB_EXECUTION_STATUS_RUNNING => JobExecutionStatus.RUNNING
      case GJobExecutionStatus.JOB_EXECUTION_STATUS_SUCCEEDED => JobExecutionStatus.SUCCEEDED
      case GJobExecutionStatus.JOB_EXECUTION_STATUS_FAILED => JobExecutionStatus.FAILED
      case GJobExecutionStatus.JOB_EXECUTION_STATUS_UNKNOWN => JobExecutionStatus.UNKNOWN
      case _ => null
    }
  }
}
