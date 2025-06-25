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
package org.apache.spark.sql.pipelines.common

// The status of the flow.
sealed trait FlowStatus
object FlowStatus {
  // Flow is queued and will be started after all its dependencies have been updated.
  case object QUEUED extends FlowStatus
  // Flow is in the process of starting.
  case object STARTING extends FlowStatus
  // A task is currently running an update for this flow.
  case object RUNNING extends FlowStatus
  // This flow's update has completed successfully.
  case object COMPLETED extends FlowStatus
  // This flow's update has failed. Additional information about the failure is present in the error
  // field.
  case object FAILED extends FlowStatus
  // This flow's update was skipped because an upstream dependency failed.
  case object SKIPPED extends FlowStatus
  // This flow's query was stopped or canceled by a user action.
  case object STOPPED extends FlowStatus
  // Flow is in the process of planning.
  case object PLANNING extends FlowStatus
  // This flow is excluded if it's not selected in the partial graph update API call.
  case object EXCLUDED extends FlowStatus
  // This flow is idle because there are no updates to be made because all available data has
  // already been processed.
  case object IDLE extends FlowStatus
}

sealed trait RunState

object RunState {
  // Run is currently executing queries.
  case object RUNNING extends RunState

  // Run is complete and all necessary resources are cleaned up.
  case object COMPLETED extends RunState

  // Run has run into an error that could not be recovered from.
  case object FAILED extends RunState

  // Run was canceled.
  case object CANCELED extends RunState
}

// The type of the dataset.
sealed trait DatasetType
object DatasetType {
  // Dataset is a materialized view.
  case object MATERIALIZED_VIEW extends DatasetType
  // Dataset is a streaming table.
  case object STREAMING_TABLE extends DatasetType
}
