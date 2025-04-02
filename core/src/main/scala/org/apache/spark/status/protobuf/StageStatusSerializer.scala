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

import org.apache.spark.status.api.v1.StageStatus
import org.apache.spark.status.protobuf.StoreTypes.{StageStatus => GStageStatus}

private[protobuf] object StageStatusSerializer {

  def serialize(input: StageStatus): GStageStatus = {
    input match {
      case StageStatus.ACTIVE => GStageStatus.STAGE_STATUS_ACTIVE
      case StageStatus.COMPLETE => GStageStatus.STAGE_STATUS_COMPLETE
      case StageStatus.FAILED => GStageStatus.STAGE_STATUS_FAILED
      case StageStatus.PENDING => GStageStatus.STAGE_STATUS_PENDING
      case StageStatus.SKIPPED => GStageStatus.STAGE_STATUS_SKIPPED
    }
  }

  def deserialize(binary: GStageStatus): StageStatus = {
    binary match {
      case GStageStatus.STAGE_STATUS_ACTIVE => StageStatus.ACTIVE
      case GStageStatus.STAGE_STATUS_COMPLETE => StageStatus.COMPLETE
      case GStageStatus.STAGE_STATUS_FAILED => StageStatus.FAILED
      case GStageStatus.STAGE_STATUS_PENDING => StageStatus.PENDING
      case GStageStatus.STAGE_STATUS_SKIPPED => StageStatus.SKIPPED
      case _ => null
    }
  }
}
