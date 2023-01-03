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

import org.apache.commons.lang3.StringUtils

import org.apache.spark.status.api.v1.StageStatus

private[protobuf] object StageStatusSerializer {

  private def PREFIX = "STAGE_STATUS_"

  def serialize(input: StageStatus): StoreTypes.StageStatus = {
    StoreTypes.StageStatus.valueOf(PREFIX + input.toString)
  }

  def deserialize(binary: StoreTypes.StageStatus): StageStatus = {
    StageStatus.valueOf(StringUtils.removeStart(binary.toString, PREFIX))
  }
}
