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

import org.apache.spark.status.AppSummary

private[protobuf] class AppSummarySerializer extends ProtobufSerDe[AppSummary] {

  override def serialize(input: AppSummary): Array[Byte] = {
    val builder = StoreTypes.AppSummary.newBuilder()
      .setNumCompletedJobs(input.numCompletedJobs)
      .setNumCompletedStages(input.numCompletedStages)
    builder.build().toByteArray
  }

  override def deserialize(bytes: Array[Byte]): AppSummary = {
    val summary = StoreTypes.AppSummary.parseFrom(bytes)
    new AppSummary(
      numCompletedJobs = summary.getNumCompletedJobs,
      numCompletedStages = summary.getNumCompletedStages
    )
  }
}
