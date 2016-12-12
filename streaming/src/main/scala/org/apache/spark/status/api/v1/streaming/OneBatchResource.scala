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

package org.apache.spark.status.api.v1.streaming

import javax.ws.rs.{GET, PathParam, Produces}
import javax.ws.rs.core.MediaType

import org.apache.spark.status.api.v1.NotFoundException
import org.apache.spark.streaming.ui.StreamingJobProgressListener

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class OneBatchResource(listener: StreamingJobProgressListener) {

  @GET
  def oneBatch(@PathParam("batchId") batchId: Long): BatchInfo = {
    val someBatch = AllBatchesResource.batchInfoList(listener)
      .find { _.batchId == batchId }
    someBatch.getOrElse(throw new NotFoundException("unknown batch: " + batchId))
  }
}
