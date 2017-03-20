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

import javax.ws.rs.Path

import org.apache.spark.status.api.v1.NotFoundException
import org.apache.spark.streaming.ui.StreamingJobProgressListener
import org.apache.spark.ui.SparkUI

private[v1] class ApiStreamingRootResource(ui: SparkUI) {

  import org.apache.spark.status.api.v1.streaming.ApiStreamingRootResource._

  @Path("statistics")
  def getStreamingStatistics(): StreamingStatisticsResource = {
    new StreamingStatisticsResource(getListener(ui))
  }

  @Path("receivers")
  def getReceivers(): AllReceiversResource = {
    new AllReceiversResource(getListener(ui))
  }

  @Path("receivers/{streamId: \\d+}")
  def getReceiver(): OneReceiverResource = {
    new OneReceiverResource(getListener(ui))
  }

  @Path("batches")
  def getBatches(): AllBatchesResource = {
    new AllBatchesResource(getListener(ui))
  }

  @Path("batches/{batchId: \\d+}")
  def getBatch(): OneBatchResource = {
    new OneBatchResource(getListener(ui))
  }

  @Path("batches/{batchId: \\d+}/operations")
  def getOutputOperations(): AllOutputOperationsResource = {
    new AllOutputOperationsResource(getListener(ui))
  }

  @Path("batches/{batchId: \\d+}/operations/{outputOpId: \\d+}")
  def getOutputOperation(): OneOutputOperationResource = {
    new OneOutputOperationResource(getListener(ui))
  }

}

private[v1] object ApiStreamingRootResource {
  def getListener(ui: SparkUI): StreamingJobProgressListener = {
    ui.getStreamingJobProgressListener match {
      case Some(listener) => listener.asInstanceOf[StreamingJobProgressListener]
      case None => throw new NotFoundException("no streaming listener attached to " + ui.getAppName)
    }
  }
}
