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

import javax.ws.rs.{Path, PathParam}

import org.apache.spark.status.api.v1.{NotFoundException, UIRootFromServletContext}
import org.apache.spark.streaming.ui.StreamingJobProgressListener
import org.apache.spark.ui.SparkUI

@Path("/v1")
private[v1] class StreamingApiRootResource extends UIRootFromServletContext {

  import org.apache.spark.status.api.v1.streaming.StreamingApiRootResource._

  @Path("applications/{appId}/streaming/statistics")
  def getStreamingStatistics(@PathParam("appId") appId: String): StreamingStatisticsResource = {
    uiRoot.withSparkUI(appId, None) { ui =>
      new StreamingStatisticsResource(getListener(ui))
    }
  }

  @Path("applications/{appId}/streaming/receivers")
  def getReceivers(@PathParam("appId") appId: String): AllReceiversResource = {
    uiRoot.withSparkUI(appId, None) { ui =>
      new AllReceiversResource(getListener(ui))
    }
  }

  @Path("applications/{appId}/streaming/receivers/{streamId: \\d+}")
  def getReceiver(@PathParam("appId") appId: String): OneReceiverResource = {
    uiRoot.withSparkUI(appId, None) { ui =>
      new OneReceiverResource(getListener(ui))
    }
  }

  @Path("applications/{appId}/streaming/batches")
  def getBatches(@PathParam("appId") appId: String): AllBatchesResource = {
    uiRoot.withSparkUI(appId, None) { ui =>
      new AllBatchesResource(getListener(ui))
    }
  }

  @Path("applications/{appId}/streaming/batches/{batchId: \\d+}")
  def getBatch(@PathParam("appId") appId: String): OneBatchResource = {
    uiRoot.withSparkUI(appId, None) { ui =>
      new OneBatchResource(getListener(ui))
    }
  }

  @Path("applications/{appId}/streaming/batches/{batchId: \\d+}/operations")
  def getOutputOperations(@PathParam("appId") appId: String): AllOutputOperationsResource = {
    uiRoot.withSparkUI(appId, None) { ui =>
      new AllOutputOperationsResource(getListener(ui))
    }
  }

  @Path("applications/{appId}/streaming/batches/{batchId: \\d+}/operations/{outputOpId: \\d+}")
  def getOutputOperation(@PathParam("appId") appId: String): OneOutputOperationResource = {
    uiRoot.withSparkUI(appId, None) { ui =>
      new OneOutputOperationResource(getListener(ui))
    }
  }

}

private[v1] object StreamingApiRootResource {
  def getListener(ui: SparkUI): StreamingJobProgressListener = {
    ui.getStreamingJobProgressListener match {
      case Some(listener) => listener.asInstanceOf[StreamingJobProgressListener]
      case None => throw new NotFoundException("no streaming listener attached to " + ui.getAppName)
    }
  }
}
