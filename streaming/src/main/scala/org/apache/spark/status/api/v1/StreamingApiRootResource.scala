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

import javax.ws.rs.{Path, PathParam}

/**
 * Main entry point for serving spark application metrics as json, using JAX-RS.
 *
 * Each resource should have endpoints that return **public** classes defined in api.scala.  Mima
 * binary compatibility checks ensure that we don't inadvertently make changes that break the api.
 * The returned objects are automatically converted to json by jackson with JacksonMessageWriter.
 * In addition, there are a number of tests in HistoryServerSuite that compare the json to "golden
 * files".  Any changes and additions should be reflected there as well -- see the notes in
 * HistoryServerSuite.
 */
@Path("/v1")
private[v1] class StreamingApiRootResource extends UIRootFromServletContext {

  @Path("applications/{appId}/streaming/receivers")
  def getStreamReceivers(@PathParam("appId") appId: String): StreamReceiverListResource = {
    uiRoot.withSparkUI(appId, None) { ui =>
      new StreamReceiverListResource(ui)
    }
  }

  @Path("applications/{appId}/streaming/batch")
  def getStreamBatchInfos(@PathParam("appId") appId: String): StreamBatchListResource = {
    uiRoot.withSparkUI(appId, None) { ui =>
      new StreamBatchListResource(ui)
    }
  }
}
