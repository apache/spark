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

import jakarta.ws.rs.{Path, PathParam}

import org.apache.spark.status.api.v1._
import org.apache.spark.streaming.ui.StreamingJobProgressListener

@Path("/v1")
private[v1] class ApiStreamingApp extends ApiRequestContext {

  @Path("applications/{appId}/streaming")
  def getStreamingRoot(@PathParam("appId") appId: String): Class[ApiStreamingRootResource] = {
    classOf[ApiStreamingRootResource]
  }

  @Path("applications/{appId}/{attemptId}/streaming")
  def getStreamingRoot(
      @PathParam("appId") appId: String,
      @PathParam("attemptId") attemptId: String): Class[ApiStreamingRootResource] = {
    classOf[ApiStreamingRootResource]
  }
}

/**
 * Base class for streaming API handlers, provides easy access to the streaming listener that
 * holds the app's information.
 */
private[v1] trait BaseStreamingAppResource extends BaseAppResource {

  protected def withListener[T](fn: StreamingJobProgressListener => T): T = withUI { ui =>
    val listener = ui.getStreamingJobProgressListener match {
      case Some(listener) => listener.asInstanceOf[StreamingJobProgressListener]
      case None => throw new NotFoundException("no streaming listener attached to " + ui.getAppName)
    }
    listener.synchronized {
      fn(listener)
    }
  }

}
