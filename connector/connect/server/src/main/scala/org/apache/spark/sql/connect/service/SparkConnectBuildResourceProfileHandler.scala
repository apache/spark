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

package org.apache.spark.sql.connect.service

import scala.jdk.CollectionConverters.MapHasAsScala

import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.resource.{ExecutorResourceRequest, ResourceProfile, TaskResourceProfile, TaskResourceRequest}

class SparkConnectBuildResourceProfileHandler(
    responseObserver: StreamObserver[proto.BuildResourceProfileResponse])
    extends Logging {

  /**
   * transform the spark connect ResourceProfile to spark ResourceProfile
   * @param rp
   *   Spark connect ResourceProfile
   * @return
   *   the Spark ResourceProfile
   */
  private def transformResourceProfile(rp: proto.ResourceProfile): ResourceProfile = {
    val ereqs = rp.getExecutorResourcesMap.asScala.map { case (name, res) =>
      name -> new ExecutorResourceRequest(
        res.getResourceName,
        res.getAmount,
        res.getDiscoveryScript,
        res.getVendor)
    }.toMap
    val treqs = rp.getTaskResourcesMap.asScala.map { case (name, res) =>
      name -> new TaskResourceRequest(res.getResourceName, res.getAmount)
    }.toMap

    if (ereqs.isEmpty) {
      new TaskResourceProfile(treqs)
    } else {
      new ResourceProfile(ereqs, treqs)
    }
  }

  def handle(request: proto.BuildResourceProfileRequest): Unit = {
    val holder = SparkConnectService
      .getOrCreateIsolatedSession(request.getUserContext.getUserId, request.getSessionId)

    val rp = transformResourceProfile(request.getProfile)

    val session = holder.session
    session.sparkContext.resourceProfileManager.addResourceProfile(rp)

    val builder = proto.BuildResourceProfileResponse.newBuilder()
    builder.setProfileId(rp.id)
    builder.setSessionId(request.getSessionId)
    builder.setServerSideSessionId(holder.serverSessionId)
    responseObserver.onNext(builder.build())
    responseObserver.onCompleted()
  }

}
