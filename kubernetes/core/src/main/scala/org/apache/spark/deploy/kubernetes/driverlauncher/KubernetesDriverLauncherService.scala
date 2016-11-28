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
package org.apache.spark.deploy.kubernetes.driverlauncher

import javax.ws.rs.{Consumes, GET, HeaderParam, Path, POST, Produces}
import javax.ws.rs.core.{HttpHeaders, MediaType}

@Path("/kubernetes-driver-launcher-service")
private[spark] trait KubernetesDriverLauncherService {

  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/submitApplication")
  def submitApplication(
      @HeaderParam("X-" + HttpHeaders.AUTHORIZATION) applicationSecret: String,
      applicationConfiguration: KubernetesSparkDriverConfiguration): Unit

  @GET
  @Produces(Array(MediaType.TEXT_PLAIN))
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Path("/ping")
  def ping(): String
}
