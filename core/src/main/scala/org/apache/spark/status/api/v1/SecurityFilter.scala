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

import javax.ws.rs.WebApplicationException
import javax.ws.rs.core.Response

import com.sun.jersey.spi.container.{ContainerRequest, ContainerRequestFilter}

private[v1] class SecurityFilter extends ContainerRequestFilter with UIRootFromServletContext {
  def filter(req: ContainerRequest): ContainerRequest = {
    val user = Option(req.getUserPrincipal).map { _.getName }.orNull
    if (uiRoot.securityManager.checkUIViewPermissions(user)) {
      req
    } else {
      throw new WebApplicationException(
        Response
          .status(Response.Status.FORBIDDEN)
          .entity(raw"""user "$user"is not authorized""")
          .build()
      )
    }
  }
}
