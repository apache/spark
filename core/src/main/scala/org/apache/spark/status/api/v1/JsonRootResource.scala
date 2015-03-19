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

import javax.servlet.ServletContext
import javax.ws.rs._
import javax.ws.rs.core.{Context, Response}

import com.sun.jersey.api.core.ResourceConfig
import com.sun.jersey.spi.container.servlet.ServletContainer
import org.eclipse.jetty.server.handler.ContextHandler
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}

import org.apache.spark.SecurityManager
import org.apache.spark.ui.SparkUI

@Path("/v1")
class JsonRootResource extends UIRootFromServletContext {

  @Path("applications")
  def getApplicationList(): ApplicationListResource = {
    new ApplicationListResource(uiRoot)
  }

  @Path("applications/{appId}")
  def getApplication(): OneApplicationResource = {
    new OneApplicationResource(uiRoot)
  }

  @Path("applications/{appId}/jobs")
  def getJobs(): AllJobsResource = {
    new AllJobsResource(uiRoot)
  }

  @Path("applications/{appId}/executors")
  def getExecutors(): ExecutorListResource = {
    new ExecutorListResource(uiRoot)
  }

  @Path("applications/{appId}/stages")
  def getStages(): AllStagesResource= {
    new AllStagesResource(uiRoot)
  }

  @Path("applications/{appId}/stages/{stageId: \\d+}")
  def getStage(): OneStageResource= {
    new OneStageResource(uiRoot)
  }

  @Path("applications/{appId}/stages/{stageId: \\d+}/{attemptId: \\d+}")
  def getStageAttempt(): OneStageAttemptResource= {
    new OneStageAttemptResource(uiRoot)
  }


  @Path("applications/{appId}/storage/rdd")
  def getRdds(): AllRDDResource = {
    new AllRDDResource(uiRoot)
  }

  @Path("applications/{appId}/storage/rdd/{rddId: \\d+}")
  def getRdd(): OneRDDResource = {
    new OneRDDResource(uiRoot)
  }

}

object JsonRootResource {
  def getJsonServlet(uiRoot: UIRoot) = {
    val jerseyContext = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
    jerseyContext.setContextPath("/json")
    val holder:ServletHolder = new ServletHolder(classOf[ServletContainer])
    holder.setInitParameter("com.sun.jersey.config.property.resourceConfigClass",
      "com.sun.jersey.api.core.PackagesResourceConfig")
    holder.setInitParameter("com.sun.jersey.config.property.packages",
      "org.apache.spark.status.api.v1")
    holder.setInitParameter(ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS,
      classOf[SecurityFilter].getCanonicalName)
    UIRootFromServletContext.setUiRoot(jerseyContext, uiRoot)
    jerseyContext.addServlet(holder, "/*")
    jerseyContext
  }
}


private[spark] trait UIRoot {
  def getSparkUI(appKey: String): Option[SparkUI]
  def getApplicationInfoList: Seq[ApplicationInfo]

  /**
   * Get the spark UI with the given appID, and apply a function
   * to it.  If there is no such app, throw an appropriate exception
   */
  def withSparkUI[T](appId: String)(f: SparkUI => T): T = {
    getSparkUI(appId) match {
      case Some(ui) =>
        f(ui)
      case None => throw new NotFoundException("no such app: " + appId)
    }
  }
  def securityManager: SecurityManager
}

object UIRootFromServletContext {
  private val attribute = getClass.getCanonicalName
  def setUiRoot(contextHandler: ContextHandler, uiRoot: UIRoot): Unit = {
    contextHandler.setAttribute(attribute, uiRoot)
  }
  def getUiRoot(context: ServletContext): UIRoot = {
    context.getAttribute(attribute).asInstanceOf[UIRoot]
  }
}

trait UIRootFromServletContext {
  @Context
  var servletContext: ServletContext = _

  def uiRoot: UIRoot = UIRootFromServletContext.getUiRoot(servletContext)
}

class NotFoundException(msg: String) extends WebApplicationException(
  new IllegalArgumentException(msg),
    Response
      .status(Response.Status.NOT_FOUND)
      .entity(msg)
      .build()
)
