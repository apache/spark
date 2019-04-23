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

import java.util.zip.ZipOutputStream
import javax.servlet.ServletContext
import javax.servlet.http.HttpServletRequest
import javax.ws.rs._
import javax.ws.rs.core.{Context, Response}

import org.eclipse.jetty.server.handler.ContextHandler
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.glassfish.jersey.server.ServerProperties
import org.glassfish.jersey.servlet.ServletContainer

import org.apache.spark.SecurityManager
import org.apache.spark.ui.{SparkUI, UIUtils}

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
private[v1] class ApiRootResource extends ApiRequestContext {

  @Path("applications")
  def applicationList(): Class[ApplicationListResource] = classOf[ApplicationListResource]

  @Path("applications/{appId}")
  def application(): Class[OneApplicationResource] = classOf[OneApplicationResource]

  @GET
  @Path("version")
  def version(): VersionInfo = new VersionInfo(org.apache.spark.SPARK_VERSION)

}

private[spark] object ApiRootResource {

  def getServletHandler(uiRoot: UIRoot): ServletContextHandler = {
    val jerseyContext = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
    jerseyContext.setContextPath("/api")
    val holder: ServletHolder = new ServletHolder(classOf[ServletContainer])
    holder.setInitParameter(ServerProperties.PROVIDER_PACKAGES, "org.apache.spark.status.api.v1")
    UIRootFromServletContext.setUiRoot(jerseyContext, uiRoot)
    jerseyContext.addServlet(holder, "/*")
    jerseyContext
  }
}

/**
 * This trait is shared by the all the root containers for application UI information --
 * the HistoryServer and the application UI.  This provides the common
 * interface needed for them all to expose application info as json.
 */
private[spark] trait UIRoot {
  /**
   * Runs some code with the current SparkUI instance for the app / attempt.
   *
   * @throws java.util.NoSuchElementException If the app / attempt pair does not exist.
   */
  def withSparkUI[T](appId: String, attemptId: Option[String])(fn: SparkUI => T): T

  def getApplicationInfoList: Iterator[ApplicationInfo]
  def getApplicationInfo(appId: String): Option[ApplicationInfo]

  /**
   * Write the event logs for the given app to the `ZipOutputStream` instance. If attemptId is
   * `None`, event logs for all attempts of this application will be written out.
   */
  def writeEventLogs(appId: String, attemptId: Option[String], zipStream: ZipOutputStream): Unit = {
    Response.serverError()
      .entity("Event logs are only available through the history server.")
      .status(Response.Status.SERVICE_UNAVAILABLE)
      .build()
  }
  def securityManager: SecurityManager
}

private[v1] object UIRootFromServletContext {

  private val attribute = getClass.getCanonicalName

  def setUiRoot(contextHandler: ContextHandler, uiRoot: UIRoot): Unit = {
    contextHandler.setAttribute(attribute, uiRoot)
  }

  def getUiRoot(context: ServletContext): UIRoot = {
    context.getAttribute(attribute).asInstanceOf[UIRoot]
  }
}

private[v1] trait ApiRequestContext {
  @Context
  protected var servletContext: ServletContext = _

  @Context
  protected var httpRequest: HttpServletRequest = _

  def uiRoot: UIRoot = UIRootFromServletContext.getUiRoot(servletContext)

}

/**
 * Base class for resource handlers that use app-specific data. Abstracts away dealing with
 * application and attempt IDs, and finding the app's UI.
 */
private[v1] trait BaseAppResource extends ApiRequestContext {

  @PathParam("appId") protected[this] var appId: String = _
  @PathParam("attemptId") protected[this] var attemptId: String = _

  protected def withUI[T](fn: SparkUI => T): T = {
    try {
      uiRoot.withSparkUI(appId, Option(attemptId)) { ui =>
        val user = httpRequest.getRemoteUser()
        if (!ui.securityManager.checkUIViewPermissions(user)) {
          throw new ForbiddenException(raw"""user "$user" is not authorized""")
        }
        fn(ui)
      }
    } catch {
      case _: NoSuchElementException =>
        val appKey = Option(attemptId).map(appId + "/" + _).getOrElse(appId)
        throw new NotFoundException(s"no such app: $appKey")
    }
  }
}

private[v1] class ForbiddenException(msg: String) extends WebApplicationException(
    UIUtils.buildErrorResponse(Response.Status.FORBIDDEN, msg))

private[v1] class NotFoundException(msg: String) extends WebApplicationException(
    UIUtils.buildErrorResponse(Response.Status.NOT_FOUND, msg))

private[v1] class ServiceUnavailable(msg: String) extends WebApplicationException(
    UIUtils.buildErrorResponse(Response.Status.SERVICE_UNAVAILABLE, msg))

private[v1] class BadParameterException(msg: String) extends WebApplicationException(
    UIUtils.buildErrorResponse(Response.Status.BAD_REQUEST, msg)) {
  def this(param: String, exp: String, actual: String) = {
    this(raw"""Bad value for parameter "$param".  Expected a $exp, got "$actual"""")
  }
}

