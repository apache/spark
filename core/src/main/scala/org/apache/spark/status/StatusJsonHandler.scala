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

package org.apache.spark.status

import javax.servlet.http.{HttpServletResponse, HttpServlet, HttpServletRequest}

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.{SerializationFeature, ObjectMapper}
import org.apache.spark.status.api.ApplicationInfo
import org.apache.spark.ui.SparkUI
import org.apache.spark.ui.exec.ExecutorsJsonRoute
import org.apache.spark.ui.jobs.{AllJobsJsonRoute, OneStageJsonRoute, AllStagesJsonRoute}
import org.apache.spark.ui.storage.{AllRDDJsonRoute, RDDJsonRoute}
import org.eclipse.jetty.servlet.{ServletHolder, ServletContextHandler}

import scala.util.matching.Regex

import org.apache.spark.{Logging, SecurityManager}
import org.apache.spark.deploy.history.{OneApplicationJsonRoute, AllApplicationsJsonRoute}


/**
 * get the response for one endpoint in the json status api.
 *
 * Implementations only need to return the objects that are to be converted to json -- the framework
 * will convert to json via jackson
 */
private[spark] trait StatusJsonRoute[T] {
  def renderJson(request: HttpServletRequest): T
}

private[spark] class JsonRequestHandler(uiRoot: UIRoot, securityManager: SecurityManager) extends Logging {
  def route(req: HttpServletRequest) : Option[StatusJsonRoute[_]] = {
    specs.collectFirst { case (pattern, route) if pattern.pattern.matcher(req.getPathInfo()).matches() =>
      route
    }
  }

  private val noSlash = """[^/]"""

  private val specs: IndexedSeq[(Regex, StatusJsonRoute[_])] = IndexedSeq(
    "/applications/?".r -> new AllApplicationsJsonRoute(uiRoot),
    s"/applications/$noSlash+/?".r -> new OneApplicationJsonRoute(uiRoot),
    s"/applications/$noSlash+/jobs/?".r -> new AllJobsJsonRoute(this),
    s"/applications/$noSlash+/executors/?".r -> new ExecutorsJsonRoute(this),
    s"/applications/$noSlash+/stages/?".r -> new AllStagesJsonRoute(this),
    s"/applications/$noSlash+/stages/$noSlash+/?".r -> new OneStageJsonRoute(this),
    s"/applications/$noSlash+/storage/rdd/?".r -> new AllRDDJsonRoute(this),
    s"/applications/$noSlash+/storage/rdd/$noSlash+/?".r -> new RDDJsonRoute(this)
  )

  private val jsonMapper = {
    val t = new ObjectMapper()
    t.registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
    t.enable(SerializationFeature.INDENT_OUTPUT)
    t.setSerializationInclusion(JsonInclude.Include.NON_NULL)
    t
  }

  val jsonContextHandler = {

    //TODO throw out all the JettyUtils stuff, so I can set the response status code, etc.
    val servlet = new HttpServlet {
      override def doGet(request: HttpServletRequest, response: HttpServletResponse) {
        if (securityManager.checkUIViewPermissions(request.getRemoteUser)) {
          response.setContentType("text/json;charset=utf-8")
          route(request) match {
            case Some(jsonRoute) =>
              response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate")
              try {
                val responseObj = jsonRoute.renderJson(request)
                val result = jsonMapper.writeValueAsString(responseObj)
                response.setStatus(HttpServletResponse.SC_OK)
                response.getWriter.println(result)
              } catch {
                case iae: IllegalArgumentException =>
                  response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
                  response.getOutputStream.print(iae.getMessage())
              }
            case None =>
              println("no match for path: " + request.getPathInfo)
              response.setStatus(HttpServletResponse.SC_NOT_FOUND)
          }
        } else {
          response.setStatus(HttpServletResponse.SC_UNAUTHORIZED)
          response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate")
          response.sendError(HttpServletResponse.SC_UNAUTHORIZED,
            "User is not authorized to access this page.")
        }
      }
    }
    val path = "/json/v1"
    val contextHandler = new ServletContextHandler
    val holder = new ServletHolder(servlet)
    contextHandler.setContextPath(path)
    contextHandler.addServlet(holder, "/*")
    contextHandler
  }

  def withSparkUI[T](request: HttpServletRequest)(f: (SparkUI, HttpServletRequest) => T): T = {
    val appIdOpt = JsonRequestHandler.extractAppId(request.getPathInfo)
    appIdOpt match {
      case Some(appId) =>
        uiRoot.getSparkUI(appId) match {
          case Some(ui) => f(ui, request)
          case None => throw new IllegalArgumentException("no such app: " + appId)
        }
      case None =>
        throw new IllegalArgumentException("no app id")
    }
  }
}

private[spark] object JsonRequestHandler {
  val appId = """/applications/([^/]+)""".r
  val stageId = """/applications/[^/]+/stages/(\d+)""".r
  val rddId = """/applications/[^/]+/storage/rdd/(\d+)""".r
  def extractAppId(pathInfo: String): Option[String] = appId.findFirstMatchIn(pathInfo).map{_.group(1)}
  def extractStageId(pathInfo: String): Option[Int] = stageId.findFirstMatchIn(pathInfo).map{_.group(1).toInt}
  def extractRDDId(pathInfo: String): Option[Int] = rddId.findFirstMatchIn(pathInfo).map{_.group(1).toInt}
}

private[spark] trait UIRoot {
  def getSparkUI(appKey: String): Option[SparkUI]
  def getApplicationInfoList: Seq[ApplicationInfo]
}


object RouteUtils {
  def extractParamSet[T <: Enum[T]](
    request: HttpServletRequest,
    paramName: String,
    enums: Seq[T]
  ): Set[T] = {
    extractParamSet(Option(request.getParameterValues(paramName)), enums)
  }
  
  def extractParamSet[T <: Enum[T]](
    raw: Option[Array[String]],
    enums: Seq[T]
  ): Set[T] = {
    raw match {
      case Some(arr) =>
        arr.flatMap { oneString => oneString.split(",").map { s =>
          enums.find { e => e.name.equalsIgnoreCase(s)}.getOrElse {
            throw new IllegalArgumentException("unknown status: " + s)
          }
        }}.toSet
      case None =>
        Set(enums: _*)
    }
  }
}
