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

package org.apache.spark.ui

import java.net.{InetSocketAddress, URL}
import javax.servlet.DispatcherType
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import scala.annotation.tailrec
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}
import scala.xml.Node

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.handler._
import org.eclipse.jetty.servlet._
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.json4s.JValue
import org.json4s.jackson.JsonMethods.{pretty, render}

import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.util.Utils

/**
 * Utilities for launching a web server using Jetty's HTTP Server class
 */
private[spark] object JettyUtils extends Logging {

  // Base type for a function that returns something based on an HTTP request. Allows for
  // implicit conversion from many types of functions to jetty Handlers.
  type Responder[T] = HttpServletRequest => T

  class ServletParams[T <% AnyRef](val responder: Responder[T],
    val contentType: String,
    val extractFn: T => String = (in: Any) => in.toString) {}

  // Conversions from various types of Responder's to appropriate servlet parameters
  implicit def jsonResponderToServlet(responder: Responder[JValue]): ServletParams[JValue] =
    new ServletParams(responder, "text/json", (in: JValue) => pretty(render(in)))

  implicit def htmlResponderToServlet(responder: Responder[Seq[Node]]): ServletParams[Seq[Node]] =
    new ServletParams(responder, "text/html", (in: Seq[Node]) => "<!DOCTYPE html>" + in.toString)

  implicit def textResponderToServlet(responder: Responder[String]): ServletParams[String] =
    new ServletParams(responder, "text/plain")

  def createServlet[T <% AnyRef](
      servletParams: ServletParams[T],
      securityMgr: SecurityManager): HttpServlet = {
    new HttpServlet {
      override def doGet(request: HttpServletRequest, response: HttpServletResponse) {
        if (securityMgr.checkUIViewPermissions(request.getRemoteUser)) {
          response.setContentType("%s;charset=utf-8".format(servletParams.contentType))
          response.setStatus(HttpServletResponse.SC_OK)
          val result = servletParams.responder(request)
          response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate")
          response.getWriter.println(servletParams.extractFn(result))
        } else {
          response.setStatus(HttpServletResponse.SC_UNAUTHORIZED)
          response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate")
          response.sendError(HttpServletResponse.SC_UNAUTHORIZED,
            "User is not authorized to access this page.")
        }
      }
    }
  }

  /** Create a context handler that responds to a request with the given path prefix */
  def createServletHandler[T <% AnyRef](
      path: String,
      servletParams: ServletParams[T],
      securityMgr: SecurityManager,
      basePath: String = ""): ServletContextHandler = {
    createServletHandler(path, createServlet(servletParams, securityMgr), basePath)
  }

  /** Create a context handler that responds to a request with the given path prefix */
  def createServletHandler(
      path: String,
      servlet: HttpServlet,
      basePath: String): ServletContextHandler = {
    val prefixedPath = attachPrefix(basePath, path)
    val contextHandler = new ServletContextHandler
    val holder = new ServletHolder(servlet)
    contextHandler.setContextPath(prefixedPath)
    contextHandler.addServlet(holder, "/")
    contextHandler
  }

  /** Create a handler that always redirects the user to the given path */
  def createRedirectHandler(
      srcPath: String,
      destPath: String,
      beforeRedirect: HttpServletRequest => Unit = x => (),
      basePath: String = ""): ServletContextHandler = {
    val prefixedDestPath = attachPrefix(basePath, destPath)
    val servlet = new HttpServlet {
      override def doGet(request: HttpServletRequest, response: HttpServletResponse) {
        beforeRedirect(request)
        // Make sure we don't end up with "//" in the middle
        val newUrl = new URL(new URL(request.getRequestURL.toString), prefixedDestPath).toString
        response.sendRedirect(newUrl)
      }
    }
    createServletHandler(srcPath, servlet, basePath)
  }

  /** Create a handler for serving files from a static directory */
  def createStaticHandler(resourceBase: String, path: String): ServletContextHandler = {
    val contextHandler = new ServletContextHandler
    contextHandler.setInitParameter("org.eclipse.jetty.servlet.Default.gzip", "false")
    val staticHandler = new DefaultServlet
    val holder = new ServletHolder(staticHandler)
    Option(Utils.getSparkClassLoader.getResource(resourceBase)) match {
      case Some(res) =>
        holder.setInitParameter("resourceBase", res.toString)
      case None =>
        throw new Exception("Could not find resource path for Web UI: " + resourceBase)
    }
    contextHandler.setContextPath(path)
    contextHandler.addServlet(holder, "/")
    contextHandler
  }

  /** Add filters, if any, to the given list of ServletContextHandlers */
  def addFilters(handlers: Seq[ServletContextHandler], conf: SparkConf) {
    val filters: Array[String] = conf.get("spark.ui.filters", "").split(',').map(_.trim())
    filters.foreach {
      case filter : String =>
        if (!filter.isEmpty) {
          logInfo("Adding filter: " + filter)
          val holder : FilterHolder = new FilterHolder()
          holder.setClassName(filter)
          // Get any parameters for each filter
          val paramName = "spark." + filter + ".params"
          val params = conf.get(paramName, "").split(',').map(_.trim()).toSet
          params.foreach {
            case param : String =>
              if (!param.isEmpty) {
                val parts = param.split("=")
                if (parts.length == 2) holder.setInitParameter(parts(0), parts(1))
             }
          }
          val enumDispatcher = java.util.EnumSet.of(DispatcherType.ASYNC, DispatcherType.ERROR,
            DispatcherType.FORWARD, DispatcherType.INCLUDE, DispatcherType.REQUEST)
          handlers.foreach { case(handler) => handler.addFilter(holder, "/*", enumDispatcher) }
        }
    }
  }

  /**
   * Attempt to start a Jetty server bound to the supplied hostName:port using the given
   * context handlers.
   *
   * If the desired port number is contended, continues incrementing ports until a free port is
   * found. Return the jetty Server object, the chosen port, and a mutable collection of handlers.
   */
  def startJettyServer(
      hostName: String,
      port: Int,
      handlers: Seq[ServletContextHandler],
      conf: SparkConf,
      serverName: String = ""): ServerInfo = {

    val collection = new ContextHandlerCollection
    collection.setHandlers(handlers.toArray)
    addFilters(handlers, conf)

    // Bind to the given port, or throw a java.net.BindException if the port is occupied
    def connect(currentPort: Int): (Server, Int) = {
      val server = new Server(new InetSocketAddress(hostName, currentPort))
      val pool = new QueuedThreadPool
      pool.setDaemon(true)
      server.setThreadPool(pool)
      server.setHandler(collection)
      try {
        server.start()
        (server, server.getConnectors.head.getLocalPort)
      } catch {
        case e: Exception =>
          server.stop()
          pool.stop()
          throw e
      }
    }

    val (server, boundPort) = Utils.startServiceOnPort[Server](port, connect, serverName)
    ServerInfo(server, boundPort, collection)
  }

  /** Attach a prefix to the given path, but avoid returning an empty path */
  private def attachPrefix(basePath: String, relativePath: String): String = {
    if (basePath == "") relativePath else (basePath + relativePath).stripSuffix("/")
  }
}

private[spark] case class ServerInfo(
    server: Server,
    boundPort: Int,
    rootHandler: ContextHandlerCollection)
