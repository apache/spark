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

import java.net.InetSocketAddress
import java.net.URL
import javax.servlet.http.HttpServlet
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}

import scala.annotation.tailrec
import scala.util.{Try, Success, Failure}
import scala.xml.Node

import net.liftweb.json.{JValue, pretty, render}

import org.eclipse.jetty.server.{DispatcherType, Server}
import org.eclipse.jetty.server.handler.{ResourceHandler, HandlerList, ContextHandler, AbstractHandler}
import org.eclipse.jetty.servlet.{DefaultServlet, FilterHolder, ServletContextHandler, ServletHolder}
import org.eclipse.jetty.util.thread.QueuedThreadPool

import org.apache.spark.Logging
import org.apache.spark.SparkEnv
import org.apache.spark.SecurityManager


/** Utilities for launching a web server using Jetty's HTTP Server class */
private[spark] object JettyUtils extends Logging {
  // Base type for a function that returns something based on an HTTP request. Allows for
  // implicit conversion from many types of functions to jetty Handlers.

  type Responder[T] = HttpServletRequest => T

  // Conversions from various types of Responder's to jetty Handlers
  implicit def jsonResponderToServlet(responder: Responder[JValue]): HttpServlet =
    createServlet(responder, "text/json", (in: JValue) => pretty(render(in)))

  implicit def htmlResponderToServlet(responder: Responder[Seq[Node]]): HttpServlet =
    createServlet(responder, "text/html", (in: Seq[Node]) => "<!DOCTYPE html>" + in.toString)

  implicit def textResponderToServlet(responder: Responder[String]): HttpServlet =
    createServlet(responder, "text/plain")

  def createServlet[T <% AnyRef](responder: Responder[T], contentType: String, 
                                 extractFn: T => String = (in: Any) => in.toString): HttpServlet = {
    new HttpServlet {
      override def doGet(request: HttpServletRequest,
                 response: HttpServletResponse) {
        // First try to get the security Manager from the SparkEnv. If that doesn't exist, create
        // a new one and rely on the configs being set
        val sparkEnv = SparkEnv.get
        val securityMgr = if (sparkEnv != null) sparkEnv.securityManager else new SecurityManager()
        if (securityMgr.checkUIViewPermissions(request.getRemoteUser())) {
          response.setContentType("%s;charset=utf-8".format(contentType))
          response.setStatus(HttpServletResponse.SC_OK)
          val result = responder(request)
          response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate")
          response.getWriter().println(extractFn(result))
        } else {
          response.setStatus(HttpServletResponse.SC_UNAUTHORIZED)
          response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate")
          response.sendError(HttpServletResponse.SC_UNAUTHORIZED,
            "User is not authorized to access this page.");
        }
      }
    }
  }

  def createServletHandler(path: String, servlet: HttpServlet): ServletContextHandler = {
    val contextHandler = new ServletContextHandler()
    val holder = new ServletHolder(servlet)
    contextHandler.setContextPath(path)
    contextHandler.addServlet(holder, "/")
    contextHandler
  }

  /** Creates a handler that always redirects the user to a given path */
  def createRedirectHandler(newPath: String, path: String): ServletContextHandler = {
    val servlet = new HttpServlet {
      override def doGet(request: HttpServletRequest,
                 response: HttpServletResponse) {
        // make sure we don't end up with // in the middle
        val newUri = new URL(new URL(request.getRequestURL.toString), newPath).toURI
        response.sendRedirect(newUri.toString)
      }
    }
    val contextHandler = new ServletContextHandler()
    val holder = new ServletHolder(servlet)
    contextHandler.setContextPath(path)
    contextHandler.addServlet(holder, "/")
    contextHandler
  }

  /** Creates a handler for serving files from a static directory */
  def createStaticHandler(resourceBase: String, path: String): ServletContextHandler = {
    val contextHandler = new ServletContextHandler()
    val staticHandler = new DefaultServlet
    val holder = new ServletHolder(staticHandler)
    Option(getClass.getClassLoader.getResource(resourceBase)) match {
      case Some(res) =>
        holder.setInitParameter("resourceBase", res.toString)
      case None =>
        throw new Exception("Could not find resource path for Web UI: " + resourceBase)
    }
    contextHandler.addServlet(holder, path)
    contextHandler
  }

  private def addFilters(handlers: Seq[ServletContextHandler]) {
    val filters: Array[String] = System.getProperty("spark.ui.filters", "").split(',').map(_.trim())
    filters.foreach {
      case filter : String => 
        if (!filter.isEmpty) {
          logInfo("Adding filter: " + filter)
          val holder : FilterHolder = new FilterHolder()
          holder.setClassName(filter)
          // get any parameters for each filter
          val paramName = filter + ".params"
          val params = System.getProperty(paramName, "").split(',').map(_.trim()).toSet
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
   * Attempts to start a Jetty server at the supplied hostName:port which uses the supplied
   * handlers.
   *
   * If the desired port number is contented, continues incrementing ports until a free port is
   * found. Returns the chosen port and the jetty Server object.
   */
  def startJettyServer(hostName: String, port: Int, handlers: Seq[ServletContextHandler]): 
    (Server, Int) = {

    addFilters(handlers)
    val handlerList = new HandlerList
    handlerList.setHandlers(handlers.toArray)

    @tailrec
    def connect(currentPort: Int): (Server, Int) = {
      val server = new Server(new InetSocketAddress(hostName, currentPort))
      val pool = new QueuedThreadPool
      pool.setDaemon(true)
      server.setThreadPool(pool)
      server.setHandler(handlerList)

      Try { server.start() } match {
        case s: Success[_] =>
          (server, server.getConnectors.head.getLocalPort)
        case f: Failure[_] =>
          server.stop()
          logInfo("Failed to create UI at port, %s. Trying again.".format(currentPort))
          logInfo("Error was: " + f.toString)
          connect((currentPort + 1) % 65536)
      }
    }

    connect(port)
  }
}
