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

import javax.servlet.http.HttpServletRequest

import scala.collection.mutable.ArrayBuffer
import scala.xml.Node

import org.eclipse.jetty.servlet.ServletContextHandler
import org.json4s.JsonAST.{JNothing, JValue}

import org.apache.spark.SecurityManager
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.Utils

/**
 * The top level component of the UI hierarchy that contains the server.
 *
 * Each WebUI represents a collection of tabs, each of which in turn represents a collection of
 * pages. The use of tabs is optional, however; a WebUI may choose to include pages directly.
 */
private[spark] abstract class WebUI(securityManager: SecurityManager, basePath: String = "") {
  protected val tabs = ArrayBuffer[UITab]()
  protected val handlers = ArrayBuffer[ServletContextHandler]()
  protected var serverInfo: Option[ServerInfo] = None

  def getTabs: Seq[UITab] = tabs.toSeq
  def getHandlers: Seq[ServletContextHandler] = handlers.toSeq
  def getListeners: Seq[SparkListener] = tabs.flatMap(_.listener)

  /** Attach a tab to this UI, along with all of its attached pages. */
  def attachTab(tab: UITab) {
    tab.start()
    tab.pages.foreach(attachPage)
    tabs += tab
  }

  /** Attach a page to this UI. */
  def attachPage(page: UIPage) {
    val pagePath = "/" + page.prefix
    attachHandler(createServletHandler(pagePath,
      (request: HttpServletRequest) => page.render(request), securityManager, basePath))
    if (page.includeJson) {
      attachHandler(createServletHandler(pagePath.stripSuffix("/") + "/json",
        (request: HttpServletRequest) => page.renderJson(request), securityManager, basePath))
    }
  }

  /** Attach a handler to this UI. */
  def attachHandler(handler: ServletContextHandler) {
    handlers += handler
    serverInfo.foreach { info =>
      info.rootHandler.addHandler(handler)
      if (!handler.isStarted) {
        handler.start()
      }
    }
  }

  /** Detach a handler from this UI. */
  def detachHandler(handler: ServletContextHandler) {
    handlers -= handler
    serverInfo.foreach { info =>
      info.rootHandler.removeHandler(handler)
      if (handler.isStarted) {
        handler.stop()
      }
    }
  }

  /** Initialize all components of the server. */
  def start()

  /**
   * Bind to the HTTP server behind this web interface.
   * Overridden implementation should set serverInfo.
   */
  def bind()

  /** Return the actual port to which this server is bound. Only valid after bind(). */
  def boundPort: Int = serverInfo.map(_.boundPort).getOrElse(-1)

  /** Stop the server behind this web interface. Only valid after bind(). */
  def stop() {
    assert(serverInfo.isDefined,
      "Attempted to stop %s before binding to a server!".format(Utils.getFormattedClassName(this)))
    serverInfo.get.server.stop()
  }
}


/**
 * A tab that represents a collection of pages and a unit of listening for Spark events.
 * Associating each tab with a listener is arbitrary and need not be the case.
 */
private[spark] abstract class UITab(val prefix: String) {
  val pages = ArrayBuffer[UIPage]()
  var listener: Option[SparkListener] = None
  var name = prefix.capitalize

  /** Attach a page to this tab. This prepends the page's prefix with the tab's own prefix. */
  def attachPage(page: UIPage) {
    page.prefix = (prefix + "/" + page.prefix).stripSuffix("/")
    pages += page
  }

  /** Initialize listener and attach pages. */
  def start()
}


/**
 * A page that represents the leaf node in the UI hierarchy.
 *
 * If includeJson is true, the parent WebUI (direct or indirect) creates handlers for both the
 * HTML and the JSON content, rather than just the former.
 */
private[spark] abstract class UIPage(var prefix: String, val includeJson: Boolean = false) {
  def render(request: HttpServletRequest): Seq[Node] = Seq[Node]()
  def renderJson(request: HttpServletRequest): JValue = JNothing
}
