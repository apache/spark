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

package org.apache.spark.ui.storage

import javax.servlet.http.HttpServletRequest

import org.eclipse.jetty.server.Handler

import org.apache.spark.ui.JettyUtils._
import org.apache.spark.ui._

/** Web UI showing storage status of all RDD's in the given SparkContext. */
private[ui] class BlockManagerUI(parent: SparkUI) {
  lazy val appName = parent.appName
  lazy val listener = _listener.get

  private val indexPage = new IndexPage(this)
  private val rddPage = new RDDPage(this)
  private var _listener: Option[BlockManagerListener] = None

  def start() {
    _listener = Some(new BlockManagerListener)
  }

  def getHandlers = Seq[(String, Handler)](
    ("/storage/rdd", (request: HttpServletRequest) => rddPage.render(request)),
    ("/storage", (request: HttpServletRequest) => indexPage.render(request))
  )
}

/**
 * A SparkListener that prepares information to be displayed on the BlockManagerUI
 */
private[ui] class BlockManagerListener extends RDDInfoSparkListener
