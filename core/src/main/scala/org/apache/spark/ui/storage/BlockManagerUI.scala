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

import scala.concurrent.duration._

import javax.servlet.http.HttpServletRequest

import org.eclipse.jetty.server.Handler

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.ui.JettyUtils._

/** Web UI showing storage status of all RDD's in the given SparkContext. */
private[spark] class BlockManagerUI(val sc: SparkContext) extends Logging {
  val indexPage = new IndexPage(this)
  val rddPage = new RDDPage(this)

  def getHandlers = Seq[(String, Handler)](
    ("/storage/rdd", (request: HttpServletRequest) => rddPage.render(request)),
    ("/storage", (request: HttpServletRequest) => indexPage.render(request))
  )
}
