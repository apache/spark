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

import scala.xml.{Node, NodeSeq}

import org.apache.spark.status.AppStatusStore

/** Page showing statistics and stage list for a given job */
private[ui] class LivePage(parent: LiveTab, store: AppStatusStore) extends WebUIPage("") {

  def render(request: HttpServletRequest): Seq[Node] = {

    val driverLink: Option[String] = store.applicationInfo().driverLink.flatMap { ui =>
      // Don't make a link if the app is finished
      if (store.applicationInfo().attempts.last.completed) {
        None
      } else {
        Some(ui)
      }
    }

    val content: NodeSeq =
      <div>
      {
        if (driverLink.isDefined) {
          <a href={"%s".format(driverLink.get)}>Live UI</a>
        } else {
          "The Spark Application is finished or did not store a UI link. Sorry."
        }
      }
      </div>

    UIUtils.headerSparkPage(
      request, s"Live Links for application", content, parent, showVisualization = true)
  }
}
