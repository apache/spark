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

package org.apache.spark.ui.exec

import scala.xml.{Node, Text}

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.SparkContext
import org.apache.spark.ui.{SparkUITab, UIUtils, WebUIPage}

private[ui] class ExecutorHeapHistogramPage(
    parent: SparkUITab,
    sc: Option[SparkContext]) extends WebUIPage("heapHistogram") {

  // Match the lines containing object informations
  val pattern = """\s*([0-9]+):\s+([0-9]+)\s+([0-9]+)\s+(\S+)(.*)""".r

  def render(request: HttpServletRequest): Seq[Node] = {
    val executorId = Option(request.getParameter("executorId")).map { executorId =>
      UIUtils.decodeURLParameter(executorId)
    }.getOrElse {
      throw new IllegalArgumentException(s"Missing executorId parameter")
    }
    val time = System.currentTimeMillis()
    val maybeHeapHistogram = sc.get.getExecutorHeapHistogram(executorId)

    val content = maybeHeapHistogram.map { heapHistogram =>
      val rows = heapHistogram.map { row =>
        row match {
          case pattern(rank, instances, bytes, name, module) =>
            <tr class="accordion-heading">
              <td>{rank}</td>
              <td>{instances}</td>
              <td>{bytes}</td>
              <td>{name}</td>
              <td>{module}</td>
            </tr>
          case pattern(rank, instances, bytes, name) =>
            <tr class="accordion-heading">
              <td>{rank}</td>
              <td>{instances}</td>
              <td>{bytes}</td>
              <td>{name}</td>
              <td></td>
            </tr>
          case _ =>
            // Ignore the first two lines and the last line
            //
            //  num     #instances         #bytes  class name (module)
            // -------------------------------------------------------
            // ...
            // Total       1267867       72845688
        }
      }
      <div class="row">
        <div class="col-12">
          <p>Updated at {UIUtils.formatDate(time)}</p>
          <table class={UIUtils.TABLE_CLASS_STRIPED + " accordion-group" + " sortable"}>
            <thead>
              <th>Rank</th>
              <th>Instances</th>
              <th>Bytes</th>
              <th>Class Name</th>
              <th>Module</th>
            </thead>
            <tbody>{rows}</tbody>
          </table>
        </div>
      </div>
    }.getOrElse(Text("Error fetching heap histogram"))
    UIUtils.headerSparkPage(request, s"Heap Histogram for Executor $executorId", content, parent)
  }
}
