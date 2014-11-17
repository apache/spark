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

import java.net.URLDecoder
import javax.servlet.http.HttpServletRequest

import scala.util.Try
import scala.xml.{Text, Node}

import org.apache.spark.ui.{UIUtils, WebUIPage}

private[ui] class ExecutorThreadDumpPage(parent: ExecutorsTab) extends WebUIPage("threadDump") {

  private val sc = parent.sc

  def render(request: HttpServletRequest): Seq[Node] = {
    val executorId = Option(request.getParameter("executorId")).map {
      executorId =>
        // Due to YARN-2844, "<driver>" in the url will be encoded to "%25253Cdriver%25253E" when
        // running in yarn-cluster mode. `request.getParameter("executorId")` will return
        // "%253Cdriver%253E". Therefore we need to decode it until we get the real id.
        var id = executorId
        var decodedId = URLDecoder.decode(id, "UTF-8")
        while (id != decodedId) {
          id = decodedId
          decodedId = URLDecoder.decode(id, "UTF-8")
        }
        id
    }.getOrElse {
      return Text(s"Missing executorId parameter")
    }
    val time = System.currentTimeMillis()
    val maybeThreadDump = sc.get.getExecutorThreadDump(executorId)

    val content = maybeThreadDump.map { threadDump =>
      val dumpRows = threadDump.map { thread =>
        <div class="accordion-group">
          <div class="accordion-heading" onclick="$(this).next().toggleClass('hidden')">
            <a class="accordion-toggle">
              Thread {thread.threadId}: {thread.threadName} ({thread.threadState})
            </a>
          </div>
          <div class="accordion-body hidden">
            <div class="accordion-inner">
              <pre>{thread.stackTrace}</pre>
            </div>
          </div>
        </div>
      }

      <div class="row-fluid">
        <p>Updated at {UIUtils.formatDate(time)}</p>
        {
          // scalastyle:off
          <p><a class="expandbutton"
                onClick="$('.accordion-body').removeClass('hidden'); $('.expandbutton').toggleClass('hidden')">
            Expand All
          </a></p>
          <p><a class="expandbutton hidden"
                onClick="$('.accordion-body').addClass('hidden'); $('.expandbutton').toggleClass('hidden')">
            Collapse All
          </a></p>
          // scalastyle:on
        }
        <div class="accordion">{dumpRows}</div>
      </div>
    }.getOrElse(Text("Error fetching thread dump"))
    UIUtils.headerSparkPage(s"Thread dump for executor $executorId", content, parent)
  }
}
