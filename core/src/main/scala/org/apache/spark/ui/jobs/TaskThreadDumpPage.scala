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

package org.apache.spark.ui.jobs

import scala.xml.{Node, Text}

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.SparkContext
import org.apache.spark.ui.{SparkUITab, UIUtils, WebUIPage}

private[spark] class TaskThreadDumpPage(
    parent: SparkUITab,
    sc: Option[SparkContext]) extends WebUIPage("taskThreadDump") {

  private def executorThreadDumpUrl(
      request: HttpServletRequest,
      executorId: String,
      blockingThreadId: Long): String = {
    val uiRoot = UIUtils.prependBaseUri(request, parent.basePath)
    s"$uiRoot/executors/?executorId=$executorId#${blockingThreadId}_td_id"
  }

  override def render(request: HttpServletRequest): Seq[Node] = {
    val executorId = Option(request.getParameter("executorId")).map { executorId =>
      UIUtils.decodeURLParameter(executorId)
    }.getOrElse {
      throw new IllegalArgumentException(s"Missing executorId parameter")
    }
    val taskId = Option(request.getParameter("taskId")).map { taskId =>
      val decoded = UIUtils.decodeURLParameter(taskId)
      decoded.toLong
    }.getOrElse {
      throw new IllegalArgumentException(s"Missing taskId parameter")
    }

    val time = System.currentTimeMillis()
    val maybeThreadDump = sc.get.getTaskThreadDump(taskId, executorId)

    val content = maybeThreadDump.map { thread =>
      val threadId = thread.threadId
      val blockedBy = thread.blockedByThreadId match {
        case Some(blockingThreadId) =>
          <div>
            Blocked by
            <a href={executorThreadDumpUrl(request, executorId, blockingThreadId)}>
              Thread
              {blockingThreadId}{thread.blockedByLock}
            </a>
          </div>
        case None => Text("")
      }
      val synchronizers = thread.synchronizers.map(l => s"Lock($l)")
      val monitors = thread.monitors.map(m => s"Monitor($m)")
      val heldLocks = (synchronizers ++ monitors).mkString(", ")

      <div class="row">
        <div class="col-12">
          <p>Updated at {UIUtils.formatDate(time)}</p>
          <table class={UIUtils.TABLE_CLASS_NOT_STRIPED + " accordion-group"}>
            <thead>
              <th>Thread ID</th>
              <th>Thread Name</th>
              <th>Thread State</th>
              <th>
                <span data-toggle="tooltip" data-placement="top"
                      title="Objects whose lock the thread currently holds">
                  Thread Locks
                </span>
              </th>
            </thead>
            <tbody>
              <tr>
                <td>{threadId}</td>
                <td>{thread.threadName}</td>
                <td>{thread.threadState}</td>
                <td>{blockedBy}{heldLocks}</td>
              </tr>
            </tbody>
          </table>
        </div>

        <div>
          <div>
            <pre>{thread.toString}</pre>
          </div>
        </div>
      </div>
    }.getOrElse{
      Text(s"Task $taskId finished or some error occurred during dumping thread")
    }
    UIUtils.headerSparkPage(request, s"Thread dump for task $taskId", content, parent)
  }
}
