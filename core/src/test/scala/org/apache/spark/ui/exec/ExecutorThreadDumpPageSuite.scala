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

import java.nio.charset.StandardCharsets

import scala.xml.{Elem, Group, Node, NodeSeq}

import jakarta.servlet.http.HttpServletRequest
import org.mockito.Mockito.{mock, when}

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.internal.config.UI.UI_THREAD_DUMP_DOWNLOAD_IN_BROWSER_ENABLED
import org.apache.spark.status.api.v1.{StackTrace, ThreadStackTrace}
import org.apache.spark.ui.SparkUITab
import org.apache.spark.util.Utils

class ExecutorThreadDumpPageSuite extends SparkFunSuite {

  // A thread whose name contains '#' is followed by another thread. A data: URI link is truncated
  // by browsers at the first '#', which would drop the rest of the dump.
  private val threadDump = Array(
    threadStackTrace(55, "Executor task idle worker#55",
      "java.base/java.lang.Thread.run(Thread.java:840)\n"),
    threadStackTrace(1, "main",
      "org.apache.spark.SparkContext.<init>(SparkContext.scala:100)\n"))

  private val threadDumpText = threadDump.map(_.toString).mkString

  private def threadStackTrace(id: Long, name: String, frame: String): ThreadStackTrace =
    ThreadStackTrace(id, name, Thread.State.RUNNABLE, StackTrace(Seq(frame)), None, "",
      Seq.empty, Seq.empty, Seq.empty, None, None, false, false, false, 5)

  private def renderPage(downloadInBrowserEnabled: Boolean): NodeSeq = {
    val conf = new SparkConf(false)
      .set(UI_THREAD_DUMP_DOWNLOAD_IN_BROWSER_ENABLED, downloadInBrowserEnabled)
    val sc = mock(classOf[SparkContext])
    when(sc.conf).thenReturn(conf)
    when(sc.getReadOnlyConf).thenReturn(conf)
    when(sc.getExecutorThreadDump("1")).thenReturn(Some(threadDump))

    val tab = mock(classOf[SparkUITab])
    when(tab.appName).thenReturn("test")
    when(tab.basePath).thenReturn("")
    when(tab.headerTabs).thenReturn(Seq.empty)

    val request = mock(classOf[HttpServletRequest])
    when(request.getParameter("executorId")).thenReturn("1")
    NodeSeq.fromSeq(new ExecutorThreadDumpPage(tab, Some(sc)).render(request))
  }

  // All elements of the page. NodeSeq's `\\` cannot be used: it fails on the page's <xml:group>.
  private def elements(nodes: Seq[Node]): Seq[Elem] = nodes.flatMap {
    case group: Group => elements(group.nodes)
    case elem: Elem => elem +: elements(elem.child)
    case _ => Nil
  }

  private def downloadControl(page: NodeSeq): Elem = {
    val controls =
      elements(page).filter(elem => (elem \@ "class").split(" ").contains("downloadbutton"))
    assert(controls.length === 1)
    controls.head
  }

  private def embeddedThreadDump(page: NodeSeq): Option[String] =
    elements(page).find(e => e.label == "pre" && e \@ "id" == "thread-dump-text").map(_.text)

  test("Download builds the file in the browser from the embedded thread dump") {
    val page = renderPage(downloadInBrowserEnabled = true)

    val download = downloadControl(page)
    assert(download.label === "a")
    assert(download.attribute("href").isEmpty)
    assert(download \@ "data-action" === "downloadThreadDump")
    assert(download \@ "data-filename" === "threaddump_1.txt")
    // The hidden element holds the complete dump, including every thread after the '#'.
    assert(embeddedThreadDump(page) === Some(threadDumpText))
  }

  test("table.js handles the Download markup the page renders") {
    // Only a browser runs table.js against the rendered page, so check that the click listener
    // and the download helper use the data-action, data-filename and element id the page emits.
    val page = renderPage(downloadInBrowserEnabled = true)
    val download = downloadControl(page)
    val dumpElement = elements(page).find(e => e.label == "pre" && e.text == threadDumpText)
    assert(dumpElement.isDefined, "the page must embed the thread dump")
    assert(download.attribute("data-filename").isDefined)

    val tableJs = Utils.tryWithResource(
        getClass.getResourceAsStream("/org/apache/spark/ui/static/table.js")) { in =>
      new String(in.readAllBytes(), StandardCharsets.UTF_8)
    }
    assert(tableJs.contains(s"[data-action=${download \@ "data-action"}]"))
    assert(tableJs.contains(""".data("filename")"""))
    assert(tableJs.contains(s"""getElementById("${dumpElement.get \@ "id"}")"""))
  }

  test("Download is the legacy data: URI link when the kill switch is off") {
    val page = renderPage(downloadInBrowserEnabled = false)

    val download = downloadControl(page)
    assert(download.label === "a")
    assert(download \@ "href" === "data:text/plain;charset=utf-8," + threadDumpText)
    assert(download \@ "download" === "threaddump_1.txt")
    assert(download.attribute("data-action").isEmpty)
    assert(embeddedThreadDump(page).isEmpty)
  }
}
