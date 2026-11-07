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

import jakarta.servlet.http.HttpServletRequest
import org.mockito.Mockito.mock

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerEnvironmentUpdate}
import org.apache.spark.status.{AppStatusListener, AppStatusStore, AppSummary, ElementTrackingStore}
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.InMemoryStore

/**
 * The History Server rebuilds an application's UI from its event log, where spark.redaction.regex
 * may have replaced any Spark property value with Utils.REDACTION_REPLACEMENT_TEXT. The Jobs page,
 * which is the first page shown for an application, must render such an application.
 */
class RedactedEnvironmentSuite extends SparkFunSuite {

  private val redacted = Utils.REDACTION_REPLACEMENT_TEXT

  /** Replays a finished application the way the History Server does (live = false). */
  private def replay(sparkProperties: Option[Seq[(String, String)]]): AppStatusStore = {
    val conf = new SparkConf(false)
    val kvstore = new ElementTrackingStore(new InMemoryStore(), conf)
    val listener = new AppStatusListener(kvstore, conf, live = false)
    listener.onApplicationStart(
      SparkListenerApplicationStart("redacted-app", Some("app-1"), 1000L, "user", None))
    sparkProperties.foreach { props =>
      listener.onEnvironmentUpdate(SparkListenerEnvironmentUpdate(Map(
        "JVM Information" -> Seq("Java Version" -> "17", "Java Home" -> "/jvm",
          "Scala Version" -> "2.13"),
        "Spark Properties" -> props,
        "Hadoop Properties" -> Nil,
        "System Properties" -> Nil,
        "Metrics Properties" -> Nil,
        "Classpath Entries" -> Nil)))
    }
    listener.onApplicationEnd(SparkListenerApplicationEnd(2000L))
    kvstore.write(new AppSummary(0, 0))
    new AppStatusStore(kvstore)
  }

  private def renderJobsPage(store: AppStatusStore): String = {
    val conf = new SparkConf(false)
    val ui = SparkUI.create(None, store, conf, new SecurityManager(conf), "redacted-app",
      "/history/app-1", 1000L)
    val jobsTab = ui.getTabs.collectFirst { case t: JobsTab => t }.get
    val page = jobsTab.pages.collectFirst { case p: AllJobsPage => p }.get
    page.render(mock(classOf[HttpServletRequest])).toString
  }

  private def schedulingModeShown(html: String): String = {
    val shown = "(?s)Scheduling Mode: </strong>\\s*([^<]*?)\\s*</li>".r
    shown.findFirstMatchIn(html).map(_.group(1)).getOrElse(fail(s"No scheduling mode in $html"))
  }

  test("SPARK-60714: AllJobsPage renders when spark.scheduler.mode is redacted") {
    val html = renderJobsPage(replay(Some(Seq("spark.scheduler.mode" -> redacted))))
    assert(schedulingModeShown(html) === redacted)
  }

  test("SPARK-60714: AllJobsPage shows a known scheduling mode in upper case") {
    val html = renderJobsPage(replay(Some(Seq("spark.scheduler.mode" -> "fair"))))
    assert(schedulingModeShown(html) === "FAIR")
  }

  test("SPARK-60714: AllJobsPage renders when the application environment is missing") {
    val html = renderJobsPage(replay(None))
    assert(schedulingModeShown(html) === "Unknown")
  }

  test("SPARK-60714: redacted spark.task.cpus keeps the application environment") {
    val store = replay(Some(Seq("spark.scheduler.mode" -> redacted,
      "spark.task.cpus" -> redacted)))
    assert(store.environmentInfo().sparkProperties.toMap.get("spark.task.cpus") ===
      Some(redacted))
    assert(schedulingModeShown(renderJobsPage(store)) === redacted)
  }
}
