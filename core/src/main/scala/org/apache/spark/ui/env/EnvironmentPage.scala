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

package org.apache.spark.ui.env

import scala.jdk.CollectionConverters._
import scala.xml.{Node, Unparsed}

import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.status.AppStatusStore
import org.apache.spark.ui._

private[ui] class EnvironmentPage(
    parent: EnvironmentTab,
    conf: SparkConf,
    store: AppStatusStore) extends WebUIPage("") {

  private val spinner =
    <div class="text-center p-3">
      <div class="spinner-border text-primary" role="status">
        <span class="visually-hidden">Loading...</span>
      </div>
    </div>

  def render(request: HttpServletRequest): Seq[Node] = {
    val defaultsMap = new java.util.HashMap[String, String]()
    ConfigEntry.knownConfigs.asScala.foreach { case (key, entry) =>
      try {
        val dvs = entry.defaultValueString
        if (dvs != ConfigEntry.UNDEFINED) {
          defaultsMap.put(key, dvs)
        }
      } catch {
        case _: Exception =>
      }
    }
    val defaultsJson = new ObjectMapper().writeValueAsString(defaultsMap)
      .replace("</", "<\\/")

    val content =
      <span>
        <div id="spark-config-defaults" class="d-none">{Unparsed(defaultsJson)}</div>
        <div class="d-flex align-items-start">
          <div class="nav flex-column nav-pills me-3" id="envTabs" role="tablist"
               aria-orientation="vertical" style="min-width: 200px;">
            <button class="nav-link active text-start" id="runtime-tab" data-bs-toggle="pill"
                    data-bs-target="#runtime" type="button" role="tab"
                    aria-controls="runtime" aria-selected="true">
              Runtime Information
            </button>
            <button class="nav-link text-start" id="spark-props-tab" data-bs-toggle="pill"
                    data-bs-target="#spark-props" type="button" role="tab"
                    aria-controls="spark-props" aria-selected="false">
              Spark Properties
            </button>
            <button class="nav-link text-start" id="resource-profiles-tab" data-bs-toggle="pill"
                    data-bs-target="#resource-profiles" type="button" role="tab"
                    aria-controls="resource-profiles" aria-selected="false">
              Resource Profiles
            </button>
            <button class="nav-link text-start" id="hadoop-props-tab" data-bs-toggle="pill"
                    data-bs-target="#hadoop-props" type="button" role="tab"
                    aria-controls="hadoop-props" aria-selected="false">
              Hadoop Properties
            </button>
            <button class="nav-link text-start" id="system-props-tab" data-bs-toggle="pill"
                    data-bs-target="#system-props" type="button" role="tab"
                    aria-controls="system-props" aria-selected="false">
              System Properties
            </button>
            <button class="nav-link text-start" id="metrics-props-tab" data-bs-toggle="pill"
                    data-bs-target="#metrics-props" type="button" role="tab"
                    aria-controls="metrics-props" aria-selected="false">
              Metrics Properties
            </button>
            <button class="nav-link text-start" id="classpath-tab" data-bs-toggle="pill"
                    data-bs-target="#classpath" type="button" role="tab"
                    aria-controls="classpath" aria-selected="false">
              Classpath Entries
            </button>
          </div>
          <div class="tab-content flex-fill" id="envTabContent">
            <div class="tab-pane fade show active" id="runtime" role="tabpanel"
                 aria-labelledby="runtime-tab">
              {spinner}
            </div>
            <div class="tab-pane fade" id="spark-props" role="tabpanel"
                 aria-labelledby="spark-props-tab">
              {spinner}
            </div>
            <div class="tab-pane fade" id="resource-profiles" role="tabpanel"
                 aria-labelledby="resource-profiles-tab">
              {spinner}
            </div>
            <div class="tab-pane fade" id="hadoop-props" role="tabpanel"
                 aria-labelledby="hadoop-props-tab">
              {spinner}
            </div>
            <div class="tab-pane fade" id="system-props" role="tabpanel"
                 aria-labelledby="system-props-tab">
              {spinner}
            </div>
            <div class="tab-pane fade" id="metrics-props" role="tabpanel"
                 aria-labelledby="metrics-props-tab">
              {spinner}
            </div>
            <div class="tab-pane fade" id="classpath" role="tabpanel"
                 aria-labelledby="classpath-tab">
              {spinner}
            </div>
          </div>
        </div>
        <script type="module"
                src={UIUtils.prependBaseUri(request, "/static/utils.js")}></script>
        <script type="module"
                src={UIUtils.prependBaseUri(request, "/static/environmentpage.js")}></script>
      </span>

    UIUtils.headerSparkPage(request, "Environment", content, parent, useDataTables = true)
  }
}

private[ui] class EnvironmentTab(
    parent: SparkUI,
    store: AppStatusStore) extends SparkUITab(parent, "environment") {
  attachPage(new EnvironmentPage(this, parent.conf, store))
}
