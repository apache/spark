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

import scala.collection.mutable.StringBuilder
import scala.xml.Node

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.SparkConf
import org.apache.spark.resource.{ExecutorResourceRequest, TaskResourceRequest}
import org.apache.spark.status.AppStatusStore
import org.apache.spark.ui._
import org.apache.spark.util.Utils

private[ui] class EnvironmentPage(
    parent: EnvironmentTab,
    conf: SparkConf,
    store: AppStatusStore) extends WebUIPage("") {

  def render(request: HttpServletRequest): Seq[Node] = {
    val appEnv = store.environmentInfo()
    val jvmInformation = Map(
      "Java Version" -> appEnv.runtime.javaVersion,
      "Java Home" -> appEnv.runtime.javaHome,
      "Scala Version" -> appEnv.runtime.scalaVersion)

    def constructExecutorRequestString(execReqs: Map[String, ExecutorResourceRequest]): String = {
      execReqs.map {
        case (_, execReq) =>
          val execStr = new StringBuilder(s"\t${execReq.resourceName}: [amount: ${execReq.amount}")
          if (execReq.discoveryScript.nonEmpty) {
            execStr ++= s", discovery: ${execReq.discoveryScript}"
          }
          if (execReq.vendor.nonEmpty) {
            execStr ++= s", vendor: ${execReq.vendor}"
          }
          execStr ++= "]"
          execStr.toString()
      }.mkString("\n")
    }

    def constructTaskRequestString(taskReqs: Map[String, TaskResourceRequest]): String = {
      taskReqs.map {
        case (_, taskReq) => s"\t${taskReq.resourceName}: [amount: ${taskReq.amount}]"
      }.mkString("\n")
    }

    val resourceProfileInfo = store.resourceProfileInfo().map { rinfo =>
      val einfo = constructExecutorRequestString(rinfo.executorResources)
      val tinfo = constructTaskRequestString(rinfo.taskResources)
      val res = s"Executor Reqs:\n$einfo\nTask Reqs:\n$tinfo"
      (rinfo.id.toString, res)
    }.toMap

    val resourceProfileInformationTable = UIUtils.listingTable(resourceProfileHeader,
      jvmRowDataPre, resourceProfileInfo.toSeq.sortWith(_._1.toInt < _._1.toInt),
      fixedWidth = true, headerClasses = headerClassesNoSortValues)
    val runtimeInformationTable = UIUtils.listingTable(
      propertyHeader, jvmRow, jvmInformation.toSeq.sorted, fixedWidth = true,
      headerClasses = headerClasses)
    val sparkProperties = Utils.redact(conf, appEnv.sparkProperties.sorted)
    val sparkPropertiesTable = UIUtils.listingTable(propertyHeader, propertyRow,
      sparkProperties, fixedWidth = true, headerClasses = headerClasses)
    val emptyProperties = collection.Seq.empty[(String, String)]
    val hadoopProperties =
      Utils.redact(conf, Option(appEnv.hadoopProperties).getOrElse(emptyProperties).sorted)
    val hadoopPropertiesTable = UIUtils.listingTable(propertyHeader, propertyRow,
      hadoopProperties, fixedWidth = true, headerClasses = headerClasses)
    val systemProperties = Utils.redact(conf, appEnv.systemProperties.sorted)
    val systemPropertiesTable = UIUtils.listingTable(propertyHeader, propertyRow,
      systemProperties, fixedWidth = true, headerClasses = headerClasses)
    val metricsProperties =
      Utils.redact(conf, Option(appEnv.metricsProperties).getOrElse(emptyProperties).sorted)
    val metricsPropertiesTable = UIUtils.listingTable(propertyHeader, propertyRow,
      metricsProperties, fixedWidth = true, headerClasses = headerClasses)
    val classpathEntries = appEnv.classpathEntries.sorted
    val classpathEntriesTable = UIUtils.listingTable(
      classPathHeader, classPathRow, classpathEntries, fixedWidth = true,
      headerClasses = headerClasses)
    val content =
      <span>
        <div class="d-flex align-items-start">
          <div class="nav flex-column nav-pills me-3" id="envTabs" role="tablist"
               aria-orientation="vertical" style="min-width: 200px;">
            <button class="nav-link active text-start" id="runtime-tab" data-bs-toggle="pill"
                    data-bs-target="#runtime" type="button" role="tab"
                    aria-controls="runtime" aria-selected="true">
              Runtime Information
              <span class="badge bg-secondary ms-1">{jvmInformation.size}</span>
            </button>
            <button class="nav-link text-start" id="spark-props-tab" data-bs-toggle="pill"
                    data-bs-target="#spark-props" type="button" role="tab"
                    aria-controls="spark-props" aria-selected="false">
              Spark Properties
              <span class="badge bg-secondary ms-1">{sparkProperties.size}</span>
            </button>
            <button class="nav-link text-start" id="resource-profiles-tab" data-bs-toggle="pill"
                    data-bs-target="#resource-profiles" type="button" role="tab"
                    aria-controls="resource-profiles" aria-selected="false">
              Resource Profiles
              <span class="badge bg-secondary ms-1">{resourceProfileInfo.size}</span>
            </button>
            <button class="nav-link text-start" id="hadoop-props-tab" data-bs-toggle="pill"
                    data-bs-target="#hadoop-props" type="button" role="tab"
                    aria-controls="hadoop-props" aria-selected="false">
              Hadoop Properties
              <span class="badge bg-secondary ms-1">{hadoopProperties.size}</span>
            </button>
            <button class="nav-link text-start" id="system-props-tab" data-bs-toggle="pill"
                    data-bs-target="#system-props" type="button" role="tab"
                    aria-controls="system-props" aria-selected="false">
              System Properties
              <span class="badge bg-secondary ms-1">{systemProperties.size}</span>
            </button>
            <button class="nav-link text-start" id="metrics-props-tab" data-bs-toggle="pill"
                    data-bs-target="#metrics-props" type="button" role="tab"
                    aria-controls="metrics-props" aria-selected="false">
              Metrics Properties
              <span class="badge bg-secondary ms-1">{metricsProperties.size}</span>
            </button>
            <button class="nav-link text-start" id="classpath-tab" data-bs-toggle="pill"
                    data-bs-target="#classpath" type="button" role="tab"
                    aria-controls="classpath" aria-selected="false">
              Classpath Entries
              <span class="badge bg-secondary ms-1">{classpathEntries.size}</span>
            </button>
          </div>
          <div class="tab-content flex-fill" id="envTabContent">
            <div class="tab-pane fade show active" id="runtime" role="tabpanel"
                 aria-labelledby="runtime-tab">
              {runtimeInformationTable}
            </div>
            <div class="tab-pane fade" id="spark-props" role="tabpanel"
                 aria-labelledby="spark-props-tab">
              {sparkPropertiesTable}
            </div>
            <div class="tab-pane fade" id="resource-profiles" role="tabpanel"
                 aria-labelledby="resource-profiles-tab">
              {resourceProfileInformationTable}
            </div>
            <div class="tab-pane fade" id="hadoop-props" role="tabpanel"
                 aria-labelledby="hadoop-props-tab">
              {hadoopPropertiesTable}
            </div>
            <div class="tab-pane fade" id="system-props" role="tabpanel"
                 aria-labelledby="system-props-tab">
              {systemPropertiesTable}
            </div>
            <div class="tab-pane fade" id="metrics-props" role="tabpanel"
                 aria-labelledby="metrics-props-tab">
              {metricsPropertiesTable}
            </div>
            <div class="tab-pane fade" id="classpath" role="tabpanel"
                 aria-labelledby="classpath-tab">
              {classpathEntriesTable}
            </div>
          </div>
        </div>
        <script src={UIUtils.prependBaseUri(request, "/static/environmentpage.js")}></script>
      </span>

    UIUtils.headerSparkPage(request, "Environment", content, parent)
  }

  private def resourceProfileHeader = Seq("Resource Profile Id", "Resource Profile Contents")
  private def propertyHeader = Seq("Name", "Value")
  private def classPathHeader = Seq("Resource", "Source")
  private def headerClasses = Seq("sorttable_alpha", "sorttable_alpha")
  private def headerClassesNoSortValues = Seq("sorttable_numeric", "sorttable_nosort")

  private def jvmRowDataPre(kv: (String, String)) =
    <tr><td>{kv._1}</td><td><pre>{kv._2}</pre></td></tr>
  private def jvmRow(kv: (String, String)) = <tr><td>{kv._1}</td><td>{kv._2}</td></tr>
  private def propertyRow(kv: (String, String)) = <tr><td>{kv._1}</td><td>{kv._2}</td></tr>
  private def classPathRow(data: (String, String)) = <tr><td>{data._1}</td><td>{data._2}</td></tr>
}

private[ui] class EnvironmentTab(
    parent: SparkUI,
    store: AppStatusStore) extends SparkUITab(parent, "environment") {
  attachPage(new EnvironmentPage(this, parent.conf, store))
}
