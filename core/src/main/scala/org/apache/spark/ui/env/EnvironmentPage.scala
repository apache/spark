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
    val sparkPropertiesTable = UIUtils.listingTable(propertyHeader, propertyRow,
      Utils.redact(conf, appEnv.sparkProperties.sorted), fixedWidth = true,
      headerClasses = headerClasses)
    val emptyProperties = collection.Seq.empty[(String, String)]
    val hadoopPropertiesTable = UIUtils.listingTable(propertyHeader, propertyRow,
      Utils.redact(conf, Option(appEnv.hadoopProperties).getOrElse(emptyProperties).sorted),
      fixedWidth = true, headerClasses = headerClasses)
    val systemPropertiesTable = UIUtils.listingTable(propertyHeader, propertyRow,
      Utils.redact(conf, appEnv.systemProperties.sorted), fixedWidth = true,
      headerClasses = headerClasses)
    val metricsPropertiesTable = UIUtils.listingTable(propertyHeader, propertyRow,
      Utils.redact(conf, Option(appEnv.metricsProperties).getOrElse(emptyProperties).sorted),
      fixedWidth = true, headerClasses = headerClasses)
    val classpathEntriesTable = UIUtils.listingTable(
      classPathHeader, classPathRow, appEnv.classpathEntries.sorted, fixedWidth = true,
      headerClasses = headerClasses)
    val content =
      <span>
        <span class="collapse-table" data-bs-toggle="collapse"
            data-bs-target="#aggregated-runtimeInformation"
            aria-expanded="true" aria-controls="aggregated-runtimeInformation"
            data-collapse-name="collapse-aggregated-runtimeInformation">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Runtime Information</a>
          </h4>
        </span>
        <div class="collapsible-table collapse show" id="aggregated-runtimeInformation">
          {runtimeInformationTable}
        </div>
        <span class="collapse-table" data-bs-toggle="collapse"
            data-bs-target="#aggregated-sparkProperties"
            aria-expanded="true" aria-controls="aggregated-sparkProperties"
            data-collapse-name="collapse-aggregated-sparkProperties">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Spark Properties</a>
          </h4>
        </span>
        <div class="collapsible-table collapse show" id="aggregated-sparkProperties">
          {sparkPropertiesTable}
        </div>
        <span class="collapse-table" data-bs-toggle="collapse"
            data-bs-target="#aggregated-execResourceProfileInformation"
            aria-expanded="true"
            aria-controls="aggregated-execResourceProfileInformation"
            data-collapse-name="collapse-aggregated-execResourceProfileInformation">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Resource Profiles</a>
          </h4>
        </span>
        <div class="collapsible-table collapse show"
            id="aggregated-execResourceProfileInformation">
          {resourceProfileInformationTable}
        </div>
        <span class="collapse-table" data-bs-toggle="collapse"
            data-bs-target="#aggregated-hadoopProperties"
            aria-expanded="false" aria-controls="aggregated-hadoopProperties"
            data-collapse-name="collapse-aggregated-hadoopProperties">
          <h4>
            <span class="collapse-table-arrow arrow-closed"></span>
            <a>Hadoop Properties</a>
          </h4>
        </span>
        <div class="collapsible-table collapse" id="aggregated-hadoopProperties">
          {hadoopPropertiesTable}
        </div>
        <span class="collapse-table" data-bs-toggle="collapse"
            data-bs-target="#aggregated-systemProperties"
            aria-expanded="false" aria-controls="aggregated-systemProperties"
            data-collapse-name="collapse-aggregated-systemProperties">
          <h4>
            <span class="collapse-table-arrow arrow-closed"></span>
            <a>System Properties</a>
          </h4>
        </span>
        <div class="collapsible-table collapse" id="aggregated-systemProperties">
          {systemPropertiesTable}
        </div>
        <span class="collapse-table" data-bs-toggle="collapse"
            data-bs-target="#aggregated-metricsProperties"
            aria-expanded="false" aria-controls="aggregated-metricsProperties"
            data-collapse-name="collapse-aggregated-metricsProperties">
          <h4>
            <span class="collapse-table-arrow arrow-closed"></span>
            <a>Metrics Properties</a>
          </h4>
        </span>
        <div class="collapsible-table collapse" id="aggregated-metricsProperties">
          {metricsPropertiesTable}
        </div>
        <span class="collapse-table" data-bs-toggle="collapse"
            data-bs-target="#aggregated-classpathEntries"
            aria-expanded="false" aria-controls="aggregated-classpathEntries"
            data-collapse-name="collapse-aggregated-classpathEntries">
          <h4>
            <span class="collapse-table-arrow arrow-closed"></span>
            <a>Classpath Entries</a>
          </h4>
        </span>
        <div class="collapsible-table collapse" id="aggregated-classpathEntries">
          {classpathEntriesTable}
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
