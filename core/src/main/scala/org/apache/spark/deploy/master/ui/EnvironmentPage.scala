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

package org.apache.spark.deploy.master.ui

import scala.jdk.CollectionConverters._
import scala.xml.Node

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.config.UI.MASTER_UI_VISIBLE_ENV_VAR_PREFIXES
import org.apache.spark.ui._
import org.apache.spark.util.Utils

private[ui] class EnvironmentPage(
    parent: MasterWebUI,
    conf: SparkConf) extends WebUIPage("Environment") {

  def render(request: HttpServletRequest): Seq[Node] = {
    val details = SparkEnv.environmentDetails(conf, SparkHadoopUtil.get.newConfiguration(conf),
      "", Seq.empty, Seq.empty, Seq.empty, Map.empty)
    val jvmInformation = details("JVM Information").sorted
    val sparkProperties = Utils.redact(conf, details("Spark Properties")).sorted
    val hadoopProperties = Utils.redact(conf, details("Hadoop Properties")).sorted
    val systemProperties = Utils.redact(conf, details("System Properties")).sorted
    val metricsProperties = Utils.redact(conf, details("Metrics Properties")).sorted
    val classpathEntries = details("Classpath Entries").sorted
    val prefixes = conf.get(MASTER_UI_VISIBLE_ENV_VAR_PREFIXES)
    val environmentVariables = System.getenv().asScala
      .filter { case (k, _) => prefixes.exists(k.startsWith(_)) }.toSeq.sorted

    val runtimeInformationTable = UIUtils.listingTable(propertyHeader, propertyRow,
      jvmInformation, fixedWidth = true, headerClasses = headerClasses)
    val sparkPropertiesTable = UIUtils.listingTable(propertyHeader, propertyRow,
      sparkProperties, fixedWidth = true, headerClasses = headerClasses)
    val hadoopPropertiesTable = UIUtils.listingTable(propertyHeader, propertyRow,
      hadoopProperties, fixedWidth = true, headerClasses = headerClasses)
    val systemPropertiesTable = UIUtils.listingTable(propertyHeader, propertyRow,
      systemProperties, fixedWidth = true, headerClasses = headerClasses)
    val metricsPropertiesTable = UIUtils.listingTable(propertyHeader, propertyRow,
      metricsProperties, fixedWidth = true, headerClasses = headerClasses)
    val classpathEntriesTable = UIUtils.listingTable(classPathHeader, classPathRow,
      classpathEntries, fixedWidth = true, headerClasses = headerClasses)
    val environmentVariablesTable = UIUtils.listingTable(propertyHeader, propertyRow,
      environmentVariables, fixedWidth = true, headerClasses = headerClasses)

    val content =
      <div>
        <p><a href="/">Back to Master</a></p>
      </div>
      <span>
        <span class="collapse-aggregated-runtimeInformation collapse-table"
            onClick="collapseTable('collapse-aggregated-runtimeInformation',
            'aggregated-runtimeInformation')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Runtime Information</a>
          </h4>
        </span>
        <div class="aggregated-runtimeInformation collapsible-table">
          {runtimeInformationTable}
        </div>
        <span class="collapse-aggregated-sparkProperties collapse-table"
            onClick="collapseTable('collapse-aggregated-sparkProperties',
            'aggregated-sparkProperties')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Spark Properties</a>
          </h4>
        </span>
        <div class="aggregated-sparkProperties collapsible-table">
          {sparkPropertiesTable}
        </div>
        <span class="collapse-aggregated-hadoopProperties collapse-table"
              onClick="collapseTable('collapse-aggregated-hadoopProperties',
            'aggregated-hadoopProperties')">
          <h4>
            <span class="collapse-table-arrow arrow-closed"></span>
            <a>Hadoop Properties</a>
          </h4>
        </span>
        <div class="aggregated-hadoopProperties collapsible-table collapsed">
          {hadoopPropertiesTable}
        </div>
        <span class="collapse-aggregated-systemProperties collapse-table"
            onClick="collapseTable('collapse-aggregated-systemProperties',
            'aggregated-systemProperties')">
          <h4>
            <span class="collapse-table-arrow arrow-closed"></span>
            <a>System Properties</a>
          </h4>
        </span>
        <div class="aggregated-systemProperties collapsible-table collapsed">
          {systemPropertiesTable}
        </div>
        <span class="collapse-aggregated-metricsProperties collapse-table"
              onClick="collapseTable('collapse-aggregated-metricsProperties',
            'aggregated-metricsProperties')">
          <h4>
            <span class="collapse-table-arrow arrow-closed"></span>
            <a>Metrics Properties</a>
          </h4>
        </span>
        <div class="aggregated-metricsProperties collapsible-table collapsed">
          {metricsPropertiesTable}
        </div>
        <span class="collapse-aggregated-classpathEntries collapse-table"
            onClick="collapseTable('collapse-aggregated-classpathEntries',
            'aggregated-classpathEntries')">
          <h4>
            <span class="collapse-table-arrow arrow-closed"></span>
            <a>Classpath Entries</a>
          </h4>
        </span>
        <div class="aggregated-classpathEntries collapsible-table collapsed">
          {classpathEntriesTable}
        </div>
        <span class="collapse-aggregated-environmentVariables collapse-table"
            onClick="collapseTable('collapse-aggregated-environmentVariables',
            'aggregated-environmentVariables')">
          <h4>
            <span class="collapse-table-arrow arrow-closed"></span>
            <a>Environment Variables</a>
          </h4>
        </span>
        <div class="aggregated-environmentVariables collapsible-table collapsed">
          {environmentVariablesTable}
        </div>
        <script src={UIUtils.prependBaseUri(request, "/static/environmentpage.js")}></script>
      </span>
    UIUtils.basicSparkPage(request, content, "Environment")
  }

  private def propertyHeader = Seq("Name", "Value")
  private def classPathHeader = Seq("Resource", "Source")
  private def headerClasses = Seq("sorttable_alpha", "sorttable_alpha")
  private def headerClassesNoSortValues = Seq("sorttable_numeric", "sorttable_nosort")

  private def jvmRowDataPre(kv: (String, String)) =
    <tr><td>{kv._1}</td><td><pre>{kv._2}</pre></td></tr>
  private def propertyRow(kv: (String, String)) = <tr><td>{kv._1}</td><td>{kv._2}</td></tr>
  private def classPathRow(data: (String, String)) = <tr><td>{data._1}</td><td>{data._2}</td></tr>
}

