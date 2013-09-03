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

import javax.servlet.http.HttpServletRequest

import scala.collection.JavaConversions._
import scala.util.Properties
import scala.xml.Node

import org.eclipse.jetty.server.Handler

import org.apache.spark.ui.JettyUtils._
import org.apache.spark.ui.UIUtils
import org.apache.spark.ui.Page.Environment
import org.apache.spark.SparkContext


private[spark] class EnvironmentUI(sc: SparkContext) {

  def getHandlers = Seq[(String, Handler)](
    ("/environment", (request: HttpServletRequest) => envDetails(request))
  )

  def envDetails(request: HttpServletRequest): Seq[Node] = {
    val jvmInformation = Seq(
      ("Java Version", "%s (%s)".format(Properties.javaVersion, Properties.javaVendor)),
      ("Java Home", Properties.javaHome),
      ("Scala Version", Properties.versionString),
      ("Scala Home", Properties.scalaHome)
    ).sorted
    def jvmRow(kv: (String, String)) = <tr><td>{kv._1}</td><td>{kv._2}</td></tr>
    def jvmTable =
      UIUtils.listingTable(Seq("Name", "Value"), jvmRow, jvmInformation, fixedWidth = true)

    val properties = System.getProperties.iterator.toSeq
    val classPathProperty = properties.find { case (k, v) =>
      k.contains("java.class.path")
    }.getOrElse(("", ""))
    val sparkProperties = properties.filter(_._1.startsWith("spark")).sorted
    val otherProperties = properties.diff(sparkProperties :+ classPathProperty).sorted

    val propertyHeaders = Seq("Name", "Value")
    def propertyRow(kv: (String, String)) = <tr><td>{kv._1}</td><td>{kv._2}</td></tr>
    val sparkPropertyTable =
      UIUtils.listingTable(propertyHeaders, propertyRow, sparkProperties, fixedWidth = true)
    val otherPropertyTable =
      UIUtils.listingTable(propertyHeaders, propertyRow, otherProperties, fixedWidth = true)

    val classPathEntries = classPathProperty._2
        .split(System.getProperty("path.separator", ":"))
        .filterNot(e => e.isEmpty)
        .map(e => (e, "System Classpath"))
    val addedJars = sc.addedJars.iterator.toSeq.map{case (path, time) => (path, "Added By User")}
    val addedFiles = sc.addedFiles.iterator.toSeq.map{case (path, time) => (path, "Added By User")}
    val classPath = (addedJars ++ addedFiles ++ classPathEntries).sorted

    val classPathHeaders = Seq("Resource", "Source")
    def classPathRow(data: (String, String)) = <tr><td>{data._1}</td><td>{data._2}</td></tr>
    val classPathTable =
      UIUtils.listingTable(classPathHeaders, classPathRow, classPath, fixedWidth = true)

    val content =
      <span>
        <h4>Runtime Information</h4> {jvmTable}
        <h4>Spark Properties</h4>
        {sparkPropertyTable}
        <h4>System Properties</h4>
        {otherPropertyTable}
        <h4>Classpath Entries</h4>
        {classPathTable}
      </span>

    UIUtils.headerSparkPage(content, sc, "Environment", Environment)
  }
}
