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

import scala.xml.Node

import org.apache.spark.ui.{UIUtils, WebUIPage}

private[ui] class EnvironmentPage(parent: EnvironmentTab) extends WebUIPage("") {
  private val listener = parent.listener

  private val propertyTable = UIUtils.stringPairTable("Name", "Value")
  private val classpathTable = UIUtils.stringPairTable("Resource", "Source")

  def render(request: HttpServletRequest): Seq[Node] = {
    val content =
      <span>
        <h4>Runtime Information</h4> {propertyTable.render(listener.jvmInformation)}
        <h4>Spark Properties</h4> {propertyTable.render(listener.sparkProperties)}
        <h4>System Properties</h4> {propertyTable.render(listener.systemProperties)}
        <h4>Classpath Entries</h4> {classpathTable.render(listener.classpathEntries)}
      </span>

    UIUtils.headerSparkPage("Environment", content, parent)
  }
}
