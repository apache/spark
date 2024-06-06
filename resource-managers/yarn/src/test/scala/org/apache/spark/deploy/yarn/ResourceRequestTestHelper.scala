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

package org.apache.spark.deploy.yarn

import scala.jdk.CollectionConverters._

import org.apache.hadoop.yarn.api.records.ResourceTypeInfo
import org.apache.hadoop.yarn.util.resource.ResourceUtils

trait ResourceRequestTestHelper {
  def initializeResourceTypes(resourceTypes: Seq[String]): Unit = {
    // ResourceUtils.reinitializeResources() is the YARN-way
    // to specify resources for the execution of the tests.
    // This method should receive standard resources with names of memory-mb and vcores.
    // Without specifying the standard resources or specifying them
    // with different names e.g. memory, YARN would throw various exceptions
    // because it relies on that standard resources are always specified.
    val defaultResourceTypes = List(
      ResourceTypeInfo.newInstance("memory-mb"),
      ResourceTypeInfo.newInstance("vcores"))
    val customResourceTypes = resourceTypes.map(ResourceTypeInfo.newInstance)
    val allResourceTypes = defaultResourceTypes ++ customResourceTypes

    ResourceUtils.reinitializeResources(allResourceTypes.asJava)
  }

  /**
   * `initializeResourceTypes` with inputs, call `f` and
   * restore `resourceTypes`` as default value.
   */
  def withResourceTypes(resourceTypes: Seq[String])(f: => Unit): Unit = {
    initializeResourceTypes(resourceTypes)
    try f finally {
      initializeResourceTypes(Seq.empty)
    }
  }
}
