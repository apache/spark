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

import jakarta.servlet.http.HttpServletRequest
import org.mockito.Mockito._

import org.apache.spark.SparkConf
import org.apache.spark.SparkFunSuite
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.{ApplicationEnvironmentInfo, RuntimeInfo}

class EnvironmentPageSuite extends SparkFunSuite {

  test("SPARK-43471: Handle missing hadoopProperties and metricsProperties") {
    val environmentTab = mock(classOf[EnvironmentTab])
    when(environmentTab.appName).thenReturn("Environment")
    when(environmentTab.basePath).thenReturn("http://localhost:4040")
    when(environmentTab.headerTabs).thenReturn(Seq.empty)

    val runtimeInfo = mock(classOf[RuntimeInfo])

    val info = mock(classOf[ApplicationEnvironmentInfo])
    when(info.runtime).thenReturn(runtimeInfo)
    when(info.sparkProperties).thenReturn(Seq.empty)
    when(info.systemProperties).thenReturn(Seq.empty)
    when(info.classpathEntries).thenReturn(Seq.empty)

    val store = mock(classOf[AppStatusStore])
    when(store.environmentInfo()).thenReturn(info)
    when(store.resourceProfileInfo()).thenReturn(Seq.empty)

    val environmentPage = new EnvironmentPage(environmentTab, new SparkConf, store)
    val request = mock(classOf[HttpServletRequest])
    environmentPage.render(request)
  }
}
