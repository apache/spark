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

import javax.servlet.http.HttpServletRequest

import org.mockito.Mockito._

import org.apache.spark.SparkFunSuite
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1

class AllJobsPageSuite extends SparkFunSuite {

  val appStatusStore = mock(classOf[AppStatusStore])
  val request = mock(classOf[HttpServletRequest])
  val appEnv = mock(classOf[v1.ApplicationEnvironmentInfo])
  val sparkProperties: Seq[(String, String)] = Seq(("spark.scheduler.mode", "fair"))
  when(appStatusStore.environmentInfo()).thenReturn(appEnv)
  when(appStatusStore.environmentInfo().sparkProperties).thenReturn(sparkProperties)

  val jobPage = new AllJobsPage(null, appStatusStore)

  test("Enumeration conversion") {
    assert(jobPage.getSchedulingMode.equals("FAIR"))
  }
}
