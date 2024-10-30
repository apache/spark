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

package org.apache.spark.ui

import java.io.File
import java.nio.charset.StandardCharsets

import jakarta.servlet.http.HttpServletRequest
import org.apache.commons.io.FileUtils
import org.mockito.Mockito.{mock, when}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.DRIVER_LOG_LOCAL_DIR


class DriverLogPageSuite extends SparkFunSuite {

  test("DriverLogTab requires driver log location") {
    val sparkUI = mock(classOf[SparkUI])
    when(sparkUI.conf).thenReturn(new SparkConf(false))

    val m = intercept[IllegalArgumentException] {
      new DriverLogTab(sparkUI)
    }.getMessage
    assert(m.contains(s"Please specify ${DRIVER_LOG_LOCAL_DIR.key}"))

  }

  test("DriverLogPage requires driver log location") {
    val conf = new SparkConf(false)
    val m = intercept[IllegalArgumentException] {
      new DriverLogPage(null, conf)
    }.getMessage
    assert(m.contains(s"Please specify ${DRIVER_LOG_LOCAL_DIR.key}"))
  }

  test("renderLog reads driver.log file") {
    val conf = new SparkConf(false)
    withTempDir { dir =>
      val page = new DriverLogPage(null, conf.set(DRIVER_LOG_LOCAL_DIR, dir.getCanonicalPath))
      val file = new File(dir, "driver.log")
      FileUtils.writeStringToFile(file, "driver log content", StandardCharsets.UTF_8)
      val request = mock(classOf[HttpServletRequest])
      val log = page.renderLog(request)
      assert(log.startsWith("==== Bytes 0-18 of 18 of"))
      assert(log.contains("driver.log"))
      assert(log.contains("driver log content"))
    }
  }
}
