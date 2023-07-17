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
package org.apache.spark.sql.connect.client.util

import java.nio.file.Path
import java.time.{Instant, LocalDateTime}
import java.time.temporal.ChronoUnit

import org.apache.commons.lang3.{JavaVersion, SystemUtils}
import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

/**
 * The basic testsuite the client tests should extend from.
 */
trait ConnectFunSuite extends AnyFunSuite { // scalastyle:ignore funsuite

  // Borrowed from SparkFunSuite
  protected def getWorkspaceFilePath(first: String, more: String*): Path = {
    if (!(sys.props.contains("spark.test.home") || sys.env.contains("SPARK_HOME"))) {
      fail("spark.test.home or SPARK_HOME is not set.")
    }
    val sparkHome = sys.props.getOrElse("spark.test.home", sys.env("SPARK_HOME"))
    java.nio.file.Paths.get(sparkHome, first +: more: _*)
  }

  protected val baseResourcePath: Path = {
    getWorkspaceFilePath(
      "connector",
      "connect",
      "client",
      "jvm",
      "src",
      "test",
      "resources").toAbsolutePath
  }

  protected val commonResourcePath: Path = {
    getWorkspaceFilePath(
      "connector",
      "connect",
      "common",
      "src",
      "test",
      "resources").toAbsolutePath
  }

  // SPARK-42770&SPARK-44457: Run `LocalDateTime.now()` and `Instant.now()` with Java 8 & 11 always
  // get microseconds on both Linux and MacOS, but there are some differences when
  // using Java 17, it will get accurate nanoseconds on Linux, but still get the microseconds
  // on MacOS. At present, Spark always converts them to microseconds, this will cause the
  // test fail when using Java 17 on Linux, so add `truncatedTo(ChronoUnit.MICROS)` when
  // testing on Linux using Java 17 to ensure the accuracy of input data is microseconds.
  private val needTruncatedJavaTimeToMicros: Boolean =
    SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_17) && SystemUtils.IS_OS_LINUX
  def instantNow(): Instant =
    if (needTruncatedJavaTimeToMicros) {
      Instant.now().truncatedTo(ChronoUnit.MICROS)
    } else {
      Instant.now()
    }

  def localDateTimeNow(): LocalDateTime =
    if (needTruncatedJavaTimeToMicros) {
      LocalDateTime.now().truncatedTo(ChronoUnit.MICROS)
    } else {
      LocalDateTime.now()
    }
}
