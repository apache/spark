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

package org.apache.spark.util

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.{ArrayList => JList}

import scala.jdk.CollectionConverters._

import org.apache.commons.io.FileUtils
import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

import org.apache.spark.internal.{Logging, LogKey}
import org.apache.spark.internal.LogKey.LogKey

// scalastyle:off line.size.limit
/**
 * To re-generate the LogKey class file, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "common-utils/testOnly org.apache.spark.util.LogKeySuite"
 * }}}
 */
// scalastyle:on line.size.limit
class LogKeySuite
    extends AnyFunSuite // scalastyle:ignore funsuite
    with Logging {

  /**
   * Get a Path relative to the root project. It is assumed that a spark home is set.
   */
  protected final def getWorkspaceFilePath(first: String, more: String*): Path = {
    if (!(sys.props.contains("spark.test.home") || sys.env.contains("SPARK_HOME"))) {
      fail("spark.test.home or SPARK_HOME is not set.")
    }
    val sparkHome = sys.props.getOrElse("spark.test.home", sys.env("SPARK_HOME"))
    java.nio.file.Paths.get(sparkHome, first +: more: _*)
  }

  private val regenerateGoldenFiles: Boolean = System.getenv("SPARK_GENERATE_GOLDEN_FILES") == "1"

  private val logKeyFilePath = getWorkspaceFilePath("common", "utils", "src", "main", "scala",
    "org", "apache", "spark", "internal", "LogKey.scala")

  // regenerate the file `LogKey.scala` with its enumeration fields sorted alphabetically
  private def regenerateLogKeyFile(
      originalKeys: Seq[LogKey], sortedKeys: Seq[LogKey]): Unit = {
    if (originalKeys != sortedKeys) {
      val logKeyFile = logKeyFilePath.toFile
      logInfo(s"Regenerating LogKey file $logKeyFile")
      val originalContents = FileUtils.readLines(logKeyFile, StandardCharsets.UTF_8)
      val sortedContents = new JList[String]()
      var firstMatch = false
      originalContents.asScala.foreach { line =>
        if (line.trim.startsWith("val ") && line.trim.endsWith(" = Value")) {
          if (!firstMatch) {
            sortedKeys.foreach { logKey =>
              sortedContents.add(s"  val ${logKey.toString} = Value")
            }
            firstMatch = true
          }
        } else {
          sortedContents.add(line)
        }
      }
      Files.delete(logKeyFile.toPath)
      FileUtils.writeLines(logKeyFile, StandardCharsets.UTF_8.name(), sortedContents)
    }
  }

  test("LogKey enumeration fields are correctly sorted") {
    val originalKeys = LogKey.values.toSeq
    val sortedKeys = originalKeys.sortBy(_.toString)
    if (regenerateGoldenFiles) {
      regenerateLogKeyFile(originalKeys, sortedKeys)
    } else {
      assert(originalKeys === sortedKeys,
        "LogKey enumeration fields must be sorted alphabetically")
    }
  }
}
