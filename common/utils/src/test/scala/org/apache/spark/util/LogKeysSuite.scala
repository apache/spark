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

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.util.ArrayImplicits._

// scalastyle:off line.size.limit
/**
 * To re-generate the file `LogKeys.java`, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "common-utils/testOnly org.apache.spark.util.LogKeysSuite"
 * }}}
 */
// scalastyle:on line.size.limit
class LogKeysSuite
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

  private val logKeyFilePath = getWorkspaceFilePath("common", "utils", "src", "main", "java",
    "org", "apache", "spark", "internal", "LogKeys.java")

  // regenerate the file `LogKeys.java` with its members sorted alphabetically
  private def regenerateLogKeyFile(
      originalKeys: Seq[String], sortedKeys: Seq[String]): Unit = {
    if (originalKeys != sortedKeys) {
      val logKeyFile = logKeyFilePath.toFile
      logInfo(s"Regenerating the file $logKeyFile")
      val originalContents = Files.readAllLines(logKeyFile.toPath, StandardCharsets.UTF_8).asScala

      val beforeFirstIndex = originalContents
        .indexWhere(_.contains("public enum LogKeys implements LogKey"))

      val content =
        s"""${originalContents.take(beforeFirstIndex + 1).mkString("\n")}
           |${sortedKeys.map { key => s"  $key" }.mkString(",\n")}
           |}
           |""".stripMargin

      Files.delete(logKeyFile.toPath)
      Files.writeString(logKeyFile.toPath, content, StandardCharsets.UTF_8)
    }
  }

  test("The members of LogKeys are correctly sorted") {
    val originalKeys = LogKeys.values.map(_.name).toImmutableArraySeq
    val sortedKeys = originalKeys.sorted
    if (regenerateGoldenFiles) {
      regenerateLogKeyFile(originalKeys, sortedKeys)
    } else {
      assert(originalKeys === sortedKeys,
        "The members of LogKeys must be sorted alphabetically")
    }
  }
}
