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

import java.io.File
import java.nio.file.Files

import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

class SparkFileUtilsSuite extends AnyFunSuite { // scalastyle:ignore funsuite

  // Returns the log content appended to `target/spark-file-utils.log` (configured for the
  // SparkFileUtils logger in log4j2.properties) while running `f`.
  private def captureLogOutput(f: () => Unit): String = {
    val logFile = new File(new File(".").getCanonicalPath, "target/spark-file-utils.log")
    val before = if (logFile.exists()) Files.readString(logFile.toPath) else ""
    f()
    val after = if (logFile.exists()) Files.readString(logFile.toPath) else ""
    after.substring(before.length)
  }

  test("recursiveList lists all nested files and directories") {
    val root = Files.createTempDirectory("spark-recursive-list").toFile
    try {
      val sub = new File(root, "sub")
      assert(sub.mkdir())
      val nested = new File(sub, "nested")
      assert(nested.mkdir())
      val topFile = new File(root, "top.txt")
      assert(topFile.createNewFile())
      val subFile = new File(sub, "sub.txt")
      assert(subFile.createNewFile())

      assert(SparkFileUtils.recursiveList(root).toSet === Set(sub, nested, topFile, subFile))
    } finally {
      SparkFileUtils.deleteQuietly(root)
    }
  }

  test("recursiveList returns empty and warns instead of throwing when a dir cannot be listed") {
    // A directory whose listFiles returns null must yield an empty result, not an NPE, and the
    // skipped directory must be logged so a partial result is traceable.
    val unreadable = new File("spark-unreadable-dir") {
      override def isDirectory: Boolean = true
      override def listFiles(): Array[File] = null
    }
    val logOutput = captureLogOutput(() => assert(SparkFileUtils.recursiveList(unreadable).isEmpty))
    assert(logOutput.contains("Failed to list directory"))
    assert(logOutput.contains("spark-unreadable-dir"))
  }

  test("recursiveList skips a subdirectory whose listFiles returns null mid-walk") {
    // The null directory is not the root but one discovered during the walk, so the guard must
    // hold at depth > 0. `unreadableSub` is a directory whose listFiles returns null; `leaf` is a
    // plain file that must be returned but never recursed into (a spy verifies that).
    var leafListed = false
    val leaf = new File("leaf.txt") {
      override def isDirectory: Boolean = false
      override def listFiles(): Array[File] = { leafListed = true; super.listFiles() }
    }
    val unreadableSub = new File("unreadable-sub") {
      override def isDirectory: Boolean = true
      override def listFiles(): Array[File] = null
    }
    val root = new File("root") {
      override def isDirectory: Boolean = true
      override def listFiles(): Array[File] = Array(leaf, unreadableSub)
    }
    assert(SparkFileUtils.recursiveList(root).toSet === Set(leaf, unreadableSub))
    assert(!leafListed, "a non-directory entry must not be recursed into")
  }
}
