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

package org.apache.spark.deploy

import org.scalatest.FunSuite

class PythonRunnerSuite extends FunSuite {

  // Test formatting a single path to be added to the PYTHONPATH
  test("format path") {
    assert(PythonRunner.formatPath("spark.py") === "spark.py")
    assert(PythonRunner.formatPath("file:/spark.py") === "/spark.py")
    assert(PythonRunner.formatPath("file:///spark.py") === "/spark.py")
    assert(PythonRunner.formatPath("local:/spark.py") === "/spark.py")
    assert(PythonRunner.formatPath("local:///spark.py") === "/spark.py")
    assert(PythonRunner.formatPath("C:/a/b/spark.py", testWindows = true) === "C:/a/b/spark.py")
    assert(PythonRunner.formatPath("/C:/a/b/spark.py", testWindows = true) === "C:/a/b/spark.py")
    assert(PythonRunner.formatPath("file:/C:/a/b/spark.py", testWindows = true) ===
      "C:/a/b/spark.py")
    intercept[IllegalArgumentException] { PythonRunner.formatPath("one:two") }
    intercept[IllegalArgumentException] { PythonRunner.formatPath("hdfs:s3:xtremeFS") }
    intercept[IllegalArgumentException] { PythonRunner.formatPath("hdfs:/path/to/some.py") }
  }

  // Test formatting multiple comma-separated paths to be added to the PYTHONPATH
  test("format paths") {
    assert(PythonRunner.formatPaths("spark.py") === Array("spark.py"))
    assert(PythonRunner.formatPaths("file:/spark.py") === Array("/spark.py"))
    assert(PythonRunner.formatPaths("file:/app.py,local:/spark.py") ===
      Array("/app.py", "/spark.py"))
    assert(PythonRunner.formatPaths("me.py,file:/you.py,local:/we.py") ===
      Array("me.py", "/you.py", "/we.py"))
    assert(PythonRunner.formatPaths("C:/a/b/spark.py", testWindows = true) ===
      Array("C:/a/b/spark.py"))
    assert(PythonRunner.formatPaths("/C:/a/b/spark.py", testWindows = true) ===
      Array("C:/a/b/spark.py"))
    assert(PythonRunner.formatPaths("C:/free.py,pie.py", testWindows = true) ===
      Array("C:/free.py", "pie.py"))
    assert(PythonRunner.formatPaths("lovely.py,C:/free.py,file:/d:/fry.py", testWindows = true) ===
      Array("lovely.py", "C:/free.py", "d:/fry.py"))
    intercept[IllegalArgumentException] { PythonRunner.formatPaths("one:two,three") }
    intercept[IllegalArgumentException] { PythonRunner.formatPaths("two,three,four:five:six") }
    intercept[IllegalArgumentException] { PythonRunner.formatPaths("hdfs:/some.py,foo.py") }
    intercept[IllegalArgumentException] { PythonRunner.formatPaths("foo.py,hdfs:/some.py") }
  }
}
