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

package org.apache.spark.api.python

import java.io.File

import org.apache.spark.SparkFunSuite

class PythonUtilsSuite extends SparkFunSuite {

  test("SPARK-60855: sparkPythonPath does not include the jar containing Spark's classes") {
    // The jar that contains PythonUtils used to be appended to the PYTHONPATH of the Python
    // processes Spark launches. Spark's jars have contained no Python code since Spark 1.4, so
    // the entry only cost time: Python probes it on every import miss and re-reads it on every
    // `importlib.invalidate_caches()`. This guards against the entry coming back. It is only
    // effective when PythonUtils is loaded from a jar: SBT exports main classes as jars
    // (`Compile / exportJars := true`), so here PythonUtils comes from the spark-core jar,
    // while a build that loads it from `target/classes` passes either way.
    val entries = PythonUtils.sparkPythonPath.split(File.pathSeparator).filter(_.nonEmpty)
    assert(!entries.exists(_.endsWith(".jar")),
      s"Unexpected jar on the PYTHONPATH: ${entries.mkString(File.pathSeparator)}")
  }
}
