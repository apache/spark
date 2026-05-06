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

import java.nio.file.{Path, Paths}

import org.scalatest.Assertions

/**
 * Shared test helpers for resolving workspace files and detecting golden-file regeneration mode.
 *
 * Lives in `common-utils` so it can be mixed into test suites across modules whose dependency
 * direction prevents extending `SparkFunSuite` (e.g. `LogKeysSuite` in `common-utils`,
 * `ConnectFunSuite` in the shaded Spark Connect client).
 */
trait SparkTestEnvHelper extends Assertions {

  /**
   * Get a Path relative to the root project. It is assumed that a spark home is set.
   */
  protected final def getWorkspaceFilePath(first: String, more: String*): Path = {
    if (!(sys.props.contains("spark.test.home") || sys.env.contains("SPARK_HOME"))) {
      fail("spark.test.home or SPARK_HOME is not set.")
    }
    val sparkHome = sys.props.getOrElse("spark.test.home", sys.env("SPARK_HOME"))
    Paths.get(sparkHome, first +: more: _*)
  }

  protected def regenerateGoldenFiles: Boolean =
    System.getenv("SPARK_GENERATE_GOLDEN_FILES") == "1"
}
