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

package org.apache.spark.sql.util

import java.nio.file.{Path, Paths}

object ArtifactUtils {

  private[sql] def concatenatePaths(basePath: Path, otherPath: Path): Path = {
    require(!otherPath.isAbsolute)
    // We avoid using the `.resolve()` method here to ensure that we're concatenating the two
    // paths.
    val concatenatedPath = Paths.get(basePath.toString + "/" + otherPath.toString)
    // Note: The normalized resulting path may still reference parent directories if the
    // `otherPath` contains sufficient number of parent operators (i.e "..").
    // Example: `basePath` = "/base", `otherPath` = "subdir/../../file.txt"
    // Then, `concatenatedPath` = "/base/subdir/../../file.txt"
    // and `normalizedPath` = "/base/file.txt".
    val normalizedPath = concatenatedPath.normalize()
    // Verify that the prefix of the `normalizedPath` starts with `basePath/`.
    require(normalizedPath != basePath && normalizedPath.startsWith(s"$basePath/"))
    normalizedPath
  }

  private[sql] def concatenatePaths(basePath: Path, otherPath: String): Path = {
    concatenatePaths(basePath, Paths.get(otherPath))
  }
}
