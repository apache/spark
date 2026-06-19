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

package org.apache.spark

import java.io.File

import org.apache.spark.util.Utils

/**
 * Resolves paths to files added through `SparkContext.addFile()`.
 */
object SparkFiles {

  /**
   * Get the absolute path of a file added through `SparkContext.addFile()`.
   */
  def get(filename: String): String = {
    val jobArtifactUUID = JobArtifactSet
      .getCurrentJobArtifactState.map(_.uuid).getOrElse("default")
    val withUuid = if (jobArtifactUUID == "default") filename else s"$jobArtifactUUID/$filename"
    val file = new File(getRootDirectory(), withUuid)
    // In local mode, `SparkContext.addFile` places files directly under the root directory
    // rather than under the job-specific artifact directory used by session isolation. Fall back
    // to the root directory when the file is not found under the job-specific directory so that
    // files added through `SparkContext.addFile` remain resolvable. This is scoped to local mode
    // to preserve session isolation semantics on real executors. See SPARK-53478.
    if (jobArtifactUUID != "default" && !file.exists() &&
        Utils.isLocalMaster(SparkEnv.get.conf)) {
      val fallbackFile = new File(getRootDirectory(), filename)
      if (fallbackFile.exists()) {
        return fallbackFile.getAbsolutePath
      }
    }
    file.getAbsolutePath
  }

  /**
   * Get the root directory that contains files added through `SparkContext.addFile()`.
   */
  def getRootDirectory(): String =
    SparkEnv.get.driverTmpDir.getOrElse(".")

}
