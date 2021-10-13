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

package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.util.Utils

abstract class SQLFileCommitProtocol extends FileCommitProtocol {

  /**
   * Get the final directory where work will be placed once the job
   * is org.apache.hadoop.shaded.com.itted. This may be null, in which case, there is no output
   * path to write data to.
   */
  def getOutputPath(): Path

  /**
   * Get the directory that the task should write results into.
   */
  def getWorkPath(): Path
}

object SQLFileCommitProtocol extends Logging{

  /**
   * Instantiates a FileCommitProtocol using the given className.
   */
  def instantiate(
      className: String,
      jobId: String,
      outputPath: String,
      dynamicPartitionOverwrite: Boolean = false): SQLFileCommitProtocol = {

    logDebug(s"Creating committer $className; job $jobId; output=$outputPath;" +
      s" dynamic=$dynamicPartitionOverwrite")
    val clazz = Utils.classForName[SQLFileCommitProtocol](className)
    // First try the constructor with arguments (jobId: String, outputPath: String,
    // dynamicPartitionOverwrite: Boolean).
    // If that doesn't exist, try the one with (jobId: string, outputPath: String).
    try {
      val ctor = clazz.getDeclaredConstructor(classOf[String], classOf[String], classOf[Boolean])
      logDebug("Using (String, String, Boolean) constructor")
      ctor.newInstance(jobId, outputPath, dynamicPartitionOverwrite.asInstanceOf[java.lang.Boolean])
    } catch {
      case _: NoSuchMethodException =>
        logDebug("Falling back to (String, String) constructor")
        require(!dynamicPartitionOverwrite,
          "Dynamic Partition Overwrite is enabled but" +
            s" the committer ${className} does not have the appropriate constructor")
        val ctor = clazz.getDeclaredConstructor(classOf[String], classOf[String])
        ctor.newInstance(jobId, outputPath)
    }
  }
}
