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

package org.apache.spark.internal.io

import org.apache.hadoop.mapreduce.TaskAttemptContext

/**
 * An interface to define how a single Spark job names its outputs. Two notes:
 *
 * 1. Implementations must be serializable, as the instance instantiated on the driver
 *    will be used for tasks on executors.
 * 2. An instance should not be reused across multiple Spark jobs.
 *
 * The proper way to call is:
 *
 * As part of each task's execution, whenever a new output file needs be created, executor calls
 * [[getTaskTempPath]] to get a valid relative file path before commit.
 */
abstract class FileNamingProtocol {

  /**
   * Gets the relative path should be used for the output file.
   *
   * Important: it is the caller's responsibility to add uniquely identifying content to
   * "fileContext" if a task is going to write out multiple files to the same directory. The file
   * naming protocol only guarantees that files written by different tasks will not conflict.
   */
  def getTaskTempPath(taskContext: TaskAttemptContext, fileContext: FileContext): String
}

/**
 * The context for Spark output file. This is used by [[FileNamingProtocol]] to create file path.
 *
 * @param ext Source specific file extension, e.g. ".snappy.parquet".
 * @param relativeDir Relative directory of file. Can be used for writing dynamic partitions.
 *                    E.g., "a=1/b=2" is directory for partition (a=1, b=2).
 * @param prefix file prefix.
 */
final case class FileContext(
  ext: String,
  relativeDir: Option[String],
  prefix: Option[String])
