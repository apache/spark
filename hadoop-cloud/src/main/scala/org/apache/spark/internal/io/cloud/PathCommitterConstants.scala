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

package org.apache.spark.internal.io.cloud

/**
 * Constants related to Hadoop committer setup and configuration.
 * Most of these are scattered around the hadoop-mapreduce classes.
 */
object PathCommitterConstants {

  /**
   * Scheme prefix for per-filesystem scheme committers.
   */
  val OUTPUTCOMMITTER_FACTORY_SCHEME = "mapreduce.outputcommitter.factory.scheme"

  /**
   * String format pattern for per-filesystem scheme committers.
   */
  val OUTPUTCOMMITTER_FACTORY_SCHEME_PATTERN: String =
    OUTPUTCOMMITTER_FACTORY_SCHEME + ".%s"

  /**
   * Name of the configuration option used to configure the
   * output committer factory to use unless there is a specific
   * one for a schema.
   */
  val OUTPUTCOMMITTER_FACTORY_CLASS = "mapreduce.pathoutputcommitter.factory.class"

  /** Default committer factory. */
  val DEFAULT_COMMITTER_FACTORY =
    "org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory"

  /**
   * The committer which can be directly instantiated and which then delegates
   * all operations to the factory-created committer it creates itself.
   */
  val BINDING_PATH_OUTPUT_COMMITTER_CLASS =
    "org.apache.hadoop.mapreduce.lib.output.BindingPathOutputCommitter"

  /**
   * Classname of a parquet committer which just hands off to the
   * `BindingPathOutputCommitter` in hadoop-mapreduce, which takes on the
   * task of binding to the current factory.
   */
  val BINDING_PARQUET_OUTPUT_COMMITTER_CLASS =
    "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter"

  /** hadoop-mapreduce option to choose the algorithm. */
  val FILEOUTPUTCOMMITTER_ALGORITHM_VERSION = "mapreduce.fileoutputcommitter.algorithm.version"

  /** The default committer is not actually safe during task commit failures. */
  val FILEOUTPUTCOMMITTER_ALGORITHM_VERSION_DEFAULT = 2

  /** Skip cleanup _temporary folders under job's output directory? */
  val FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED = "mapreduce.fileoutputcommitter.cleanup.skipped"

  /**
   * This is the "Pending" directory of the FileOutputCommitter;
   * data written here is, in that algorithm, renamed into place.
   */
  val TEMP_DIR_NAME = "_temporary"

  /**
   * Name of the marker file created on success.
   * This is a 0-byte file with the FileOutputCommitter; object store committers
   * often add a (non-standard) manifest here.
   */
  val SUCCESS_FILE_NAME = "_SUCCESS"

  /** hadoop-mapreduce option to enable the _SUCCESS marker. */
  val CREATE_SUCCESSFUL_JOB_OUTPUT_DIR_MARKER = "mapreduce.fileoutputcommitter.marksuccessfuljobs"
}
