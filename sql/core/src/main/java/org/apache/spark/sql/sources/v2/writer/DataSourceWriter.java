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

package org.apache.spark.sql.sources.v2.writer;

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.StructType;

/**
 * A data source writer that is returned by
 * {@link WriteSupport#createWriter(String, StructType, SaveMode, DataSourceOptions)}/
 * {@link org.apache.spark.sql.sources.v2.streaming.StreamWriteSupport#createStreamWriter(
 * String, StructType, OutputMode, DataSourceOptions)}.
 * It can mix in various writing optimization interfaces to speed up the data saving. The actual
 * writing logic is delegated to {@link DataWriter}.
 *
 * If an exception was throw when applying any of these writing optimizations, the action would fail
 * and no Spark job was submitted.
 *
 * The writing procedure is:
 *   1. Create a writer factory by {@link #createWriterFactory()}, serialize and send it to all the
 *      partitions of the input data(RDD).
 *   2. For each partition, create the data writer, and write the data of the partition with this
 *      writer. If one data writer finishes successfully, the commit message will be sent back to
 *      the driver side and Spark will call {@link #add(WriterCommitMessage)}.
 *      If exception happens during the writing, call {@link DataWriter#abort()}.
 *   3. If all the data writers finish successfully, and {@link #add(WriterCommitMessage)} is
 *      successfully called for all the commit messages, Spark will call {@link #commit()}.
 *      If any of the data writers failed, or any of the {@link #add(WriterCommitMessage)}
 *      calls failed, or the job failed with an unknown reason, call {@link #abort()}.
 *
 * While Spark will retry failed writing tasks, Spark won't retry failed writing jobs. Users should
 * do it manually in their Spark applications if they want to retry.
 *
 * All these methods are guaranteed to be called in a single thread on driver side.
 * No concurrency control is needed.
 *
 * Please refer to the documentation of add/commit/abort methods for detailed specifications.
 */
@InterfaceStability.Evolving
public interface DataSourceWriter {

  /**
   * Creates a writer factory which will be serialized and sent to executors.
   *
   * If this method fails (by throwing an exception), the action would fail and no Spark job was
   * submitted.
   */
  DataWriterFactory<Row> createWriterFactory();

  /**
   * Handles a commit message which is collected from a successful data writer.
   *
   * Note that, implementations might need to cache all commit messages before calling
   * {@link #commit()} or {@link #abort()}.
   *
   * If this method fails (by throwing an exception), this writing job is considered to to have been
   * failed, and {@link #abort()} would be called. The state of the destination
   * is undefined and @{@link #abort()} may not be able to deal with it.
   */
  void add(WriterCommitMessage message);

  /**
   * Commits this writing job.
   * When this method is called, the number of commit messages added by
   * {@link #add(WriterCommitMessage)} equals to the number of input data partitions.
   *
   * If this method fails (by throwing an exception), this writing job is considered to to have been
   * failed, and {@link #abort()} would be called. The state of the destination
   * is undefined and @{@link #abort()} may not be able to deal with it.
   */
  void commit();

  /**
   * Aborts this writing job because some data writers are failed and keep failing when retry,
   * or the Spark job fails with some unknown reasons,
   * or {@link #commit()} / {@link #add(WriterCommitMessage)} fails.
   *
   * If this method fails (by throwing an exception), the underlying data source may require manual
   * cleanup.
   *
   * Unless the abort is triggered by the failure of {@link #commit()}, the number of commit
   * messages added by {@link #add(WriterCommitMessage)} should be smaller than the number
   * of input data partitions, as there may be only a few data writers that are committed
   * before the abort happens, or some data writers were committed but their commit messages
   * haven't reached the driver when the abort is triggered. So this is just a "best effort"
   * for data sources to clean up the data left by data writers.
   */
  void abort();
}
