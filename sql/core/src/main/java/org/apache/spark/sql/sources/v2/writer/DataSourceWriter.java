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
 *      writer. If all the data are written successfully, call {@link DataWriter#commit()}.
 *      On a writer being successfully committed, call {@link #add(WriterCommitMessage)} to
 *      handle its commit message.
 *      If exception happens during the writing, call {@link DataWriter#abort()}.
 *   3. If all writers are successfully committed, call {@link #commit()}. If
 *      some writers are aborted, or the job failed with an unknown reason, call
 *      {@link #abort()}.
 *
 * While Spark will retry failed writing tasks, Spark won't retry failed writing jobs. Users should
 * do it manually in their Spark applications if they want to retry.
 *
 * Please refer to the documentation of commit/abort methods for detailed specifications.
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
   * Handles a commit message produced by {@link DataWriter#commit()}.
   *
   * If this method fails (by throwing an exception), this writing job is considered to to have been
   * failed, and {@link #abort()} would be called. The state of the destination
   * is undefined and @{@link #abort()} may not be able to deal with it.
   */
  void add(WriterCommitMessage message);

  /**
   * Commits this writing job.
   *
   * If this method fails (by throwing an exception), this writing job is considered to to have been
   * failed, and {@link #abort()} would be called. The state of the destination
   * is undefined and @{@link #abort()} may not be able to deal with it.
   */
  void commit();

  /**
   * Aborts this writing job because some data writers are failed and keep failing when retry,
   * or the Spark job fails with some unknown reasons,
   * or {@link #commit()} /{@link #add(WriterCommitMessage)} fails.
   *
   * If this method fails (by throwing an exception), the underlying data source may require manual
   * cleanup.
   */
  void abort();
}
