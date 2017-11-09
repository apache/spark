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
import org.apache.spark.sql.sources.v2.DataSourceV2Options;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.types.StructType;

/**
 * A data source writer that is returned by
 * {@link WriteSupport#createWriter(String, StructType, SaveMode, DataSourceV2Options)}.
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
 *      writer. If all the data are written successfully, call {@link DataWriter#commit()}. If
 *      exception happens during the writing, call {@link DataWriter#abort()}.
 *   3. If all writers are successfully committed, call {@link #commit(WriterCommitMessage[])}. If
 *      some writers are aborted, or the job failed with an unknown reason, call
 *      {@link #abort(WriterCommitMessage[])}.
 *
 * While Spark will retry failed writing tasks, Spark won't retry failed writing jobs. Users should
 * do it manually in their Spark applications if they want to retry.
 *
 * Please refer to the documentation of commit/abort methods for detailed specifications.
 */
@InterfaceStability.Evolving
public interface DataSourceV2Writer {

  /**
   * Creates a writer factory which will be serialized and sent to executors.
   *
   * If this method fails (by throwing an exception), the action would fail and no Spark job was
   * submitted.
   */
  DataWriterFactory<Row> createWriterFactory();

  /**
   * Commits this writing job with a list of commit messages. The commit messages are collected from
   * successful data writers and are produced by {@link DataWriter#commit()}.
   *
   * If this method fails (by throwing an exception), this writing job is considered to to have been
   * failed, and {@link #abort(WriterCommitMessage[])} would be called. The state of the destination
   * is undefined and @{@link #abort(WriterCommitMessage[])} may not be able to deal with it.
   *
   * Note that, one partition may have multiple committed data writers because of speculative tasks.
   * Spark will pick the first successful one and get its commit message. Implementations should be
   * aware of this and handle it correctly, e.g., have a coordinator to make sure only one data
   * writer can commit, or have a way to clean up the data of already-committed writers.
   */
  void commit(WriterCommitMessage[] messages);

  /**
   * Aborts this writing job because some data writers are failed and keep failing when retry, or
   * the Spark job fails with some unknown reasons, or {@link #commit(WriterCommitMessage[])} fails.
   *
   * If this method fails (by throwing an exception), the underlying data source may require manual
   * cleanup.
   *
   * Unless the abort is triggered by the failure of commit, the given messages should have some
   * null slots as there maybe only a few data writers that are committed before the abort
   * happens, or some data writers were committed but their commit messages haven't reached the
   * driver when the abort is triggered. So this is just a "best effort" for data sources to
   * clean up the data left by data writers.
   */
  void abort(WriterCommitMessage[] messages);
}
