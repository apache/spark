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

import java.io.IOException;

import org.apache.spark.annotation.InterfaceStability;

/**
 * A data writer returned by {@link DataWriterFactory#createDataWriter(int, int)} and is
 * responsible for writing data for an input RDD partition.
 *
 * One Spark task has one exclusive data writer, so there is no thread-safe concern.
 *
 * {@link #write(Object)} is called for each record in the input RDD partition. If one record fails
 * the {@link #write(Object)}, {@link #abort()} is called afterwards and the remaining records will
 * not be processed. If all records are successfully written, {@link #commit()} is called.
 *
 * If this data writer succeeds(all records are successfully written and {@link #commit()}
 * succeeds), a {@link WriterCommitMessage} will be sent to the driver side and pass to
 * {@link DataSourceV2Writer#commit(WriterCommitMessage[])} with commit messages from other data
 * writers. If this data writer fails(one record fails to write or {@link #commit()} fails), an
 * exception will be sent to the driver side, and Spark will retry this writing task for some times,
 * each time {@link DataWriterFactory#createDataWriter(int, int)} gets a different `attemptNumber`,
 * and finally call {@link DataSourceV2Writer#abort(WriterCommitMessage[])} if all retry fail.
 *
 * Besides the retry mechanism, Spark may launch speculative tasks if the existing writing task
 * takes too long to finish. Different from retried tasks, which are launched one by one after the
 * previous one fails, speculative tasks are running simultaneously. It's possible that one input
 * RDD partition has multiple data writers with different `attemptNumber` running at the same time,
 * and data sources should guarantee that these data writers don't conflict and can work together.
 * Implementations can coordinate with driver during {@link #commit()} to make sure only one of
 * these data writers can commit successfully. Or implementations can allow all of them to commit
 * successfully, and have a way to revert committed data writers without the commit message, because
 * Spark only accepts the commit message that arrives first and ignore others.
 *
 * Note that, Currently the type `T` can only be {@link org.apache.spark.sql.Row} for normal data
 * source writers, or {@link org.apache.spark.sql.catalyst.InternalRow} for data source writers
 * that mix in {@link SupportsWriteInternalRow}.
 */
@InterfaceStability.Evolving
public interface DataWriter<T> {

  /**
   * Writes one record.
   *
   * If this method fails (by throwing an exception), {@link #abort()} will be called and this
   * data writer is considered to have been failed.
   *
   * @throws IOException if failure happens during disk/network IO like writing files.
   */
  void write(T record) throws IOException;

  /**
   * Commits this writer after all records are written successfully, returns a commit message which
   * will be sent back to driver side and passed to
   * {@link DataSourceV2Writer#commit(WriterCommitMessage[])}.
   *
   * The written data should only be visible to data source readers after
   * {@link DataSourceV2Writer#commit(WriterCommitMessage[])} succeeds, which means this method
   * should still "hide" the written data and ask the {@link DataSourceV2Writer} at driver side to
   * do the final commit via {@link WriterCommitMessage}.
   *
   * If this method fails (by throwing an exception), {@link #abort()} will be called and this
   * data writer is considered to have been failed.
   *
   * @throws IOException if failure happens during disk/network IO like writing files.
   */
  WriterCommitMessage commit() throws IOException;

  /**
   * Aborts this writer if it is failed. Implementations should clean up the data for already
   * written records.
   *
   * This method will only be called if there is one record failed to write, or {@link #commit()}
   * failed.
   *
   * If this method fails(by throwing an exception), the underlying data source may have garbage
   * that need to be cleaned by {@link DataSourceV2Writer#abort(WriterCommitMessage[])} or manually,
   * but these garbage should not be visible to data source readers.
   *
   * @throws IOException if failure happens during disk/network IO like writing files.
   */
  void abort() throws IOException;
}
