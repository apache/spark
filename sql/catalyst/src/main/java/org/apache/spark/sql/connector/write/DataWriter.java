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

package org.apache.spark.sql.connector.write;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;

/**
 * A data writer returned by {@link DataWriterFactory#createWriter(int, long)} and is
 * responsible for writing data for an input RDD partition.
 * <p>
 * One Spark task has one exclusive data writer, so there is no thread-safe concern.
 * <p>
 * {@link #write(Object)} is called for each record in the input RDD partition. If one record fails
 * the {@link #write(Object)}, {@link #abort()} is called afterwards and the remaining records will
 * not be processed. If all records are successfully written, {@link #commit()} is called.
 * <p>
 * Once a data writer returns successfully from {@link #commit()} or {@link #abort()}, Spark will
 * call {@link #close()} to let DataWriter doing resource cleanup. After calling {@link #close()},
 * its lifecycle is over and Spark will not use it again.
 * <p>
 * If this data writer succeeds(all records are successfully written and {@link #commit()}
 * succeeds), a {@link WriterCommitMessage} will be sent to the driver side and pass to
 * {@link BatchWrite#commit(WriterCommitMessage[])} with commit messages from other data
 * writers. If this data writer fails(one record fails to write or {@link #commit()} fails), an
 * exception will be sent to the driver side, and Spark may retry this writing task a few times.
 * In each retry, {@link DataWriterFactory#createWriter(int, long)} will receive a
 * different `taskId`. Spark will call {@link BatchWrite#abort(WriterCommitMessage[])}
 * when the configured number of retries is exhausted.
 * <p>
 * Besides the retry mechanism, Spark may launch speculative tasks if the existing writing task
 * takes too long to finish. Different from retried tasks, which are launched one by one after the
 * previous one fails, speculative tasks are running simultaneously. It's possible that one input
 * RDD partition has multiple data writers with different `taskId` running at the same time,
 * and data sources should guarantee that these data writers don't conflict and can work together.
 * Implementations can coordinate with driver during {@link #commit()} to make sure only one of
 * these data writers can commit successfully. Or implementations can allow all of them to commit
 * successfully, and have a way to revert committed data writers without the commit message, because
 * Spark only accepts the commit message that arrives first and ignore others.
 * <p>
 * Note that, Currently the type {@code T} can only be
 * {@link org.apache.spark.sql.catalyst.InternalRow}.
 *
 * @since 3.0.0
 */
@Evolving
public interface DataWriter<T> extends Closeable {

  /**
   * Writes one record.
   * <p>
   * If this method fails (by throwing an exception), {@link #abort()} will be called and this
   * data writer is considered to have been failed.
   *
   * @throws IOException if failure happens during disk/network IO like writing files.
   */
  void write(T record) throws IOException;

  /**
   * Writes all records provided by the given iterator. By default, it calls the {@link #write}
   * method for each record in the iterator.
   * <p>
   * If this method fails (by throwing an exception), {@link #abort()} will be called and this
   * data writer is considered to have been failed.
   *
   * @throws IOException if failure happens during disk/network IO like writing files.
   *
   * @since 4.0.0
   */
  default void writeAll(Iterator<T> records) throws IOException {
    while (records.hasNext()) {
      write(records.next());
    }
  }

  /**
   * Commits this writer after all records are written successfully, returns a commit message which
   * will be sent back to driver side and passed to
   * {@link BatchWrite#commit(WriterCommitMessage[])}.
   * <p>
   * The written data should only be visible to data source readers after
   * {@link BatchWrite#commit(WriterCommitMessage[])} succeeds, which means this method
   * should still "hide" the written data and ask the {@link BatchWrite} at driver side to
   * do the final commit via {@link WriterCommitMessage}.
   * <p>
   * If this method fails (by throwing an exception), {@link #abort()} will be called and this
   * data writer is considered to have been failed.
   *
   * @throws IOException if failure happens during disk/network IO like writing files.
   */
  WriterCommitMessage commit() throws IOException;

  default WriterCommitMessage commit(long taskWriteDataTime) throws IOException {
    // Do nothing
    return null;
  }

  /**
   * Aborts this writer if it is failed. Implementations should clean up the data for already
   * written records.
   * <p>
   * This method will only be called if there is one record failed to write, or {@link #commit()}
   * failed.
   * <p>
   * If this method fails(by throwing an exception), the underlying data source may have garbage
   * that need to be cleaned by {@link BatchWrite#abort(WriterCommitMessage[])} or manually,
   * but these garbage should not be visible to data source readers.
   *
   * @throws IOException if failure happens during disk/network IO like writing files.
   */
  void abort() throws IOException;

  /**
   * Returns an array of custom task metrics. By default it returns empty array. Note that it is
   * not recommended to put heavy logic in this method as it may affect writing performance.
   */
  default CustomTaskMetric[] currentMetricsValues() {
    return new CustomTaskMetric[]{};
  }
}
