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

package org.apache.spark.sql.connector.read;

import org.apache.spark.annotation.Evolving;

/**
 * A mix-in interface for a {@link PartitionReaderFactory} that also reads a partition as an
 * <a href="https://arrow.apache.org/docs/format/CStreamInterface.html">Arrow C stream</a>, for an
 * engine that processes Arrow data in native code: the engine imports the arrays of the stream
 * itself, instead of reading the {@link org.apache.spark.sql.vectorized.ColumnarBatch}es of
 * {@link #createColumnarReader(InputPartition)}. The reader factories of the batch and
 * micro-batch scans of native data sources implement it, with the stream that the library
 * returns.
 * <p>
 * Each array of the stream is a struct array whose fields are the columns of the read schema of
 * the scan. Spark does not check the stream, while it checks the streams that it reads itself, so
 * the engine checks what its import of the arrays requires, such as the schema of the stream
 * against the read schema, and the offsets and the lengths of the arrays. The callbacks of the
 * stream can be called from any thread, but never concurrently.
 *
 * @since 4.4.0
 */
@Evolving
public interface SupportsArrowStreamRead extends PartitionReaderFactory {

  /**
   * Initializes an Arrow C stream with the data of a partition. It is called on the thread of the
   * running task that reads the partition, and fails otherwise. The caller reads the stream in
   * that task: once the task ended, the source may stop returning data and fail. The caller owns
   * the stream, which it may move, and releases it, which may happen after the task ended: Spark
   * frees what the stream uses, such as the context of the task, once the stream is released. A
   * stream that is never released leaks.
   *
   * @param partition a partition of the scan, as the scan planned it
   * @param streamAddress the address of a {@code struct ArrowArrayStream} that the caller
   *                      allocated, and whose release callback is NULL, as it is in a
   *                      zero-initialized struct. If this method fails after it checked its
   *                      arguments, the stream is released. A stream whose release callback
   *                      is not NULL is left as it is.
   */
  void openArrowStream(InputPartition partition, long streamAddress);
}
