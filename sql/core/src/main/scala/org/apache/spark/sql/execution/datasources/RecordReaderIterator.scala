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

import java.io.Closeable

import org.apache.hadoop.mapreduce.RecordReader

import org.apache.spark.sql.catalyst.InternalRow

/**
 * An adaptor from a Hadoop [[RecordReader]] to an [[Iterator]] over the values returned.
 *
 * Note that this returns [[Object]]s instead of [[InternalRow]] because we rely on erasure to pass
 * column batches by pretending they are rows.
 */
class RecordReaderIterator[T](
    private[this] var rowReader: RecordReader[_, T]) extends Iterator[T] with Closeable {
  private[this] var havePair = false
  private[this] var finished = false

  override def hasNext: Boolean = {
    if (!finished && !havePair) {
      finished = !rowReader.nextKeyValue
      if (finished) {
        // Close and release the reader here; close() will also be called when the task
        // completes, but for tasks that read from many files, it helps to release the
        // resources early.
        close()
      }
      havePair = !finished
    }
    !finished
  }

  override def next(): T = {
    if (!hasNext) {
      throw new java.util.NoSuchElementException("End of stream")
    }
    havePair = false
    rowReader.getCurrentValue
  }

  override def close(): Unit = {
    if (rowReader != null) {
      try {
        rowReader.close()
      } finally {
        rowReader = null
      }
    }
  }
}
