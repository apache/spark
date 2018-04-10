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

package org.apache.spark.sql.sources.v2.reader;

import java.io.Closeable;
import java.io.IOException;

import org.apache.spark.annotation.InterfaceStability;

/**
 * A data reader returned by the create data reader method in {@link DataReaderFactory} and is
 * responsible for outputting data for a RDD partition.
 *
 * Note that, Currently the type `T` can only be {@link org.apache.spark.sql.Row},
 * or {@link org.apache.spark.sql.catalyst.expressions.UnsafeRow}, or
 * {@link org.apache.spark.sql.vectorized.ColumnarBatch}, depending on the return type of
 * {@link DataReaderFactory#dataFormat()}.
 */
@InterfaceStability.Evolving
public interface DataReader<T> extends Closeable {

  /**
   * Proceed to next record, returns false if there is no more records.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   *
   * @throws IOException if failure happens during disk/network IO like reading files.
   */
  boolean next() throws IOException;

  /**
   * Return the current record. This method should return same value until `next` is called.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   */
  T get();
}
