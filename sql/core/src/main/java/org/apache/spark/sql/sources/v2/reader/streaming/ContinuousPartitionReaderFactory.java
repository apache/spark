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

package org.apache.spark.sql.sources.v2.reader.streaming;

import java.io.Serializable;

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * A factory used to create {@link ContinuousPartitionReader} instances.
 */
@InterfaceStability.Evolving
public interface ContinuousPartitionReaderFactory extends Serializable {

  /**
   * Returns a row-based partition reader to read data from the given {@link InputPartition}.
   *
   * Implementations probably need to cast the input partition to the concrete
   * {@link InputPartition} class defined for the data source.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   */
  ContinuousPartitionReader<InternalRow> createContinuousReader(InputPartition partition);

  /**
   * Returns a columnar partition reader to read data from the given {@link InputPartition}.
   *
   * Implementations probably need to cast the input partition to the concrete
   * {@link InputPartition} class defined for the data source.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   */
  default ContinuousPartitionReader<ColumnarBatch> createContinuousColumnarReader(
      InputPartition partition) {
    throw new UnsupportedOperationException("Cannot create columnar reader.");
  }

  /**
   * Returns true if the given {@link InputPartition} should be read by Spark in a columnar way.
   * This means, implementations must also implement
   * {@link #createContinuousColumnarReader(InputPartition)} for the input partitions that this
   * method returns true.
   *
   * As of Spark 2.4, Spark can only read all input partition in a columnar way, or none of them.
   * Data source can't mix columnar and row-based partitions. This will be relaxed in future
   * versions.
   */
  default boolean supportColumnarReads(InputPartition partition) {
    return false;
  }
}
