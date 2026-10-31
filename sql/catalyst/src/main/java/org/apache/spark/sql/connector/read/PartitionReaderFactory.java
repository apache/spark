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

import java.io.Serializable;
import java.util.Map;

import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * A factory used to create {@link PartitionReader} instances.
 * <p>
 * If Spark fails to execute any methods in the implementations of this interface or in the returned
 * {@link PartitionReader} (by throwing an exception), corresponding Spark task would fail and
 * get retried until hitting the maximum retry times.
 *
 * @since 3.0.0
 */
@Evolving
public interface PartitionReaderFactory extends Serializable {

  /**
   * Returns a row-based partition reader to read data from the given {@link InputPartition}.
   * <p>
   * Implementations probably need to cast the input partition to the concrete
   * {@link InputPartition} class defined for the data source.
   */
  PartitionReader<InternalRow> createReader(InputPartition partition);

  /**
   * Returns a columnar partition reader to read data from the given {@link InputPartition}.
   * <p>
   * Implementations probably need to cast the input partition to the concrete
   * {@link InputPartition} class defined for the data source.
   */
  default PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
    throw new SparkUnsupportedOperationException(
      "DATA_SOURCE_COLUMNAR_READER_NOT_SUPPORTED", Map.of("class", getClass().getName()));
  }

  /**
   * Returns a row-based partition reader that may apply the storage filter of {@code request} while
   * it reads the given {@link InputPartition}. Spark calls this instead of
   * {@link #createReader(InputPartition)} for a {@link SupportsStorageFilters} scan it offered a
   * filter, so the factory of such a scan must implement it for row-based reads.
   *
   * @since 5.0.0
   */
  default PartitionReader<InternalRow> createReader(
      InputPartition partition, StorageFilterRequest request) {
    throw new SparkUnsupportedOperationException(
      "DATA_SOURCE_STORAGE_FILTER_READER_NOT_SUPPORTED", Map.of("class", getClass().getName()));
  }

  /**
   * Returns a columnar partition reader that may apply the storage filter of {@code request} while
   * it reads the given {@link InputPartition}. Spark calls this instead of
   * {@link #createColumnarReader(InputPartition)} for a {@link SupportsStorageFilters} scan it
   * offered a filter, so the factory of such a scan must implement it for columnar reads.
   *
   * @since 5.0.0
   */
  default PartitionReader<ColumnarBatch> createColumnarReader(
      InputPartition partition, StorageFilterRequest request) {
    throw new SparkUnsupportedOperationException(
      "DATA_SOURCE_STORAGE_FILTER_READER_NOT_SUPPORTED", Map.of("class", getClass().getName()));
  }

  /**
   * Returns true if the given {@link InputPartition} should be read by Spark in a columnar way.
   * This means, implementations must also implement {@link #createColumnarReader(InputPartition)}
   * for the input partitions that this method returns true, and
   * {@link #createColumnarReader(InputPartition, StorageFilterRequest)} for a scan offered a
   * storage filter.
   * <p>
   * As of Spark 2.4, Spark can only read all input partition in a columnar way, or none of them.
   * Data source can't mix columnar and row-based partitions. This may be relaxed in future
   * versions.
   */
  default boolean supportColumnarReads(InputPartition partition) {
    return false;
  }
}
