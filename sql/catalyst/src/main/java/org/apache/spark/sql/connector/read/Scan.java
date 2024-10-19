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

import java.util.Map;

import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.streaming.ContinuousStream;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;

/**
 * A logical representation of a data source scan. This interface is used to provide logical
 * information, like what the actual read schema is.
 * <p>
 * This logical representation is shared between batch scan, micro-batch streaming scan and
 * continuous streaming scan. Data sources must implement the corresponding methods in this
 * interface, to match what the table promises to support. For example, {@link #toBatch()} must be
 * implemented, if the {@link Table} that creates this {@link Scan} returns
 * {@link TableCapability#BATCH_READ} support in its {@link Table#capabilities()}.
 * </p>
 *
 * @since 3.0.0
 */
@Evolving
public interface Scan {

  /**
   * Returns the actual schema of this data source scan, which may be different from the physical
   * schema of the underlying storage, as column pruning or other optimizations may happen.
   */
  StructType readSchema();

  /**
   * A description string of this scan, which may includes information like: what filters are
   * configured for this scan, what's the value of some important options like path, etc. The
   * description doesn't need to include {@link #readSchema()}, as Spark already knows it.
   * <p>
   * By default this returns the class name of the implementation. Please override it to provide a
   * meaningful description.
   * </p>
   */
  default String description() {
    return this.getClass().toString();
  }

  /**
   * Returns the physical representation of this scan for batch query. By default this method throws
   * exception, data sources must overwrite this method to provide an implementation, if the
   * {@link Table} that creates this scan returns {@link TableCapability#BATCH_READ} support in its
   * {@link Table#capabilities()}.
   * <p>
   * If the scan supports runtime filtering and implements {@link SupportsRuntimeFiltering},
   * this method may be called multiple times. Therefore, implementations can cache some state
   * to avoid planning the job twice.
   *
   * @throws UnsupportedOperationException
   */
  default Batch toBatch() {
    throw new SparkUnsupportedOperationException(
      "_LEGACY_ERROR_TEMP_3147", Map.of("description", description()));
  }

  /**
   * Returns the physical representation of this scan for streaming query with micro-batch mode. By
   * default this method throws exception, data sources must overwrite this method to provide an
   * implementation, if the {@link Table} that creates this scan returns
   * {@link TableCapability#MICRO_BATCH_READ} support in its {@link Table#capabilities()}.
   *
   * @param checkpointLocation a path to Hadoop FS scratch space that can be used for failure
   *                           recovery. Data streams for the same logical source in the same query
   *                           will be given the same checkpointLocation.
   *
   * @throws UnsupportedOperationException
   */
  default MicroBatchStream toMicroBatchStream(String checkpointLocation) {
    throw new SparkUnsupportedOperationException(
      "_LEGACY_ERROR_TEMP_3148", Map.of("description", description()));
  }

  /**
   * Returns the physical representation of this scan for streaming query with continuous mode. By
   * default this method throws exception, data sources must overwrite this method to provide an
   * implementation, if the {@link Table} that creates this scan returns
   * {@link TableCapability#CONTINUOUS_READ} support in its {@link Table#capabilities()}.
   *
   * @param checkpointLocation a path to Hadoop FS scratch space that can be used for failure
   *                           recovery. Data streams for the same logical source in the same query
   *                           will be given the same checkpointLocation.
   *
   * @throws UnsupportedOperationException
   */
  default ContinuousStream toContinuousStream(String checkpointLocation) {
    throw new SparkUnsupportedOperationException(
      "_LEGACY_ERROR_TEMP_3149", Map.of("description", description()));
  }

  /**
   * Returns an array of supported custom metrics with name and description.
   * By default it returns empty array.
   */
  default CustomMetric[] supportedCustomMetrics() {
    return new CustomMetric[]{};
  }

  /**
   * Returns an array of custom metrics which are collected with values at the driver side only.
   * Note that these metrics must be included in the supported custom metrics reported by
   * `supportedCustomMetrics`.
   *
   * @since 3.4.0
   */
  default CustomTaskMetric[] reportDriverMetrics() {
    return new CustomTaskMetric[]{};
  }

  /**
   * This enum defines how the columnar support for the partitions of the data source
   * should be determined. The default value is `PARTITION_DEFINED` which indicates that each
   * partition can determine if it should be columnar or not. SUPPORTED and UNSUPPORTED provide
   * default shortcuts to indicate support for columnar data or not.
   *
   * @since 3.5.0
   */
  enum ColumnarSupportMode {
    PARTITION_DEFINED,
    SUPPORTED,
    UNSUPPORTED
  }

  /**
   * Subclasses can implement this method to indicate if the support for columnar data should
   * be determined by each partition or is set as a default for the whole scan.
   *
   * @since 3.5.0
   */
  default ColumnarSupportMode columnarSupportMode() {
    return ColumnarSupportMode.PARTITION_DEFINED;
  }
}
