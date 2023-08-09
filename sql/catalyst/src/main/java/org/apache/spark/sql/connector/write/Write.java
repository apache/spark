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

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;

/**
 * A logical representation of a data source write.
 * <p>
 * This logical representation is shared between batch and streaming write. Data sources must
 * implement the corresponding methods in this interface to match what the table promises
 * to support. For example, {@link #toBatch()} must be implemented if the {@link Table} that
 * creates this {@link Write} returns {@link TableCapability#BATCH_WRITE} support in its
 * {@link Table#capabilities()}.
 *
 * @since 3.2.0
 */
@Evolving
public interface Write {

  /**
   * Returns the description associated with this write.
   */
  default String description() {
    return this.getClass().toString();
  }

  /**
   * Returns a {@link BatchWrite} to write data to batch source. By default this method throws
   * exception, data sources must overwrite this method to provide an implementation, if the
   * {@link Table} that creates this write returns {@link TableCapability#BATCH_WRITE} support in
   * its {@link Table#capabilities()}.
   */
  default BatchWrite toBatch() {
    throw new UnsupportedOperationException(description() + ": Batch write is not supported");
  }

  /**
   * Returns a {@link StreamingWrite} to write data to streaming source. By default this method
   * throws exception, data sources must overwrite this method to provide an implementation, if the
   * {@link Table} that creates this write returns {@link TableCapability#STREAMING_WRITE} support
   * in its {@link Table#capabilities()}.
   */
  default StreamingWrite toStreaming() {
    throw new UnsupportedOperationException(description() + ": Streaming write is not supported");
  }

  /**
   * Returns an array of supported custom metrics with name and description.
   * By default it returns empty array.
   */
  default CustomMetric[] supportedCustomMetrics() {
    return new CustomMetric[]{};
  }
}
