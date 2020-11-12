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
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;

/**
 * An interface for building the {@link BatchWrite}. Implementations can mix in some interfaces to
 * support different ways to write data to data sources.
 *
 * Unless modified by a mixin interface, the {@link BatchWrite} configured by this builder is to
 * append data without affecting existing data.
 *
 * @since 3.0.0
 */
@Evolving
public interface WriteBuilder {

  /**
   * Returns a {@link BatchWrite} to write data to batch source. By default this method throws
   * exception, data sources must overwrite this method to provide an implementation, if the
   * {@link Table} that creates this write returns {@link TableCapability#BATCH_WRITE} support in
   * its {@link Table#capabilities()}.
   */
  default BatchWrite buildForBatch() {
    throw new UnsupportedOperationException(getClass().getName() +
      " does not support batch write");
  }

  /**
   * Returns a {@link StreamingWrite} to write data to streaming source. By default this method
   * throws exception, data sources must overwrite this method to provide an implementation, if the
   * {@link Table} that creates this write returns {@link TableCapability#STREAMING_WRITE} support
   * in its {@link Table#capabilities()}.
   */
  default StreamingWrite buildForStreaming() {
    throw new UnsupportedOperationException(getClass().getName() +
      " does not support streaming write");
  }
}
