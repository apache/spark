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

import java.util.Map;

import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;

/**
 * An interface for building the {@link Write}. Implementations can mix in some interfaces to
 * support different ways to write data to data sources.
 * <p>
 * Unless modified by a mixin interface, the {@link Write} configured by this builder is to
 * append data without affecting existing data.
 *
 * @since 3.0.0
 */
@Evolving
public interface WriteBuilder {

  /**
   * Returns a logical {@link Write} shared between batch and streaming.
   *
   * @since 3.2.0
   */
  default Write build() {
    return new Write() {
      @Override
      public BatchWrite toBatch() {
        return buildForBatch();
      }

      @Override
      public StreamingWrite toStreaming() {
        return buildForStreaming();
      }
    };
  }

  /**
   * Returns a {@link BatchWrite} to write data to batch source.
   *
   * @deprecated use {@link #build()} instead.
   */
  @Deprecated(since = "3.2.0")
  default BatchWrite buildForBatch() {
    throw new SparkUnsupportedOperationException(
      "_LEGACY_ERROR_TEMP_3135", Map.of("class", getClass().getName()));
  }

  /**
   * Returns a {@link StreamingWrite} to write data to streaming source.
   *
   * @deprecated use {@link #build()} instead.
   */
  @Deprecated(since = "3.2.0")
  default StreamingWrite buildForStreaming() {
    throw new SparkUnsupportedOperationException(
      "_LEGACY_ERROR_TEMP_3136", Map.of("class", getClass().getName()));
  }
}
