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

package org.apache.spark.sql.sources.v2.writer;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.sources.v2.Table;
import org.apache.spark.sql.sources.v2.TableCapability;
import org.apache.spark.sql.sources.v2.writer.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructType;

/**
 * An interface for building the {@link BatchWrite}. Implementations can mix in some interfaces to
 * support different ways to write data to data sources.
 *
 * Unless modified by a mixin interface, the {@link BatchWrite} configured by this builder is to
 * append data without affecting existing data.
 */
@Evolving
public interface WriteBuilder {

  /**
   * Passes the `queryId` from Spark to data source. `queryId` is a unique string of the query. It's
   * possible that there are many queries running at the same time, or a query is restarted and
   * resumed. {@link BatchWrite} can use this id to identify the query.
   *
   * @return a new builder with the `queryId`. By default it returns `this`, which means the given
   *         `queryId` is ignored. Please override this method to take the `queryId`.
   */
  default WriteBuilder withQueryId(String queryId) {
    return this;
  }

  /**
   * Passes the schema of the input data from Spark to data source.
   *
   * @return a new builder with the `schema`. By default it returns `this`, which means the given
   *         `schema` is ignored. Please override this method to take the `schema`.
   */
  default WriteBuilder withInputDataSchema(StructType schema) {
    return this;
  }

  /**
   * Returns a {@link BatchWrite} to write data to batch source. By default this method throws
   * exception, data sources must overwrite this method to provide an implementation, if the
   * {@link Table} that creates this write returns {@link TableCapability#BATCH_WRITE} support in
   * its {@link Table#capabilities()}.
   *
   * Note that, the returned {@link BatchWrite} can be null if the implementation supports SaveMode,
   * to indicate that no writing is needed. We can clean it up after removing
   * {@link SupportsSaveMode}.
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
