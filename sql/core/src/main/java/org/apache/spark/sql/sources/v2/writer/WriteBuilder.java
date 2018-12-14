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
import org.apache.spark.sql.sources.v2.SupportsBatchWrite;
import org.apache.spark.sql.sources.v2.Table;

/**
 * An interface for building the {@link BatchWrite}. Implementations can mix in interfaces like
 * {@link SupportsSaveMode} to support different ways to write data to data sources.
 */
@Evolving
public interface WriteBuilder {

  /**
   * Returns a {@link BatchWrite} to write data to batch source. By default this method throws
   * exception, data sources must overwrite this method to provide an implementation, if the
   * {@link Table} that creates this scan implements {@link SupportsBatchWrite}.
   *
   * Note that, the returned {@link BatchWrite} can be null if the implementation supports SaveMode,
   * to indicate that no writing is needed. We can clean it up after removing
   * {@link SupportsSaveMode}.
   */
  default BatchWrite buildForBatch() {
    throw new UnsupportedOperationException("Batch scans are not supported");
  }
}
