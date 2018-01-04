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

import java.util.List;

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * A mix-in interface for {@link DataSourceV2Reader}. Data source readers can implement this
 * interface to output {@link ColumnarBatch} and make the scan faster.
 */
@InterfaceStability.Evolving
public interface SupportsScanColumnarBatch extends DataSourceV2Reader {
  @Override
  default List<ReadTask<Row>> createReadTasks() {
    throw new IllegalStateException(
      "createReadTasks should not be called with SupportsScanColumnarBatch.");
  }

  /**
   * Similar to {@link DataSourceV2Reader#createReadTasks()}, but returns columnar data in batches.
   */
  List<ReadTask<ColumnarBatch>> createBatchReadTasks();

  /**
   * A safety door for columnar batch reader. It's possible that the implementation can only support
   * some certain columns with certain types. Users can overwrite this method and
   * {@link #createReadTasks()} to fallback to normal read path under some conditions.
   */
  default boolean enableBatchRead() {
    return true;
  }
}
