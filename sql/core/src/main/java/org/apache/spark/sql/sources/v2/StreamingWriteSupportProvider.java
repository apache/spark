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

package org.apache.spark.sql.sources.v2;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.execution.streaming.BaseStreamingSink;
import org.apache.spark.sql.sources.v2.writer.streaming.StreamingWriteSupport;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.StructType;

/**
 * A mix-in interface for {@link DataSourceV2}. Data sources can implement this interface to
 * provide data writing ability for structured streaming.
 *
 * This interface is used to create {@link StreamingWriteSupport} instances when end users run
 * {@code Dataset.writeStream.format(...).option(...).start()}.
 */
@Evolving
public interface StreamingWriteSupportProvider extends DataSourceV2, BaseStreamingSink {

  /**
   * Creates a {@link StreamingWriteSupport} instance to save the data to this data source, which is
   * called by Spark at the beginning of each streaming query.
   *
   * @param queryId A unique string for the writing query. It's possible that there are many
   *                writing queries running at the same time, and the returned
   *                {@link StreamingWriteSupport} can use this id to distinguish itself from others.
   * @param schema the schema of the data to be written.
   * @param mode the output mode which determines what successive epoch output means to this
   *             sink, please refer to {@link OutputMode} for more details.
   * @param options the options for the returned data source writer, which is an immutable
   *                case-insensitive string-to-string map.
   */
  StreamingWriteSupport createStreamingWriteSupport(
      String queryId,
      StructType schema,
      OutputMode mode,
      DataSourceOptions options);
}
