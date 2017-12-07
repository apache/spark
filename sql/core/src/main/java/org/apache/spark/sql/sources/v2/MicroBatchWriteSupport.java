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

import java.util.Optional;

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.execution.streaming.BaseStreamingSink;
import org.apache.spark.sql.sources.v2.writer.DataSourceV2Writer;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.StructType;

/**
 * A mix-in interface for {@link DataSourceV2}. Data sources can implement this interface to
 * provide data writing ability and save the data from a microbatch to the data source.
 */
@InterfaceStability.Evolving
public interface MicroBatchWriteSupport extends BaseStreamingSink {

  /**
   * Creates an optional {@link DataSourceV2Writer} to save the data to this data source. Data
   * sources can return None if there is no writing needed to be done.
   *
   * @param queryId A unique string for the writing query. It's possible that there are many writing
   *                queries running at the same time, and the returned {@link DataSourceV2Writer}
   *                can use this id to distinguish itself from others.
   * @param epochId The uniquenumeric ID of the batch within this writing query. This is an
   *                incrementing counter representing a consistent set of data; the same batch may
   *                be started multiple times in failure recovery scenarios, but it will always
   *                contain the same records.
   * @param schema the schema of the data to be written.
   * @param mode the output mode which determines what successive batch output means to this
   *             source, please refer to {@link OutputMode} for more details.
   * @param options the options for the returned data source writer, which is an immutable
   *                case-insensitive string-to-string map.
   */
  Optional<DataSourceV2Writer> createMicroBatchWriter(
      String queryId,
      long epochId,
      StructType schema,
      OutputMode mode,
      DataSourceV2Options options);
}
