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
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.v2.writer.BatchWriteSupport;

/**
 * A mix-in interface for {@link DataSourceV2}. Data sources can implement this interface to
 * provide data writing ability for batch processing.
 *
 * This interface is used to create {@link BatchWriteSupport} instances when end users run
 * {@code Dataset.write.format(...).option(...).save()}.
 */
@InterfaceStability.Evolving
public interface BatchWriteSupportProvider extends DataSourceV2 {

  /**
   * Creates a {@link BatchWriteSupport} instance to save the data to a data source. Called by
   * Spark at the beginning of each batch query.
   *
   * Data sources can return None if there is no writing needed to be done according to the save
   * mode.
   *
   * @param mode the save mode which determines what to do when the data are already in this data
   *             source, please refer to {@link SaveMode} for more details.
   * @param options the options for the returned data source writer, which is an immutable
   *                case-insensitive string-to-string map.
   * @return a write support to write data to this data source.
   */
  // TODO: remove SaveMode
  Optional<BatchWriteSupport> createBatchWriteSupport(SaveMode mode, DataSourceOptions options);
}
