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

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.types.StructType;

/**
 * A mix-in interface for {@link DataSourceReader}. Data source readers can implement this
 * interface to push down required columns to the data source and only read these columns during
 * scan to reduce the size of the data to be read.
 */
@InterfaceStability.Evolving
public interface SupportsPushDownRequiredColumns extends DataSourceReader {

  /**
   * Applies column pruning w.r.t. the given requiredSchema.
   *
   * Implementation should try its best to prune the unnecessary columns or nested fields, but it's
   * also OK to do the pruning partially, e.g., a data source may not be able to prune nested
   * fields, and only prune top-level columns.
   *
   * Note that, data source readers should update {@link DataSourceReader#readSchema()} after
   * applying column pruning.
   */
  void pruneColumns(StructType requiredSchema);
}
