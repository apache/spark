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

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.sources.v2.reader.DataSourceV2Reader;
import org.apache.spark.sql.types.StructType;

/**
 * A mix-in interface for {@link DataSourceV2}. Data sources can implement this interface to
 * provide data reading ability and scan the data from the data source.
 *
 * This is a variant of {@link ReadSupport} that accepts user-specified schema when reading data.
 * A data source can implement both {@link ReadSupport} and {@link ReadSupportWithSchema} if it
 * supports both schema inference and user-specified schema.
 */
@InterfaceStability.Evolving
public interface ReadSupportWithSchema {

  /**
   * Create a {@link DataSourceV2Reader} to scan the data from this data source.
   *
   * If this method fails (by throwing an exception), the action would fail and no Spark job was
   * submitted.
   *
   * @param schema the full schema of this data source reader. Full schema usually maps to the
   *               physical schema of the underlying storage of this data source reader, e.g.
   *               CSV files, JSON files, etc, while this reader may not read data with full
   *               schema, as column pruning or other optimizations may happen.
   * @param options the options for the returned data source reader, which is an immutable
   *                case-insensitive string-to-string map.
   */
  DataSourceV2Reader createReader(StructType schema, DataSourceV2Options options);
}
