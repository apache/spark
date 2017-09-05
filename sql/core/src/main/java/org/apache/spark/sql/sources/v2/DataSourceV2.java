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

import org.apache.spark.sql.sources.v2.reader.DataSourceV2Reader;
import org.apache.spark.sql.types.StructType;

/**
 * The main interface for data source v2 implementations. Users can mix in more interfaces to
 * implement more functions other than just scan.
 */
public interface DataSourceV2 {

  /**
   * Create a `DataSourceV2Reader` to scan the data for this data source.
   *
   * @param options the options for this data source reader, which is an immutable case-insensitive
   *                string-to-string map.
   * @return a reader that implements the actual read logic.
   */
  DataSourceV2Reader createReader(DataSourceV2Options options);
}
