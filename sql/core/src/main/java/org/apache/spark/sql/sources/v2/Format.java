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
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;

/**
 * The base interface for data source v2. Implementations must have a public, 0-arg constructor.
 *
 * The major responsibility of this interface is to return a {@link Table} for read/write.
 */
@InterfaceStability.Evolving
public interface Format extends DataSourceV2 {

  /**
   * Return a {@link Table} instance to do read/write with user-specified options.
   *
   * @param options the user-specified options that can identify a table, e.g. path, table name,
   *                Kafka topic name, etc. It's an immutable case-insensitive string-to-string map.
   */
  Table getTable(DataSourceOptions options);

  /**
   * Return a {@link Table} instance to do read/write with user-specified schema and options.
   *
   * By default this method throws {@link UnsupportedOperationException}, implementations should
   * override this method to handle user-specified schema.
   *
   * @param options the user-specified options that can identify a table, e.g. path, table name,
   *                Kafka topic name, etc. It's an immutable case-insensitive string-to-string map.
   * @param schema the user-specified schema.
   */
  default Table getTable(DataSourceOptions options, StructType schema) {
    String name;
    if (this instanceof DataSourceRegister) {
      name = ((DataSourceRegister) this).shortName();
    } else {
      name = this.getClass().getName();
    }
    throw new UnsupportedOperationException(
      name + " source does not support user-specified schema");
  }
}
