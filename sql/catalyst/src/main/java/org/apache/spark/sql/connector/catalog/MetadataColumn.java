/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.connector.catalog;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataType;

/**
 * Interface for a metadata column.
 * <p>
 * A metadata column can expose additional metadata about a row. For example, rows from Kafka can
 * use metadata columns to expose a message's topic, partition number, and offset.
 * <p>
 * A metadata column could also be the result of a transform applied to a value in the row. For
 * example, a partition value produced by bucket(id, 16) could be exposed by a metadata column. In
 * this case, {@link #transform()} should return a non-null {@link Transform} that produced the
 * metadata column's values.
 *
 * @since 3.1.0
 */
@Evolving
public interface MetadataColumn {
  /**
   * The name of this metadata column.
   *
   * @return a String name
   */
  String name();

  /**
   * The data type of values in this metadata column.
   *
   * @return a {@link DataType}
   */
  DataType dataType();

  /**
   * @return whether values produced by this metadata column may be null
   */
  default boolean isNullable() {
    return true;
  }

  /**
   * Documentation for this metadata column, or null.
   *
   * @return a documentation String
   */
  default String comment() {
    return null;
  }

  /**
   * The {@link Transform} used to produce this metadata column from data rows, or null.
   *
   * @return a {@link Transform} used to produce the column's values, or null if there isn't one
   */
  default Transform transform() {
    return null;
  }
}
