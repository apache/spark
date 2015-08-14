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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{MapData, ArrayData}

// TODO Removes this while fixing SPARK-8848
private[sql] object CatalystConverter {
  // This is mostly Parquet convention (see, e.g., `ConversionPatterns`).
  // Note that "array" for the array elements is chosen by ParquetAvro.
  // Using a different value will result in Parquet silently dropping columns.
  val ARRAY_CONTAINS_NULL_BAG_SCHEMA_NAME = "bag"
  val ARRAY_ELEMENTS_SCHEMA_NAME = "array"

  val MAP_KEY_SCHEMA_NAME = "key"
  val MAP_VALUE_SCHEMA_NAME = "value"
  val MAP_SCHEMA_NAME = "map"

  // TODO: consider using Array[T] for arrays to avoid boxing of primitive types
  type ArrayScalaType = ArrayData
  type StructScalaType = InternalRow
  type MapScalaType = MapData
}
