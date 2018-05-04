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

import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;

/**
 * An enum returned by {@link DataReaderFactory#dataFormat()}, representing the output data format of
 * a data source scan.
 *
 * <ul>
 *   <li>{@link #ROW}</li>
 *   <li>{@link #UNSAFE_ROW}</li>
 *   <li>{@link #COLUMNAR_BATCH}</li>
 * </ul>
 *
 * TODO: add INTERNAL_ROW
 */
public enum DataFormat {
  /**
   * Refers to {@link org.apache.spark.sql.Row}, which is very stable and guaranteed to be backward
   * compatible. Spark needs to convert data of row format to the internal format, data source developers
   * should consider using other formats for better performance.
   */
  ROW,

  /**
   * Refers to {@link org.apache.spark.sql.catalyst.expressions.UnsafeRow}, which is an unstable and
   * internal API. It's already the internal format in Spark, so there is no extra conversion needed.
   */
  UNSAFE_ROW,

  /**
   * Refers to {@link org.apache.spark.sql.vectorized.ColumnarBatch}, which is a public but experimental
   * API. It's already the internal format in Spark, so there is no extra conversion needed. This format
   * is recommended over others as columnar format has other advantages like vectorization, to further
   * speed up the data processing.
   */
  COLUMNAR_BATCH
}
