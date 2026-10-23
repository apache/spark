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

package org.apache.spark.sql.connector.read;

import java.io.Serializable;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.filter.StorageFilter;

/**
 * What Spark hands a reader of a {@link SupportsStorageFilters} scan, through
 * {@link PartitionReaderFactory#createReader(InputPartition, StorageFilterRequest)} or
 * {@link PartitionReaderFactory#createColumnarReader(InputPartition, StorageFilterRequest)}.
 *
 * @since 5.0.0
 */
@Evolving
public interface StorageFilterRequest extends Serializable {

  /** The filter the reader may apply. */
  StorageFilter filter();

  /**
   * Whether the reader must append a boolean column after the columns of
   * {@link Scan#readSchema()}. It is true only on a row the filter was evaluated on and kept. It is
   * false on every other row the reader returns, such as a row read without the filter, or one
   * kept although the filter rejected it. Spark skips the filter's predicate above the scan on the
   * rows marked true.
   * <p>
   * Spark asks for it only where whole-stage codegen is on and the scan's output still fits it with
   * the column.
   * Without it, the reader may still apply the filter, and the plan above the scan checks every row
   * the reader returns.
   */
  boolean appendsCheckedColumn();
}
