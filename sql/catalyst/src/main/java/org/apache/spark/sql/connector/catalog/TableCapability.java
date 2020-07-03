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

package org.apache.spark.sql.connector.catalog;

import org.apache.spark.annotation.Evolving;

/**
 * Capabilities that can be provided by a {@link Table} implementation.
 * <p>
 * Tables use {@link Table#capabilities()} to return a set of capabilities. Each capability signals
 * to Spark that the table supports a feature identified by the capability. For example, returning
 * {@link #BATCH_READ} allows Spark to read from the table using a batch scan.
 *
 * @since 3.0.0
 */
@Evolving
public enum TableCapability {
  /**
   * Signals that the table supports reads in batch execution mode.
   */
  BATCH_READ,

  /**
   * Signals that the table supports reads in micro-batch streaming execution mode.
   */
  MICRO_BATCH_READ,

  /**
   * Signals that the table supports reads in continuous streaming execution mode.
   */
  CONTINUOUS_READ,

  /**
   * Signals that the table supports append writes in batch execution mode.
   * <p>
   * Tables that return this capability must support appending data and may also support additional
   * write modes, like {@link #TRUNCATE}, {@link #OVERWRITE_BY_FILTER}, and
   * {@link #OVERWRITE_DYNAMIC}.
   */
  BATCH_WRITE,

  /**
   * Signals that the table supports append writes in streaming execution mode.
   * <p>
   * Tables that return this capability must support appending data and may also support additional
   * write modes, like {@link #TRUNCATE}, {@link #OVERWRITE_BY_FILTER}, and
   * {@link #OVERWRITE_DYNAMIC}.
   */
  STREAMING_WRITE,

  /**
   * Signals that the table can be truncated in a write operation.
   * <p>
   * Truncating a table removes all existing rows.
   * <p>
   * See {@link org.apache.spark.sql.connector.write.SupportsTruncate}.
   */
  TRUNCATE,

  /**
   * Signals that the table can replace existing data that matches a filter with appended data in
   * a write operation.
   * <p>
   * See {@link org.apache.spark.sql.connector.write.SupportsOverwrite}.
   */
  OVERWRITE_BY_FILTER,

  /**
   * Signals that the table can dynamically replace existing data partitions with appended data in
   * a write operation.
   * <p>
   * See {@link org.apache.spark.sql.connector.write.SupportsDynamicOverwrite}.
   */
  OVERWRITE_DYNAMIC,

  /**
   * Signals that the table accepts input of any schema in a write operation.
   */
  ACCEPT_ANY_SCHEMA,

  /**
   * Signals that the table supports append writes using the V1 InsertableRelation interface.
   * <p>
   * Tables that return this capability must create a V1WriteBuilder and may also support additional
   * write modes, like {@link #TRUNCATE}, and {@link #OVERWRITE_BY_FILTER}, but cannot support
   * {@link #OVERWRITE_DYNAMIC}.
   */
  V1_BATCH_WRITE
}
