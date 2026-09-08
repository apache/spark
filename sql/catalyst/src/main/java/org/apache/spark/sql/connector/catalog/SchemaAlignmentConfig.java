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
 * Schema-alignment configuration for writes to a {@link Table}. This allows connectors to
 * configure casting behavior and handling of schema mismatches during writes.
 *
 * @since 4.3.0
 */
@Evolving
public interface SchemaAlignmentConfig {

  /** The strict data source v2 configuration, returned by {@link Table} by default. */
  SchemaAlignmentConfig DEFAULT = new SchemaAlignmentConfig() {};

  /**
   * Whether {@code spark.sql.storeAssignmentPolicy=LEGACY} is allowed for writes and row-level
   * operations targeting this table. Data source v2 rejects LEGACY by default; a table can decide
   * to opt-out from this restriction.
   */
  default boolean allowLegacyStoreAssignmentPolicy() {
    return false;
  }

  /**
   * Whether the {@code ANSI} store-assignment cast check is deferred from analysis to runtime under
   * {@code spark.sql.storeAssignmentPolicy=ANSI}. When {@code true}, the analyzer skips the
   * store-assignment compatibility check and inserts an ANSI cast, so malformed values or
   * overflows surface at execution time.
   */
  default boolean deferCastValidationToRuntime() {
    return false;
  }
}
