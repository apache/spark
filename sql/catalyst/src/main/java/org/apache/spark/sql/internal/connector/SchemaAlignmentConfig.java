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

package org.apache.spark.sql.internal.connector;

import org.apache.spark.sql.connector.catalog.Table;

/**
 * Schema alignment configuration for DSv2 batch/row-level writes to a {@link Table}, exposed via
 * {@link ConfigurableSchemaAlignment}.
 */
public interface SchemaAlignmentConfig {

  /** The default data source v2 configuration. */
  SchemaAlignmentConfig DEFAULT = new SchemaAlignmentConfig() {};

  /**
   * When the {@code ANSI} store-assignment cast check runs for writes to a table, under
   * {@code spark.sql.storeAssignmentPolicy=ANSI}. Has no effect under the {@code STRICT} policy.
   */
  enum AnsiStoreAssignmentCastCheck {
    /** (default) Reject incompatible casts at analysis. See {@code Cast.canANSIStoreAssign}. */
    AT_ANALYSIS,

    /**
     * Insert an ANSI cast check, so malformed values or overflows fail at execution time instead
     * of being rejected during analysis.
     */
    AT_RUNTIME
  }

  /** When the {@code ANSI} store-assignment cast check runs. */
  default AnsiStoreAssignmentCastCheck ansiStoreAssignmentCastCheck() {
    return AnsiStoreAssignmentCastCheck.AT_ANALYSIS;
  }
}
