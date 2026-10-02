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
 * The write distribution a {@code CREATE}/{@code REPLACE TABLE} statement declares as the default
 * for writes into the table.
 *
 * @since 4.4.0
 */
@Evolving
public enum WriteDistributionMode {
  /**
   * Requested with {@code DISTRIBUTED BY PARTITION}: cluster each write by the table's
   * partitioning.
   */
  HASH,
  /**
   * Implied by a bare {@code ORDERED BY}: range-partition each write so the ordering holds across
   * its tasks, not only within one.
   */
  RANGE,
  /**
   * Requested with {@code UNORDERED} or {@code LOCALLY ORDERED BY}: do not distribute, so any
   * ordering holds within a write task only.
   */
  NONE;

  @Override
  public String toString() {
    return switch (this) {
      case HASH -> "hash";
      case RANGE -> "range";
      case NONE -> "none";
    };
  }
}
