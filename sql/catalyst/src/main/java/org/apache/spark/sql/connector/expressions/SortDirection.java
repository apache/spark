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

package org.apache.spark.sql.connector.expressions;

import org.apache.spark.annotation.Experimental;

import static org.apache.spark.sql.connector.expressions.NullOrdering.NULLS_FIRST;
import static org.apache.spark.sql.connector.expressions.NullOrdering.NULLS_LAST;

/**
 * A sort direction used in sorting expressions.
 * <p>
 * Each direction has a default null ordering that is implied if no null ordering is specified
 * explicitly.
 *
 * @since 3.2.0
 */
@Experimental
public enum SortDirection {
  ASCENDING(NULLS_FIRST), DESCENDING(NULLS_LAST);

  private final NullOrdering defaultNullOrdering;

  SortDirection(NullOrdering defaultNullOrdering) {
    this.defaultNullOrdering = defaultNullOrdering;
  }

  /**
   * Returns the default null ordering to use if no null ordering is specified explicitly.
   */
  public NullOrdering defaultNullOrdering() {
    return defaultNullOrdering;
  }

  @Override
  public String toString() {
    return switch (this) {
      case ASCENDING -> "ASC";
      case DESCENDING -> "DESC";
    };
  }
}
