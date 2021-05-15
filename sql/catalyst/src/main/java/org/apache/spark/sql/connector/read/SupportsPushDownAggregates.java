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

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.sources.Aggregation;
import org.apache.spark.sql.types.StructType;

/**
 * A mix-in interface for {@link ScanBuilder}. Data source can implement this interface to
 * push down aggregates to the data source.
 *
 * @since 3.2.0
 */
@Evolving
public interface SupportsPushDownAggregates extends ScanBuilder {

  /**
   * Pushes down Aggregation to datasource.
   * The Aggregation can be pushed down only if all the Aggregate Functions can
   * be pushed down.
   */
  void pushAggregation(Aggregation aggregation);

  /**
   * Returns the aggregation that are pushed to the data source via
   * {@link #pushAggregation(Aggregation aggregation)}.
   */
  Aggregation pushedAggregation();

  /**
   * Returns the schema of the pushed down aggregates
   */
  StructType getPushDownAggSchema();

  /**
   * Indicate if the data source only supports global aggregated push down
   */
  boolean supportsGlobalAggregatePushDownOnly();

  /**
   * Indicate if the data source supports push down aggregates along with filters
   */
  boolean supportsPushDownAggregateWithFilter();
}
