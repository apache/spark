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
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;

/**
 * A mix-in interface for {@link ScanBuilder}. Data sources can implement this interface to
 * push down aggregates.
 * <p>
 * If the data source can't fully complete the grouping work, then
 * {@link #supportCompletePushDown()} should return false, and Spark will group the data source
 * output again. For queries like "SELECT min(value) AS m FROM t GROUP BY key", after pushing down
 * the aggregate to the data source, the data source can still output data with duplicated keys,
 * which is OK as Spark will do GROUP BY key again. The final query plan can be something like this:
 * <pre>
 *   Aggregate [key#1], [min(min_value#2) AS m#3]
 *     +- RelationV2[key#1, min_value#2]
 * </pre>
 * Similarly, if there is no grouping expression, the data source can still output more than one
 * rows.
 * <p>
 * When pushing down operators, Spark pushes down filters to the data source first, then push down
 * aggregates or apply column pruning. Depends on data source implementation, aggregates may or
 * may not be able to be pushed down with filters. If pushed filters still need to be evaluated
 * after scanning, aggregates can't be pushed down.
 *
 * @since 3.2.0
 */
@Evolving
public interface SupportsPushDownAggregates extends ScanBuilder {

  /**
   * Whether the datasource support complete aggregation push-down. Spark will do grouping again
   * if this method returns false.
   *
   * @return true if the aggregation can be pushed down to datasource completely, false otherwise.
   */
  default boolean supportCompletePushDown() { return false; }

  /**
   * Pushes down Aggregation to datasource. The order of the datasource scan output columns should
   * be: grouping columns, aggregate columns (in the same order as the aggregate functions in
   * the given Aggregation).
   *
   * @return true if the aggregation can be pushed down to datasource, false otherwise.
   */
  boolean pushAggregation(Aggregation aggregation);
}
