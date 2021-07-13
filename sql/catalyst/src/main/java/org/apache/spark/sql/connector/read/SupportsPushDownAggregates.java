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
import org.apache.spark.sql.connector.expressions.Aggregation;

/**
 * A mix-in interface for {@link ScanBuilder}. Data source can implement this interface to
 * push down aggregates. Depends on the data source implementation, the aggregates may not
 * be able to push down, or partially push down and have a final aggregate at Spark.
 * For example, "SELECT min(_1) FROM t GROUP BY _2" can be pushed down to data source,
 * the partially aggregated result min(_1) grouped by _2 will be returned to Spark, and
 * then have a final aggregation.
 * {{{
 *   Aggregate [_2#10], [min(_2#10) AS min(_1)#16]
 *     +- RelationV2[_2#10, min(_1)#18]
 * }}}
 *
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
   * Pushes down Aggregation to datasource. The order of the datasource scan output columns should
   * be: grouping columns, aggregate columns (in the same order as the aggregate functions in
   * the given Aggregation).
   */
  boolean pushAggregation(Aggregation aggregation);
}
