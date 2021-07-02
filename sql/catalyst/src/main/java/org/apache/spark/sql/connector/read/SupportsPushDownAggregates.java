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
import org.apache.spark.sql.types.StructType;

/**
 * A mix-in interface for {@link ScanBuilder}. Data source can implement this interface to
 * push down aggregates. Depends on the data source implementation, the aggregates may not
 * be able to push down, partially push down and have final aggregate at Spark, or completely
 * push down.
 *
 * When pushing down operators, Spark pushes down filter to the data source first, then push down
 * aggregates or apply column pruning. Depends on data source implementation, aggregates may or
 * may not be able to be pushed down with filters. If pushed filters still need to be evaluated
 * after scanning, aggregates can't be pushed down.
 *
 * @since 3.2.0
 */
@Evolving
public interface SupportsPushDownAggregates extends ScanBuilder {

  /**
   * Pushes down Aggregation to datasource.
   */
  AggregatePushDownResult pushAggregation(Aggregation aggregation);

  class AggregatePushDownResult {

    // 0: aggregates not pushed down
    // 1: aggregates partially pushed down, need to final aggregate in Spark
    int pushedDownResult = 0;
    StructType pushedDownAggSchema;

    public AggregatePushDownResult(int pushedDownResult, StructType pushedDownAggSchema) {
      this.pushedDownResult = pushedDownResult;
      this.pushedDownAggSchema = pushedDownAggSchema;
    }

    public int getPushedDownResult() {
      return pushedDownResult;
    }
  }
}
