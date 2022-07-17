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

package org.apache.spark.sql.connector.read.partitioning;

import java.util.Arrays;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.SortOrder;

/**
 * Represents a partitioning where rows are ordered based on order expressions
 * returned by {@link RangePartitioning#order} and split across partitions at
 * certain value boundaries. This is similar to {@link KeyGroupedPartitioning}
 * with `SortOrder::expression` being the keys and keys ordered and split according
 * to {@link RangePartitioning#order}.
 * <p>
 * Note: Data source implementations should make sure for a single partition, all of its rows
 * must be evaluated to the same partition value after being applied by
 * {@link RangePartitioning#keys()} expressions. Different partitions can share the same
 * partition value: Spark will group these into a single logical partition during planning phase.
 *
 * @since 3.4.0
 */
@Evolving
public class RangePartitioning extends KeyGroupedPartitioning {
    private final SortOrder[] order;

    private static Expression[] orderExpressions(SortOrder[] order) {
        return Arrays.stream(order)
                .map(SortOrder::expression)
                .toArray(Expression[]::new);
    }

    public RangePartitioning(SortOrder[] order, int numPartitions) {
        super(orderExpressions(order), numPartitions);
        this.order = order;
    }

    /**
     * Returns the partition order expressions for this partitioning.
     */
    public SortOrder[] order() {
        return order;
    }

}
