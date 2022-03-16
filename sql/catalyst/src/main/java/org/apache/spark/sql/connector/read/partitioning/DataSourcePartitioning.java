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

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.Expression;

/**
 * Represents a partitioning where rows are split across partitions based on the expressions
 * returned by {@link DataSourcePartitioning#clustering}.
 * <p>
 * Data source implementations should make sure that all rows where
 * {@link DataSourcePartitioning#clustering} evaluate to the same value should be in the same
 * partition.
 *
 * @since 3.3.0
 */
@Evolving
public class DataSourcePartitioning implements Partitioning {
  private final Expression[] clustering;
  private final int numPartitions;

  public DataSourcePartitioning(Expression[] clustering, int numPartitions) {
    this.clustering = clustering;
    this.numPartitions = numPartitions;
  }

  /**
   * Returns the clustering expressions for this partitioning.
   */
  public Expression[] clustering() {
    return clustering;
  }

  @Override
  public int numPartitions() {
    return numPartitions;
  }
}
