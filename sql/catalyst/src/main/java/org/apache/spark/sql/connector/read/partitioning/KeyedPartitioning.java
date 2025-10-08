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
import org.apache.spark.sql.connector.read.SupportsReportPartitioning;

/**
 * Represents a partitioning where rows are split across partitions based on the
 * partition transform expressions returned by {@link KeyedPartitioning#keys}.
 * <p>
 * @since 4.1.0
 */
@Evolving
public class KeyedPartitioning implements Partitioning {
  private final Expression[] keys;
  private final int numPartitions;

  public KeyedPartitioning(Expression[] keys, int numPartitions) {
    this.keys = keys;
    this.numPartitions = numPartitions;
  }

  /**
   * Returns the partition transform expressions for this partitioning.
   */
  public Expression[] keys() {
    return keys;
  }

  @Override
  public int numPartitions() {
    return numPartitions;
  }
}
