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

import org.apache.spark.sql.catalyst.InternalRow;

/**
 * A mix-in for input partitions whose records are clustered on the same set of partition keys
 * (provided via {@link SupportsReportPartitioning}, see below). Data sources can opt-in to
 * implement this interface for the partitions they report to Spark, which will use the
 * information to avoid data shuffling in certain scenarios, such as join, aggregate, etc. Note
 * that Spark requires ALL input partitions to implement this interface, otherwise it can't take
 * advantage of it.
 * <p>
 * This interface should be used in combination with {@link SupportsReportPartitioning}, which
 * allows data sources to report distribution and ordering spec to Spark. In particular, Spark
 * expects data sources to report
 * {@link org.apache.spark.sql.connector.distributions.ClusteredDistribution} whenever its input
 * partitions implement this interface. Spark derives partition keys spec (e.g., column names,
 * transforms) from the distribution, and partition values from the input partitions.
 * <p>
 * It is implementor's responsibility to ensure that when an input partition implements this
 * interface, its records all have the same value for the partition keys. Spark doesn't check
 * this property.
 *
 * @see org.apache.spark.sql.connector.read.SupportsReportPartitioning
 * @see org.apache.spark.sql.connector.read.partitioning.Partitioning
 * @since 3.3.0
 */
public interface HasPartitionKey extends InputPartition {
  /**
   * Returns the value of the partition key(s) associated to this partition. An input partition
   * implementing this interface needs to ensure that all its records have the same value for the
   * partition keys. Note that the value is after partition transform has been applied, if there
   * is any.
   */
  InternalRow partitionKey();
}
