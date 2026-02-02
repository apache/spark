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

import java.util.OptionalLong;

/**
 * A mix-in for input partitions whose records are clustered on the same set of partition keys
 * (provided via {@link SupportsReportPartitioning}, see below). Data sources can opt-in to
 * implement this interface for the partitions they report to Spark, which will use the info
 * to decide whether partition grouping should be applied or not.
 *
 * @see org.apache.spark.sql.connector.read.SupportsReportPartitioning
 * @since 4.0.0
 */
public interface HasPartitionStatistics extends InputPartition {

  /**
   * Returns the size in bytes of the partition statistics associated to this partition.
   */
  OptionalLong sizeInBytes();

  /**
   * Returns the number of rows in the partition statistics associated to this partition.
   */
  OptionalLong numRows();

  /**
   * Returns the count of files in the partition statistics associated to this partition.
   */
  OptionalLong filesCount();
}
