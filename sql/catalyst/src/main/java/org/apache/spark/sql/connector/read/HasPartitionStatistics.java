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
 * A mix-in for input partitions that can report size, row count and file count statistics.
 * Data sources can implement this interface for keyed or unkeyed input partitions.
 * <p>
 * When batch scan partition packing is enabled and the physical plan does not require the
 * scan's partitioning or ordering, Spark can use the reported sizes to pack multiple inputs
 * into one task. Packing is disabled by default and does not split individual inputs.
 *
 * @since 4.0.0
 */
public interface HasPartitionStatistics extends InputPartition {

  /**
   * Returns the estimated size in bytes of this input, or an empty value if unknown.
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
