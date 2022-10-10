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

package org.apache.spark.sql.connector.catalog;

import java.util.Map;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionException;
import org.apache.spark.sql.catalyst.analysis.PartitionsAlreadyExistException;

/**
 * An atomic partition interface of {@link Table} to operate multiple partitions atomically.
 * <p>
 * These APIs are used to modify table partition or partition metadata,
 * they will change the table data as well.
 * <ul>
 *   <li>{@link #createPartitions}: add an array of partitions and any data they contain to the
 *   table</li>
 *   <li>{@link #dropPartitions}: remove an array of partitions and any data they contain from
 *   the table</li>
 *   <li>{@link #purgePartitions}: remove an array of partitions and any data they contain from
 *   the table by skipping a trash even if it is supported</li>
 *   <li>{@link #truncatePartitions}: truncate an array of partitions by removing partitions
 *   data</li>
 * </ul>
 *
 * @since 3.1.0
 */
@Experimental
public interface SupportsAtomicPartitionManagement extends SupportsPartitionManagement {

  @Override
  default void createPartition(
      InternalRow ident,
      Map<String, String> properties)
      throws PartitionsAlreadyExistException, UnsupportedOperationException {
    try {
      createPartitions(new InternalRow[]{ident}, new Map[]{properties});
    } catch (PartitionsAlreadyExistException e) {
      throw new PartitionsAlreadyExistException(e.getMessage());
    }
  }

  @Override
  default boolean dropPartition(InternalRow ident) {
    return dropPartitions(new InternalRow[]{ident});
  }

  /**
   * Create an array of partitions atomically in table.
   * <p>
   * If any partition already exists,
   * the operation of createPartitions need to be safely rolled back.
   *
   * @param idents an array of new partition identifiers
   * @param properties the metadata of the partitions
   * @throws PartitionsAlreadyExistException If any partition already exists for the identifier
   * @throws UnsupportedOperationException If partition property is not supported
   */
  void createPartitions(
      InternalRow[] idents,
      Map<String, String>[] properties)
      throws PartitionsAlreadyExistException, UnsupportedOperationException;

  /**
   * Drop an array of partitions atomically from table.
   * <p>
   * If any partition doesn't exists,
   * the operation of dropPartitions need to be safely rolled back.
   *
   * @param idents an array of partition identifiers
   * @return true if partitions were deleted, false if any partition not exists
   */
  boolean dropPartitions(InternalRow[] idents);

  /**
   * Drop an array of partitions atomically from table, and completely remove partitions data
   * by skipping a trash even if it is supported.
   * <p>
   * If any partition doesn't exists,
   * the operation of purgePartitions need to be safely rolled back.
   *
   * @param idents an array of partition identifiers
   * @return true if partitions were deleted, false if any partition not exists
   * @throws NoSuchPartitionException If any partition identifier to alter doesn't exist
   * @throws UnsupportedOperationException If partition purging is not supported
   *
   * @since 3.2.0
   */
  default boolean purgePartitions(InternalRow[] idents)
    throws NoSuchPartitionException, UnsupportedOperationException {
    throw new UnsupportedOperationException("Partition purge is not supported");
  }

  /**
   * Truncate an array of partitions atomically from table, and completely remove partitions data.
   * <p>
   * If any partition doesn't exists,
   * the operation of truncatePartitions need to be safely rolled back.
   *
   * @param idents an array of partition identifiers
   * @return true if partitions were truncated successfully otherwise false
   * @throws NoSuchPartitionException If any partition identifier to truncate doesn't exist
   * @throws UnsupportedOperationException If partition truncate is not supported
   *
   * @since 3.2.0
   */
  default boolean truncatePartitions(InternalRow[] idents)
      throws NoSuchPartitionException, UnsupportedOperationException {
    throw new UnsupportedOperationException("Partitions truncate is not supported");
  }
}
