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

import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionException;
import org.apache.spark.sql.catalyst.analysis.PartitionsAlreadyExistException;
import org.apache.spark.sql.errors.QueryExecutionErrors;
import org.apache.spark.sql.types.StructType;

/**
 * A partition interface of {@link Table}.
 * A partition is composed of identifier and properties,
 * and properties contains metadata information of the partition.
 * <p>
 * These APIs are used to modify table partition identifier or partition metadata.
 * In some cases, they will change the table data as well.
 * <ul>
 *   <li>{@link #createPartition}: add a partition and any data it contains to the table</li>
 *   <li>{@link #dropPartition}: remove a partition and any data it contains from the table</li>
 *   <li>{@link #purgePartition}: remove a partition and any data it contains from the table by
 *   skipping a trash even if it is supported.</li>
 *   <li>{@link #replacePartitionMetadata}: point a partition to a new location, which will swap
 *   one location's data for the other</li>
 *   <li>{@link #truncatePartition}: remove partition data from the table</li>
 * </ul>
 *
 * @since 3.1.0
 */
@Experimental
public interface SupportsPartitionManagement extends Table {

    /**
     * Get the partition schema of table,
     * this must be consistent with ${@link Table#partitioning()}.
     * @return the partition schema of table
     */
    StructType partitionSchema();

    /**
     * Create a partition in table.
     *
     * @param ident a new partition identifier
     * @param properties the metadata of a partition
     * @throws PartitionsAlreadyExistException If a partition already exists for the identifier
     * @throws UnsupportedOperationException If partition property is not supported
     */
    void createPartition(
        InternalRow ident,
        Map<String, String> properties)
        throws PartitionsAlreadyExistException, UnsupportedOperationException;

    /**
     * Drop a partition from table.
     *
     * @param ident a partition identifier
     * @return true if a partition was deleted, false if no partition exists for the identifier
     */
    boolean dropPartition(InternalRow ident);

    /**
     * Drop a partition from the table and completely remove partition data by skipping a trash
     * even if it is supported.
     *
     * @param ident a partition identifier
     * @return true if a partition was deleted, false if no partition exists for the identifier
     * @throws NoSuchPartitionException If the partition identifier to alter doesn't exist
     * @throws UnsupportedOperationException If partition purging is not supported
     *
     * @since 3.2.0
     */
    default boolean purgePartition(InternalRow ident)
      throws NoSuchPartitionException, UnsupportedOperationException {
      throw QueryExecutionErrors.unsupportedPurgePartitionError();
    }

    /**
     * Test whether a partition exists using an {@link InternalRow ident} from the table.
     *
     * @param ident a partition identifier which must contain all partition fields in order
     * @return true if the partition exists, false otherwise
     */
    default boolean partitionExists(InternalRow ident) {
      String[] partitionNames = partitionSchema().names();
      if (ident.numFields() == partitionNames.length) {
        return listPartitionIdentifiers(partitionNames, ident).length > 0;
      } else {
        throw QueryExecutionErrors.partitionNumMismatchError(
          ident.numFields(), partitionNames.length);
      }
    }

    /**
     * Replace the partition metadata of the existing partition.
     *
     * @param ident the partition identifier of the existing partition
     * @param properties the new metadata of the partition
     * @throws NoSuchPartitionException If the partition identifier to alter doesn't exist
     * @throws UnsupportedOperationException If partition property is not supported
     */
    void replacePartitionMetadata(
        InternalRow ident,
        Map<String, String> properties)
        throws NoSuchPartitionException, UnsupportedOperationException;

    /**
     * Retrieve the partition metadata of the existing partition.
     *
     * @param ident a partition identifier
     * @return the metadata of the partition
     * @throws UnsupportedOperationException If partition property is not supported
     */
    Map<String, String> loadPartitionMetadata(InternalRow ident)
        throws UnsupportedOperationException;

    /**
     * List the identifiers of all partitions that match to the ident by names.
     *
     * @param names the names of partition values in the identifier.
     * @param ident a partition identifier values.
     * @return an array of Identifiers for the partitions
     */
    InternalRow[] listPartitionIdentifiers(String[] names, InternalRow ident);

    /**
     * Rename an existing partition of the table.
     *
     * @param from an existing partition identifier to rename
     * @param to new partition identifier
     * @return true if renaming completes successfully otherwise false
     * @throws UnsupportedOperationException If partition renaming is not supported
     * @throws PartitionsAlreadyExistException If the `to` partition exists already
     * @throws NoSuchPartitionException If the `from` partition does not exist
     *
     * @since 3.2.0
     */
    default boolean renamePartition(InternalRow from, InternalRow to)
        throws SparkUnsupportedOperationException,
               PartitionsAlreadyExistException,
               NoSuchPartitionException {
      throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3143");
    }

    /**
     * Truncate a partition in the table by completely removing partition data.
     *
     * @param ident a partition identifier
     * @return true if the partition was truncated successfully otherwise false
     * @throws NoSuchPartitionException If the partition identifier to alter doesn't exist
     * @throws UnsupportedOperationException If partition truncation is not supported
     *
     * @since 3.2.0
     */
    default boolean truncatePartition(InternalRow ident)
        throws NoSuchPartitionException, SparkUnsupportedOperationException {
      throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3144");
    }
}
