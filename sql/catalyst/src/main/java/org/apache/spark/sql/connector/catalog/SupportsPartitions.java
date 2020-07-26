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
import org.apache.spark.sql.catalyst.analysis.PartitionAlreadyExistsException;

/**
 * A partition interface of {@link Table} to indicate partition APIs.
 * A partition is composed of identifier and properties,
 * and properties contains metadata information of the partition.
 * <p>
 * These APIs are used to modify table partition identifier or partition metadata,
 * in some cases, they will change actual value of table data as well.
 *
 * @since 3.0.0
 */
@Experimental
public interface SupportsPartitions extends Table {

    /**
     * Create a partition in table.
     *
     * @param ident a new partition identifier
     * @param properties the metadata of a partition
     * @throws PartitionAlreadyExistsException If a partition already exists for the identifier
     */
    void createPartition(
            InternalRow ident,
            Map<String, String> properties) throws PartitionAlreadyExistsException;

    /**
     * Drop a partition from table.
     *
     * @param ident a partition identifier
     * @return true if a partition was deleted, false if no partition exists for the identifier
     */
    Boolean dropPartition(InternalRow ident);

    /**
     * Rename a Partition from old identifier to new identifier with no metadata changed.
     *
     * @param oldIdent the partition identifier of the existing partition
     * @param newIdent the new partition identifier of the partition
     * @throws NoSuchPartitionException If the partition identifier to rename doesn't exist
     * @throws PartitionAlreadyExistsException If the new partition identifier already exists
     */
    void renamePartition(
            InternalRow oldIdent,
            InternalRow newIdent) throws NoSuchPartitionException, PartitionAlreadyExistsException;

    /**
     * Replace the partition metadata of the existing partition.
     *
     * @param ident the partition identifier of the existing partition
     * @param properties the new metadata of the partition
     * @throws NoSuchPartitionException If the partition identifier to rename doesn't exist
     */
    void replacePartitionMetadata(
            InternalRow ident,
            Map<String, String> properties) throws NoSuchPartitionException;

    /**
     * Retrieve the partition metadata of the existing partition.
     *
     * @param ident a partition identifier
     * @return the metadata of the partition
     */
    Map<String, String> getPartitionMetadata(InternalRow ident);

    /**
     * List the identifiers of all partitions that contains the ident in a table.
     *
     * @param ident a prefix of partition identifier
     * @return an array of Identifiers for the partitions
     */
    InternalRow[] listPartitionIdentifiers(InternalRow ident);
}
