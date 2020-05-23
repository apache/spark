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

/**
 * Catalog methods for working with Partitions.
 */
@Experimental
public interface SupportsPartitions extends TableCatalog {

    /**
     * Create partitions in an existing table, assuming it exists.
     *
     * @param ident a table identifier
     * @param partitions transforms to use for partitioning data in the table
     * @param ignoreIfExists
     */
    void createPartitions(
            Identifier ident,
            TablePartition[] partitions,
            Boolean ignoreIfExists);

    /**
     * Drop partitions from a table, assuming they exist.
     *
     * @param ident a table identifier
     * @param partitions a list of string map for existing partitions
     * @param ignoreIfNotExists
     */
    void dropPartitions(
            Identifier ident,
            Map<String, String>[] partitions,
            Boolean ignoreIfNotExists);

    /**
     * Override the specs of one or many existing table partitions, assuming they exist.
     *
     * @param ident a table identifier
     * @param oldpartitions a list of string map for existing partitions to be renamed
     * @param newPartitions a list of string map for new partitions
     */
    void renamePartitions(
            Identifier ident,
            Map<String, String>[] oldpartitions,
            Map<String, String>[] newPartitions);

    /**
     * Alter one or many table partitions whose specs that match those specified in `parts`,
     * assuming the partitions exist.
     *
     * @param ident a table identifier
     * @param partitions transforms to use for partitioning data in the table
     */
    void alterPartitions(
            Identifier ident,
            TablePartition[] partitions);

    /**
     * Retrieve the metadata of a table partition, assuming it exists.
     *
     * @param ident a table identifier
     * @param partition a list of string map for existing partitions
     */
    TablePartition getPartition(
            Identifier ident,
            Map<String, String> partition);

    /**
     * List the names of all partitions that belong to the specified table, assuming it exists.
     *
     * @param ident a table identifier
     * @param partition a list of string map for existing partitions
     */
    String[] listPartitionNames(
            Identifier ident,
            Map<String, String> partition);

    /**
     * List the metadata of all partitions that belong to the specified table, assuming it exists.
     *
     * @param ident a table identifier
     * @param partition a list of string map for existing partitions
     */
    TablePartition[] listPartitions(
            Identifier ident,
            Map<String, String> partition);
}
