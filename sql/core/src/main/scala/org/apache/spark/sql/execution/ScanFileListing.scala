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
package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BasePredicate, Expression}
import org.apache.spark.sql.execution.datasources.{FileStatusWithMetadata, PartitionedFile}

case class ListingPartition(
    values: InternalRow,
    numFiles: Long,
    files: Iterator[FileStatusWithMetadata])

/**
 * Trait used to represent the selected partitions and dynamically selected partitions
 * during file listing.
 *
 * The `ScanFileListing` trait defines the core API for interacting with selected partitions,
 * establishing a contract for subclasses. It is situated at the root of this package and it is
 * designed to provide a widely accessible definition, that is accessible to other packages and
 * classes that need a way to represent the selected partitions and dynamically selected partitions.
 */
trait ScanFileListing {

  /**
   * Returns the number of partitions for the current partition representation.
   */
  def partitionCount: Int

  /**
   * Calculates the total size in bytes of all files across the current file listing representation.
   */
  def totalFileSize: Long

  /**
   * Returns the total number of files across the current file listing representation.
   */
  def totalNumberOfFiles: Long

  /**
   * Filters and prunes files from the current scan file listing representation based on the given
   * predicate and dynamic file filters. Initially, it filters partitions based on a static
   * predicate. For partitions that pass this filter, it further prunes files using dynamic file
   * filters, if any are provided. This method assumes that dynamic file filters are applicable
   * only to files within partitions that have already passed the static predicate filter.
   */
  def filterAndPruneFiles(
      boundPredicate: BasePredicate, dynamicFileFilters: Seq[Expression]): ScanFileListing

  /**
   * Returns an [[Array[PartitionedFile]] from the current ScanFileListing representation.
   */
  def toPartitionArray: Array[PartitionedFile]

  /**
   * Returns the total partition size in bytes for the current ScanFileListing representation.
   */
  def calculateTotalPartitionBytes : Long

  /**
   * Returns an iterator of over the partitions and their files for the file listing representation.
   * This allows us to iterate over the partitions without the additional overhead of materializing
   * the whole collection.
   */
  def filePartitionIterator: Iterator[ListingPartition]

  /**
   * Determines if each bucket in the current file listing representation contains at most one file.
   * This function returns true if it does, or false otherwise.
   */
  def bucketsContainSingleFile: Boolean
}
