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

package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.fs._

import org.apache.spark.sql.catalyst.{InternalRow, QueryPlanningTracker}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.StructType

/**
 * A collection of data files from a partitioned relation, along with the partition values in the
 * form of an [[InternalRow]].
 */
case class PartitionDirectory(values: InternalRow, files: Seq[FileStatus])

/**
 * An interface for objects capable of enumerating the root paths of a relation as well as the
 * partitions of a relation subject to some pruning expressions.
 */
trait FileIndex {
  import QueryPlanningTracker._

  private var _fileListingPhase: Option[PhaseSummary] = None

  /**
   * Returns the list of root input paths from which the catalog will get files. There may be a
   * single root path from which partitions are discovered, or individual partitions may be
   * specified by each path.
   */
  def rootPaths: Seq[Path]

  /**
   * Returns all valid files grouped into partitions when the data is partitioned. If the data is
   * unpartitioned, this will return a single partition with no partition values.
   *
   * @param partitionFilters The filters used to prune which partitions are returned. These filters
   *                         must only refer to partition columns and this method will only return
   *                         files where these predicates are guaranteed to evaluate to `true`.
   *                         Thus, these filters will not need to be evaluated again on the
   *                         returned data.
   * @param dataFilters Filters that can be applied on non-partitioned columns. The implementation
   *                    does not need to guarantee these filters are applied, i.e. the execution
   *                    engine will ensure these filters are still applied on the returned files.
   */
  def listFiles(
      partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory]

  /**
   * Returns the list of files that will be read when scanning this relation. This call may be
   * very expensive for large tables.
   */
  def inputFiles: Array[String]

  /** Refresh any cached file listings */
  def refresh(): Unit

  /** Sum of table file sizes, in bytes */
  def sizeInBytes: Long

  /** Schema of the partitioning columns, or the empty schema if the table is not partitioned. */
  def partitionSchema: StructType

  /** Measure the start and end time of file listing phase and memorize result. */
  protected def measureFileListingPhase[T](f: => T): T = {
    val startTime = System.currentTimeMillis()
    val ret = f
    val endTime = System.currentTimeMillis
    _fileListingPhase = Some(new PhaseSummary(startTime, endTime))
    ret
  }

  /**
   * Returns an optional file listing phase summary.
   *
   * We call this in physical execution while we want to account for the file listing time as
   * metrics. If partition pruning happened in query planning, the phase also contains this
   * part of the cost, otherwise, it only contains file listing time of FileIndex initialize.
   */
  final def fileListingPhase: Option[PhaseSummary] = _fileListingPhase
}
