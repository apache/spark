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

import scala.collection.mutable

import org.apache.hadoop.fs._

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ArrayImplicits._

/**
 * A file status augmented with optional metadata, which tasks and file readers can use however they
 * see fit. For example, a custom [[FileIndex]] and [[FileFormat]] working together could expose
 * this extra metadata as file-constant fields of the file source metadata column.
 */
case class FileStatusWithMetadata(fileStatus: FileStatus, metadata: Map[String, Any] = Map.empty) {
  // Wrapper methods to improve source compatibility in code that still expects a [[FileStatus]].
  // NOTE: getPath() is very expensive, so we only want to call it once (if accessed at all).
  lazy val getPath: Path = fileStatus.getPath
  def getLen: Long = fileStatus.getLen
  def getModificationTime: Long = fileStatus.getModificationTime
  def isDirectory: Boolean = fileStatus.isDirectory
}

/**
 * A collection of data files from a partitioned relation, along with the partition values in the
 * form of an [[InternalRow]].
 */
case class PartitionDirectory(values: InternalRow, files: Seq[FileStatusWithMetadata])

/**
 * A runner that extracts file metadata filters from the given `filters` and use it to prune files
 * in `PartitionDirectory`.
 */
class FilePruningRunner(filters: Seq[Expression]) {
  // retrieve the file constant metadata filters and reduce to a final filter expression that can
  // be applied to files.
  val fileMetadataFilterOpt = filters.filter { f =>
    f.references.nonEmpty && f.references.forall {
      case FileSourceConstantMetadataAttribute(metadataAttr) =>
        // we only know block start and length after splitting files, so skip it here
        metadataAttr.name != FileFormat.FILE_BLOCK_START &&
          metadataAttr.name != FileFormat.FILE_BLOCK_LENGTH
      case _ => false
    }
  }.reduceOption(And)

  // - Retrieve all required metadata attributes and put them into a sequence
  // - Bind all file constant metadata attribute references to their respective index
  val requiredMetadataColumnNames: mutable.Buffer[String] = mutable.Buffer.empty
  val boundedFilterMetadataStructOpt = fileMetadataFilterOpt.map { fileMetadataFilter =>
    Predicate.createInterpreted(fileMetadataFilter.transform {
      case attr: AttributeReference =>
        val existingMetadataColumnIndex = requiredMetadataColumnNames.indexOf(attr.name)
        val metadataColumnIndex = if (existingMetadataColumnIndex >= 0) {
          existingMetadataColumnIndex
        } else {
          requiredMetadataColumnNames += attr.name
          requiredMetadataColumnNames.length - 1
        }
        BoundReference(metadataColumnIndex, attr.dataType, nullable = true)
    })
  }

  private def matchFileMetadataPredicate(partitionValues: InternalRow, f: FileStatus): Boolean = {
    // use option.forall, so if there is no filter no metadata struct, return true
    boundedFilterMetadataStructOpt.forall { boundedFilter =>
      val row =
        FileFormat.createMetadataInternalRow(partitionValues, requiredMetadataColumnNames.toSeq,
          SparkPath.fromFileStatus(f), f.getLen, f.getModificationTime)
      boundedFilter.eval(row)
    }
  }

  def prune(pd: PartitionDirectory): PartitionDirectory = {
    val prunedFiles = pd.files.filter { f =>
      matchFileMetadataPredicate(InternalRow.empty, f.fileStatus)
    }
    pd.copy(files = prunedFiles)
  }
}

object PartitionDirectory {
  // For backward compat with code that does not know about extra file metadata
  def apply(values: InternalRow, files: Array[FileStatus]): PartitionDirectory =
    PartitionDirectory(values, files.map(FileStatusWithMetadata(_)).toImmutableArraySeq)
}

/**
 * An interface for objects capable of enumerating the root paths of a relation as well as the
 * partitions of a relation subject to some pruning expressions.
 */
trait FileIndex {

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
   * The strings returned are expected to be url-encoded paths.
   */
  def inputFiles: Array[String]

  /** Refresh any cached file listings */
  def refresh(): Unit

  /** Sum of table file sizes, in bytes */
  def sizeInBytes: Long

  /** Schema of the partitioning columns, or the empty schema if the table is not partitioned. */
  def partitionSchema: StructType

  /**
   * Returns an optional metadata operation time, in nanoseconds, for listing files.
   *
   * We do file listing in query optimization (in order to get the proper statistics) and we want
   * to account for file listing time in physical execution (as metrics). To do that, we save the
   * file listing time in some implementations and physical execution calls it in this method
   * to update the metrics.
   */
  def metadataOpsTimeNs: Option[Long] = None

  override def toString: String = s"${getClass.getName}(${rootPaths.mkString(",")})"
}
