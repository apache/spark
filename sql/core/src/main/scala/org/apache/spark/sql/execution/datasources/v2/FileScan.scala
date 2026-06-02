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
package org.apache.spark.sql.execution.datasources.v2

import java.util.{Locale, Optional, OptionalLong}

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{PATH, REASON}
import org.apache.spark.internal.config.IO_WARNING_LARGEFILETHRESHOLD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{And, AttributeSet, Expression, ExpressionSet, Or}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.connector.catalog.SupportsRead
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.{SessionStateHelper, SQLConf}
import org.apache.spark.sql.internal.connector.SupportsMetadata
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils

trait FileScan extends Scan
  with Batch
  with SupportsReportStatistics
  with SupportsMetadata
  with SupportsScanMerging
  with SQLConfHelper
  with Logging {
  /**
   * Returns whether a file with `path` could be split or not.
   */
  def isSplitable(path: Path): Boolean = {
    false
  }

  def sparkSession: SparkSession

  def fileIndex: PartitioningAwareFileIndex

  /** The scan options, used to obtain a fresh [[ScanBuilder]] when merging. */
  def options: CaseInsensitiveStringMap

  def dataSchema: StructType

  /**
   * Returns the required data schema
   */
  def readDataSchema: StructType

  /**
   * Returns the required partition schema
   */
  def readPartitionSchema: StructType

  /**
   * Returns the filters that can be use for partition pruning
   */
  def partitionFilters: Seq[Expression]

  /**
   * Returns the data filters that can be use for file listing
   */
  def dataFilters: Seq[Expression]

  /**
   * If a file with `path` is unsplittable, return the unsplittable reason,
   * otherwise return `None`.
   */
  def getFileUnSplittableReason(path: Path): String = {
    assert(!isSplitable(path))
    "undefined"
  }

  protected def seqToString(seq: Seq[Any]): String = seq.mkString("[", ", ", "]")

  protected lazy val (normalizedPartitionFilters, normalizedDataFilters) = {
    val partitionFilterAttributes = AttributeSet(partitionFilters).map(a => a.name -> a).toMap
    val normalizedPartitionFilters = ExpressionSet(partitionFilters.map(
      QueryPlan.normalizeExpressions(_, toAttributes(fileIndex.partitionSchema)
        .map(a => partitionFilterAttributes.getOrElse(a.name, a)))))
    val dataFiltersAttributes = AttributeSet(dataFilters).map(a => a.name -> a).toMap
    val normalizedDataFilters = ExpressionSet(dataFilters.map(
      QueryPlan.normalizeExpressions(_, toAttributes(dataSchema)
        .map(a => dataFiltersAttributes.getOrElse(a.name, a)))))
    (normalizedPartitionFilters, normalizedDataFilters)
  }

  override def equals(obj: Any): Boolean = obj match {
    case f: FileScan =>
      fileIndex == f.fileIndex && readSchema == f.readSchema &&
        normalizedPartitionFilters == f.normalizedPartitionFilters &&
        normalizedDataFilters == f.normalizedDataFilters

    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()

  override def conf: SQLConf = SessionStateHelper.getSqlConf(sparkSession)

  val maxMetadataValueLength = conf.maxMetadataStringLength

  override def description(): String = {
    val metadataStr = getMetaData().toSeq.sorted.map {
      case (key, value) =>
        val redactedValue =
          Utils.redact(conf.stringRedactionPattern, value)
        key + ": " + Utils.abbreviate(redactedValue, maxMetadataValueLength)
    }.mkString(", ")
    s"${this.getClass.getSimpleName} $metadataStr"
  }

  override def getMetaData(): Map[String, String] = {
    val locationDesc =
      fileIndex.getClass.getSimpleName +
        Utils.buildLocationMetadata(fileIndex.rootPaths, maxMetadataValueLength)
    Map(
      "Format" -> s"${this.getClass.getSimpleName.replace("Scan", "").toLowerCase(Locale.ROOT)}",
      "ReadSchema" -> readDataSchema.catalogString,
      "PartitionFilters" -> seqToString(partitionFilters),
      "DataFilters" -> seqToString(dataFilters),
      "Location" -> locationDesc)
  }

  protected def partitions: Seq[FilePartition] = {
    val selectedPartitions = fileIndex.listFiles(partitionFilters, dataFilters)
    val maxSplitBytes = FilePartition.maxSplitBytes(sparkSession, selectedPartitions)
    val partitionAttributes = toAttributes(fileIndex.partitionSchema)
    val attributeMap = partitionAttributes.map(a => normalizeName(a.name) -> a).toMap
    val readPartitionAttributes = readPartitionSchema.map { readField =>
      attributeMap.getOrElse(normalizeName(readField.name),
        throw QueryCompilationErrors.cannotFindPartitionColumnInPartitionSchemaError(
          readField, fileIndex.partitionSchema)
      )
    }
    lazy val partitionValueProject =
      GenerateUnsafeProjection.generate(readPartitionAttributes, partitionAttributes)
    val splitFiles = selectedPartitions.flatMap { partition =>
      // Prune partition values if part of the partition columns are not required.
      val partitionValues = if (readPartitionAttributes != partitionAttributes) {
        partitionValueProject(partition.values).copy()
      } else {
        partition.values
      }
      partition.files.flatMap { file =>
        val filePath = file.getPath
        PartitionedFileUtil.splitFiles(
          file = file,
          filePath = filePath,
          isSplitable = isSplitable(filePath),
          maxSplitBytes = maxSplitBytes,
          partitionValues = partitionValues
        )
      }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    }

    if (splitFiles.length == 1) {
      val path = splitFiles(0).toPath
      if (!isSplitable(path) && splitFiles(0).length >
        SessionStateHelper.getSparkConf(sparkSession).get(IO_WARNING_LARGEFILETHRESHOLD)) {
        logWarning(log"Loading one large unsplittable file ${MDC(PATH, path.toString)} with only " +
          log"one partition, the reason is: ${MDC(REASON, getFileUnSplittableReason(path))}")
      }
    }

    FilePartition.getFilePartitions(sparkSession, splitFiles, maxSplitBytes)
  }

  override def planInputPartitions(): Array[InputPartition] = {
    partitions.toArray
  }

  override def estimateStatistics(): Statistics = {
    new Statistics {
      override def sizeInBytes(): OptionalLong = {
        val compressionFactor = conf.fileCompressionFactor
        val size = (compressionFactor * fileIndex.sizeInBytes /
          (dataSchema.defaultSize + fileIndex.partitionSchema.defaultSize) *
          (readDataSchema.defaultSize + readPartitionSchema.defaultSize)).toLong

        OptionalLong.of(size)
      }

      override def numRows(): OptionalLong = OptionalLong.empty()
    }
  }

  override def toBatch: Batch = this

  override def readSchema(): StructType =
    StructType(readDataSchema.fields ++ readPartitionSchema.fields)

  // Returns whether the two given arrays of [[Filter]]s are equivalent.
  protected def equivalentFilters(a: Array[Filter], b: Array[Filter]): Boolean = {
    a.sortBy(_.hashCode()).sameElements(b.sortBy(_.hashCode()))
  }

  // ===== SupportsScanMerging =================================================
  //
  // Two file scans of the same table can be fused into one when they read the same data and
  // differ only in projected columns and/or pushed data filters. This brings V2 file sources to
  // parity with V1, where MergeSubplans already merges such subplans (V1 keeps the Filter/Project
  // nodes in the logical plan, so the relation leaves are identical). The logic here is
  // format-agnostic; format-specific state is handled via the hooks below.

  /**
   * Format-specific scan state (beyond fileIndex / schema / options / partition filters / data
   * filters / projected columns) that must match for two scans to be mergeable. The default
   * requires no extra state. Overridden e.g. by ParquetScan to require equal variant extractions.
   * `other` is guaranteed to be the same concrete class as `this`.
   */
  protected def canMergeScanStateWith(other: FileScan): Boolean = true

  /**
   * Whether this scan has pushed-down aggregation. When true, the scan emits aggregated rows with
   * no post-scan Filter to reconcile per-side predicates, so merging is declined. Overridden by
   * ParquetScan / OrcScan which support aggregate pushdown.
   */
  protected def hasAggregatePushedDown: Boolean = false

  private def mergeEligible(o: FileScan): Boolean =
    getClass == o.getClass &&
      fileIndex == o.fileIndex &&
      dataSchema == o.dataSchema &&
      options == o.options &&
      normalizedPartitionFilters == o.normalizedPartitionFilters &&
      !hasAggregatePushedDown && !o.hasAggregatePushedDown &&
      canMergeScanStateWith(o)

  override def mergeWith(
      other: SupportsScanMerging,
      table: SupportsRead): Optional[SupportsScanMerging] = other match {
    case o: FileScan if mergeEligible(o) =>
      // Strict when the two scans already agree on pushed data filters: rebuild with the union of
      // read schemas, rows unchanged. Otherwise (relaxed) widen the best-effort pushed filter to
      // OR(f1, f2) so the merged scan reads a superset; the exact per-side predicate is reapplied
      // by the post-scan Filter node (split into aggregate FILTER clauses by PlanMerger). The
      // relaxed path is gated by `spark.sql.files.scanMerge.ignorePushedDataFilters`.
      val strict = normalizedDataFilters == o.normalizedDataFilters
      if (!strict && !conf.fileScanMergeIgnorePushedDataFilters) {
        Optional.empty()
      } else {
        table.newScanBuilder(options) match {
          case builder: FileScanBuilder =>
            // Partition filters are guaranteed equal (see `mergeEligible`), so push them as-is --
            // never widen, which would needlessly produce OR(pf, pf) and obscure partition
            // pruning. Only data filters use the strict/relaxed distinction.
            builder.pushFilters(mergedDataFilters(o, strict) ++ partitionFilters)
            builder.pruneColumns(readSchema().merge(o.readSchema()))
            builder.build() match {
              case merged: SupportsScanMerging => Optional.of(merged)
              case _ => Optional.empty()
            }
          case _ => Optional.empty()
        }
      }
    case _ => Optional.empty()
  }

  // Strict: the two data-filter lists are equivalent, so push this scan's. Relaxed: widen to
  // OR(AND(df1), AND(df2)) so the merged scan reads a superset; an empty list on either side
  // means that side reads everything, so the union also reads everything (no data filter).
  private def mergedDataFilters(o: FileScan, strict: Boolean): Seq[Expression] = {
    if (strict) {
      dataFilters
    } else if (dataFilters.nonEmpty && o.dataFilters.nonEmpty) {
      Seq(Or(dataFilters.reduce(And), o.dataFilters.reduce(And)))
    } else {
      Seq.empty
    }
  }

  private val isCaseSensitive = conf.caseSensitiveAnalysis

  private def normalizeName(name: String): String = {
    if (isCaseSensitive) {
      name
    } else {
      name.toLowerCase(Locale.ROOT)
    }
  }
}
