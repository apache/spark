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

import java.util.{Locale, OptionalLong}

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{PATH, REASON}
import org.apache.spark.internal.config.IO_WARNING_LARGEFILETHRESHOLD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeSet, BoundReference, Expression, ExpressionSet, Predicate}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.{SessionStateHelper, SQLConf}
import org.apache.spark.sql.internal.connector.{SupportsMetadata, SupportsRuntimeCatalystFiltering}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

trait FileScan extends Scan
  with Batch
  with SupportsReportStatistics
  with SupportsMetadata
  with SupportsRuntimeCatalystFiltering
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

  private lazy val (normalizedPartitionFilters, normalizedDataFilters) = {
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

  /**
   * The partitions `partitions` planned, computed once per scan instance. Both the plain read path
   * and the runtime-filter path go through this, so a scan node that gets runtime filters lists the
   * files once and filters that listing, rather than reading the index twice and risking two
   * snapshots. A subclass still customizes `partitions`.
   */
  @transient private lazy val plannedPartitions: Seq[FilePartition] = partitions

  override def planInputPartitions(): Array[InputPartition] = {
    plannedPartitions.toArray
  }

  /**
   * The partition columns Spark can derive a runtime filter on (SPARK-30628), restricted to the
   * ones `readSchema()` still exposes: a reference missing from the scan relation output fails to
   * resolve, and a pushed-down aggregate keeps only the partition columns it groups by.
   *
   * A filter over one of them is evaluated against each file's partition values, so the scan
   * evaluates it in full and Spark does not evaluate it again after the scan --
   * `FileScanBuilder.pushFilters` already keeps compile-time partition filters out of the post-scan
   * filters for the same reason.
   */
  override def filterAttributes(): Array[NamedReference] = {
    val readFields = readSchema().fieldNames.map(normalizeName).toSet
    readPartitionSchema.fieldNames
      .filter(name => readFields.contains(normalizeName(name)))
      .map(FieldReference.column)
  }

  override def fullyPushedFilterAttributes(): Array[NamedReference] = filterAttributes()

  /**
   * Plans the files whose partition values satisfy `expressions`, taken from what `partitions`
   * planned and bin-packed again for the surviving set.
   *
   * Filtering what was planned rather than listing the files again keeps one listing per scan, and
   * the files a subclass excluded in `partitions` stay excluded. How the survivors group into
   * partitions is decided here again, for their own size. Where each file was cut into splits is
   * not: `partitions` cut them for the whole set it listed, before these filters existed.
   *
   * A partition value is fixed within a file, so every row of every partition returned satisfies
   * the expressions even though one partition can still hold files from several partition
   * directories.
   */
  override def planInputPartitionsWithRuntimeFilters(
      expressions: Array[Expression]): Array[InputPartition] = {
    val fieldIndex = readPartitionSchema.fieldNames.zipWithIndex
      .map { case (name, i) => normalizeName(name) -> i }.toMap
    // Evaluating these is the scan's only chance to apply them, since the attributes are declared
    // fully pushed and Spark drops the post-scan filter. A reference outside the read partition
    // schema has no value to evaluate against, so fail loudly rather than return wrong rows. Spark
    // screens against `filterAttributes()` before it gets here, which is a subset of that schema.
    val notApplicable = expressions.filterNot(
      _.references.forall(a => fieldIndex.contains(normalizeName(a.name))))
    if (notApplicable.nonEmpty) {
      throw SparkException.internalError("A file scan can only apply a runtime filter over its " +
        s"read partition columns ${readPartitionSchema.fieldNames.mkString("[", ", ", "]")}, " +
        s"got ${notApplicable.mkString(", ")}")
    }
    val bound = expressions.reduce(And).transform {
      case a: Attribute => BoundReference(fieldIndex(normalizeName(a.name)), a.dataType, a.nullable)
    }
    val predicate = Predicate.createInterpreted(bound)
    val keptFiles = plannedPartitions
      .flatMap(_.files.filter(file => predicate.eval(file.partitionValues)))
      .sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    // Pack the survivors instead of handing back the planned partitions with files removed:
    // `partitions` sized those bins for the whole file set, so a scan that pruned most of its files
    // would run the same few tasks over a fraction of the data. V1 packs the pruned set the same
    // way, from `dynamicallySelectedPartitions`.
    val openCostInBytes = conf.filesOpenCostInBytes
    val maxSplitBytes = FilePartition.maxSplitBytes(
      sparkSession, keptFiles.map(_.length + openCostInBytes).sum)
    FilePartition.getFilePartitions(sparkSession, keptFiles, maxSplitBytes).toArray
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

  private val isCaseSensitive = conf.caseSensitiveAnalysis

  private def normalizeName(name: String): String = {
    if (isCaseSensitive) {
      name
    } else {
      name.toLowerCase(Locale.ROOT)
    }
  }
}
