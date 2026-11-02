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

import java.util.Locale
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, BoundReference, Expression, Predicate}
import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.internal.connector.SupportsRuntimeCatalystFiltering

/**
 * A [[FileScan]] that prunes its partition directories with runtime filters, the DPP filters and
 * scalar subquery filters Spark derives over its partition columns (SPARK-30628). The built-in
 * file formats mix this in; a [[FileScan]] that does not extend one of their scans keeps its
 * previous behavior.
 *
 * The runtime-filter path returns plain [[FilePartition]]s packed from what `partitions` planned,
 * so a scan that overrides `planInputPartitions()` or reports a partitioning or an ordering should
 * not mix this in.
 */
trait RuntimePartitionPruningFileScan extends FileScan with SupportsRuntimeCatalystFiltering {

  @transient private lazy val plannedPartitionsRef = new AtomicReference[Seq[FilePartition]]()

  /**
   * The partitions `partitions` planned, stored once per scan instance. Both the plain read path
   * and the runtime-filter path go through this, so a scan node that gets runtime filters lists the
   * files once and filters that listing, rather than reading the index twice and risking two
   * snapshots. A subclass still customizes `partitions`.
   *
   * `partitions` runs outside the scan's monitor, which a `lazy val` would hold while it runs. Two
   * threads asking first at once may both list; both get the result that was stored.
   */
  private def plannedPartitions: Seq[FilePartition] = {
    val ref = plannedPartitionsRef
    Option(ref.get()).getOrElse {
      ref.compareAndSet(null, partitions)
      ref.get()
    }
  }

  override def planInputPartitions(): Array[InputPartition] = {
    plannedPartitions.toArray
  }

  /**
   * The partition columns Spark can derive a runtime filter on, restricted to the ones
   * `readSchema()` still exposes: a reference missing from the scan relation output fails to
   * resolve, and a pushed-down aggregate keeps only the partition columns it groups by.
   *
   * A filter over one of them is evaluated against each file's partition values, so the scan
   * evaluates it in full and Spark does not evaluate it again after the scan --
   * `FileScanBuilder.pushFilters` already keeps compile-time partition filters out of the post-scan
   * filters for the same reason.
   */
  override def filterAttributes(): Array[NamedReference] = {
    val readFields = readSchema().fieldNames.map(normalizePartitionName).toSet
    readPartitionSchema.fieldNames
      .filter(name => readFields.contains(normalizePartitionName(name)))
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
      .map { case (name, i) => normalizePartitionName(name) -> i }.toMap
    // Evaluating these is the scan's only chance to apply them, since the attributes are declared
    // fully pushed and Spark drops the post-scan filter. A reference outside the read partition
    // schema has no value to evaluate against, so fail loudly rather than return wrong rows. Spark
    // screens against `filterAttributes()` before it gets here, which is a subset of that schema.
    val notApplicable = expressions.filterNot(
      _.references.forall(a => fieldIndex.contains(normalizePartitionName(a.name))))
    if (notApplicable.nonEmpty) {
      throw SparkException.internalError("A file scan can only apply a runtime filter over its " +
        s"read partition columns ${readPartitionSchema.fieldNames.mkString("[", ", ", "]")}, " +
        s"got ${notApplicable.mkString(", ")}")
    }
    val bound = expressions.reduce(And).transform {
      case a: Attribute =>
        BoundReference(fieldIndex(normalizePartitionName(a.name)), a.dataType, a.nullable)
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

  // Read when the scan is built, as `FileScan` reads it for the names `partitions` matches, so the
  // runtime filters bind the same way even if the session's setting changes before they arrive.
  private val isCaseSensitive = conf.caseSensitiveAnalysis

  private def normalizePartitionName(name: String): String = {
    if (isCaseSensitive) name else name.toLowerCase(Locale.ROOT)
  }
}
