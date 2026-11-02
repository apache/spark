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

import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, Expression, Predicate}
import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.InSubqueryExec
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionDirectory}
import org.apache.spark.sql.internal.connector.SupportsRuntimeCatalystFiltering

/**
 * A [[FileScan]] that prunes its partition directories with the runtime filters Spark derives over
 * its partition columns (SPARK-30628). The built-in file formats mix this in.
 *
 * Both read paths start from one listing of the file index and split and pack the files they
 * keep, so `partitions` is not used. A scan that overrides it or `planInputPartitions()`, or that
 * reports a partitioning or an ordering, should not mix this in.
 */
trait RuntimePartitionPruningFileScan extends FileScan with SupportsRuntimeCatalystFiltering {

  @transient private lazy val listingRef = new AtomicReference[Seq[PartitionDirectory]]()
  @transient private lazy val allPartitionsRef = new AtomicReference[Seq[FilePartition]]()

  /**
   * The file index listing and its packing, each taken once per scan so planning and execution
   * read one snapshot. A `lazy val` would compute them while holding the scan's monitor.
   */
  private def listing: Seq[PartitionDirectory] =
    once(listingRef)(fileIndex.listFiles(partitionFilters, dataFilters))

  private def allPartitions: Seq[FilePartition] = once(allPartitionsRef)(filePartitions(listing))

  private def once[T >: Null <: AnyRef](ref: AtomicReference[T])(compute: => T): T =
    Option(ref.get()).getOrElse {
      ref.compareAndSet(null, compute)
      ref.get()
    }

  override def planInputPartitions(): Array[InputPartition] = allPartitions.toArray

  /**
   * The read partition columns that `readSchema()` still exposes; a pushed-down aggregate keeps
   * only the ones it groups by. The scan evaluates a filter over them in full.
   */
  override def filterAttributes(): Array[NamedReference] = {
    val readFields = readSchema().fieldNames.map(normalizePartitionName).toSet
    readPartitionSchema.fieldNames
      .filter(name => readFields.contains(normalizePartitionName(name)))
      .map(FieldReference.column)
  }

  override def fullyPushedFilterAttributes(): Array[NamedReference] = filterAttributes()

  /**
   * Keeps the listed partition directories whose values satisfy `expressions`, then splits and
   * packs their files for their own size, as V1 does for `dynamicallySelectedPartitions`.
   *
   * A DPP filter that fails to evaluate on a directory keeps it, since the join applies the filter
   * again. Any other filter is fully pushed, so its failure fails the query.
   */
  override def planInputPartitionsWithRuntimeFilters(
      expressions: Array[Expression]): Array[InputPartition] = {
    if (expressions.isEmpty) return allPartitions.toArray
    val readFields = readPartitionSchema.fieldNames.map(normalizePartitionName).toSet
    val notApplicable = expressions.filterNot(
      _.references.forall(a => readFields.contains(normalizePartitionName(a.name))))
    if (notApplicable.nonEmpty) {
      throw SparkException.internalError("A file scan can only apply a runtime filter over its " +
        s"read partition columns ${readPartitionSchema.fieldNames.mkString("[", ", ", "]")}, " +
        s"got ${notApplicable.mkString(", ")}")
    }
    // Directory values follow the table's partition schema, not the read one.
    val fieldIndex = fileIndex.partitionSchema.fieldNames.zipWithIndex
      .map { case (name, i) => normalizePartitionName(name) -> i }.toMap
    val predicates = expressions.toSeq.map { e =>
      val bound = e.transform {
        case a: Attribute =>
          BoundReference(fieldIndex(normalizePartitionName(a.name)), a.dataType, a.nullable)
      }
      (Predicate.createInterpreted(bound), e.isInstanceOf[InSubqueryExec])
    }
    // A directory with no file to read is dropped before any filter is evaluated on it.
    val kept = listing.filter { dir =>
      dir.files.nonEmpty && predicates.forall { case (predicate, keepOnFailure) =>
        try predicate.eval(dir.values) catch { case NonFatal(_) if keepOnFailure => true }
      }
    }
    filePartitions(kept).toArray
  }

  // Read when the scan is built, as `FileScan` does.
  private val isCaseSensitive = conf.caseSensitiveAnalysis

  private def normalizePartitionName(name: String): String = {
    if (isCaseSensitive) name else name.toLowerCase(Locale.ROOT)
  }
}
