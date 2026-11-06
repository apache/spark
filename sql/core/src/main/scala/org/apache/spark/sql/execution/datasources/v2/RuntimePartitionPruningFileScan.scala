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

import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, Expression, Predicate}
import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.internal.connector.SupportsRuntimeCatalystFiltering
import org.apache.spark.util.TransientBestEffortLazyVal

/**
 * A [[FileScan]] that prunes its partition directories with the runtime filters Spark derives over
 * its partition columns (SPARK-30628). The built-in file formats mix this in.
 *
 * Both read paths start from one listing of the file index and split and pack the files they
 * keep through `filePartitions`, so `partitions` and `planInputPartitions()` are final here. A
 * scan that reports a partitioning or an ordering should not mix this in.
 */
trait RuntimePartitionPruningFileScan extends FileScan with SupportsRuntimeCatalystFiltering {

  /**
   * The file index listing and its packing, each taken once per scan so planning and execution
   * read one snapshot. A `lazy val` would compute them while holding the scan's monitor.
   */
  private val listing = new TransientBestEffortLazyVal(() =>
    fileIndex.listFiles(partitionFilters, dataFilters))

  private val allPartitions = new TransientBestEffortLazyVal(() => filePartitions(listing()))

  // Final so that a subclass of a built-in scan cannot change one read path without the other.
  final override protected def partitions: Seq[FilePartition] = allPartitions()

  final override def planInputPartitions(): Array[InputPartition] = allPartitions().toArray

  /**
   * The read partition columns that `readSchema()` still exposes; a pushed-down aggregate keeps
   * only the ones it groups by. None is declared fully pushed, so a scalar subquery filter over
   * them is evaluated again above the scan.
   */
  override def filterAttributes(): Array[NamedReference] = {
    val readFields = readSchema().fieldNames.map(normalizePartitionName).toSet
    readPartitionSchema.fieldNames
      .filter(name => readFields.contains(normalizePartitionName(name)))
      .map(FieldReference.column)
  }

  /**
   * Keeps the listed partition directories whose values satisfy `expressions`, then splits and
   * packs their files for their own size, as V1 does for `dynamicallySelectedPartitions`.
   *
   * A filter that fails to evaluate on a directory keeps it, since the join or the `FilterExec`
   * above the scan applies the filter again.
   */
  override def planInputPartitionsWithRuntimeFilters(
      expressions: Array[Expression]): Array[InputPartition] = {
    if (expressions.isEmpty) return allPartitions().toArray
    // Directory values follow the table's partition schema, not the read one, and bind as V1
    // binds them: with that schema's types, nullable.
    val partitionSchema = fileIndex.partitionSchema
    val fieldIndex = partitionSchema.fieldNames.zipWithIndex
      .map { case (name, i) => normalizePartitionName(name) -> i }.toMap
    val predicates = expressions.toSeq.map { e =>
      Predicate.createInterpreted(e.transform {
        case a: Attribute =>
          // PushDownUtils passes only filters over `filterAttributes()`.
          val i = fieldIndex.getOrElse(normalizePartitionName(a.name),
            throw SparkException.internalError("A file scan can only apply a runtime filter " +
              s"over its partition columns ${partitionSchema.fieldNames.mkString("[", ", ", "]")}" +
              s", got $e"))
          BoundReference(i, partitionSchema(i).dataType, nullable = true)
      })
    }
    val all = listing()
    val kept = all.filter { dir =>
      predicates.forall { predicate =>
        try predicate.eval(dir.values) catch { case NonFatal(_) => true }
      }
    }
    if (kept.length == all.length) allPartitions().toArray else filePartitions(kept).toArray
  }

  // Read when the scan is built, as `FileScan` does.
  private val isCaseSensitive = conf.caseSensitiveAnalysis

  private def normalizePartitionName(name: String): String = {
    if (isCaseSensitive) name else name.toLowerCase(Locale.ROOT)
  }
}
