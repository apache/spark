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

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path

import org.apache.spark.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{DataSourceScan, SparkPlan}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/**
 * A strategy for planning scans over collections of files that might be partitioned or bucketed
 * by user specified columns.
 *
 * At a high level planning occurs in several phases:
 *  - Split filters by when they need to be evaluated.
 *  - Prune the schema of the data requested based on any projections present. Today this pruning
 *    is only done on top level columns, but formats should support pruning of nested columns as
 *    well.
 *  - Construct a reader function by passing filters and the schema into the FileFormat.
 *  - Using an partition pruning predicates, enumerate the list of files that should be read.
 *  - Split the files into tasks and construct a FileScanRDD.
 *  - Add any projection or filters that must be evaluated after the scan.
 *
 * Files are assigned into tasks using the following algorithm:
 *  - If the table is bucketed, group files by bucket id into the correct number of partitions.
 *  - If the table is not bucketed or bucketing is turned off:
 *   - If any file is larger than the threshold, split it into pieces based on that threshold
 *   - Sort the files by decreasing file size.
 *   - Assign the ordered files to buckets using the following algorithm.  If the current partition
 *     is under the threshold with the addition of the next file, add it.  If not, open a new bucket
 *     and add it.  Proceed to the next file.
 */
private[sql] object FileSourceStrategy extends Strategy with Logging {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(projects, filters, l@LogicalRelation(files: HadoopFsRelation, _, _))
      if files.fileFormat.toString == "TestFileFormat" =>
      // Filters on this relation fall into four categories based on where we can use them to avoid
      // reading unneeded data:
      //  - partition keys only - used to prune directories to read
      //  - bucket keys only - optionally used to prune files to read
      //  - keys stored in the data only - optionally used to skip groups of data in files
      //  - filters that need to be evaluated again after the scan
      val filterSet = ExpressionSet(filters)

      val partitionColumns =
        AttributeSet(l.resolve(files.partitionSchema, files.sqlContext.analyzer.resolver))
      val partitionKeyFilters =
        ExpressionSet(filters.filter(_.references.subsetOf(partitionColumns)))
      logInfo(s"Pruning directories with: ${partitionKeyFilters.mkString(",")}")

      val bucketColumns =
        AttributeSet(
          files.bucketSpec
              .map(_.bucketColumnNames)
              .getOrElse(Nil)
              .map(l.resolveQuoted(_, files.sqlContext.conf.resolver)
                  .getOrElse(sys.error(""))))

      // Partition keys are not available in the statistics of the files.
      val dataFilters = filters.filter(_.references.intersect(partitionColumns).isEmpty)

      // Predicates with both partition keys and attributes need to be evaluated after the scan.
      val afterScanFilters = filterSet -- partitionKeyFilters
      logInfo(s"Post-Scan Filters: ${afterScanFilters.mkString(",")}")

      val selectedPartitions = files.location.listFiles(partitionKeyFilters.toSeq)

      val filterAttributes = AttributeSet(afterScanFilters)
      val requiredExpressions: Seq[NamedExpression] = filterAttributes.toSeq ++ projects
      val requiredAttributes = AttributeSet(requiredExpressions).map(_.name).toSet

      val prunedDataSchema =
        StructType(
          files.dataSchema.filter(f => requiredAttributes.contains(f.name)))
      logInfo(s"Pruned Data Schema: ${prunedDataSchema.simpleString(5)}")

      val pushedDownFilters = dataFilters.flatMap(DataSourceStrategy.translateFilter)
      logInfo(s"Pushed Filters: ${pushedDownFilters.mkString(",")}")

      val readFile = files.fileFormat.buildReader(
        sqlContext = files.sqlContext,
        partitionSchema = files.partitionSchema,
        dataSchema = prunedDataSchema,
        filters = pushedDownFilters,
        options = files.options)

      val plannedPartitions = files.bucketSpec match {
        case Some(bucketing) if files.sqlContext.conf.bucketingEnabled =>
          logInfo(s"Planning with ${bucketing.numBuckets} buckets")
          val bucketed =
            selectedPartitions
                .flatMap { p =>
                  p.files.map(f => PartitionedFile(p.values, f.getPath.toUri.toString, 0, f.getLen))
                }.groupBy { f =>
              BucketingUtils
                  .getBucketId(new Path(f.filePath).getName)
                  .getOrElse(sys.error(s"Invalid bucket file ${f.filePath}"))
            }

          (0 until bucketing.numBuckets).map { bucketId =>
            FilePartition(bucketId, bucketed.getOrElse(bucketId, Nil))
          }

        case _ =>
          val maxSplitBytes = files.sqlContext.conf.filesMaxPartitionBytes
          logInfo(s"Planning scan with bin packing, max size: $maxSplitBytes bytes")

          val splitFiles = selectedPartitions.flatMap { partition =>
            partition.files.flatMap { file =>
              assert(file.getLen != 0)
              (0L to file.getLen by maxSplitBytes).map { offset =>
                val remaining = file.getLen - offset
                val size = if (remaining > maxSplitBytes) maxSplitBytes else remaining
                PartitionedFile(partition.values, file.getPath.toUri.toString, offset, size)
              }
            }
          }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)

          val partitions = new ArrayBuffer[FilePartition]
          val currentFiles = new ArrayBuffer[PartitionedFile]
          var currentSize = 0L

          /** Add the given file to the current partition. */
          def addFile(file: PartitionedFile): Unit = {
            currentSize += file.length
            currentFiles.append(file)
          }

          /** Close the current partition and move to the next. */
          def closePartition(): Unit = {
            if (currentFiles.nonEmpty) {
              val newPartition =
                FilePartition(
                  partitions.size,
                  currentFiles.toArray.toSeq) // Copy to a new Array.
              partitions.append(newPartition)
            }
            currentFiles.clear()
            currentSize = 0
          }

          // Assign files to partitions using "First Fit Decreasing" (FFD)
          // TODO: consider adding a slop factor here?
          splitFiles.foreach { file =>
            if (currentSize + file.length > maxSplitBytes) {
              closePartition()
              addFile(file)
            } else {
              addFile(file)
            }
          }
          closePartition()
          partitions
      }

      val scan =
        DataSourceScan(
          l.output,
          new FileScanRDD(
            files.sqlContext,
            readFile,
            plannedPartitions),
          files,
          Map("format" -> files.fileFormat.toString))

      val afterScanFilter = afterScanFilters.toSeq.reduceOption(expressions.And)
      val withFilter = afterScanFilter.map(execution.Filter(_, scan)).getOrElse(scan)
      val withProjections = if (projects.forall(_.isInstanceOf[AttributeReference])) {
        withFilter
      } else {
        execution.Project(projects, withFilter)
      }

      withProjections :: Nil

    case _ => Nil
  }
}
