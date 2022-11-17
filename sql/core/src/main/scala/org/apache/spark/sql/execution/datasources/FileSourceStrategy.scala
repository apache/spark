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

import java.util.Locale

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.FileFormat.METADATA_NAME
import org.apache.spark.sql.types.{DoubleType, FloatType, LongType, StructType}
import org.apache.spark.util.collection.BitSet

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
 *  - Using a partition pruning predicates, enumerate the list of files that should be read.
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
object FileSourceStrategy extends Strategy with PredicateHelper with Logging {

  // should prune buckets iff num buckets is greater than 1 and there is only one bucket column
  private def shouldPruneBuckets(bucketSpec: Option[BucketSpec]): Boolean = {
    bucketSpec match {
      case Some(spec) => spec.bucketColumnNames.length == 1 && spec.numBuckets > 1
      case None => false
    }
  }

  private def getExpressionBuckets(
      expr: Expression,
      bucketColumnName: String,
      numBuckets: Int): BitSet = {

    def getBucketNumber(attr: Attribute, v: Any): Int = {
      BucketingUtils.getBucketIdFromValue(attr, numBuckets, v)
    }

    def getBucketSetFromIterable(attr: Attribute, iter: Iterable[Any]): BitSet = {
      val matchedBuckets = new BitSet(numBuckets)
      iter
        .map(v => getBucketNumber(attr, v))
        .foreach(bucketNum => matchedBuckets.set(bucketNum))
      matchedBuckets
    }

    def getBucketSetFromValue(attr: Attribute, v: Any): BitSet = {
      val matchedBuckets = new BitSet(numBuckets)
      matchedBuckets.set(getBucketNumber(attr, v))
      matchedBuckets
    }

    expr match {
      case expressions.Equality(a: Attribute, Literal(v, _)) if a.name == bucketColumnName =>
        getBucketSetFromValue(a, v)
      case expressions.In(a: Attribute, list)
        if list.forall(_.isInstanceOf[Literal]) && a.name == bucketColumnName =>
        getBucketSetFromIterable(a, list.map(e => e.eval(EmptyRow)))
      case expressions.InSet(a: Attribute, hset) if a.name == bucketColumnName =>
        getBucketSetFromIterable(a, hset)
      case expressions.IsNull(a: Attribute) if a.name == bucketColumnName =>
        getBucketSetFromValue(a, null)
      case expressions.IsNaN(a: Attribute)
        if a.name == bucketColumnName && a.dataType == FloatType =>
        getBucketSetFromValue(a, Float.NaN)
      case expressions.IsNaN(a: Attribute)
        if a.name == bucketColumnName && a.dataType == DoubleType =>
        getBucketSetFromValue(a, Double.NaN)
      case expressions.And(left, right) =>
        getExpressionBuckets(left, bucketColumnName, numBuckets) &
          getExpressionBuckets(right, bucketColumnName, numBuckets)
      case expressions.Or(left, right) =>
        getExpressionBuckets(left, bucketColumnName, numBuckets) |
        getExpressionBuckets(right, bucketColumnName, numBuckets)
      case _ =>
        val matchedBuckets = new BitSet(numBuckets)
        matchedBuckets.setUntil(numBuckets)
        matchedBuckets
    }
  }

  private def genBucketSet(
      normalizedFilters: Seq[Expression],
      bucketSpec: BucketSpec): Option[BitSet] = {
    if (normalizedFilters.isEmpty) {
      return None
    }

    val bucketColumnName = bucketSpec.bucketColumnNames.head
    val numBuckets = bucketSpec.numBuckets

    val normalizedFiltersAndExpr = normalizedFilters
      .reduce(expressions.And)
    val matchedBuckets = getExpressionBuckets(normalizedFiltersAndExpr, bucketColumnName,
      numBuckets)

    val numBucketsSelected = matchedBuckets.cardinality()

    logInfo {
      s"Pruned ${numBuckets - numBucketsSelected} out of $numBuckets buckets."
    }

    // None means all the buckets need to be scanned
    if (numBucketsSelected == numBuckets) {
      None
    } else {
      Some(matchedBuckets)
    }
  }

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ScanOperation(projects, stayUpFilters, filters,
      l @ LogicalRelation(fsRelation: HadoopFsRelation, _, table, _)) =>
      // Filters on this relation fall into four categories based on where we can use them to avoid
      // reading unneeded data:
      //  - partition keys only - used to prune directories to read
      //  - bucket keys only - optionally used to prune files to read
      //  - keys stored in the data only - optionally used to skip groups of data in files
      //  - filters that need to be evaluated again after the scan
      val filterSet = ExpressionSet(filters)

      val normalizedFilters = DataSourceStrategy.normalizeExprs(
        filters.filter(_.deterministic), l.output)

      val partitionColumns =
        l.resolve(
          fsRelation.partitionSchema, fsRelation.sparkSession.sessionState.analyzer.resolver)
      val partitionSet = AttributeSet(partitionColumns)

      // this partitionKeyFilters should be the same with the ones being executed in
      // PruneFileSourcePartitions
      val partitionKeyFilters = DataSourceStrategy.getPushedDownFilters(partitionColumns,
        normalizedFilters)

      // subquery expressions are filtered out because they can't be used to prune buckets or pushed
      // down as data filters, yet they would be executed
      val normalizedFiltersWithoutSubqueries =
        normalizedFilters.filterNot(SubqueryExpression.hasSubquery)

      val bucketSpec: Option[BucketSpec] = fsRelation.bucketSpec
      val bucketSet = if (shouldPruneBuckets(bucketSpec)) {
        genBucketSet(normalizedFiltersWithoutSubqueries, bucketSpec.get)
      } else {
        None
      }

      val dataColumns =
        l.resolve(fsRelation.dataSchema, fsRelation.sparkSession.sessionState.analyzer.resolver)

      // Partition keys are not available in the statistics of the files.
      // `dataColumns` might have partition columns, we need to filter them out.
      val dataColumnsWithoutPartitionCols = dataColumns.filterNot(partitionSet.contains)
      val dataFilters = normalizedFiltersWithoutSubqueries.flatMap { f =>
        if (f.references.intersect(partitionSet).nonEmpty) {
          extractPredicatesWithinOutputSet(f, AttributeSet(dataColumnsWithoutPartitionCols))
        } else {
          Some(f)
        }
      }
      val supportNestedPredicatePushdown =
        DataSourceUtils.supportNestedPredicatePushdown(fsRelation)
      val pushedFilters = dataFilters
        .flatMap(DataSourceStrategy.translateFilter(_, supportNestedPredicatePushdown))
      logInfo(s"Pushed Filters: ${pushedFilters.mkString(",")}")

      // Predicates with both partition keys and attributes need to be evaluated after the scan.
      val afterScanFilters = filterSet -- partitionKeyFilters.filter(_.references.nonEmpty)
      logInfo(s"Post-Scan Filters: ${afterScanFilters.mkString(",")}")

      val filterAttributes = AttributeSet(afterScanFilters ++ stayUpFilters)
      val requiredExpressions: Seq[NamedExpression] = filterAttributes.toSeq ++ projects
      val requiredAttributes = AttributeSet(requiredExpressions)

      val metadataStructOpt = l.output.collectFirst {
        case FileSourceMetadataAttribute(attr) => attr
      }

      val metadataColumns = metadataStructOpt.map { metadataStruct =>
        metadataStruct.dataType.asInstanceOf[StructType].fields.map { field =>
          FileSourceMetadataAttribute(field.name, field.dataType)
        }.toSeq
      }.getOrElse(Seq.empty)

      val fileConstantMetadataColumns: Seq[Attribute] =
        metadataColumns.filter(_.name != FileFormat.ROW_INDEX)

      val readDataColumns = dataColumns
        .filter(requiredAttributes.contains)
        .filterNot(partitionColumns.contains)

      val fileFormatReaderGeneratedMetadataColumns: Seq[Attribute] =
        metadataColumns.map(_.name).flatMap {
          case FileFormat.ROW_INDEX =>
            if ((readDataColumns ++ partitionColumns).map(_.name.toLowerCase(Locale.ROOT))
                .contains(FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME)) {
              throw new AnalysisException(FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME +
                " is a reserved column name that cannot be read in combination with " +
                s"${FileFormat.METADATA_NAME}.${FileFormat.ROW_INDEX} column.")
            }
            Some(AttributeReference(FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME, LongType)())
          case _ => None
        }

      val outputDataSchema = (readDataColumns ++ fileFormatReaderGeneratedMetadataColumns)
        .toStructType

      // The output rows will be produced during file scan operation in three steps:
      //  (1) File format reader populates a `Row` with `readDataColumns` and
      //      `fileFormatReaderGeneratedMetadataColumns`
      //  (2) Then, a row containing `partitionColumns` is joined at the end.
      //  (3) Finally, a row containing `fileConstantMetadataColumns` is also joined at the end.
      // By placing `fileFormatReaderGeneratedMetadataColumns` before `partitionColumns` and
      // `fileConstantMetadataColumns` in the `outputAttributes` we make these row operations
      // simpler and more efficient.
      val outputAttributes = readDataColumns ++ fileFormatReaderGeneratedMetadataColumns ++
        partitionColumns ++ fileConstantMetadataColumns

      val scan =
        FileSourceScanExec(
          fsRelation,
          outputAttributes,
          outputDataSchema,
          partitionKeyFilters.toSeq,
          bucketSet,
          None,
          dataFilters,
          table.map(_.identifier))

      // extra Project node: wrap flat metadata columns to a metadata struct
      val withMetadataProjections = metadataStructOpt.map { metadataStruct =>
        val structColumns = metadataColumns.map { col => col.name match {
            case FileFormat.FILE_PATH | FileFormat.FILE_NAME | FileFormat.FILE_SIZE |
                 FileFormat.FILE_MODIFICATION_TIME =>
              col
            case FileFormat.ROW_INDEX =>
              fileFormatReaderGeneratedMetadataColumns
                .find(_.name == FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME)
                .get.withName(FileFormat.ROW_INDEX)
          }
        }
        val metadataAlias =
          Alias(CreateStruct(structColumns), METADATA_NAME)(exprId = metadataStruct.exprId)
        execution.ProjectExec(
          readDataColumns ++ partitionColumns :+ metadataAlias, scan)
      }.getOrElse(scan)

      // bottom-most filters are put in the left of the list.
      val finalFilters = afterScanFilters.toSeq.reduceOption(expressions.And).toSeq ++ stayUpFilters
      val withFilter = finalFilters.foldLeft(withMetadataProjections)((plan, cond) => {
        execution.FilterExec(cond, plan)
      })
      val withProjections = if (projects == withFilter.output) {
        withFilter
      } else {
        execution.ProjectExec(projects, withFilter)
      }

      withProjections :: Nil

    case _ => Nil
  }
}
