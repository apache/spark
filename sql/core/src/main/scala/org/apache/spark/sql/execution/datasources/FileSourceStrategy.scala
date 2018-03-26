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

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.SparkPlan
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
object FileSourceStrategy extends Strategy with Logging {

  // should prune buckets iff num buckets is greater than 1 and there is only one bucket column
  private def shouldPruneBuckets(bucketSpec: Option[BucketSpec]): Boolean = {
    bucketSpec match {
      case Some(spec) => spec.bucketColumnNames.length == 1 && spec.numBuckets > 1
      case None => false
    }
  }

  private def getExpressionBuckets(expr: Expression,
                                   bucketColumnName: String,
                                   numBuckets: Int): BitSet = {

    def getMatchedBucketBitSet(attr: Attribute, v: Any): BitSet = {
      val matchedBuckets = new BitSet(numBuckets)
      matchedBuckets.set(BucketingUtils.getBucketIdFromValue(attr, numBuckets, v))
      matchedBuckets
    }

    expr match {
      case expressions.EqualTo(a: Attribute, Literal(v, _)) if a.name == bucketColumnName =>
        getMatchedBucketBitSet(a, v)
      case expressions.EqualTo(Literal(v, _), a: Attribute) if a.name == bucketColumnName =>
        getMatchedBucketBitSet(a, v)
      case expressions.EqualNullSafe(a: Attribute, Literal(v, _)) if a.name == bucketColumnName =>
        getMatchedBucketBitSet(a, v)
      case expressions.EqualNullSafe(Literal(v, _), a: Attribute) if a.name == bucketColumnName =>
        getMatchedBucketBitSet(a, v)
      case expressions.In(a: Attribute, list)
        if list.forall(_.isInstanceOf[Literal]) && a.name == bucketColumnName =>
        val valuesSet = list.map(e => e.eval(EmptyRow))
        valuesSet
          .map(v => getMatchedBucketBitSet(a, v))
          .fold(new BitSet(numBuckets))(_ | _)
      case expressions.IsNull(a: Attribute) if a.name == bucketColumnName =>
        getMatchedBucketBitSet(a, null)
      case expressions.And(left, right) =>
        getExpressionBuckets(left, bucketColumnName, numBuckets) |
          getExpressionBuckets(right, bucketColumnName, numBuckets)
      case expressions.Or(left, right) =>
        val leftBuckets = getExpressionBuckets(left, bucketColumnName, numBuckets)
        val rightBuckets = getExpressionBuckets(right, bucketColumnName, numBuckets)

        // if some expression in OR condition requires all buckets, return an empty BitSet
        if (leftBuckets.cardinality() == 0 || rightBuckets.cardinality() == 0) {
          new BitSet(numBuckets)
        } else {
          // return a BitSet that includes all required buckets
          leftBuckets | rightBuckets
        }
      case _ => new BitSet(numBuckets)
    }
  }

  private def getBuckets(normalizedFilters: Seq[Expression],
                         bucketSpec: BucketSpec): Option[BitSet] = {

    val bucketColumnName = bucketSpec.bucketColumnNames.head
    val numBuckets = bucketSpec.numBuckets

    val matchedBuckets = normalizedFilters
      .map(f => getExpressionBuckets(f, bucketColumnName, numBuckets))
      .fold(new BitSet(numBuckets))(_ | _)

    val numBucketsSelected = if (matchedBuckets.cardinality() != 0) matchedBuckets.cardinality()
    else numBuckets
    logInfo {
      s"Pruned ${numBuckets - numBucketsSelected} out of $numBuckets buckets."
    }

    // None means all the buckets need to be scanned
    if (matchedBuckets.cardinality() == 0) None else Some(matchedBuckets)
  }

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(projects, filters,
      l @ LogicalRelation(fsRelation: HadoopFsRelation, _, table, _)) =>
      // Filters on this relation fall into four categories based on where we can use them to avoid
      // reading unneeded data:
      //  - partition keys only - used to prune directories to read
      //  - bucket keys only - optionally used to prune files to read
      //  - keys stored in the data only - optionally used to skip groups of data in files
      //  - filters that need to be evaluated again after the scan
      val filterSet = ExpressionSet(filters)

      // The attribute name of predicate could be different than the one in schema in case of
      // case insensitive, we should change them to match the one in schema, so we do not need to
      // worry about case sensitivity anymore.
      val normalizedFilters = filters.map { e =>
        e transform {
          case a: AttributeReference =>
            a.withName(l.output.find(_.semanticEquals(a)).get.name)
        }
      }

      val partitionColumns =
        l.resolve(
          fsRelation.partitionSchema, fsRelation.sparkSession.sessionState.analyzer.resolver)
      val partitionSet = AttributeSet(partitionColumns)
      val partitionKeyFilters =
        ExpressionSet(normalizedFilters.filter(_.references.subsetOf(partitionSet)))
      logInfo(s"Pruning directories with: ${partitionKeyFilters.mkString(",")}")

      val bucketSpec: Option[BucketSpec] = fsRelation.bucketSpec
      val bucketSet = if (shouldPruneBuckets(bucketSpec)) {
        getBuckets(normalizedFilters, bucketSpec.get)
      } else {
        None
      }

      val dataColumns =
        l.resolve(fsRelation.dataSchema, fsRelation.sparkSession.sessionState.analyzer.resolver)

      // Partition keys are not available in the statistics of the files.
      val dataFilters = normalizedFilters.filter(_.references.intersect(partitionSet).isEmpty)

      // Predicates with both partition keys and attributes need to be evaluated after the scan.
      val afterScanFilters = filterSet -- partitionKeyFilters.filter(_.references.nonEmpty)
      logInfo(s"Post-Scan Filters: ${afterScanFilters.mkString(",")}")

      val filterAttributes = AttributeSet(afterScanFilters)
      val requiredExpressions: Seq[NamedExpression] = filterAttributes.toSeq ++ projects
      val requiredAttributes = AttributeSet(requiredExpressions)

      val readDataColumns =
        dataColumns
          .filter(requiredAttributes.contains)
          .filterNot(partitionColumns.contains)
      val outputSchema = readDataColumns.toStructType
      logInfo(s"Output Data Schema: ${outputSchema.simpleString(5)}")

      val outputAttributes = readDataColumns ++ partitionColumns

      val scan =
        FileSourceScanExec(
          fsRelation,
          outputAttributes,
          outputSchema,
          partitionKeyFilters.toSeq,
          bucketSet,
          dataFilters,
          table.map(_.identifier))

      val afterScanFilter = afterScanFilters.toSeq.reduceOption(expressions.And)
      val withFilter = afterScanFilter.map(execution.FilterExec(_, scan)).getOrElse(scan)
      val withProjections = if (projects == withFilter.output) {
        withFilter
      } else {
        execution.ProjectExec(projects, withFilter)
      }

      withProjections :: Nil

    case _ => Nil
  }
}
