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

import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeSet, BitwiseAnd, HiveHash, Literal, Pmod, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Sort}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.internal.SQLConf

/**
 * V1 write includes both datasoruce and hive, that requires a specific ordering of data.
 * It should be resolved by [[V1Writes]].
 *
 * TODO(SPARK-37333): Specify the required distribution at V1Write
 */
trait V1Write extends DataWritingCommand with V1WritesHelper {
  def partitionColumns: Seq[Attribute] = Seq.empty
  def numStaticPartitions: Int = 0
  def bucketSpec: Option[BucketSpec] = None
  def options: Map[String, String] = Map.empty

  final def requiredOrdering: Seq[SortOrder] = {
    getSortOrder(
      outputColumns,
      partitionColumns,
      numStaticPartitions,
      bucketSpec,
      options)
  }
}

/**
 * A rule that makes sure the v1 write requirement, e.g. requiredOrdering
 */
object V1Writes extends Rule[LogicalPlan] with V1WritesHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case write: V1Write =>
      val partitionSet = AttributeSet(write.partitionColumns)
      val dataColumns = write.outputColumns.filterNot(partitionSet.contains)
      val sortColumns = getBucketSortColumns(write.bucketSpec, dataColumns)
      val newQuery = prepareQuery(write.query, write.requiredOrdering, sortColumns)
      write.withNewChildren(newQuery :: Nil)

    case _ => plan
  }
}

trait V1WritesHelper {

  def getBucketSpec(
      bucketSpec: Option[BucketSpec],
      dataColumns: Seq[Attribute],
      options: Map[String, String]): Option[WriterBucketSpec] = {
    bucketSpec.map { spec =>
      val bucketColumns = spec.bucketColumnNames.map(c => dataColumns.find(_.name == c).get)
      if (options.getOrElse(BucketingUtils.optionForHiveCompatibleBucketWrite, "false") ==
        "true") {
        // Hive bucketed table: use `HiveHash` and bitwise-and as bucket id expression.
        // Without the extra bitwise-and operation, we can get wrong bucket id when hash value of
        // columns is negative. See Hive implementation in
        // `org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils#getBucketNumber()`.
        val hashId = BitwiseAnd(HiveHash(bucketColumns), Literal(Int.MaxValue))
        val bucketIdExpression = Pmod(hashId, Literal(spec.numBuckets))

        // The bucket file name prefix is following Hive, Presto and Trino conversion, so this
        // makes sure Hive bucketed table written by Spark, can be read by other SQL engines.
        //
        // Hive: `org.apache.hadoop.hive.ql.exec.Utilities#getBucketIdFromFile()`.
        // Trino: `io.trino.plugin.hive.BackgroundHiveSplitLoader#BUCKET_PATTERNS`.
        val fileNamePrefix = (bucketId: Int) => f"$bucketId%05d_0_"
        WriterBucketSpec(bucketIdExpression, fileNamePrefix)
      } else {
        // Spark bucketed table: use `HashPartitioning.partitionIdExpression` as bucket id
        // expression, so that we can guarantee the data distribution is same between shuffle and
        // bucketed data source, which enables us to only shuffle one side when join a bucketed
        // table and a normal one.
        val bucketIdExpression = HashPartitioning(bucketColumns, spec.numBuckets)
          .partitionIdExpression
        WriterBucketSpec(bucketIdExpression, (_: Int) => "")
      }
    }
  }

  def getBucketSortColumns(
      bucketSpec: Option[BucketSpec], dataColumns: Seq[Attribute]): Seq[Attribute] = {
    bucketSpec.toSeq.flatMap {
      spec => spec.sortColumnNames.map(c => dataColumns.find(_.name == c).get)
    }
  }

  def getSortOrder(
      outputColumns: Seq[Attribute],
      partitionColumns: Seq[Attribute],
      numStaticPartitions: Int,
      bucketSpec: Option[BucketSpec],
      options: Map[String, String]): Seq[SortOrder] = {
    val partitionSet = AttributeSet(partitionColumns)
    val dataColumns = outputColumns.filterNot(partitionSet.contains)
    val writerBucketSpec = getBucketSpec(bucketSpec, dataColumns, options)
    val sortColumns = getBucketSortColumns(bucketSpec, dataColumns)

    assert(partitionColumns.size >= numStaticPartitions)
    // We should first sort by partition columns, then bucket id, and finally sorting columns.
    (partitionColumns.takeRight(partitionColumns.size - numStaticPartitions) ++
      writerBucketSpec.map(_.bucketIdExpression) ++ sortColumns)
      .map(SortOrder(_, Ascending))
  }

  def prepareQuery(
      query: LogicalPlan,
      requiredOrdering: Seq[SortOrder],
      sortColumns: Seq[Attribute]): LogicalPlan = {
    val actualOrdering = query.outputOrdering
    val orderingMatched = if (requiredOrdering.length > actualOrdering.length) {
      false
    } else {
      requiredOrdering.zip(actualOrdering).forall {
        case (requiredOrder, childOutputOrder) =>
          requiredOrder.semanticEquals(childOutputOrder)
      }
    }

    if (orderingMatched ||
      (SQLConf.get.maxConcurrentOutputFileWriters > 0 && sortColumns.isEmpty)) {
      query
    } else {
      Sort(requiredOrdering, false, query)
    }
  }
}
