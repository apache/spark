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

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeSet, BitwiseAnd, HiveHash, Literal, Pmod, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Sort}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.internal.SQLConf

trait V1WriteCommand extends DataWritingCommand {
  // Specify the required ordering for the V1 write command. `FileFormatWriter` will
  // add SortExec if necessary when the requiredOrdering is empty.
  def requiredOrdering: Seq[SortOrder]
}

/**
 * A rule that adds logical sorts to V1 data writing commands.
 */
object V1Writes extends Rule[LogicalPlan] with SQLConfHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (conf.plannedWriteEnabled) {
      plan.transformDown {
        case write: V1WriteCommand =>
          val newQuery = prepareQuery(write, write.query)
          write.withNewChildren(newQuery :: Nil)
      }
    } else {
      plan
    }
  }

  private def prepareQuery(write: V1WriteCommand, query: LogicalPlan): LogicalPlan = {
    val requiredOrdering = write.requiredOrdering
    val outputOrdering = query.outputOrdering
    // Check if the ordering is already matched. It is needed to ensure the
    // idempotency of the rule.
    val orderingMatched = if (requiredOrdering.length > outputOrdering.length) {
      false
    } else {
      requiredOrdering.zip(outputOrdering).forall {
        case (requiredOrder, outputOrder) => requiredOrder.semanticEquals(outputOrder)
      }
    }
    if (orderingMatched) {
      query
    } else {
      Sort(requiredOrdering, global = false, query)
    }
  }
}

object V1WritesUtils {

  def getWriterBucketSpec(
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
      bucketSpec: Option[BucketSpec],
      dataColumns: Seq[Attribute]): Seq[Attribute] = {
    bucketSpec.toSeq.flatMap {
      spec => spec.sortColumnNames.map(c => dataColumns.find(_.name == c).get)
    }
  }

  def getSortOrder(
      outputColumns: Seq[Attribute],
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      options: Map[String, String]): Seq[SortOrder] = {
    val partitionSet = AttributeSet(partitionColumns)
    val dataColumns = outputColumns.filterNot(partitionSet.contains)
    val writerBucketSpec = V1WritesUtils.getWriterBucketSpec(bucketSpec, dataColumns, options)
    val sortColumns = V1WritesUtils.getBucketSortColumns(bucketSpec, dataColumns)

    if (SQLConf.get.maxConcurrentOutputFileWriters > 0 && sortColumns.isEmpty) {
      // Do not insert logical sort when concurrent output writers are enabled.
      Seq.empty
    } else {
      // We should first sort by partition columns, then bucket id, and finally sorting columns.
      // Note we do not need to convert empty string partition columns to null when sorting the
      // columns since null and empty string values will be next to each other.
      (partitionColumns ++writerBucketSpec.map(_.bucketIdExpression) ++ sortColumns)
        .map(SortOrder(_, Ascending))
    }
  }
}
