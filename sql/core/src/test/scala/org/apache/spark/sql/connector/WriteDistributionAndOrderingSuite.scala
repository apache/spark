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

package org.apache.spark.sql.connector


import java.sql.Date
import java.util.Collections

import org.apache.spark.sql.{catalyst, AnalysisException, DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.{ApplyFunctionExpression, Cast, Literal}
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.catalyst.plans.physical
import org.apache.spark.sql.catalyst.plans.physical.{CoalescedBoundary, CoalescedHashPartitioning, HashPartitioning, RangePartitioning, UnknownPartitioning}
import org.apache.spark.sql.connector.catalog.{Column, Identifier}
import org.apache.spark.sql.connector.catalog.functions._
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.connector.expressions.LogicalExpressions._
import org.apache.spark.sql.execution.{QueryExecution, SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AQEShuffleReadExec
import org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.sources.ContinuousMemoryStream
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{StreamingQueryException, Trigger}
import org.apache.spark.sql.types.{DateType, IntegerType, LongType, ObjectType, StringType, TimestampType}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.tags.SlowSQLTest

@SlowSQLTest
class WriteDistributionAndOrderingSuite extends DistributionAndOrderingSuiteBase {
  import testImplicits._

  before {
    Seq(
      UnboundYearsFunction,
      UnboundBucketFunction,
      UnboundStringSelfFunction,
      UnboundTruncateFunction).foreach { f =>
      catalog.createFunction(Identifier.of(Array.empty, f.name()), f)
    }
  }

  after {
    catalog.clearTables()
    catalog.clearFunctions()
    spark.sessionState.catalogManager.reset()
  }

  private val microBatchPrefix = "micro_batch_"
  private val namespace = Array("ns1")
  private val ident = Identifier.of(namespace, "test_table")
  private val tableNameAsString = "testcat." + ident.toString
  private val emptyProps = Collections.emptyMap[String, String]
  private val columns = Array(
    Column.create("id", IntegerType),
    Column.create("data", StringType),
    Column.create("day", DateType))

  test("ordered distribution and sort with same exprs: append") {
    checkOrderedDistributionAndSortWithSameExprsInVariousCases("append")
  }

  test("ordered distribution and sort with same exprs: overwrite") {
    checkOrderedDistributionAndSortWithSameExprsInVariousCases("overwrite")
  }

  test("ordered distribution and sort with same exprs: overwriteDynamic") {
    checkOrderedDistributionAndSortWithSameExprsInVariousCases("overwriteDynamic")
  }

  test("ordered distribution and sort with same exprs: micro-batch append") {
    checkOrderedDistributionAndSortWithSameExprs(microBatchPrefix + "append")
  }

  test("ordered distribution and sort with same exprs: micro-batch update") {
    checkOrderedDistributionAndSortWithSameExprs(microBatchPrefix + "update")
  }

  test("ordered distribution and sort with same exprs: micro-batch complete") {
    checkOrderedDistributionAndSortWithSameExprs(microBatchPrefix + "complete")
  }

  test("ordered distribution and sort with same exprs with numPartitions: append") {
    checkOrderedDistributionAndSortWithSameExprs("append", Some(10))
  }

  test("ordered distribution and sort with same exprs with numPartitions: overwrite") {
    checkOrderedDistributionAndSortWithSameExprs("overwrite", Some(10))
  }

  test("ordered distribution and sort with same exprs with numPartitions: overwriteDynamic") {
    checkOrderedDistributionAndSortWithSameExprs("overwriteDynamic", Some(10))
  }

  test("ordered distribution and sort with same exprs with numPartitions: micro-batch append") {
    checkOrderedDistributionAndSortWithSameExprs(microBatchPrefix + "append", Some(10))
  }

  test("ordered distribution and sort with same exprs with numPartitions: micro-batch update") {
    checkOrderedDistributionAndSortWithSameExprs(microBatchPrefix + "update", Some(10))
  }

  test("ordered distribution and sort with same exprs with numPartitions: micro-batch complete") {
    checkOrderedDistributionAndSortWithSameExprs(microBatchPrefix + "complete", Some(10))
  }

  private def checkOrderedDistributionAndSortWithSameExprsInVariousCases(cmd: String) = {
    Seq(true, false).foreach { distributionStrictlyRequired =>
      Seq(true, false).foreach { dataSkewed =>
        Seq(true, false).foreach { coalesce =>
          partitionSizes(dataSkewed, coalesce).foreach { partitionSize =>
            checkOrderedDistributionAndSortWithSameExprs(
              cmd, None, partitionSize, distributionStrictlyRequired, dataSkewed, coalesce)
          }
        }
      }
    }
  }

  private def checkOrderedDistributionAndSortWithSameExprs(
      command: String,
      targetNumPartitions: Option[Int] = None,
      targetPartitionSize: Option[Long] = None,
      distributionStrictlyRequired: Boolean = true,
      dataSkewed: Boolean = false,
      coalesce: Boolean = false): Unit = {
    val tableOrdering = Array[SortOrder](
      sort(FieldReference("data"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    )
    val tableDistribution = Distributions.ordered(tableOrdering)

    val writeOrdering = Seq(
      catalyst.expressions.SortOrder(
        attr("data"),
        catalyst.expressions.Ascending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      )
    )
    val writePartitioning = if (!coalesce) {
      orderedWritePartitioning(writeOrdering, targetNumPartitions)
    } else {
      orderedWritePartitioning(writeOrdering, Some(1))
    }

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      targetPartitionSize,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeCommand = command,
      distributionStrictlyRequired = distributionStrictlyRequired,
      dataSkewed = dataSkewed,
      coalesce = coalesce)
  }

  test("clustered distribution and sort with same exprs: append") {
    checkClusteredDistributionAndSortWithSameExprsInVariousCases("append")
  }

  test("clustered distribution and sort with same exprs: overwrite") {
    checkClusteredDistributionAndSortWithSameExprsInVariousCases("overwrite")
  }

  test("clustered distribution and sort with same exprs: overwriteDynamic") {
    checkClusteredDistributionAndSortWithSameExprsInVariousCases("overwriteDynamic")
  }

  test("clustered distribution and sort with same exprs: micro-batch append") {
    checkClusteredDistributionAndSortWithSameExprs(microBatchPrefix + "append")
  }

  test("clustered distribution and sort with same exprs: micro-batch update") {
    checkClusteredDistributionAndSortWithSameExprs(microBatchPrefix + "update")
  }

  test("clustered distribution and sort with same exprs: micro-batch complete") {
    checkClusteredDistributionAndSortWithSameExprs(microBatchPrefix + "complete")
  }

  test("clustered distribution and sort with same exprs with numPartitions: append") {
    checkClusteredDistributionAndSortWithSameExprs("append", Some(10))
  }

  test("clustered distribution and sort with same exprs with numPartitions: overwrite") {
    checkClusteredDistributionAndSortWithSameExprs("overwrite", Some(10))
  }

  test("clustered distribution and sort with same exprs with numPartitions: overwriteDynamic") {
    checkClusteredDistributionAndSortWithSameExprs("overwriteDynamic", Some(10))
  }

  test("clustered distribution and sort with same exprs with numPartitions: micro-batch append") {
    checkClusteredDistributionAndSortWithSameExprs(microBatchPrefix + "append", Some(10))
  }

  test("clustered distribution and sort with same exprs with numPartitions: micro-batch update") {
    checkClusteredDistributionAndSortWithSameExprs(microBatchPrefix + "update", Some(10))
  }

  test("clustered distribution and sort with same exprs with numPartitions: micro-batch complete") {
    checkClusteredDistributionAndSortWithSameExprs(microBatchPrefix + "complete", Some(10))
  }

  private def checkClusteredDistributionAndSortWithSameExprsInVariousCases(cmd: String) = {
    Seq(true, false).foreach { distributionStrictlyRequired =>
      Seq(true, false).foreach { dataSkewed =>
        Seq(true, false).foreach { coalesce =>
          partitionSizes(dataSkewed, coalesce).foreach { partitionSize =>
            checkClusteredDistributionAndSortWithSameExprs(
              cmd, None, partitionSize, distributionStrictlyRequired, dataSkewed, coalesce)
          }
        }
      }
    }
  }

  private def checkClusteredDistributionAndSortWithSameExprs(
      command: String,
      targetNumPartitions: Option[Int] = None,
      targetPartitionSize: Option[Long] = None,
      distributionStrictlyRequired: Boolean = true,
      dataSkewed: Boolean = false,
      coalesce: Boolean = false): Unit = {
    val tableOrdering = Array[SortOrder](
      sort(FieldReference("data"), SortDirection.DESCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    )
    val clustering = Array[Expression](FieldReference("data"), FieldReference("id"))
    val tableDistribution = Distributions.clustered(clustering)

    val writeOrdering = Seq(
      catalyst.expressions.SortOrder(
        attr("data"),
        catalyst.expressions.Descending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      ),
      catalyst.expressions.SortOrder(
        attr("id"),
        catalyst.expressions.Ascending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      )
    )
    val writePartitioningExprs = Seq(attr("data"), attr("id"))
    val writePartitioning = clusteredWritePartitioning(
      writePartitioningExprs, targetNumPartitions, coalesce)

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      targetPartitionSize,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeCommand = command,
      distributionStrictlyRequired = distributionStrictlyRequired,
      dataSkewed = dataSkewed,
      coalesce = coalesce)
  }

  test("clustered distribution and sort with extended exprs: append") {
    checkClusteredDistributionAndSortWithExtendedExprsInVariousCases("append")
  }

  test("clustered distribution and sort with extended exprs: overwrite") {
    checkClusteredDistributionAndSortWithExtendedExprsInVariousCases("overwrite")
  }

  test("clustered distribution and sort with extended exprs: overwriteDynamic") {
    checkClusteredDistributionAndSortWithExtendedExprsInVariousCases("overwriteDynamic")
  }

  test("clustered distribution and sort with extended exprs: micro-batch append") {
    checkClusteredDistributionAndSortWithExtendedExprs(microBatchPrefix + "append")
  }

  test("clustered distribution and sort with extended exprs: micro-batch update") {
    checkClusteredDistributionAndSortWithExtendedExprs(microBatchPrefix + "update")
  }

  test("clustered distribution and sort with extended exprs: micro-batch complete") {
    checkClusteredDistributionAndSortWithExtendedExprs(microBatchPrefix + "complete")
  }

  test("clustered distribution and sort with extended exprs with numPartitions: append") {
    checkClusteredDistributionAndSortWithExtendedExprs("append", Some(10))
  }

  test("clustered distribution and sort with extended exprs with numPartitions: overwrite") {
    checkClusteredDistributionAndSortWithExtendedExprs("overwrite", Some(10))
  }

  test("clustered distribution and sort with extended exprs with numPartitions: " +
    "overwriteDynamic") {
    checkClusteredDistributionAndSortWithExtendedExprs("overwriteDynamic", Some(10))
  }

  test("clustered distribution and sort with extended exprs with numPartitions: " +
    "micro-batch append") {
    checkClusteredDistributionAndSortWithExtendedExprs(microBatchPrefix + "append", Some(10))
  }

  test("clustered distribution and sort with extended exprs with numPartitions: " +
    "micro-batch update") {
    checkClusteredDistributionAndSortWithExtendedExprs(microBatchPrefix + "update", Some(10))
  }

  test("clustered distribution and sort with extended exprs with numPartitions: " +
    "micro-batch complete") {
    checkClusteredDistributionAndSortWithExtendedExprs(microBatchPrefix + "complete", Some(10))
  }

  private def checkClusteredDistributionAndSortWithExtendedExprsInVariousCases(cmd: String) = {
    Seq(true, false).foreach { distributionStrictlyRequired =>
      Seq(true, false).foreach { dataSkewed =>
        Seq(true, false).foreach { coalesce =>
          partitionSizes(dataSkewed, coalesce).foreach { partitionSize =>
            checkClusteredDistributionAndSortWithExtendedExprs(
              cmd, None, partitionSize, distributionStrictlyRequired, dataSkewed, coalesce)
          }
        }
      }
    }
  }

  private def checkClusteredDistributionAndSortWithExtendedExprs(
      command: String,
      targetNumPartitions: Option[Int] = None,
      targetPartitionSize: Option[Long] = None,
      distributionStrictlyRequired: Boolean = true,
      dataSkewed: Boolean = false,
      coalesce: Boolean = false): Unit = {
    val tableOrdering = Array[SortOrder](
      sort(FieldReference("data"), SortDirection.DESCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    )
    val clustering = Array[Expression](FieldReference("data"))
    val tableDistribution = Distributions.clustered(clustering)

    val writeOrdering = Seq(
      catalyst.expressions.SortOrder(
        attr("data"),
        catalyst.expressions.Descending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      ),
      catalyst.expressions.SortOrder(
        attr("id"),
        catalyst.expressions.Ascending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      )
    )
    val writePartitioningExprs = Seq(attr("data"))
    val writePartitioning = clusteredWritePartitioning(
      writePartitioningExprs, targetNumPartitions, coalesce)

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      targetPartitionSize,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeCommand = command,
      distributionStrictlyRequired = distributionStrictlyRequired,
      dataSkewed = dataSkewed,
      coalesce = coalesce)
  }

  test("unspecified distribution and local sort: append") {
    checkUnspecifiedDistributionAndLocalSort("append")
  }

  test("unspecified distribution and local sort: overwrite") {
    checkUnspecifiedDistributionAndLocalSort("overwrite")
  }

  test("unspecified distribution and local sort: overwriteDynamic") {
    checkUnspecifiedDistributionAndLocalSort("overwriteDynamic")
  }

  test("unspecified distribution and local sort: micro-batch append") {
    checkUnspecifiedDistributionAndLocalSort(microBatchPrefix + "append")
  }

  test("unspecified distribution and local sort: micro-batch update") {
    checkUnspecifiedDistributionAndLocalSort(microBatchPrefix + "update")
  }

  test("unspecified distribution and local sort: micro-batch complete") {
    checkUnspecifiedDistributionAndLocalSort(microBatchPrefix + "complete")
  }

  test("unspecified distribution and local sort with numPartitions: append") {
    checkUnspecifiedDistributionAndLocalSort("append", Some(10))
  }

  test("unspecified distribution and local sort with numPartitions: overwrite") {
    checkUnspecifiedDistributionAndLocalSort("overwrite", Some(10))
  }

  test("unspecified distribution and local sort with numPartitions: overwriteDynamic") {
    checkUnspecifiedDistributionAndLocalSort("overwriteDynamic", Some(10))
  }

  test("unspecified distribution and local sort with numPartitions: micro-batch append") {
    checkUnspecifiedDistributionAndLocalSort(microBatchPrefix + "append", Some(10))
  }

  test("unspecified distribution and local sort with numPartitions: micro-batch update") {
    checkUnspecifiedDistributionAndLocalSort(microBatchPrefix + "update", Some(10))
  }

  test("unspecified distribution and local sort with numPartitions: micro-batch complete") {
    checkUnspecifiedDistributionAndLocalSort(microBatchPrefix + "complete", Some(10))
  }

  private def checkUnspecifiedDistributionAndLocalSort(command: String): Unit = {
    checkUnspecifiedDistributionAndLocalSort(command, None)
  }

  private def checkUnspecifiedDistributionAndLocalSort(
      command: String,
      targetNumPartitions: Option[Int]): Unit = {
    val tableOrdering = Array[SortOrder](
      sort(FieldReference("data"), SortDirection.DESCENDING, NullOrdering.NULLS_FIRST)
    )
    val tableDistribution = Distributions.unspecified()

    val writeOrdering = Seq(
      catalyst.expressions.SortOrder(
        attr("data"),
        catalyst.expressions.Descending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      )
    )

    val writePartitioning = UnknownPartitioning(0)

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeCommand = command,
      // if the number of partitions is specified, we expect query to fail
      expectAnalysisException = targetNumPartitions.isDefined)
  }

  test("unspecified distribution and no sort: append") {
    checkUnspecifiedDistributionAndNoSort("append")
  }

  test("unspecified distribution and no sort: overwrite") {
    checkUnspecifiedDistributionAndNoSort("overwrite")
  }

  test("unspecified distribution and no sort: overwriteDynamic") {
    checkUnspecifiedDistributionAndNoSort("overwriteDynamic")
  }

  test("unspecified distribution and no sort: micro-batch append") {
    checkUnspecifiedDistributionAndNoSort(microBatchPrefix + "append")
  }

  test("unspecified distribution and no sort: micro-batch update") {
    checkUnspecifiedDistributionAndNoSort(microBatchPrefix + "update")
  }

  test("unspecified distribution and no sort: micro-batch complete") {
    checkUnspecifiedDistributionAndNoSort(microBatchPrefix + "complete")
  }

  test("unspecified distribution and no sort with numPartitions: append") {
    checkUnspecifiedDistributionAndNoSort("append", Some(10))
  }

  test("unspecified distribution and no sort with numPartitions: overwrite") {
    checkUnspecifiedDistributionAndNoSort("overwrite", Some(10))
  }

  test("unspecified distribution and no sort with numPartitions: overwriteDynamic") {
    checkUnspecifiedDistributionAndNoSort("overwriteDynamic", Some(10))
  }

  test("unspecified distribution and no sort with numPartitions: micro-batch append") {
    checkUnspecifiedDistributionAndNoSort(microBatchPrefix + "append", Some(10))
  }

  test("unspecified distribution and no sort with numPartitions: micro-batch update") {
    checkUnspecifiedDistributionAndNoSort(microBatchPrefix + "update", Some(10))
  }

  test("unspecified distribution and no sort with numPartitions: micro-batch complete") {
    checkUnspecifiedDistributionAndNoSort(microBatchPrefix + "complete", Some(10))
  }

  private def checkUnspecifiedDistributionAndNoSort(command: String): Unit = {
    checkUnspecifiedDistributionAndNoSort(command, None)
  }

  private def checkUnspecifiedDistributionAndNoSort(
      command: String,
      targetNumPartitions: Option[Int]): Unit = {
    val tableOrdering = Array.empty[SortOrder]
    val tableDistribution = Distributions.unspecified()

    val writeOrdering = Seq.empty[catalyst.expressions.SortOrder]
    val writePartitioning = UnknownPartitioning(0)

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeCommand = command,
      // if the number of partitions is specified, we expect query to fail
      expectAnalysisException = targetNumPartitions.isDefined)
  }

  test("ordered distribution and sort with manual global sort: append") {
    checkOrderedDistributionAndSortWithManualGlobalSortInVariousCases("append")
  }

  test("ordered distribution and sort with manual global sort: overwrite") {
    checkOrderedDistributionAndSortWithManualGlobalSortInVariousCases("overwrite")
  }

  test("ordered distribution and sort with manual global sort: overwriteDynamic") {
    checkOrderedDistributionAndSortWithManualGlobalSortInVariousCases("overwriteDynamic")
  }

  test("ordered distribution and sort with manual global sort with numPartitions: append") {
    checkOrderedDistributionAndSortWithManualGlobalSort("append", Some(10))
  }

  test("ordered distribution and sort with manual global sort with numPartitions: overwrite") {
    checkOrderedDistributionAndSortWithManualGlobalSort("overwrite", Some(10))
  }

  test("ordered distribution and sort with manual global sort with numPartitions: " +
    "overwriteDynamic") {
    checkOrderedDistributionAndSortWithManualGlobalSort("overwriteDynamic", Some(10))
  }

  private def checkOrderedDistributionAndSortWithManualGlobalSortInVariousCases(cmd: String) = {
    Seq(true, false).foreach { distributionStrictlyRequired =>
      Seq(true, false).foreach { dataSkewed =>
        Seq(true, false).foreach { coalesce =>
          partitionSizes(dataSkewed, coalesce).foreach { partitionSize =>
            checkOrderedDistributionAndSortWithManualGlobalSort(
              cmd, None, partitionSize, distributionStrictlyRequired, dataSkewed, coalesce)
          }
        }
      }
    }
  }

  private def checkOrderedDistributionAndSortWithManualGlobalSort(
      command: String,
      targetNumPartitions: Option[Int] = None,
      targetPartitionSize: Option[Long] = None,
      distributionStrictlyRequired: Boolean = true,
      dataSkewed: Boolean = false,
      coalesce: Boolean = false): Unit = {
    val tableOrdering = Array[SortOrder](
      sort(FieldReference("data"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    )
    val tableDistribution = Distributions.ordered(tableOrdering)

    val writeOrdering = Seq(
      catalyst.expressions.SortOrder(
        attr("data"),
        catalyst.expressions.Ascending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      ),
      catalyst.expressions.SortOrder(
        attr("id"),
        catalyst.expressions.Ascending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      )
    )
    val writePartitioning = if (!coalesce) {
      orderedWritePartitioning(writeOrdering, targetNumPartitions)
    } else {
      orderedWritePartitioning(writeOrdering, Some(1))
    }

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      targetPartitionSize,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeTransform = df => df.orderBy("data", "id"),
      writeCommand = command,
      distributionStrictlyRequired = distributionStrictlyRequired,
      dataSkewed = dataSkewed,
      coalesce = coalesce)
  }

  test("ordered distribution and sort with incompatible global sort: append") {
    checkOrderedDistributionAndSortWithIncompatibleGlobalSortInVariousCases("append")
  }

  test("ordered distribution and sort with incompatible global sort: overwrite") {
    checkOrderedDistributionAndSortWithIncompatibleGlobalSortInVariousCases("overwrite")
  }

  test("ordered distribution and sort with incompatible global sort: overwriteDynamic") {
    checkOrderedDistributionAndSortWithIncompatibleGlobalSortInVariousCases("overwriteDynamic")
  }

  test("ordered distribution and sort with incompatible global sort with numPartitions: append") {
    checkOrderedDistributionAndSortWithIncompatibleGlobalSort("append", Some(10))
  }

  test("ordered distribution and sort with incompatible global sort with numPartitions: " +
    "overwrite") {
    checkOrderedDistributionAndSortWithIncompatibleGlobalSort("overwrite", Some(10))
  }

  test("ordered distribution and sort with incompatible global sort with numPartitions: " +
    "overwriteDynamic") {
    checkOrderedDistributionAndSortWithIncompatibleGlobalSort("overwriteDynamic", Some(10))
  }

  private def checkOrderedDistributionAndSortWithIncompatibleGlobalSortInVariousCases(
      cmd: String) = {
    Seq(true, false).foreach { distributionStrictlyRequired =>
      Seq(true, false).foreach { dataSkewed =>
        Seq(true, false).foreach { coalesce =>
          partitionSizes(dataSkewed, coalesce).foreach { partitionSize =>
            checkOrderedDistributionAndSortWithIncompatibleGlobalSort(
              cmd, None, partitionSize, distributionStrictlyRequired, dataSkewed, coalesce)
          }
        }
      }
    }
  }

  private def checkOrderedDistributionAndSortWithIncompatibleGlobalSort(
      command: String,
      targetNumPartitions: Option[Int] = None,
      targetPartitionSize: Option[Long] = None,
      distributionStrictlyRequired: Boolean = true,
      dataSkewed: Boolean = false,
      coalesce: Boolean = false): Unit = {
    val tableOrdering = Array[SortOrder](
      sort(FieldReference("data"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    )
    val tableDistribution = Distributions.ordered(tableOrdering)

    val writeOrdering = Seq(
      catalyst.expressions.SortOrder(
        attr("data"),
        catalyst.expressions.Ascending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      ),
      catalyst.expressions.SortOrder(
        attr("id"),
        catalyst.expressions.Ascending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      )
    )
    val writePartitioning = if (!coalesce) {
      orderedWritePartitioning(writeOrdering, targetNumPartitions)
    } else {
      orderedWritePartitioning(writeOrdering, Some(1))
    }

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      targetPartitionSize,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeTransform = df => df.orderBy(df("data").desc, df("id").asc),
      writeCommand = command,
      distributionStrictlyRequired = distributionStrictlyRequired,
      dataSkewed = dataSkewed,
      coalesce = coalesce)
  }

  test("ordered distribution and sort with manual local sort: append") {
    checkOrderedDistributionAndSortWithManualLocalSortInVariousCases("append")
  }

  test("ordered distribution and sort with manual local sort: overwrite") {
    checkOrderedDistributionAndSortWithManualLocalSortInVariousCases("overwrite")
  }

  test("ordered distribution and sort with manual local sort: overwriteDynamic") {
    checkOrderedDistributionAndSortWithManualLocalSortInVariousCases("overwriteDynamic")
  }

  test("ordered distribution and sort with manual local sort with numPartitions: append") {
    checkOrderedDistributionAndSortWithManualLocalSort("append", Some(10))
  }

  test("ordered distribution and sort with manual local sort with numPartitions: overwrite") {
    checkOrderedDistributionAndSortWithManualLocalSort("overwrite", Some(10))
  }

  test("ordered distribution and sort with manual local sort with numPartitions: " +
    "overwriteDynamic") {
    checkOrderedDistributionAndSortWithManualLocalSort("overwriteDynamic", Some(10))
  }

  private def checkOrderedDistributionAndSortWithManualLocalSortInVariousCases(cmd: String) = {
    Seq(true, false).foreach { distributionStrictlyRequired =>
      Seq(true, false).foreach { dataSkewed =>
        Seq(true, false).foreach { coalesce =>
          partitionSizes(dataSkewed, coalesce).foreach { partitionSize =>
            checkOrderedDistributionAndSortWithManualLocalSort(
              cmd, None, partitionSize, distributionStrictlyRequired, dataSkewed, coalesce)
          }
        }
      }
    }
  }

  private def checkOrderedDistributionAndSortWithManualLocalSort(
      command: String,
      targetNumPartitions: Option[Int] = None,
      targetPartitionSize: Option[Long] = None,
      distributionStrictlyRequired: Boolean = true,
      dataSkewed: Boolean = false,
      coalesce: Boolean = false): Unit = {
    val tableOrdering = Array[SortOrder](
      sort(FieldReference("data"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    )
    val tableDistribution = Distributions.ordered(tableOrdering)

    val writeOrdering = Seq(
      catalyst.expressions.SortOrder(
        attr("data"),
        catalyst.expressions.Ascending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      ),
      catalyst.expressions.SortOrder(
        attr("id"),
        catalyst.expressions.Ascending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      )
    )
    val writePartitioning = if (!coalesce) {
      orderedWritePartitioning(writeOrdering, targetNumPartitions)
    } else {
      orderedWritePartitioning(writeOrdering, Some(1))
    }

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      targetPartitionSize,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeTransform = df => df.sortWithinPartitions("data", "id"),
      writeCommand = command,
      distributionStrictlyRequired = distributionStrictlyRequired,
      dataSkewed = dataSkewed,
      coalesce = coalesce)
  }

  test("clustered distribution and local sort with manual global sort: append") {
    checkClusteredDistributionAndLocalSortWithManualGlobalSortInVariousCases("append")
  }

  test("clustered distribution and local sort with manual global sort: overwrite") {
    checkClusteredDistributionAndLocalSortWithManualGlobalSortInVariousCases("overwrite")
  }

  test("clustered distribution and local sort with manual global sort: overwriteDynamic") {
    checkClusteredDistributionAndLocalSortWithManualGlobalSortInVariousCases("overwriteDynamic")
  }

  test("clustered distribution and local sort with manual global sort with numPartitions: append") {
    checkClusteredDistributionAndLocalSortWithManualGlobalSort("append", Some(10))
  }

  test("clustered distribution and local sort with manual global sort with numPartitions: " +
    "overwrite") {
    checkClusteredDistributionAndLocalSortWithManualGlobalSort("overwrite", Some(10))
  }

  test("clustered distribution and local sort with manual global sort with numPartitions: " +
    "overwriteDynamic") {
    checkClusteredDistributionAndLocalSortWithManualGlobalSort("overwriteDynamic", Some(10))
  }

  private def checkClusteredDistributionAndLocalSortWithManualGlobalSortInVariousCases(
      cmd: String) = {
    Seq(true, false).foreach { distributionStrictlyRequired =>
      Seq(true, false).foreach { dataSkewed =>
        Seq(true, false).foreach { coalesce =>
          partitionSizes(dataSkewed, coalesce).foreach { partitionSize =>
            checkClusteredDistributionAndLocalSortWithManualGlobalSort(
              cmd, None, partitionSize, distributionStrictlyRequired, dataSkewed, coalesce)
          }
        }
      }
    }
  }

  private def checkClusteredDistributionAndLocalSortWithManualGlobalSort(
      command: String,
      targetNumPartitions: Option[Int] = None,
      targetPartitionSize: Option[Long] = None,
      distributionStrictlyRequired: Boolean = true,
      dataSkewed: Boolean = false,
      coalesce: Boolean = false): Unit = {
    val tableOrdering = Array[SortOrder](
      sort(FieldReference("data"), SortDirection.DESCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    )
    val tableDistribution = Distributions.clustered(Array(FieldReference("data")))

    val writeOrdering = Seq(
      catalyst.expressions.SortOrder(
        attr("data"),
        catalyst.expressions.Descending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      ),
      catalyst.expressions.SortOrder(
        attr("id"),
        catalyst.expressions.Ascending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      )
    )
    val writePartitioningExprs = Seq(attr("data"))
    val writePartitioning = clusteredWritePartitioning(
      writePartitioningExprs, targetNumPartitions, coalesce)

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      targetPartitionSize,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeTransform = df => df.orderBy("data", "id"),
      writeCommand = command,
      distributionStrictlyRequired = distributionStrictlyRequired,
      dataSkewed = dataSkewed,
      coalesce = coalesce)
  }

  test("clustered distribution and local sort with manual local sort: append") {
    checkClusteredDistributionAndLocalSortWithManualLocalSortInVariousCases("append")
  }

  test("clustered distribution and local sort with manual local sort: overwrite") {
    checkClusteredDistributionAndLocalSortWithManualLocalSortInVariousCases("overwrite")
  }

  test("clustered distribution and local sort with manual local sort: overwriteDynamic") {
    checkClusteredDistributionAndLocalSortWithManualLocalSortInVariousCases("overwriteDynamic")
  }

  test("clustered distribution and local sort with manual local sort with numPartitions: append") {
    checkClusteredDistributionAndLocalSortWithManualLocalSort("append", Some(10))
  }

  test("clustered distribution and local sort with manual local sort with numPartitions: " +
    "overwrite") {
    checkClusteredDistributionAndLocalSortWithManualLocalSort("overwrite", Some(10))
  }

  test("clustered distribution and local sort with manual local sort with numPartitions: " +
    "overwriteDynamic") {
    checkClusteredDistributionAndLocalSortWithManualLocalSort("overwriteDynamic", Some(10))
  }

  private def checkClusteredDistributionAndLocalSortWithManualLocalSortInVariousCases(
      cmd: String) = {
    Seq(true, false).foreach { distributionStrictlyRequired =>
      Seq(true, false).foreach { dataSkewed =>
        Seq(true, false).foreach { coalesce =>
          partitionSizes(dataSkewed, coalesce).foreach { partitionSize =>
            checkClusteredDistributionAndLocalSortWithManualLocalSort(
              cmd, None, partitionSize, distributionStrictlyRequired, dataSkewed, coalesce)
          }
        }
      }
    }
  }

  private def checkClusteredDistributionAndLocalSortWithManualLocalSort(
      command: String,
      targetNumPartitions: Option[Int] = None,
      targetPartitionSize: Option[Long] = None,
      distributionStrictlyRequired: Boolean = true,
      dataSkewed: Boolean = false,
      coalesce: Boolean = false): Unit = {
    val tableOrdering = Array[SortOrder](
      sort(FieldReference("data"), SortDirection.DESCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    )
    val tableDistribution = Distributions.clustered(Array(FieldReference("data")))

    val writeOrdering = Seq(
      catalyst.expressions.SortOrder(
        attr("data"),
        catalyst.expressions.Descending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      ),
      catalyst.expressions.SortOrder(
        attr("id"),
        catalyst.expressions.Ascending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      )
    )
    val writePartitioningExprs = Seq(attr("data"))
    val writePartitioning = clusteredWritePartitioning(
      writePartitioningExprs, targetNumPartitions, coalesce)

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      targetPartitionSize,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeTransform = df => df.sortWithinPartitions("data", "id"),
      writeCommand = command,
      distributionStrictlyRequired = distributionStrictlyRequired,
      dataSkewed = dataSkewed,
      coalesce = coalesce)
  }

  test("continuous mode does not support write distribution and ordering") {
    val ordering = Array[SortOrder](
      sort(FieldReference("data"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    )
    val distribution = Distributions.ordered(ordering)

    catalog.createTable(ident, columns, Array.empty, emptyProps, distribution, ordering, None, None)

    withTempDir { checkpointDir =>
      val inputData = ContinuousMemoryStream[(Long, String, Date)]
      val inputDF = inputData.toDF().toDF("id", "data", "day")

      val writer = inputDF
        .writeStream
        .trigger(Trigger.Continuous(100))
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .outputMode("append")

      val analysisException = intercept[AnalysisException] {
        val query = writer.toTable(tableNameAsString)

        inputData.addData(
          (1, "a", Date.valueOf("2021-01-01")),
          (2, "b", Date.valueOf("2022-02-02")))

        query.processAllAvailable()
        query.stop()
      }

      assert(analysisException.message.contains("Sinks cannot request distribution and ordering"))
    }
  }

  test("continuous mode allows unspecified distribution and empty ordering") {
    catalog.createTable(ident, columns, Array.empty[Transform], emptyProps)

    withTempDir { checkpointDir =>
      val inputData = ContinuousMemoryStream[(Long, String, Date)]
      val inputDF = inputData.toDF().toDF("id", "data", "day")

      val writer = inputDF
        .writeStream
        .trigger(Trigger.Continuous(100))
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .outputMode("append")

      val query = writer.toTable(tableNameAsString)

      inputData.addData(
        (1, "a", Date.valueOf("2021-01-01")),
        (2, "b", Date.valueOf("2022-02-02")))

      query.processAllAvailable()
      query.stop()

      checkAnswer(
        spark.table(tableNameAsString),
        Row(1, "a", Date.valueOf("2021-01-01")) ::
        Row(2, "b", Date.valueOf("2022-02-02")) :: Nil)
    }
  }

  test("clustered distribution and local sort contains v2 function: append") {
    checkClusteredDistributionAndLocalSortContainsV2FunctionInVariousCases("append")
  }

  test("clustered distribution and local sort contains v2 function: overwrite") {
    checkClusteredDistributionAndLocalSortContainsV2FunctionInVariousCases("overwrite")
  }

  test("clustered distribution and local sort contains v2 function: overwriteDynamic") {
    checkClusteredDistributionAndLocalSortContainsV2FunctionInVariousCases("overwriteDynamic")
  }

  test("clustered distribution and local sort contains v2 function with numPartitions: append") {
    checkClusteredDistributionAndLocalSortContainsV2Function("append", Some(10))
  }

  test("clustered distribution and local sort contains v2 function with numPartitions: " +
    "overwrite") {
    checkClusteredDistributionAndLocalSortContainsV2Function("overwrite", Some(10))
  }

  test("clustered distribution and local sort contains v2 function with numPartitions: " +
    "overwriteDynamic") {
    checkClusteredDistributionAndLocalSortContainsV2Function("overwriteDynamic", Some(10))
  }

  private def checkClusteredDistributionAndLocalSortContainsV2FunctionInVariousCases(
    cmd: String): Unit = {
    Seq(true, false).foreach { distributionStrictlyRequired =>
      Seq(true, false).foreach { dataSkewed =>
        Seq(true, false).foreach { coalesce =>
          partitionSizes(dataSkewed, coalesce).foreach { partitionSize =>
            checkClusteredDistributionAndLocalSortContainsV2Function(
              cmd, None, partitionSize, distributionStrictlyRequired, dataSkewed, coalesce)
          }
        }
      }
    }
  }

  private def checkClusteredDistributionAndLocalSortContainsV2Function(
      command: String,
      targetNumPartitions: Option[Int] = None,
      targetPartitionSize: Option[Long] = None,
      distributionStrictlyRequired: Boolean = true,
      dataSkewed: Boolean = false,
      coalesce: Boolean = false): Unit = {

    val stringSelfTransform = ApplyTransform(
      "string_self",
      Seq(FieldReference("data")))
    val truncateTransform = ApplyTransform(
      "truncate",
      Seq(stringSelfTransform, LiteralValue(2, IntegerType)))
    val yearsTransform = ApplyTransform(
      "years",
      Seq(FieldReference("day")))

    val tableOrdering = Array[SortOrder](
      sort(
        stringSelfTransform,
        SortDirection.DESCENDING,
        NullOrdering.NULLS_FIRST),
      sort(
        BucketTransform(LiteralValue(10, IntegerType), Seq(FieldReference("id"))),
        SortDirection.DESCENDING,
        NullOrdering.NULLS_FIRST),
      sort(
        yearsTransform,
        SortDirection.DESCENDING,
        NullOrdering.NULLS_FIRST)
    )
    val tableDistribution = Distributions.clustered(Array(truncateTransform))

    val stringSelfExpr = ApplyFunctionExpression(
      StringSelfFunction,
      Seq(attr("data")))
    val truncateExpr = ApplyFunctionExpression(
      TruncateFunction,
      Seq(stringSelfExpr, Literal(2)))

    val writeOrdering = Seq(
      catalyst.expressions.SortOrder(
        stringSelfExpr,
        catalyst.expressions.Descending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      ),
      catalyst.expressions.SortOrder(
        ApplyFunctionExpression(BucketFunction, Seq(Literal(10), Cast(attr("id"), LongType))),
        catalyst.expressions.Descending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      ),
      catalyst.expressions.SortOrder(
        Invoke(
          Literal.create(YearsFunction, ObjectType(YearsFunction.getClass)),
          "invoke",
          LongType,
          Seq(Cast(attr("day"), TimestampType, Some("America/Los_Angeles"))),
          Seq(TimestampType),
          propagateNull = false),
        catalyst.expressions.Descending,
        catalyst.expressions.NullsFirst,
        Seq.empty
      )
    )

    val writePartitioningExprs = Seq(truncateExpr)
    val writePartitioning = clusteredWritePartitioning(
      writePartitioningExprs, targetNumPartitions, coalesce)

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      targetPartitionSize,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeCommand = command,
      distributionStrictlyRequired = distributionStrictlyRequired,
      dataSkewed = dataSkewed,
      coalesce = coalesce)
  }

  // scalastyle:off argcount
  private def checkWriteRequirements(
      tableDistribution: Distribution,
      tableOrdering: Array[SortOrder],
      tableNumPartitions: Option[Int],
      tablePartitionSize: Option[Long] = None,
      expectedWritePartitioning: physical.Partitioning,
      expectedWriteOrdering: Seq[catalyst.expressions.SortOrder],
      writeTransform: DataFrame => DataFrame = df => df,
      writeCommand: String,
      expectAnalysisException: Boolean = false,
      distributionStrictlyRequired: Boolean = true,
      dataSkewed: Boolean = false,
      coalesce: Boolean = false): Unit = {
    // scalastyle:on argcount
    if (writeCommand.startsWith(microBatchPrefix)) {
      checkMicroBatchWriteRequirements(
        tableDistribution,
        tableOrdering,
        tableNumPartitions,
        tablePartitionSize,
        expectedWritePartitioning,
        expectedWriteOrdering,
        writeTransform,
        outputMode = writeCommand.stripPrefix(microBatchPrefix),
        expectAnalysisException)
    } else {
      checkBatchWriteRequirements(
        tableDistribution,
        tableOrdering,
        tableNumPartitions,
        tablePartitionSize,
        expectedWritePartitioning,
        expectedWriteOrdering,
        writeTransform,
        writeCommand,
        expectAnalysisException,
        distributionStrictlyRequired,
        dataSkewed,
        coalesce)
    }
  }

  // scalastyle:off argcount
  private def checkBatchWriteRequirements(
      tableDistribution: Distribution,
      tableOrdering: Array[SortOrder],
      tableNumPartitions: Option[Int],
      tablePartitionSize: Option[Long],
      expectedWritePartitioning: physical.Partitioning,
      expectedWriteOrdering: Seq[catalyst.expressions.SortOrder],
      writeTransform: DataFrame => DataFrame = df => df,
      writeCommand: String = "append",
      expectAnalysisException: Boolean = false,
      distributionStrictlyRequired: Boolean = true,
      dataSkewed: Boolean = false,
      coalesce: Boolean = false): Unit = {
    // scalastyle:on argcount

    catalog.createTable(ident, columns, Array.empty, emptyProps, tableDistribution,
      tableOrdering, tableNumPartitions, tablePartitionSize, distributionStrictlyRequired)

    val df = if (!dataSkewed) {
      spark.createDataFrame(Seq(
        (1, "a", Date.valueOf("2021-01-01")),
        (2, "b", Date.valueOf("2022-02-02")),
        (3, "c", Date.valueOf("2023-03-03")))
      ).toDF("id", "data", "day")
    } else {
      spark.sparkContext.parallelize(
        (1 to 10).map {
          i => (if (i > 4) 5 else i, i.toString, Date.valueOf(s"${2020 + i}-$i-$i"))
        }, 3)
        .toDF("id", "data", "day")
    }
    val writer = writeTransform(df).writeTo(tableNameAsString)

    def executeCommand(): SparkPlan = writeCommand match {
      case "append" => execute(writer.append())
      case "overwrite" => execute(writer.overwrite(lit(true)))
      case "overwriteDynamic" => execute(writer.overwritePartitions())
    }

    if (expectAnalysisException) {
      intercept[AnalysisException] {
        executeCommand()
      }
    } else {
      if (coalesce) {
        // if the partition size is configured for the table, set the SQL conf to something small
        // so that the overriding behavior is tested
        val defaultAdvisoryPartitionSize = if (tablePartitionSize.isDefined) "15" else "32MB"
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
          SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.SHUFFLE_PARTITIONS.key -> "5",
          SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> defaultAdvisoryPartitionSize,
          SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1") {

          val executedPlan = executeCommand()
          val read = collect(executedPlan) {
            case r: AQEShuffleReadExec => r
          }
          assert(read.size == 1)
          assert(read.head.partitionSpecs.size == 1)
          checkPartitioningAndOrdering(
            // num of partition in expectedWritePartitioning is 1
            executedPlan, expectedWritePartitioning, expectedWriteOrdering, 1)
        }
      } else {
        // if the partition size is configured for the table, set the SQL conf to something big
        // so that the overriding behavior is tested
        val defaultAdvisoryPartitionSize = if (tablePartitionSize.isDefined) "64MB" else "100"
        if (dataSkewed && !distributionStrictlyRequired) {
          withSQLConf(
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
            SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
            SQLConf.ADAPTIVE_OPTIMIZE_SKEWS_IN_REBALANCE_PARTITIONS_ENABLED.key -> "true",
            SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
            SQLConf.SHUFFLE_PARTITIONS.key -> "5",
            SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> defaultAdvisoryPartitionSize,
            SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1") {
            val executedPlan = executeCommand()
            val read = collect(executedPlan) {
              case r: AQEShuffleReadExec => r
            }
            assert(read.size == 1)
            // skew data: 144, 88, 88, 144, 80
            // after repartition: 72, 72, 88, 88, 72, 72, 80
            assert(read.head.partitionSpecs.size >= 7)

            checkPartitioningAndOrdering(
              executedPlan, expectedWritePartitioning, expectedWriteOrdering, 1, true)
          }
        } else {
          withSQLConf(
            SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false") {
            val executedPlan = executeCommand()
            checkPartitioningAndOrdering(
              executedPlan, expectedWritePartitioning, expectedWriteOrdering, 1)
          }
        }
      }

      checkAnswer(spark.table(tableNameAsString), df)
    }
    catalog.dropTable(ident)
  }

  private def checkMicroBatchWriteRequirements(
      tableDistribution: Distribution,
      tableOrdering: Array[SortOrder],
      tableNumPartitions: Option[Int],
      tablePartitionSize: Option[Long],
      expectedWritePartitioning: physical.Partitioning,
      expectedWriteOrdering: Seq[catalyst.expressions.SortOrder],
      writeTransform: DataFrame => DataFrame = df => df,
      outputMode: String = "append",
      expectAnalysisException: Boolean = false): Unit = {

    catalog.createTable(ident, columns, Array.empty, emptyProps, tableDistribution,
      tableOrdering, tableNumPartitions, tablePartitionSize)

    withTempDir { checkpointDir =>
      val inputData = MemoryStream[(Long, String, Date)]
      val inputDF = inputData.toDF().toDF("id", "data", "day")

      val queryDF = outputMode match {
        case "append" | "update" =>
          inputDF
        case "complete" =>
          // add an aggregate for complete mode
          inputDF
            .groupBy("id")
            .agg(Map("data" -> "count", "day" -> "max"))
            .select(
              $"id",
              $"count(data)".cast("string").as("data"),
              $"max(day)".cast("date").as("day"))
      }

      val writer = writeTransform(queryDF)
        .writeStream
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .outputMode(outputMode)

      def executeCommand(): SparkPlan = execute {
        val query = writer.toTable(tableNameAsString)

        inputData.addData(
          (1, "a", Date.valueOf("2021-01-01")),
          (2, "b", Date.valueOf("2022-02-02")))

        query.processAllAvailable()
        query.stop()
      }

      if (expectAnalysisException) {
        val streamingQueryException = intercept[StreamingQueryException] {
          executeCommand()
        }
        val cause = streamingQueryException.cause
        assert(cause.getMessage.contains("number of partitions can't be specified"))

      } else {
        val executedPlan = executeCommand()

        checkPartitioningAndOrdering(
          executedPlan,
          expectedWritePartitioning,
          expectedWriteOrdering,
          // there is an extra shuffle for groupBy in complete mode
          maxNumShuffles = if (outputMode != "complete") 1 else 2)

        val expectedRows = outputMode match {
          case "append" | "update" =>
            Row(1, "a", Date.valueOf("2021-01-01")) ::
            Row(2, "b", Date.valueOf("2022-02-02")) :: Nil
          case "complete" =>
            Row(1, "1", Date.valueOf("2021-01-01")) ::
            Row(2, "1", Date.valueOf("2022-02-02")) :: Nil
        }
        checkAnswer(spark.table(tableNameAsString), expectedRows)
      }
    }
  }

  private def checkPartitioningAndOrdering(
      plan: SparkPlan,
      partitioning: physical.Partitioning,
      ordering: Seq[catalyst.expressions.SortOrder],
      maxNumShuffles: Int = 1,
      skewSplit: Boolean = false): Unit = {

    val sorts = collect(plan) { case s: SortExec => s }
    assert(sorts.size <= 1, "must be at most one sort")
    val shuffles = collect(plan) { case s: ShuffleExchangeLike => s }
    assert(shuffles.size <= maxNumShuffles, $"must be at most $maxNumShuffles shuffles")

    val actualPartitioning = plan.outputPartitioning
    val expectedPartitioning = partitioning match {
      case p: physical.RangePartitioning =>
        val resolvedOrdering = p.ordering.map(resolveAttrs(_, plan))
        p.copy(ordering = resolvedOrdering.asInstanceOf[Seq[catalyst.expressions.SortOrder]])
      case p: physical.HashPartitioning =>
        val resolvedExprs = p.expressions.map(resolveAttrs(_, plan))
        p.copy(expressions = resolvedExprs)
      case c: physical.CoalescedHashPartitioning =>
        val resolvedExprs = c.from.expressions.map(resolveAttrs(_, plan))
        c.copy(from = c.from.copy(expressions = resolvedExprs))
      case _: UnknownPartitioning =>
        // don't check partitioning if no particular one is expected
        actualPartitioning
      case other => other
    }

    if (skewSplit) {
      assert(actualPartitioning.numPartitions > conf.numShufflePartitions)
    } else {
      (actualPartitioning, expectedPartitioning) match {
        case (actual: catalyst.expressions.Expression, expected: catalyst.expressions.Expression) =>
          assert(actual semanticEquals expected, "partitioning must match")
        case (actual, expected) =>
          assert(actual == expected, "partitioning must match")
      }
    }

    val actualOrdering = plan.outputOrdering
    val expectedOrdering = ordering.map(resolveAttrs(_, plan))
    assert(actualOrdering.length == expectedOrdering.length)
    (actualOrdering zip expectedOrdering).foreach { case (actual, expected) =>
      assert(actual semanticEquals expected, "ordering must match")
    }
  }

  // executes a write operation and keeps the executed physical plan
  private def execute(writeFunc: => Unit): SparkPlan = {
    var executedPlan: SparkPlan = null

    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        executedPlan = qe.executedPlan
      }
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
      }
    }
    spark.listenerManager.register(listener)

    writeFunc

    sparkContext.listenerBus.waitUntilEmpty()

    executedPlan match {
      case w: V2TableWriteExec =>
        stripAQEPlan(w.query)
      case _ =>
        fail("expected V2TableWriteExec")
    }
  }

  private def orderedWritePartitioning(
      writeOrdering: Seq[catalyst.expressions.SortOrder],
      targetNumPartitions: Option[Int]): physical.Partitioning = {
    RangePartitioning(writeOrdering, targetNumPartitions.getOrElse(conf.numShufflePartitions))
  }

  private def clusteredWritePartitioning(
      writePartitioningExprs: Seq[catalyst.expressions.Expression],
      targetNumPartitions: Option[Int],
      coalesce: Boolean): physical.Partitioning = {
    val partitioning = HashPartitioning(writePartitioningExprs,
        targetNumPartitions.getOrElse(conf.numShufflePartitions))
    if (coalesce)  {
      CoalescedHashPartitioning(
        partitioning, Seq(CoalescedBoundary(0, partitioning.numPartitions)))
    } else {
      partitioning
    }
  }

  private def partitionSizes(dataSkew: Boolean, coalesce: Boolean): Seq[Option[Long]] = {
    if (coalesce) {
      Seq(Some(1000L), None)
    } else if (dataSkew) {
      Seq(Some(100L), None)
    } else {
      Seq(None)
    }
  }
}
