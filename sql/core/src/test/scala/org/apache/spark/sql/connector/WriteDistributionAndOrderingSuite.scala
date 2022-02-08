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

import java.util.Collections

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{catalyst, AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.plans.physical
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, RangePartitioning, UnknownPartitioning}
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryTableCatalog}
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference, NullOrdering, SortDirection, SortOrder}
import org.apache.spark.sql.connector.expressions.LogicalExpressions._
import org.apache.spark.sql.execution.{QueryExecution, SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.sources.ContinuousMemoryStream
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming.{StreamingQueryException, Trigger}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.util.QueryExecutionListener

class WriteDistributionAndOrderingSuite
  extends QueryTest with SharedSparkSession with BeforeAndAfter with AdaptiveSparkPlanHelper {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
  import testImplicits._

  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.unsetConf("spark.sql.catalog.testcat")
  }

  private val microBatchPrefix = "micro_batch_"
  private val namespace = Array("ns1")
  private val ident = Identifier.of(namespace, "test_table")
  private val tableNameAsString = "testcat." + ident.toString
  private val emptyProps = Collections.emptyMap[String, String]
  private val schema = new StructType()
    .add("id", IntegerType)
    .add("data", StringType)

  private val resolver = conf.resolver

  test("ordered distribution and sort with same exprs: append") {
    checkOrderedDistributionAndSortWithSameExprs("append")
  }

  test("ordered distribution and sort with same exprs: overwrite") {
    checkOrderedDistributionAndSortWithSameExprs("overwrite")
  }

  test("ordered distribution and sort with same exprs: overwriteDynamic") {
    checkOrderedDistributionAndSortWithSameExprs("overwriteDynamic")
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

  private def checkOrderedDistributionAndSortWithSameExprs(command: String): Unit = {
    checkOrderedDistributionAndSortWithSameExprs(command, None)
  }

  private def checkOrderedDistributionAndSortWithSameExprs(
      command: String,
      targetNumPartitions: Option[Int]): Unit = {
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
    val writePartitioning = orderedWritePartitioning(writeOrdering, targetNumPartitions)

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeCommand = command)
  }

  test("clustered distribution and sort with same exprs: append") {
    checkClusteredDistributionAndSortWithSameExprs("append")
  }

  test("clustered distribution and sort with same exprs: overwrite") {
    checkClusteredDistributionAndSortWithSameExprs("overwrite")
  }

  test("clustered distribution and sort with same exprs: overwriteDynamic") {
    checkClusteredDistributionAndSortWithSameExprs("overwriteDynamic")
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

  private def checkClusteredDistributionAndSortWithSameExprs(command: String): Unit = {
    checkClusteredDistributionAndSortWithSameExprs(command, None)
  }

  private def checkClusteredDistributionAndSortWithSameExprs(
      command: String,
      targetNumPartitions: Option[Int]): Unit = {
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
    val writePartitioning = clusteredWritePartitioning(writePartitioningExprs, targetNumPartitions)

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeCommand = command)
  }

  test("clustered distribution and sort with extended exprs: append") {
    checkClusteredDistributionAndSortWithExtendedExprs("append")
  }

  test("clustered distribution and sort with extended exprs: overwrite") {
    checkClusteredDistributionAndSortWithExtendedExprs("overwrite")
  }

  test("clustered distribution and sort with extended exprs: overwriteDynamic") {
    checkClusteredDistributionAndSortWithExtendedExprs("overwriteDynamic")
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

  private def checkClusteredDistributionAndSortWithExtendedExprs(command: String): Unit = {
    checkClusteredDistributionAndSortWithExtendedExprs(command, None)
  }

  private def checkClusteredDistributionAndSortWithExtendedExprs(
      command: String,
      targetNumPartitions: Option[Int]): Unit = {
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
    val writePartitioning = clusteredWritePartitioning(writePartitioningExprs, targetNumPartitions)

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeCommand = command)
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
    checkOrderedDistributionAndSortWithManualGlobalSort("append")
  }

  test("ordered distribution and sort with manual global sort: overwrite") {
    checkOrderedDistributionAndSortWithManualGlobalSort("overwrite")
  }

  test("ordered distribution and sort with manual global sort: overwriteDynamic") {
    checkOrderedDistributionAndSortWithManualGlobalSort("overwriteDynamic")
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

  private def checkOrderedDistributionAndSortWithManualGlobalSort(command: String): Unit = {
    checkOrderedDistributionAndSortWithManualGlobalSort(command, None)
  }

  private def checkOrderedDistributionAndSortWithManualGlobalSort(
      command: String,
      targetNumPartitions: Option[Int]): Unit = {
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
    val writePartitioning = orderedWritePartitioning(writeOrdering, targetNumPartitions)

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeTransform = df => df.orderBy("data", "id"),
      writeCommand = command)
  }

  test("ordered distribution and sort with incompatible global sort: append") {
    checkOrderedDistributionAndSortWithIncompatibleGlobalSort("append")
  }

  test("ordered distribution and sort with incompatible global sort: overwrite") {
    checkOrderedDistributionAndSortWithIncompatibleGlobalSort("overwrite")
  }

  test("ordered distribution and sort with incompatible global sort: overwriteDynamic") {
    checkOrderedDistributionAndSortWithIncompatibleGlobalSort("overwriteDynamic")
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

  private def checkOrderedDistributionAndSortWithIncompatibleGlobalSort(command: String): Unit = {
    checkOrderedDistributionAndSortWithIncompatibleGlobalSort(command, None)
  }

  private def checkOrderedDistributionAndSortWithIncompatibleGlobalSort(
      command: String,
      targetNumPartitions: Option[Int]): Unit = {
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
    val writePartitioning = orderedWritePartitioning(writeOrdering, targetNumPartitions)

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeTransform = df => df.orderBy(df("data").desc, df("id").asc),
      writeCommand = command)
  }

  test("ordered distribution and sort with manual local sort: append") {
    checkOrderedDistributionAndSortWithManualLocalSort("append")
  }

  test("ordered distribution and sort with manual local sort: overwrite") {
    checkOrderedDistributionAndSortWithManualLocalSort("overwrite")
  }

  test("ordered distribution and sort with manual local sort: overwriteDynamic") {
    checkOrderedDistributionAndSortWithManualLocalSort("overwriteDynamic")
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

  private def checkOrderedDistributionAndSortWithManualLocalSort(command: String): Unit = {
    checkOrderedDistributionAndSortWithManualLocalSort(command, None)
  }

  private def checkOrderedDistributionAndSortWithManualLocalSort(
      command: String,
      targetNumPartitions: Option[Int]): Unit = {
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
    val writePartitioning = orderedWritePartitioning(writeOrdering, targetNumPartitions)

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeTransform = df => df.sortWithinPartitions("data", "id"),
      writeCommand = command)
  }

  test("clustered distribution and local sort with manual global sort: append") {
    checkClusteredDistributionAndLocalSortWithManualGlobalSort("append")
  }

  test("clustered distribution and local sort with manual global sort: overwrite") {
    checkClusteredDistributionAndLocalSortWithManualGlobalSort("overwrite")
  }

  test("clustered distribution and local sort with manual global sort: overwriteDynamic") {
    checkClusteredDistributionAndLocalSortWithManualGlobalSort("overwriteDynamic")
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

  private def checkClusteredDistributionAndLocalSortWithManualGlobalSort(command: String): Unit = {
    checkClusteredDistributionAndLocalSortWithManualGlobalSort(command, None)
  }

  private def checkClusteredDistributionAndLocalSortWithManualGlobalSort(
      command: String,
      targetNumPartitions: Option[Int]): Unit = {
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
    val writePartitioning = clusteredWritePartitioning(writePartitioningExprs, targetNumPartitions)

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeTransform = df => df.orderBy("data", "id"),
      writeCommand = command)
  }

  test("clustered distribution and local sort with manual local sort: append") {
    checkClusteredDistributionAndLocalSortWithManualLocalSort("append")
  }

  test("clustered distribution and local sort with manual local sort: overwrite") {
    checkClusteredDistributionAndLocalSortWithManualLocalSort("overwrite")
  }

  test("clustered distribution and local sort with manual local sort: overwriteDynamic") {
    checkClusteredDistributionAndLocalSortWithManualLocalSort("overwriteDynamic")
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

  private def checkClusteredDistributionAndLocalSortWithManualLocalSort(command: String): Unit = {
    checkClusteredDistributionAndLocalSortWithManualLocalSort(command, None)
  }

  private def checkClusteredDistributionAndLocalSortWithManualLocalSort(
      command: String,
      targetNumPartitions: Option[Int]): Unit = {
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
    val writePartitioning = clusteredWritePartitioning(writePartitioningExprs, targetNumPartitions)

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      targetNumPartitions,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeTransform = df => df.orderBy("data", "id"),
      writeCommand = command)
  }

  test("continuous mode does not support write distribution and ordering") {
    val ordering = Array[SortOrder](
      sort(FieldReference("data"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    )
    val distribution = Distributions.ordered(ordering)

    catalog.createTable(ident, schema, Array.empty, emptyProps, distribution, ordering, None)

    withTempDir { checkpointDir =>
      val inputData = ContinuousMemoryStream[(Long, String)]
      val inputDF = inputData.toDF().toDF("id", "data")

      val writer = inputDF
        .writeStream
        .trigger(Trigger.Continuous(100))
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .outputMode("append")

      val analysisException = intercept[AnalysisException] {
        val query = writer.toTable(tableNameAsString)

        inputData.addData((1, "a"), (2, "b"))

        query.processAllAvailable()
        query.stop()
      }

      assert(analysisException.message.contains("Sinks cannot request distribution and ordering"))
    }
  }

  test("continuous mode allows unspecified distribution and empty ordering") {
    catalog.createTable(ident, schema, Array.empty, emptyProps)

    withTempDir { checkpointDir =>
      val inputData = ContinuousMemoryStream[(Long, String)]
      val inputDF = inputData.toDF().toDF("id", "data")

      val writer = inputDF
        .writeStream
        .trigger(Trigger.Continuous(100))
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .outputMode("append")

      val query = writer.toTable(tableNameAsString)

      inputData.addData((1, "a"), (2, "b"))

      query.processAllAvailable()
      query.stop()

      checkAnswer(spark.table(tableNameAsString), Row(1, "a") :: Row(2, "b") :: Nil)
    }
  }

  private def checkWriteRequirements(
      tableDistribution: Distribution,
      tableOrdering: Array[SortOrder],
      tableNumPartitions: Option[Int],
      expectedWritePartitioning: physical.Partitioning,
      expectedWriteOrdering: Seq[catalyst.expressions.SortOrder],
      writeTransform: DataFrame => DataFrame = df => df,
      writeCommand: String,
      expectAnalysisException: Boolean = false): Unit = {

    if (writeCommand.startsWith(microBatchPrefix)) {
      checkMicroBatchWriteRequirements(
        tableDistribution,
        tableOrdering,
        tableNumPartitions,
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
        expectedWritePartitioning,
        expectedWriteOrdering,
        writeTransform,
        writeCommand,
        expectAnalysisException)
    }
  }

  private def checkBatchWriteRequirements(
      tableDistribution: Distribution,
      tableOrdering: Array[SortOrder],
      tableNumPartitions: Option[Int],
      expectedWritePartitioning: physical.Partitioning,
      expectedWriteOrdering: Seq[catalyst.expressions.SortOrder],
      writeTransform: DataFrame => DataFrame = df => df,
      writeCommand: String = "append",
      expectAnalysisException: Boolean = false): Unit = {

    catalog.createTable(ident, schema, Array.empty, emptyProps, tableDistribution,
      tableOrdering, tableNumPartitions)

    val df = spark.createDataFrame(Seq((1, "a"), (2, "b"), (3, "c"))).toDF("id", "data")
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
      val executedPlan = executeCommand()

      checkPartitioningAndOrdering(executedPlan, expectedWritePartitioning, expectedWriteOrdering)

      checkAnswer(spark.table(tableNameAsString), df)
    }
  }

  private def checkMicroBatchWriteRequirements(
      tableDistribution: Distribution,
      tableOrdering: Array[SortOrder],
      tableNumPartitions: Option[Int],
      expectedWritePartitioning: physical.Partitioning,
      expectedWriteOrdering: Seq[catalyst.expressions.SortOrder],
      writeTransform: DataFrame => DataFrame = df => df,
      outputMode: String = "append",
      expectAnalysisException: Boolean = false): Unit = {

    catalog.createTable(ident, schema, Array.empty, emptyProps, tableDistribution,
      tableOrdering, tableNumPartitions)

    withTempDir { checkpointDir =>
      val inputData = MemoryStream[(Long, String)]
      val inputDF = inputData.toDF().toDF("id", "data")

      val queryDF = outputMode match {
        case "append" | "update" =>
          inputDF
        case "complete" =>
          // add an aggregate for complete mode
          inputDF
            .groupBy("id")
            .agg(Map("data" -> "count"))
            .select($"id", $"count(data)".cast("string").as("data"))
      }

      val writer = writeTransform(queryDF)
        .writeStream
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .outputMode(outputMode)

      def executeCommand(): SparkPlan = execute {
        val query = writer.toTable(tableNameAsString)

        inputData.addData((1, "a"), (2, "b"))

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
          case "append" | "update" => Row(1, "a") :: Row(2, "b") :: Nil
          case "complete" => Row(1, "1") :: Row(2, "1") :: Nil
        }
        checkAnswer(spark.table(tableNameAsString), expectedRows)
      }
    }
  }

  private def checkPartitioningAndOrdering(
      plan: SparkPlan,
      partitioning: physical.Partitioning,
      ordering: Seq[catalyst.expressions.SortOrder],
      maxNumShuffles: Int = 1): Unit = {

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
      case _: UnknownPartitioning =>
        // don't check partitioning if no particular one is expected
        actualPartitioning
      case other => other
    }
    assert(actualPartitioning == expectedPartitioning, "partitioning must match")

    val actualOrdering = plan.outputOrdering
    val expectedOrdering = ordering.map(resolveAttrs(_, plan))
    assert(actualOrdering == expectedOrdering, "ordering must match")
  }

  private def resolveAttrs(
      expr: catalyst.expressions.Expression,
      plan: SparkPlan): catalyst.expressions.Expression = {

    expr.transform {
      case UnresolvedAttribute(Seq(attrName)) =>
        plan.output.find(attr => resolver(attr.name, attrName)).get
      case UnresolvedAttribute(nameParts) =>
        val attrName = nameParts.mkString(".")
        fail(s"cannot resolve a nested attr: $attrName")
    }
  }

  private def attr(name: String): UnresolvedAttribute = {
    UnresolvedAttribute(name)
  }

  private def catalog: InMemoryTableCatalog = {
    val catalog = spark.sessionState.catalogManager.catalog("testcat")
    catalog.asTableCatalog.asInstanceOf[InMemoryTableCatalog]
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
      targetNumPartitions: Option[Int]): physical.Partitioning = {
    HashPartitioning(writePartitioningExprs,
      targetNumPartitions.getOrElse(conf.numShufflePartitions))
  }
}
