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

import org.apache.spark.sql.{catalyst, DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.plans.physical
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, RangePartitioning, UnknownPartitioning}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference, NullOrdering, SortDirection, SortOrder}
import org.apache.spark.sql.connector.expressions.LogicalExpressions._
import org.apache.spark.sql.execution.{QueryExecution, SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.util.QueryExecutionListener

class WriteDistributionAndOrderingSuite
  extends QueryTest with SharedSparkSession with BeforeAndAfter with AdaptiveSparkPlanHelper {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.unsetConf("spark.sql.catalog.testcat")
  }

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

  private def checkOrderedDistributionAndSortWithSameExprs(command: String): Unit = {
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
    val writePartitioning = RangePartitioning(writeOrdering, conf.numShufflePartitions)

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
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

  private def checkClusteredDistributionAndSortWithSameExprs(command: String): Unit = {
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
    val writePartitioning = HashPartitioning(writePartitioningExprs, conf.numShufflePartitions)

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
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

  private def checkClusteredDistributionAndSortWithExtendedExprs(command: String): Unit = {
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
    val writePartitioning = HashPartitioning(writePartitioningExprs, conf.numShufflePartitions)

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
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

  private def checkUnspecifiedDistributionAndLocalSort(command: String): Unit = {
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
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeCommand = command)
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

  private def checkUnspecifiedDistributionAndNoSort(command: String): Unit = {
    val tableOrdering = Array.empty[SortOrder]
    val tableDistribution = Distributions.unspecified()

    val writeOrdering = Seq.empty[catalyst.expressions.SortOrder]
    val writePartitioning = UnknownPartitioning(0)

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeCommand = command)
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

  private def checkOrderedDistributionAndSortWithManualGlobalSort(command: String): Unit = {
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
    val writePartitioning = RangePartitioning(writeOrdering, conf.numShufflePartitions)

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
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

  private def checkOrderedDistributionAndSortWithIncompatibleGlobalSort(command: String): Unit = {
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
    val writePartitioning = RangePartitioning(writeOrdering, conf.numShufflePartitions)

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
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

  private def checkOrderedDistributionAndSortWithManualLocalSort(command: String): Unit = {
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
    val writePartitioning = RangePartitioning(writeOrdering, conf.numShufflePartitions)

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
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

  private def checkClusteredDistributionAndLocalSortWithManualGlobalSort(command: String): Unit = {
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
    val writePartitioning = HashPartitioning(writePartitioningExprs, conf.numShufflePartitions)

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
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

  private def checkClusteredDistributionAndLocalSortWithManualLocalSort(command: String): Unit = {
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
    val writePartitioning = HashPartitioning(writePartitioningExprs, conf.numShufflePartitions)

    checkWriteRequirements(
      tableDistribution,
      tableOrdering,
      expectedWritePartitioning = writePartitioning,
      expectedWriteOrdering = writeOrdering,
      writeTransform = df => df.orderBy("data", "id"),
      writeCommand = command)
  }

  private def checkWriteRequirements(
      tableDistribution: Distribution,
      tableOrdering: Array[SortOrder],
      expectedWritePartitioning: physical.Partitioning,
      expectedWriteOrdering: Seq[catalyst.expressions.SortOrder],
      writeTransform: DataFrame => DataFrame = df => df,
      writeCommand: String = "append"): Unit = {

    catalog.createTable(ident, schema, Array.empty, emptyProps, tableDistribution, tableOrdering)

    val df = spark.createDataFrame(Seq((1, "a"), (2, "b"), (3, "c"))).toDF("id", "data")
    val writer = writeTransform(df).writeTo(tableNameAsString)
    val executedPlan = writeCommand match {
      case "append" => execute(writer.append())
      case "overwrite" => execute(writer.overwrite(lit(true)))
      case "overwriteDynamic" => execute(writer.overwritePartitions())
    }

    checkPartitioningAndOrdering(executedPlan, expectedWritePartitioning, expectedWriteOrdering)

    checkAnswer(spark.table(tableNameAsString), df)
  }

  private def checkPartitioningAndOrdering(
      plan: SparkPlan,
      partitioning: physical.Partitioning,
      ordering: Seq[catalyst.expressions.SortOrder]): Unit = {

    val sorts = collect(plan) { case s: SortExec => s }
    assert(sorts.size <= 1, "must be at most one sort")
    val shuffles = collect(plan) { case s: ShuffleExchangeLike => s }
    assert(shuffles.size <= 1, "must be at most one shuffle")

    val actualPartitioning = plan.outputPartitioning
    val expectedPartitioning = partitioning match {
      case p: physical.RangePartitioning =>
        val resolvedOrdering = p.ordering.map(resolveAttrs(_, plan))
        p.copy(ordering = resolvedOrdering.asInstanceOf[Seq[catalyst.expressions.SortOrder]])
      case p: physical.HashPartitioning =>
        val resolvedExprs = p.expressions.map(resolveAttrs(_, plan))
        p.copy(expressions = resolvedExprs)
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
}
