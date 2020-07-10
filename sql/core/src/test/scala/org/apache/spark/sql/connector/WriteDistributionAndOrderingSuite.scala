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

import java.util
import java.util.Collections

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{catalyst, DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.analysis.{TableAlreadyExistsException, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.plans.physical
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, RangePartitioning, UnknownPartitioning}
import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference, NullOrdering, SortDirection, SortOrder, Transform}
import org.apache.spark.sql.connector.expressions.LogicalExpressions._
import org.apache.spark.sql.connector.write.{BatchWrite, LogicalWriteInfo, RequiresDistributionAndOrdering, SupportsDynamicOverwrite, SupportsOverwrite, SupportsTruncate, Write, WriteBuilder}
import org.apache.spark.sql.execution.{QueryExecution, SortExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.util.{CaseInsensitiveStringMap, QueryExecutionListener}

class WriteDistributionAndOrderingSuite
  extends QueryTest with SharedSparkSession with BeforeAndAfter {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[ExtendedInMemoryTableCatalog].getName)
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.clear()
  }

  private val writeOperations = Seq("append", "overwrite", "overwriteDynamic")

  private val namespace = Array("ns1")
  private val ident = Identifier.of(namespace, "test_table")
  private val tableNameAsString = "testcat." + ident.toString
  private val emptyProps = Collections.emptyMap[String, String]
  private val schema = new StructType()
    .add("id", IntegerType)
    .add("data", StringType)

  writeOperations.foreach { operation =>
    test(s"ordered distribution and sort with same exprs ($operation)") {
      val ordering = Array[SortOrder](
        sort(FieldReference("data"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
      )
      val distribution = Distributions.ordered(ordering)

      val writeOrdering = Seq(
        catalyst.expressions.SortOrder(
          UnresolvedAttribute("data"),
          catalyst.expressions.Ascending,
          catalyst.expressions.NullsFirst,
          Seq.empty
        )
      )
      val numShufflePartitions = SQLConf.get.numShufflePartitions
      val writePartitioning = RangePartitioning(writeOrdering, numShufflePartitions)

      checkWriteRequirements(
        tableDistribution = distribution,
        tableOrdering = ordering,
        expectedWritePartitioning = writePartitioning,
        expectedWriteOrdering = writeOrdering,
        writeOperation = operation
      )
    }
  }

  writeOperations.foreach { operation =>
    test(s"clustered distribution and sort with same exprs ($operation)") {
      val ordering = Array[SortOrder](
        sort(FieldReference("data"), SortDirection.DESCENDING, NullOrdering.NULLS_FIRST),
        sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
      )
      val clustering = Array[Expression](FieldReference("data"), FieldReference("id"))
      val distribution = Distributions.clustered(clustering)

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
      val numShufflePartitions = SQLConf.get.numShufflePartitions
      val writePartitioning = HashPartitioning(writePartitioningExprs, numShufflePartitions)

      checkWriteRequirements(
        tableDistribution = distribution,
        tableOrdering = ordering,
        expectedWritePartitioning = writePartitioning,
        expectedWriteOrdering = writeOrdering,
        writeOperation = operation
      )
    }
  }

  writeOperations.foreach { operation =>
    test(s"clustered distribution and sort with extended exprs ($operation)") {
      val ordering = Array[SortOrder](
        sort(FieldReference("data"), SortDirection.DESCENDING, NullOrdering.NULLS_FIRST),
        sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
      )
      val clustering = Array[Expression](FieldReference("data"))
      val distribution = Distributions.clustered(clustering)

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
      val numShufflePartitions = SQLConf.get.numShufflePartitions
      val writePartitioning = HashPartitioning(writePartitioningExprs, numShufflePartitions)

      checkWriteRequirements(
        tableDistribution = distribution,
        tableOrdering = ordering,
        expectedWritePartitioning = writePartitioning,
        expectedWriteOrdering = writeOrdering,
        writeOperation = operation
      )
    }
  }

  writeOperations.foreach { operation =>
    test(s"unspecified distribution and local sort ($operation)") {
      val ordering = Array[SortOrder](
        sort(FieldReference("data"), SortDirection.DESCENDING, NullOrdering.NULLS_FIRST)
      )
      val distribution = Distributions.unspecified()

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
        tableDistribution = distribution,
        tableOrdering = ordering,
        expectedWritePartitioning = writePartitioning,
        expectedWriteOrdering = writeOrdering,
        writeOperation = operation
      )
    }
  }

  writeOperations.foreach { operation =>
    test(s"unspecified distribution and no sort ($operation)") {
      val ordering = Array.empty[SortOrder]
      val distribution = Distributions.unspecified()

      val writeOrdering = Seq.empty[catalyst.expressions.SortOrder]
      val writePartitioning = UnknownPartitioning(0)

      checkWriteRequirements(
        tableDistribution = distribution,
        tableOrdering = ordering,
        expectedWritePartitioning = writePartitioning,
        expectedWriteOrdering = writeOrdering,
        writeOperation = operation
      )
    }
  }

  writeOperations.foreach { operation =>
    test(s"ordered distribution and sort with manual global sort ($operation)") {
      val ordering = Array[SortOrder](
        sort(FieldReference("data"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST),
        sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
      )
      val distribution = Distributions.ordered(ordering)

      val writeOrdering = Seq(
        catalyst.expressions.SortOrder(
          UnresolvedAttribute("data"),
          catalyst.expressions.Ascending,
          catalyst.expressions.NullsFirst,
          Seq.empty
        ),
        catalyst.expressions.SortOrder(
          UnresolvedAttribute("id"),
          catalyst.expressions.Ascending,
          catalyst.expressions.NullsFirst,
          Seq.empty
        )
      )
      val numShufflePartitions = SQLConf.get.numShufflePartitions
      val writePartitioning = RangePartitioning(writeOrdering, numShufflePartitions)

      checkWriteRequirements(
        tableDistribution = distribution,
        tableOrdering = ordering,
        expectedWritePartitioning = writePartitioning,
        expectedWriteOrdering = writeOrdering,
        writeTransform = df => df.orderBy("data", "id"),
        writeOperation = operation
      )
    }
  }

  writeOperations.foreach { operation =>
    test(s"ordered distribution and sort with incompatible global sort ($operation)") {
      val ordering = Array[SortOrder](
        sort(FieldReference("data"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST),
        sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
      )
      val distribution = Distributions.ordered(ordering)

      val writeOrdering = Seq(
        catalyst.expressions.SortOrder(
          UnresolvedAttribute("data"),
          catalyst.expressions.Ascending,
          catalyst.expressions.NullsFirst,
          Seq.empty
        ),
        catalyst.expressions.SortOrder(
          UnresolvedAttribute("id"),
          catalyst.expressions.Ascending,
          catalyst.expressions.NullsFirst,
          Seq.empty
        )
      )
      val numShufflePartitions = SQLConf.get.numShufflePartitions
      val writePartitioning = RangePartitioning(writeOrdering, numShufflePartitions)

      checkWriteRequirements(
        tableDistribution = distribution,
        tableOrdering = ordering,
        expectedWritePartitioning = writePartitioning,
        expectedWriteOrdering = writeOrdering,
        writeTransform = df => df.orderBy(df("data").desc, df("id").asc),
        writeOperation = operation
      )
    }
  }

  writeOperations.foreach { operation =>
    test(s"ordered distribution and sort with manual local sort ($operation)") {
      val ordering = Array[SortOrder](
        sort(FieldReference("data"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST),
        sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
      )
      val distribution = Distributions.ordered(ordering)

      val writeOrdering = Seq(
        catalyst.expressions.SortOrder(
          UnresolvedAttribute("data"),
          catalyst.expressions.Ascending,
          catalyst.expressions.NullsFirst,
          Seq.empty
        ),
        catalyst.expressions.SortOrder(
          UnresolvedAttribute("id"),
          catalyst.expressions.Ascending,
          catalyst.expressions.NullsFirst,
          Seq.empty
        )
      )
      val numShufflePartitions = SQLConf.get.numShufflePartitions
      val writePartitioning = RangePartitioning(writeOrdering, numShufflePartitions)

      checkWriteRequirements(
        tableDistribution = distribution,
        tableOrdering = ordering,
        expectedWritePartitioning = writePartitioning,
        expectedWriteOrdering = writeOrdering,
        writeTransform = df => df.sortWithinPartitions("data", "id"),
        writeOperation = operation
      )
    }
  }

  // TODO: do we need to dedup repartitions too? RepartitionByExpr -> Projects -> RepartitionByExpr
  writeOperations.foreach { operation =>
    ignore(s"ordered distribution and sort with manual repartition ($operation)") {
      val ordering = Array[SortOrder](
        sort(FieldReference("data"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST),
        sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
      )
      val distribution = Distributions.ordered(ordering)

      val writeOrdering = Seq(
        catalyst.expressions.SortOrder(
          UnresolvedAttribute("data"),
          catalyst.expressions.Ascending,
          catalyst.expressions.NullsFirst,
          Seq.empty
        ),
        catalyst.expressions.SortOrder(
          UnresolvedAttribute("id"),
          catalyst.expressions.Ascending,
          catalyst.expressions.NullsFirst,
          Seq.empty
        )
      )
      val numShufflePartitions = SQLConf.get.numShufflePartitions
      val writePartitioning = RangePartitioning(writeOrdering, numShufflePartitions)

      checkWriteRequirements(
        tableDistribution = distribution,
        tableOrdering = ordering,
        expectedWritePartitioning = writePartitioning,
        expectedWriteOrdering = writeOrdering,
        writeTransform = df => df.repartitionByRange(df("data"), df("id")),
        writeOperation = operation
      )
    }
  }

  writeOperations.foreach { operation =>
    test(s"clustered distribution and local sort with manual global sort ($operation)") {
      val ordering = Array[SortOrder](
        sort(FieldReference("data"), SortDirection.DESCENDING, NullOrdering.NULLS_FIRST),
        sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
      )
      val distribution = Distributions.clustered(Array(FieldReference("data")))

      val writeOrdering = Seq(
        catalyst.expressions.SortOrder(
          UnresolvedAttribute("data"),
          catalyst.expressions.Descending,
          catalyst.expressions.NullsFirst,
          Seq.empty
        ),
        catalyst.expressions.SortOrder(
          UnresolvedAttribute("id"),
          catalyst.expressions.Ascending,
          catalyst.expressions.NullsFirst,
          Seq.empty
        )
      )
      val writePartitioningExprs = Seq(attr("data"))
      val numShufflePartitions = SQLConf.get.numShufflePartitions
      val writePartitioning = HashPartitioning(writePartitioningExprs, numShufflePartitions)

      checkWriteRequirements(
        tableDistribution = distribution,
        tableOrdering = ordering,
        expectedWritePartitioning = writePartitioning,
        expectedWriteOrdering = writeOrdering,
        writeTransform = df => df.orderBy("data", "id"),
        writeOperation = operation
      )
    }
  }

  writeOperations.foreach { operation =>
    test(s"clustered distribution and local sort with manual local sort ($operation)") {
      val ordering = Array[SortOrder](
        sort(FieldReference("data"), SortDirection.DESCENDING, NullOrdering.NULLS_FIRST),
        sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
      )
      val distribution = Distributions.clustered(Array(FieldReference("data")))

      val writeOrdering = Seq(
        catalyst.expressions.SortOrder(
          UnresolvedAttribute("data"),
          catalyst.expressions.Descending,
          catalyst.expressions.NullsFirst,
          Seq.empty
        ),
        catalyst.expressions.SortOrder(
          UnresolvedAttribute("id"),
          catalyst.expressions.Ascending,
          catalyst.expressions.NullsFirst,
          Seq.empty
        )
      )
      val writePartitioningExprs = Seq(attr("data"))
      val numShufflePartitions = SQLConf.get.numShufflePartitions
      val writePartitioning = HashPartitioning(writePartitioningExprs, numShufflePartitions)

      checkWriteRequirements(
        tableDistribution = distribution,
        tableOrdering = ordering,
        expectedWritePartitioning = writePartitioning,
        expectedWriteOrdering = writeOrdering,
        writeTransform = df => df.orderBy("data", "id"),
        writeOperation = operation
      )
    }
  }

  private def checkWriteRequirements(
      tableDistribution: Distribution,
      tableOrdering: Array[SortOrder],
      expectedWritePartitioning: physical.Partitioning,
      expectedWriteOrdering: Seq[catalyst.expressions.SortOrder],
      writeTransform: DataFrame => DataFrame = df => df,
      writeOperation: String = "append"): Unit = {

    catalog.createTable(ident, schema, Array.empty, emptyProps, tableDistribution, tableOrdering)

    val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
    val writer = writeTransform(df).writeTo(tableNameAsString)
    val executedPlan = writeOperation match {
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

    val sorts = plan.collect { case s: SortExec => s }
    assert(sorts.size <= 1, "must be at most one sort")
    val shuffles = plan.collect { case s: ShuffleExchangeExec => s }
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
    // TODO: can be compatible, does not have to match 100%
    assert(actualPartitioning == expectedPartitioning, "partitioning must match")

    val actualOrdering = plan.outputOrdering
    val expectedOrdering = ordering.map(resolveAttrs(_, plan))
    // TODO: can be compatible, does not have to match 100%
    assert(actualOrdering == expectedOrdering, "ordering must match")
  }

  private def resolveAttrs(
      expr: catalyst.expressions.Expression,
      plan: SparkPlan): catalyst.expressions.Expression = {

    expr.transform {
      case UnresolvedAttribute(parts) =>
        val attrName = parts.mkString(",")
        plan.output.find(a => a.name == attrName).get
    }
  }

  private def attr(name: String): UnresolvedAttribute = {
    UnresolvedAttribute(name)
  }

  private def catalog: ExtendedInMemoryTableCatalog = {
    val catalog = spark.sessionState.catalogManager.catalog("testcat")
    catalog.asTableCatalog.asInstanceOf[ExtendedInMemoryTableCatalog]
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

    executedPlan.asInstanceOf[V2TableWriteExec].query
  }
}

class ExtendedInMemoryTableCatalog extends InMemoryTableCatalog {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String],
      distribution: Distribution,
      ordering: Array[SortOrder]): Table = {

    if (tables.containsKey(ident)) {
      throw new TableAlreadyExistsException(ident)
    }

    InMemoryTableCatalog.maybeSimulateFailedTableCreation(properties)

    val table = new ExtendedInMemoryTable(
      s"$name.${ident.quoted}", schema, partitions, properties, distribution, ordering)
    tables.put(ident, table)
    namespaces.putIfAbsent(ident.namespace.toList, Map())
    table
  }
}

class ExtendedInMemoryTable(
    override val name: String,
    override val schema: StructType,
    override val partitioning: Array[Transform],
    override val properties: util.Map[String, String],
    distribution: Distribution,
    ordering: Array[SortOrder])
  extends InMemoryTable(name, schema, partitioning, properties) {

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    InMemoryTable.maybeSimulateFailedTableWrite(new CaseInsensitiveStringMap(properties))
    InMemoryTable.maybeSimulateFailedTableWrite(info.options)

    new WriteBuilder with SupportsTruncate with SupportsOverwrite with SupportsDynamicOverwrite {
      private var writer: BatchWrite = Append

      override def truncate(): WriteBuilder = {
        assert(writer == Append)
        writer = TruncateAndAppend
        this
      }

      override def overwrite(filters: Array[Filter]): WriteBuilder = {
        assert(writer == Append)
        writer = new Overwrite(filters)
        this
      }

      override def overwriteDynamicPartitions(): WriteBuilder = {
        assert(writer == Append)
        writer = DynamicOverwrite
        this
      }

      override def build(): Write = new RequiresDistributionAndOrdering {
        override def requiredDistribution(): Distribution = distribution
        override def requiredOrdering(): Array[SortOrder] = ordering
        override def toBatch: BatchWrite = writer
      }
    }
  }
}
