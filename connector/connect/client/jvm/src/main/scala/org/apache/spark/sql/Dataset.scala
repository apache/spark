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
package org.apache.spark.sql

import java.util

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.java.function._
import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders._
import org.apache.spark.sql.catalyst.expressions.OrderUtils
import org.apache.spark.sql.connect.ConnectConversions._
import org.apache.spark.sql.connect.client.SparkResult
import org.apache.spark.sql.connect.common.{DataTypeProtoConverter, StorageLevelProtoConverter}
import org.apache.spark.sql.errors.DataTypeErrors.toSQLId
import org.apache.spark.sql.expressions.SparkUserDefinedFunction
import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql.internal.{ColumnNodeToProtoConverter, DataFrameWriterImpl, DataFrameWriterV2Impl, MergeIntoWriterImpl, ToScalaUDF, UDFAdaptors, UnresolvedAttribute, UnresolvedRegex}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types.{Metadata, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.SparkClassUtils

/**
 * A Dataset is a strongly typed collection of domain-specific objects that can be transformed in
 * parallel using functional or relational operations. Each Dataset also has an untyped view
 * called a `DataFrame`, which is a Dataset of [[Row]].
 *
 * Operations available on Datasets are divided into transformations and actions. Transformations
 * are the ones that produce new Datasets, and actions are the ones that trigger computation and
 * return results. Example transformations include map, filter, select, and aggregate (`groupBy`).
 * Example actions count, show, or writing data out to file systems.
 *
 * Datasets are "lazy", i.e. computations are only triggered when an action is invoked.
 * Internally, a Dataset represents a logical plan that describes the computation required to
 * produce the data. When an action is invoked, Spark's query optimizer optimizes the logical plan
 * and generates a physical plan for efficient execution in a parallel and distributed manner. To
 * explore the logical plan as well as optimized physical plan, use the `explain` function.
 *
 * To efficiently support domain-specific objects, an [[Encoder]] is required. The encoder maps
 * the domain specific type `T` to Spark's internal type system. For example, given a class
 * `Person` with two fields, `name` (string) and `age` (int), an encoder is used to tell Spark to
 * generate code at runtime to serialize the `Person` object into a binary structure. This binary
 * structure often has much lower memory footprint as well as are optimized for efficiency in data
 * processing (e.g. in a columnar format). To understand the internal binary representation for
 * data, use the `schema` function.
 *
 * There are typically two ways to create a Dataset. The most common way is by pointing Spark to
 * some files on storage systems, using the `read` function available on a `SparkSession`.
 * {{{
 *   val people = spark.read.parquet("...").as[Person]  // Scala
 *   Dataset<Person> people = spark.read().parquet("...").as(Encoders.bean(Person.class)); // Java
 * }}}
 *
 * Datasets can also be created through transformations available on existing Datasets. For
 * example, the following creates a new Dataset by applying a filter on the existing one:
 * {{{
 *   val names = people.map(_.name)  // in Scala; names is a Dataset[String]
 *   Dataset<String> names = people.map((Person p) -> p.name, Encoders.STRING));
 * }}}
 *
 * Dataset operations can also be untyped, through various domain-specific-language (DSL)
 * functions defined in: Dataset (this class), [[Column]], and [[functions]]. These operations are
 * very similar to the operations available in the data frame abstraction in R or Python.
 *
 * To select a column from the Dataset, use `apply` method in Scala and `col` in Java.
 * {{{
 *   val ageCol = people("age")  // in Scala
 *   Column ageCol = people.col("age"); // in Java
 * }}}
 *
 * Note that the [[Column]] type can also be manipulated through its various functions.
 * {{{
 *   // The following creates a new column that increases everybody's age by 10.
 *   people("age") + 10  // in Scala
 *   people.col("age").plus(10);  // in Java
 * }}}
 *
 * A more concrete example in Scala:
 * {{{
 *   // To create Dataset[Row] using SparkSession
 *   val people = spark.read.parquet("...")
 *   val department = spark.read.parquet("...")
 *
 *   people.filter("age > 30")
 *     .join(department, people("deptId") === department("id"))
 *     .groupBy(department("name"), people("gender"))
 *     .agg(avg(people("salary")), max(people("age")))
 * }}}
 *
 * and in Java:
 * {{{
 *   // To create Dataset<Row> using SparkSession
 *   Dataset<Row> people = spark.read().parquet("...");
 *   Dataset<Row> department = spark.read().parquet("...");
 *
 *   people.filter(people.col("age").gt(30))
 *     .join(department, people.col("deptId").equalTo(department.col("id")))
 *     .groupBy(department.col("name"), people.col("gender"))
 *     .agg(avg(people.col("salary")), max(people.col("age")));
 * }}}
 *
 * @groupname basic Basic Dataset functions
 * @groupname action Actions
 * @groupname untypedrel Untyped transformations
 * @groupname typedrel Typed transformations
 *
 * @since 3.4.0
 */
class Dataset[T] private[sql] (
    val sparkSession: SparkSession,
    @DeveloperApi val plan: proto.Plan,
    val encoder: Encoder[T])
    extends api.Dataset[T] {
  type DS[U] = Dataset[U]

  import sparkSession.RichColumn

  // Make sure we don't forget to set plan id.
  assert(plan.getRoot.getCommon.hasPlanId)

  private[sql] val agnosticEncoder: AgnosticEncoder[T] = agnosticEncoderFor(encoder)

  override def toString: String = {
    try {
      val builder = new mutable.StringBuilder
      val fields = schema.take(2).map { f =>
        s"${f.name}: ${f.dataType.simpleString(2)}"
      }
      builder.append("[")
      builder.append(fields.mkString(", "))
      if (schema.length > 2) {
        if (schema.length - fields.size == 1) {
          builder.append(" ... 1 more field")
        } else {
          builder.append(" ... " + (schema.length - 2) + " more fields")
        }
      }
      builder.append("]").toString()
    } catch {
      case NonFatal(e) =>
        s"Invalid Dataframe; ${e.getMessage}"
    }
  }

  /** @inheritdoc */
  def toDF(): DataFrame = new Dataset(sparkSession, plan, UnboundRowEncoder)

  /** @inheritdoc */
  def as[U: Encoder]: Dataset[U] = {
    val encoder = implicitly[Encoder[U]].asInstanceOf[AgnosticEncoder[U]]
    // We should add some validation/coercion here. We cannot use `to`
    // because that does not work with positional arguments.
    new Dataset[U](sparkSession, plan, encoder)
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  def toDF(colNames: String*): DataFrame = sparkSession.newDataFrame { builder =>
    builder.getToDfBuilder
      .setInput(plan.getRoot)
      .addAllColumnNames(colNames.asJava)
  }

  /** @inheritdoc */
  def to(schema: StructType): DataFrame = sparkSession.newDataFrame { builder =>
    builder.getToSchemaBuilder
      .setInput(plan.getRoot)
      .setSchema(DataTypeProtoConverter.toConnectProtoType(schema))
  }

  /** @inheritdoc */
  def schema: StructType = cachedSchema

  /**
   * The cached schema.
   *
   * Schema caching is correct in most cases. Connect is lazy by nature. This means that we only
   * resolve the plan when it is submitted for execution or analysis. We do not cache intermediate
   * resolved plans. If the input (changes table, view redefinition, etc...) of the plan changes
   * between the schema() call, and a subsequent action, the cached schema might be inconsistent
   * with the end schema.
   */
  private lazy val cachedSchema: StructType = {
    DataTypeProtoConverter
      .toCatalystType(
        sparkSession
          .analyze(plan, proto.AnalyzePlanRequest.AnalyzeCase.SCHEMA)
          .getSchema
          .getSchema)
      .asInstanceOf[StructType]
  }

  /** @inheritdoc */
  def explain(mode: String): Unit = {
    val protoMode = mode.trim.toLowerCase(util.Locale.ROOT) match {
      case "simple" => proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_SIMPLE
      case "extended" => proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_EXTENDED
      case "codegen" => proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_CODEGEN
      case "cost" => proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_COST
      case "formatted" => proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_FORMATTED
      case _ => throw new IllegalArgumentException("Unsupported explain mode: " + mode)
    }
    explain(protoMode)
  }

  private def explain(mode: proto.AnalyzePlanRequest.Explain.ExplainMode): Unit = {
    // scalastyle:off println
    println(
      sparkSession
        .analyze(plan, proto.AnalyzePlanRequest.AnalyzeCase.EXPLAIN, Some(mode))
        .getExplain
        .getExplainString)
    // scalastyle:on println
  }

  /** @inheritdoc */
  def isLocal: Boolean = sparkSession
    .analyze(plan, proto.AnalyzePlanRequest.AnalyzeCase.IS_LOCAL)
    .getIsLocal
    .getIsLocal

  /** @inheritdoc */
  def isEmpty: Boolean = select().limit(1).withResult { result =>
    result.length == 0
  }

  /** @inheritdoc */
  def isStreaming: Boolean = sparkSession
    .analyze(plan, proto.AnalyzePlanRequest.AnalyzeCase.IS_STREAMING)
    .getIsStreaming
    .getIsStreaming

  /** @inheritdoc */
  // scalastyle:off println
  def show(numRows: Int, truncate: Boolean): Unit = {
    val truncateValue = if (truncate) 20 else 0
    show(numRows, truncateValue, vertical = false)
  }

  /** @inheritdoc */
  def show(numRows: Int, truncate: Int, vertical: Boolean): Unit = {
    val df = sparkSession.newDataset(StringEncoder) { builder =>
      builder.getShowStringBuilder
        .setInput(plan.getRoot)
        .setNumRows(numRows)
        .setTruncate(truncate)
        .setVertical(vertical)
    }
    df.withResult { result =>
      assert(result.length == 1)
      assert(result.schema.size == 1)
      print(result.toArray.head)
    }
  }

  /** @inheritdoc */
  def na: DataFrameNaFunctions = new DataFrameNaFunctions(sparkSession, plan.getRoot)

  /** @inheritdoc */
  def stat: DataFrameStatFunctions = new DataFrameStatFunctions(toDF())

  private def buildJoin(right: Dataset[_])(f: proto.Join.Builder => Unit): DataFrame = {
    checkSameSparkSession(right)
    sparkSession.newDataFrame { builder =>
      val joinBuilder = builder.getJoinBuilder
      joinBuilder.setLeft(plan.getRoot).setRight(right.plan.getRoot)
      f(joinBuilder)
    }
  }

  private def toJoinType(name: String, skipSemiAnti: Boolean = false): proto.Join.JoinType = {
    name.trim.toLowerCase(util.Locale.ROOT) match {
      case "inner" =>
        proto.Join.JoinType.JOIN_TYPE_INNER
      case "cross" =>
        proto.Join.JoinType.JOIN_TYPE_CROSS
      case "outer" | "full" | "fullouter" | "full_outer" =>
        proto.Join.JoinType.JOIN_TYPE_FULL_OUTER
      case "left" | "leftouter" | "left_outer" =>
        proto.Join.JoinType.JOIN_TYPE_LEFT_OUTER
      case "right" | "rightouter" | "right_outer" =>
        proto.Join.JoinType.JOIN_TYPE_RIGHT_OUTER
      case "semi" | "leftsemi" | "left_semi" if !skipSemiAnti =>
        proto.Join.JoinType.JOIN_TYPE_LEFT_SEMI
      case "anti" | "leftanti" | "left_anti" if !skipSemiAnti =>
        proto.Join.JoinType.JOIN_TYPE_LEFT_ANTI
      case e =>
        throw new IllegalArgumentException(s"Unsupported join type '$e'.")
    }
  }

  /** @inheritdoc */
  def join(right: Dataset[_]): DataFrame = buildJoin(right) { builder =>
    builder.setJoinType(proto.Join.JoinType.JOIN_TYPE_INNER)
  }

  /** @inheritdoc */
  def join(right: Dataset[_], usingColumns: Seq[String], joinType: String): DataFrame = {
    buildJoin(right) { builder =>
      builder
        .setJoinType(toJoinType(joinType))
        .addAllUsingColumns(usingColumns.asJava)
    }
  }

  /** @inheritdoc */
  def join(right: Dataset[_], joinExprs: Column, joinType: String): DataFrame = {
    buildJoin(right) { builder =>
      builder
        .setJoinType(toJoinType(joinType))
        .setJoinCondition(joinExprs.expr)
    }
  }

  /** @inheritdoc */
  def crossJoin(right: Dataset[_]): DataFrame = buildJoin(right) { builder =>
    builder.setJoinType(proto.Join.JoinType.JOIN_TYPE_CROSS)
  }

  /** @inheritdoc */
  def joinWith[U](other: Dataset[U], condition: Column, joinType: String): Dataset[(T, U)] = {
    val joinTypeValue = toJoinType(joinType, skipSemiAnti = true)
    val (leftNullable, rightNullable) = joinTypeValue match {
      case proto.Join.JoinType.JOIN_TYPE_INNER | proto.Join.JoinType.JOIN_TYPE_CROSS =>
        (false, false)
      case proto.Join.JoinType.JOIN_TYPE_FULL_OUTER =>
        (true, true)
      case proto.Join.JoinType.JOIN_TYPE_LEFT_OUTER =>
        (false, true)
      case proto.Join.JoinType.JOIN_TYPE_RIGHT_OUTER =>
        (true, false)
      case e =>
        throw new IllegalArgumentException(s"Unsupported join type '$e'.")
    }

    val tupleEncoder =
      ProductEncoder[(T, U)](
        ClassTag(SparkClassUtils.getContextOrSparkClassLoader.loadClass(s"scala.Tuple2")),
        Seq(
          EncoderField(s"_1", this.agnosticEncoder, leftNullable, Metadata.empty),
          EncoderField(s"_2", other.agnosticEncoder, rightNullable, Metadata.empty)),
        None)

    sparkSession.newDataset(tupleEncoder) { builder =>
      val joinBuilder = builder.getJoinBuilder
      joinBuilder
        .setLeft(plan.getRoot)
        .setRight(other.plan.getRoot)
        .setJoinType(joinTypeValue)
        .setJoinCondition(condition.expr)
        .setJoinDataType(joinBuilder.getJoinDataTypeBuilder
          .setIsLeftStruct(this.agnosticEncoder.isStruct)
          .setIsRightStruct(other.agnosticEncoder.isStruct))
    }
  }

  override protected def sortInternal(global: Boolean, sortCols: Seq[Column]): Dataset[T] = {
    val sortExprs = sortCols.map { c =>
      ColumnNodeToProtoConverter(c.sortOrder).getSortOrder
    }
    sparkSession.newDataset(agnosticEncoder) { builder =>
      builder.getSortBuilder
        .setInput(plan.getRoot)
        .setIsGlobal(global)
        .addAllOrder(sortExprs.asJava)
    }
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  def hint(name: String, parameters: Any*): Dataset[T] =
    sparkSession.newDataset(agnosticEncoder) { builder =>
      builder.getHintBuilder
        .setInput(plan.getRoot)
        .setName(name)
        .addAllParameters(parameters.map(p => functions.lit(p).expr).asJava)
    }

  private def getPlanId: Option[Long] =
    if (plan.getRoot.hasCommon && plan.getRoot.getCommon.hasPlanId) {
      Option(plan.getRoot.getCommon.getPlanId)
    } else {
      None
    }

  /** @inheritdoc */
  def col(colName: String): Column = new Column(colName, getPlanId)

  /** @inheritdoc */
  def metadataColumn(colName: String): Column = {
    Column(UnresolvedAttribute(colName, getPlanId, isMetadataColumn = true))
  }

  /** @inheritdoc */
  def colRegex(colName: String): Column = {
    Column(UnresolvedRegex(colName, getPlanId))
  }

  /** @inheritdoc */
  def as(alias: String): Dataset[T] = sparkSession.newDataset(agnosticEncoder) { builder =>
    builder.getSubqueryAliasBuilder
      .setInput(plan.getRoot)
      .setAlias(alias)
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  def select(cols: Column*): DataFrame =
    selectUntyped(UnboundRowEncoder, cols).asInstanceOf[DataFrame]

  /** @inheritdoc */
  def select[U1](c1: TypedColumn[T, U1]): Dataset[U1] = {
    val encoder = agnosticEncoderFor(c1.encoder)
    val col = if (encoder.schema == encoder.dataType) {
      functions.inline(functions.array(c1))
    } else {
      c1
    }
    sparkSession.newDataset(encoder) { builder =>
      builder.getProjectBuilder
        .setInput(plan.getRoot)
        .addExpressions(col.typedExpr(this.encoder))
    }
  }

  /** @inheritdoc */
  protected def selectUntyped(columns: TypedColumn[_, _]*): Dataset[_] = {
    val encoder = ProductEncoder.tuple(columns.map(c => agnosticEncoderFor(c.encoder)))
    selectUntyped(encoder, columns)
  }

  /**
   * Internal helper function for all select methods. The only difference between the select
   * methods and typed select methods is the encoder used to build the return dataset.
   */
  private def selectUntyped(encoder: AgnosticEncoder[_], cols: Seq[Column]): Dataset[_] = {
    sparkSession.newDataset(encoder) { builder =>
      builder.getProjectBuilder
        .setInput(plan.getRoot)
        .addAllExpressions(cols.map(_.typedExpr(this.encoder)).asJava)
    }
  }

  /** @inheritdoc */
  def filter(condition: Column): Dataset[T] = sparkSession.newDataset(agnosticEncoder) {
    builder =>
      builder.getFilterBuilder.setInput(plan.getRoot).setCondition(condition.expr)
  }

  private def buildUnpivot(
      ids: Array[Column],
      valuesOption: Option[Array[Column]],
      variableColumnName: String,
      valueColumnName: String): DataFrame = sparkSession.newDataFrame { builder =>
    val unpivot = builder.getUnpivotBuilder
      .setInput(plan.getRoot)
      .addAllIds(ids.toImmutableArraySeq.map(_.expr).asJava)
      .setVariableColumnName(variableColumnName)
      .setValueColumnName(valueColumnName)
    valuesOption.foreach { values =>
      unpivot.getValuesBuilder
        .addAllValues(values.toImmutableArraySeq.map(_.expr).asJava)
    }
  }

  private def buildTranspose(indices: Seq[Column]): DataFrame =
    sparkSession.newDataFrame { builder =>
      val transpose = builder.getTransposeBuilder.setInput(plan.getRoot)
      indices.foreach { indexColumn =>
        transpose.addIndexColumns(indexColumn.expr)
      }
    }

  /** @inheritdoc */
  @scala.annotation.varargs
  def groupBy(cols: Column*): RelationalGroupedDataset = {
    new RelationalGroupedDataset(toDF(), cols, proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY)
  }

  /** @inheritdoc */
  def reduce(func: (T, T) => T): T = {
    val udf = SparkUserDefinedFunction(
      function = func,
      inputEncoders = agnosticEncoder :: agnosticEncoder :: Nil,
      outputEncoder = agnosticEncoder)
    val reduceExpr = Column.fn("reduce", udf.apply(col("*"), col("*"))).expr

    val result = sparkSession
      .newDataset(agnosticEncoder) { builder =>
        builder.getAggregateBuilder
          .setInput(plan.getRoot)
          .addAggregateExpressions(reduceExpr)
          .setGroupType(proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY)
      }
      .collect()
    assert(result.length == 1)
    result(0)
  }

  /** @inheritdoc */
  def groupByKey[K: Encoder](func: T => K): KeyValueGroupedDataset[K, T] = {
    KeyValueGroupedDatasetImpl[K, T](this, agnosticEncoderFor[K], func)
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  def rollup(cols: Column*): RelationalGroupedDataset = {
    new RelationalGroupedDataset(toDF(), cols, proto.Aggregate.GroupType.GROUP_TYPE_ROLLUP)
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  def cube(cols: Column*): RelationalGroupedDataset = {
    new RelationalGroupedDataset(toDF(), cols, proto.Aggregate.GroupType.GROUP_TYPE_CUBE)
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  def groupingSets(groupingSets: Seq[Seq[Column]], cols: Column*): RelationalGroupedDataset = {
    val groupingSetMsgs = groupingSets.map { groupingSet =>
      val groupingSetMsg = proto.Aggregate.GroupingSets.newBuilder()
      for (groupCol <- groupingSet) {
        groupingSetMsg.addGroupingSet(groupCol.expr)
      }
      groupingSetMsg.build()
    }
    new RelationalGroupedDataset(
      toDF(),
      cols,
      proto.Aggregate.GroupType.GROUP_TYPE_GROUPING_SETS,
      groupingSets = Some(groupingSetMsgs))
  }

  /** @inheritdoc */
  def unpivot(
      ids: Array[Column],
      values: Array[Column],
      variableColumnName: String,
      valueColumnName: String): DataFrame = {
    buildUnpivot(ids, Option(values), variableColumnName, valueColumnName)
  }

  /** @inheritdoc */
  def unpivot(
      ids: Array[Column],
      variableColumnName: String,
      valueColumnName: String): DataFrame = {
    buildUnpivot(ids, None, variableColumnName, valueColumnName)
  }

  /** @inheritdoc */
  def transpose(indexColumn: Column): DataFrame =
    buildTranspose(Seq(indexColumn))

  /** @inheritdoc */
  def transpose(): DataFrame =
    buildTranspose(Seq.empty)

  /** @inheritdoc */
  def limit(n: Int): Dataset[T] = sparkSession.newDataset(agnosticEncoder) { builder =>
    builder.getLimitBuilder
      .setInput(plan.getRoot)
      .setLimit(n)
  }

  /** @inheritdoc */
  def offset(n: Int): Dataset[T] = sparkSession.newDataset(agnosticEncoder) { builder =>
    builder.getOffsetBuilder
      .setInput(plan.getRoot)
      .setOffset(n)
  }

  private def buildSetOp(right: Dataset[T], setOpType: proto.SetOperation.SetOpType)(
      f: proto.SetOperation.Builder => Unit): Dataset[T] = {
    checkSameSparkSession(right)
    sparkSession.newDataset(agnosticEncoder) { builder =>
      f(
        builder.getSetOpBuilder
          .setSetOpType(setOpType)
          .setLeftInput(plan.getRoot)
          .setRightInput(right.plan.getRoot))
    }
  }

  private def checkSameSparkSession(other: Dataset[_]): Unit = {
    if (this.sparkSession.sessionId != other.sparkSession.sessionId) {
      throw new SparkException(
        errorClass = "CONNECT.SESSION_NOT_SAME",
        messageParameters = Map.empty,
        cause = null)
    }
  }

  /** @inheritdoc */
  def union(other: Dataset[T]): Dataset[T] = {
    buildSetOp(other, proto.SetOperation.SetOpType.SET_OP_TYPE_UNION) { builder =>
      builder.setIsAll(true)
    }
  }

  /** @inheritdoc */
  def unionByName(other: Dataset[T], allowMissingColumns: Boolean): Dataset[T] = {
    buildSetOp(other, proto.SetOperation.SetOpType.SET_OP_TYPE_UNION) { builder =>
      builder.setByName(true).setIsAll(true).setAllowMissingColumns(allowMissingColumns)
    }
  }

  /** @inheritdoc */
  def intersect(other: Dataset[T]): Dataset[T] = {
    buildSetOp(other, proto.SetOperation.SetOpType.SET_OP_TYPE_INTERSECT) { builder =>
      builder.setIsAll(false)
    }
  }

  /** @inheritdoc */
  def intersectAll(other: Dataset[T]): Dataset[T] = {
    buildSetOp(other, proto.SetOperation.SetOpType.SET_OP_TYPE_INTERSECT) { builder =>
      builder.setIsAll(true)
    }
  }

  /** @inheritdoc */
  def except(other: Dataset[T]): Dataset[T] = {
    buildSetOp(other, proto.SetOperation.SetOpType.SET_OP_TYPE_EXCEPT) { builder =>
      builder.setIsAll(false)
    }
  }

  /** @inheritdoc */
  def exceptAll(other: Dataset[T]): Dataset[T] = {
    buildSetOp(other, proto.SetOperation.SetOpType.SET_OP_TYPE_EXCEPT) { builder =>
      builder.setIsAll(true)
    }
  }

  /** @inheritdoc */
  def sample(withReplacement: Boolean, fraction: Double, seed: Long): Dataset[T] = {
    sparkSession.newDataset(agnosticEncoder) { builder =>
      builder.getSampleBuilder
        .setInput(plan.getRoot)
        .setWithReplacement(withReplacement)
        .setLowerBound(0.0d)
        .setUpperBound(fraction)
        .setSeed(seed)
    }
  }

  /** @inheritdoc */
  def randomSplit(weights: Array[Double], seed: Long): Array[Dataset[T]] = {
    require(
      weights.forall(_ >= 0),
      s"Weights must be nonnegative, but got ${weights.mkString("[", ",", "]")}")
    require(
      weights.sum > 0,
      s"Sum of weights must be positive, but got ${weights.mkString("[", ",", "]")}")

    // It is possible that the underlying dataframe doesn't guarantee the ordering of rows in its
    // constituent partitions each time a split is materialized which could result in
    // overlapping splits. To prevent this, we explicitly sort each input partition to make the
    // ordering deterministic. Note that MapTypes cannot be sorted and are explicitly pruned out
    // from the sort order.
    // TODO we need to have a proper way of stabilizing the input data. The current approach does
    //  not work well with spark connects' extremely lazy nature. When the schema is modified
    //  between construction and execution the query might fail or produce wrong results. Another
    //  problem can come from data that arrives between the execution of the returned datasets.
    val sortOrder = schema.collect {
      case f if OrderUtils.isOrderable(f.dataType) => col(f.name).asc
    }
    val sortedInput = sortWithinPartitions(sortOrder: _*).plan.getRoot
    val sum = weights.sum
    val normalizedCumWeights = weights.map(_ / sum).scanLeft(0.0d)(_ + _)
    normalizedCumWeights
      .sliding(2)
      .map { case Array(low, high) =>
        sparkSession.newDataset(agnosticEncoder) { builder =>
          builder.getSampleBuilder
            .setInput(sortedInput)
            .setWithReplacement(false)
            .setLowerBound(low)
            .setUpperBound(high)
            .setSeed(seed)
        }
      }
      .toArray
  }

  /** @inheritdoc */
  override def randomSplitAsList(weights: Array[Double], seed: Long): util.List[Dataset[T]] =
    util.Arrays.asList(randomSplit(weights, seed): _*)

  /** @inheritdoc */
  override def randomSplit(weights: Array[Double]): Array[Dataset[T]] =
    randomSplit(weights, SparkClassUtils.random.nextLong())

  /** @inheritdoc */
  protected def withColumns(names: Seq[String], values: Seq[Column]): DataFrame = {
    require(
      names.size == values.size,
      s"The size of column names: ${names.size} isn't equal to " +
        s"the size of columns: ${values.size}")
    val aliases = values.zip(names).map { case (value, name) =>
      value.name(name).expr.getAlias
    }
    sparkSession.newDataFrame { builder =>
      builder.getWithColumnsBuilder
        .setInput(plan.getRoot)
        .addAllAliases(aliases.asJava)
    }
  }

  override protected def withColumnsRenamed(
      colNames: Seq[String],
      newColNames: Seq[String]): DataFrame = {
    require(
      colNames.size == newColNames.size,
      s"The size of existing column names: ${colNames.size} isn't equal to " +
        s"the size of new column names: ${newColNames.size}")
    sparkSession.newDataFrame { builder =>
      val b = builder.getWithColumnsRenamedBuilder
        .setInput(plan.getRoot)
      colNames.zip(newColNames).foreach { case (colName, newColName) =>
        b.addRenames(
          proto.WithColumnsRenamed.Rename
            .newBuilder()
            .setColName(colName)
            .setNewColName(newColName))
      }
    }
  }

  /** @inheritdoc */
  def withMetadata(columnName: String, metadata: Metadata): DataFrame = {
    val newAlias = proto.Expression.Alias
      .newBuilder()
      .setExpr(col(columnName).expr)
      .addName(columnName)
      .setMetadata(metadata.json)
    sparkSession.newDataFrame { builder =>
      builder.getWithColumnsBuilder
        .setInput(plan.getRoot)
        .addAliases(newAlias)
    }
  }

  protected def createTempView(viewName: String, replace: Boolean, global: Boolean): Unit = {
    val command = sparkSession.newCommand { builder =>
      builder.getCreateDataframeViewBuilder
        .setInput(plan.getRoot)
        .setName(viewName)
        .setIsGlobal(global)
        .setReplace(replace)
    }
    sparkSession.execute(command)
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  def drop(colNames: String*): DataFrame = buildDropByNames(colNames)

  /** @inheritdoc */
  @scala.annotation.varargs
  def drop(col: Column, cols: Column*): DataFrame = buildDrop(col +: cols)

  private def buildDrop(cols: Seq[Column]): DataFrame = sparkSession.newDataFrame { builder =>
    builder.getDropBuilder
      .setInput(plan.getRoot)
      .addAllColumns(cols.map(_.expr).asJava)
  }

  private def buildDropByNames(cols: Seq[String]): DataFrame = sparkSession.newDataFrame {
    builder =>
      builder.getDropBuilder
        .setInput(plan.getRoot)
        .addAllColumnNames(cols.asJava)
  }

  private def buildDropDuplicates(
      columns: Option[Seq[String]],
      withinWaterMark: Boolean): Dataset[T] = sparkSession.newDataset(agnosticEncoder) {
    builder =>
      val dropBuilder = builder.getDeduplicateBuilder
        .setInput(plan.getRoot)
        .setWithinWatermark(withinWaterMark)
      if (columns.isDefined) {
        dropBuilder.addAllColumnNames(columns.get.asJava)
      } else {
        dropBuilder.setAllColumnsAsKeys(true)
      }
  }

  /** @inheritdoc */
  def dropDuplicates(): Dataset[T] = buildDropDuplicates(None, withinWaterMark = false)

  /** @inheritdoc */
  def dropDuplicates(colNames: Seq[String]): Dataset[T] = {
    buildDropDuplicates(Option(colNames), withinWaterMark = false)
  }

  /** @inheritdoc */
  def dropDuplicatesWithinWatermark(): Dataset[T] =
    buildDropDuplicates(None, withinWaterMark = true)

  /** @inheritdoc */
  def dropDuplicatesWithinWatermark(colNames: Seq[String]): Dataset[T] = {
    buildDropDuplicates(Option(colNames), withinWaterMark = true)
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  override def describe(cols: String*): DataFrame = sparkSession.newDataFrame { builder =>
    builder.getDescribeBuilder
      .setInput(plan.getRoot)
      .addAllCols(cols.asJava)
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  def summary(statistics: String*): DataFrame = sparkSession.newDataFrame { builder =>
    builder.getSummaryBuilder
      .setInput(plan.getRoot)
      .addAllStatistics(statistics.asJava)
  }

  /** @inheritdoc */
  def head(n: Int): Array[T] = limit(n).collect()

  /** @inheritdoc */
  def filter(func: T => Boolean): Dataset[T] = {
    val udf = SparkUserDefinedFunction(
      function = func,
      inputEncoders = agnosticEncoder :: Nil,
      outputEncoder = PrimitiveBooleanEncoder)
    sparkSession.newDataset[T](agnosticEncoder) { builder =>
      builder.getFilterBuilder
        .setInput(plan.getRoot)
        .setCondition(udf.apply(col("*")).expr)
    }
  }

  /** @inheritdoc */
  def filter(f: FilterFunction[T]): Dataset[T] = {
    filter(ToScalaUDF(f))
  }

  /** @inheritdoc */
  def map[U: Encoder](f: T => U): Dataset[U] = {
    mapPartitions(UDFAdaptors.mapToMapPartitions(f))
  }

  /** @inheritdoc */
  def map[U](f: MapFunction[T, U], encoder: Encoder[U]): Dataset[U] = {
    mapPartitions(UDFAdaptors.mapToMapPartitions(f))(encoder)
  }

  /** @inheritdoc */
  def mapPartitions[U: Encoder](func: Iterator[T] => Iterator[U]): Dataset[U] = {
    val outputEncoder = agnosticEncoderFor[U]
    val udf = SparkUserDefinedFunction(
      function = func,
      inputEncoders = agnosticEncoder :: Nil,
      outputEncoder = outputEncoder)
    sparkSession.newDataset(outputEncoder) { builder =>
      builder.getMapPartitionsBuilder
        .setInput(plan.getRoot)
        .setFunc(udf.apply(col("*")).expr.getCommonInlineUserDefinedFunction)
    }
  }

  /** @inheritdoc */
  @deprecated("use flatMap() or select() with functions.explode() instead", "3.5.0")
  def explode[A <: Product: TypeTag](input: Column*)(f: Row => IterableOnce[A]): DataFrame = {
    val generator = SparkUserDefinedFunction(
      UDFAdaptors.iterableOnceToSeq(f),
      UnboundRowEncoder :: Nil,
      ScalaReflection.encoderFor[Seq[A]])
    select(col("*"), functions.inline(generator(struct(input: _*))))
  }

  /** @inheritdoc */
  @deprecated("use flatMap() or select() with functions.explode() instead", "3.5.0")
  def explode[A, B: TypeTag](inputColumn: String, outputColumn: String)(
      f: A => IterableOnce[B]): DataFrame = {
    val generator = SparkUserDefinedFunction(
      UDFAdaptors.iterableOnceToSeq(f),
      Nil,
      ScalaReflection.encoderFor[Seq[B]])
    select(col("*"), functions.explode(generator(col(inputColumn))).as((outputColumn)))
  }

  /** @inheritdoc */
  def foreachPartition(f: Iterator[T] => Unit): Unit = {
    // Delegate to mapPartition with empty result.
    mapPartitions(UDFAdaptors.foreachPartitionToMapPartitions(f))(NullEncoder).collect()
  }

  /** @inheritdoc */
  def tail(n: Int): Array[T] = {
    val lastN = sparkSession.newDataset(agnosticEncoder) { builder =>
      builder.getTailBuilder
        .setInput(plan.getRoot)
        .setLimit(n)
    }
    lastN.collect()
  }

  /** @inheritdoc */
  def collect(): Array[T] = withResult { result =>
    result.toArray
  }

  /** @inheritdoc */
  def collectAsList(): java.util.List[T] = {
    java.util.Arrays.asList(collect(): _*)
  }

  /** @inheritdoc */
  def toLocalIterator(): java.util.Iterator[T] = {
    collectResult().destructiveIterator.asJava
  }

  /** @inheritdoc */
  def count(): Long = {
    groupBy().count().as(PrimitiveLongEncoder).collect().head
  }

  private def buildRepartition(numPartitions: Int, shuffle: Boolean): Dataset[T] = {
    sparkSession.newDataset(agnosticEncoder) { builder =>
      builder.getRepartitionBuilder
        .setInput(plan.getRoot)
        .setNumPartitions(numPartitions)
        .setShuffle(shuffle)
    }
  }

  private def buildRepartitionByExpression(
      numPartitions: Option[Int],
      partitionExprs: Seq[Column]): Dataset[T] = sparkSession.newDataset(agnosticEncoder) {
    builder =>
      val repartitionBuilder = builder.getRepartitionByExpressionBuilder
        .setInput(plan.getRoot)
        .addAllPartitionExprs(partitionExprs.map(_.expr).asJava)
      numPartitions.foreach(repartitionBuilder.setNumPartitions)
  }

  /** @inheritdoc */
  def repartition(numPartitions: Int): Dataset[T] = {
    buildRepartition(numPartitions, shuffle = true)
  }

  protected def repartitionByExpression(
      numPartitions: Option[Int],
      partitionExprs: Seq[Column]): Dataset[T] = {
    // The underlying `LogicalPlan` operator special-cases all-`SortOrder` arguments.
    // However, we don't want to complicate the semantics of this API method.
    // Instead, let's give users a friendly error message, pointing them to the new method.
    val sortOrders = partitionExprs.filter(_.expr.hasSortOrder)
    if (sortOrders.nonEmpty) {
      throw new IllegalArgumentException(
        s"Invalid partitionExprs specified: $sortOrders\n" +
          s"For range partitioning use repartitionByRange(...) instead.")
    }
    buildRepartitionByExpression(numPartitions, partitionExprs)
  }

  protected def repartitionByRange(
      numPartitions: Option[Int],
      partitionExprs: Seq[Column]): Dataset[T] = {
    require(partitionExprs.nonEmpty, "At least one partition-by expression must be specified.")
    val sortExprs = partitionExprs.map {
      case e if e.expr.hasSortOrder => e
      case e => e.asc
    }
    buildRepartitionByExpression(numPartitions, sortExprs)
  }

  /** @inheritdoc */
  def coalesce(numPartitions: Int): Dataset[T] = {
    buildRepartition(numPartitions, shuffle = false)
  }

  /** @inheritdoc */
  def inputFiles: Array[String] =
    sparkSession
      .analyze(plan, proto.AnalyzePlanRequest.AnalyzeCase.INPUT_FILES)
      .getInputFiles
      .getFilesList
      .asScala
      .toArray

  /** @inheritdoc */
  def write: DataFrameWriter[T] = {
    new DataFrameWriterImpl[T](this)
  }

  /** @inheritdoc */
  def writeTo(table: String): DataFrameWriterV2[T] = {
    new DataFrameWriterV2Impl[T](table, this)
  }

  /** @inheritdoc */
  def mergeInto(table: String, condition: Column): MergeIntoWriter[T] = {
    if (isStreaming) {
      throw new AnalysisException(
        errorClass = "CALL_ON_STREAMING_DATASET_UNSUPPORTED",
        messageParameters = Map("methodName" -> toSQLId("mergeInto")))
    }

    new MergeIntoWriterImpl[T](table, this, condition)
  }

  /** @inheritdoc */
  def writeStream: DataStreamWriter[T] = {
    new DataStreamWriter[T](this)
  }

  /** @inheritdoc */
  override def cache(): this.type = persist()

  /** @inheritdoc */
  def persist(): this.type = {
    sparkSession.analyze { builder =>
      builder.getPersistBuilder.setRelation(plan.getRoot)
    }
    this
  }

  /** @inheritdoc */
  def persist(newLevel: StorageLevel): this.type = {
    sparkSession.analyze { builder =>
      builder.getPersistBuilder
        .setRelation(plan.getRoot)
        .setStorageLevel(StorageLevelProtoConverter.toConnectProtoType(newLevel))
    }
    this
  }

  /** @inheritdoc */
  def unpersist(blocking: Boolean): this.type = {
    sparkSession.analyze { builder =>
      builder.getUnpersistBuilder
        .setRelation(plan.getRoot)
        .setBlocking(blocking)
    }
    this
  }

  /** @inheritdoc */
  override def unpersist(): this.type = unpersist(blocking = false)

  /** @inheritdoc */
  def storageLevel: StorageLevel = {
    StorageLevelProtoConverter.toStorageLevel(
      sparkSession
        .analyze { builder =>
          builder.getGetStorageLevelBuilder.setRelation(plan.getRoot)
        }
        .getGetStorageLevel
        .getStorageLevel)
  }

  /** @inheritdoc */
  def withWatermark(eventTime: String, delayThreshold: String): Dataset[T] = {
    sparkSession.newDataset(agnosticEncoder) { builder =>
      builder.getWithWatermarkBuilder
        .setInput(plan.getRoot)
        .setEventTime(eventTime)
        .setDelayThreshold(delayThreshold)
    }
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  def observe(name: String, expr: Column, exprs: Column*): Dataset[T] = {
    sparkSession.newDataset(agnosticEncoder) { builder =>
      builder.getCollectMetricsBuilder
        .setInput(plan.getRoot)
        .setName(name)
        .addAllMetrics((expr +: exprs).map(_.expr).asJava)
    }
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  def observe(observation: Observation, expr: Column, exprs: Column*): Dataset[T] = {
    val df = observe(observation.name, expr, exprs: _*)
    sparkSession.registerObservation(df.getPlanId.get, observation)
    df
  }

  /** @inheritdoc */
  protected def checkpoint(eager: Boolean, reliableCheckpoint: Boolean): Dataset[T] = {
    sparkSession.newDataset(agnosticEncoder) { builder =>
      val command = sparkSession.newCommand { builder =>
        builder.getCheckpointCommandBuilder
          .setLocal(!reliableCheckpoint)
          .setEager(eager)
          .setRelation(this.plan.getRoot)
      }
      val responseIter = sparkSession.execute(command)
      try {
        val response = responseIter
          .find(_.hasCheckpointCommandResult)
          .getOrElse(throw new RuntimeException("CheckpointCommandResult must be present"))

        val cachedRemoteRelation = response.getCheckpointCommandResult.getRelation
        sparkSession.cleaner.register(cachedRemoteRelation)

        // Update the builder with the values from the result.
        builder.setCachedRemoteRelation(cachedRemoteRelation)
      } finally {
        // consume the rest of the iterator
        responseIter.foreach(_ => ())
      }
    }
  }

  /** @inheritdoc */
  @DeveloperApi
  def sameSemantics(other: Dataset[T]): Boolean = {
    sparkSession.sameSemantics(this.plan, other.plan)
  }

  /** @inheritdoc */
  @DeveloperApi
  def semanticHash(): Int = {
    sparkSession.semanticHash(this.plan)
  }

  /** @inheritdoc */
  def toJSON: Dataset[String] = {
    select(to_json(struct(col("*")))).as(StringEncoder)
  }

  private[sql] def analyze: proto.AnalyzePlanResponse = {
    sparkSession.analyze(plan, proto.AnalyzePlanRequest.AnalyzeCase.SCHEMA)
  }

  def collectResult(): SparkResult[T] = sparkSession.execute(plan, agnosticEncoder)

  private[sql] def withResult[E](f: SparkResult[T] => E): E = {
    val result = collectResult()
    try f(result)
    finally {
      result.close()
    }
  }

  /**
   * We cannot deserialize a connect [[Dataset]] because of a class clash on the server side. We
   * null out the instance for now.
   */
  @scala.annotation.unused("this is used by java serialization")
  private def writeReplace(): Any = null

  ////////////////////////////////////////////////////////////////////////////
  // Return type overrides to make sure we return the implementation instead
  // of the interface. This is done for a couple of reasons:
  // - Retain the old signatures for binary compatibility;
  // - Java compatibility . The java compiler uses the byte code signatures,
  //   and those would point to api.Dataset being returned instead of Dataset.
  //   This causes issues when the java code tries to materialize results, or
  //   tries to use functionality that is implementation specfic.
  // - Scala method resolution runs into problems when the ambiguous methods are
  //   scattered across the interface and implementation. `drop` and `select`
  //   suffered from this.
  ////////////////////////////////////////////////////////////////////////////

  /** @inheritdoc */
  override def drop(colName: String): DataFrame = super.drop(colName)

  /** @inheritdoc */
  override def drop(col: Column): DataFrame = super.drop(col)

  /** @inheritdoc */
  override def join(right: Dataset[_], usingColumn: String): DataFrame =
    super.join(right, usingColumn)

  /** @inheritdoc */
  override def join(right: Dataset[_], usingColumns: Array[String]): DataFrame =
    super.join(right, usingColumns)

  /** @inheritdoc */
  override def join(right: Dataset[_], usingColumns: Seq[String]): DataFrame =
    super.join(right, usingColumns)

  /** @inheritdoc */
  override def join(right: Dataset[_], usingColumn: String, joinType: String): DataFrame =
    super.join(right, usingColumn, joinType)

  /** @inheritdoc */
  override def join(right: Dataset[_], usingColumns: Array[String], joinType: String): DataFrame =
    super.join(right, usingColumns, joinType)

  /** @inheritdoc */
  override def join(right: Dataset[_], joinExprs: Column): DataFrame =
    super.join(right, joinExprs)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def select(col: String, cols: String*): DataFrame = super.select(col, cols: _*)

  /** @inheritdoc */
  override def select[U1, U2](c1: TypedColumn[T, U1], c2: TypedColumn[T, U2]): Dataset[(U1, U2)] =
    super.select(c1, c2)

  /** @inheritdoc */
  override def select[U1, U2, U3](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3]): Dataset[(U1, U2, U3)] =
    super.select(c1, c2, c3)

  /** @inheritdoc */
  override def select[U1, U2, U3, U4](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3],
      c4: TypedColumn[T, U4]): Dataset[(U1, U2, U3, U4)] =
    super.select(c1, c2, c3, c4)

  /** @inheritdoc */
  override def select[U1, U2, U3, U4, U5](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3],
      c4: TypedColumn[T, U4],
      c5: TypedColumn[T, U5]): Dataset[(U1, U2, U3, U4, U5)] =
    super.select(c1, c2, c3, c4, c5)

  override def melt(
      ids: Array[Column],
      values: Array[Column],
      variableColumnName: String,
      valueColumnName: String): DataFrame =
    super.melt(ids, values, variableColumnName, valueColumnName)

  /** @inheritdoc */
  override def melt(
      ids: Array[Column],
      variableColumnName: String,
      valueColumnName: String): DataFrame =
    super.melt(ids, variableColumnName, valueColumnName)

  /** @inheritdoc */
  override def withColumn(colName: String, col: Column): DataFrame =
    super.withColumn(colName, col)

  /** @inheritdoc */
  override def withColumns(colsMap: Map[String, Column]): DataFrame =
    super.withColumns(colsMap)

  /** @inheritdoc */
  override def withColumns(colsMap: util.Map[String, Column]): DataFrame =
    super.withColumns(colsMap)

  /** @inheritdoc */
  override def withColumnRenamed(existingName: String, newName: String): DataFrame =
    super.withColumnRenamed(existingName, newName)

  /** @inheritdoc */
  override def withColumnsRenamed(colsMap: Map[String, String]): DataFrame =
    super.withColumnsRenamed(colsMap)

  /** @inheritdoc */
  override def withColumnsRenamed(colsMap: util.Map[String, String]): DataFrame =
    super.withColumnsRenamed(colsMap)

  /** @inheritdoc */
  override def checkpoint(): Dataset[T] = super.checkpoint()

  /** @inheritdoc */
  override def checkpoint(eager: Boolean): Dataset[T] = super.checkpoint(eager)

  /** @inheritdoc */
  override def localCheckpoint(): Dataset[T] = super.localCheckpoint()

  /** @inheritdoc */
  override def localCheckpoint(eager: Boolean): Dataset[T] = super.localCheckpoint(eager)

  /** @inheritdoc */
  override def joinWith[U](other: Dataset[U], condition: Column): Dataset[(T, U)] =
    super.joinWith(other, condition)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def sortWithinPartitions(sortCol: String, sortCols: String*): Dataset[T] =
    super.sortWithinPartitions(sortCol, sortCols: _*)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def sortWithinPartitions(sortExprs: Column*): Dataset[T] =
    super.sortWithinPartitions(sortExprs: _*)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def sort(sortCol: String, sortCols: String*): Dataset[T] =
    super.sort(sortCol, sortCols: _*)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def sort(sortExprs: Column*): Dataset[T] = super.sort(sortExprs: _*)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def orderBy(sortCol: String, sortCols: String*): Dataset[T] =
    super.orderBy(sortCol, sortCols: _*)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def orderBy(sortExprs: Column*): Dataset[T] = super.orderBy(sortExprs: _*)

  /** @inheritdoc */
  override def as(alias: Symbol): Dataset[T] = super.as(alias)

  /** @inheritdoc */
  override def alias(alias: String): Dataset[T] = super.alias(alias)

  /** @inheritdoc */
  override def alias(alias: Symbol): Dataset[T] = super.alias(alias)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def selectExpr(exprs: String*): DataFrame = super.selectExpr(exprs: _*)

  /** @inheritdoc */
  override def filter(conditionExpr: String): Dataset[T] = super.filter(conditionExpr)

  /** @inheritdoc */
  override def where(condition: Column): Dataset[T] = super.where(condition)

  /** @inheritdoc */
  override def where(conditionExpr: String): Dataset[T] = super.where(conditionExpr)

  /** @inheritdoc */
  override def unionAll(other: Dataset[T]): Dataset[T] = super.unionAll(other)

  /** @inheritdoc */
  override def unionByName(other: Dataset[T]): Dataset[T] = super.unionByName(other)

  /** @inheritdoc */
  override def sample(fraction: Double, seed: Long): Dataset[T] = super.sample(fraction, seed)

  /** @inheritdoc */
  override def sample(fraction: Double): Dataset[T] = super.sample(fraction)

  /** @inheritdoc */
  override def sample(withReplacement: Boolean, fraction: Double): Dataset[T] =
    super.sample(withReplacement, fraction)

  /** @inheritdoc */
  override def dropDuplicates(colNames: Array[String]): Dataset[T] =
    super.dropDuplicates(colNames)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def dropDuplicates(col1: String, cols: String*): Dataset[T] =
    super.dropDuplicates(col1, cols: _*)

  /** @inheritdoc */
  override def dropDuplicatesWithinWatermark(colNames: Array[String]): Dataset[T] =
    super.dropDuplicatesWithinWatermark(colNames)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def dropDuplicatesWithinWatermark(col1: String, cols: String*): Dataset[T] =
    super.dropDuplicatesWithinWatermark(col1, cols: _*)

  /** @inheritdoc */
  override def mapPartitions[U](f: MapPartitionsFunction[T, U], encoder: Encoder[U]): Dataset[U] =
    super.mapPartitions(f, encoder)

  /** @inheritdoc */
  override def flatMap[U: Encoder](func: T => IterableOnce[U]): Dataset[U] =
    super.flatMap(func)

  /** @inheritdoc */
  override def flatMap[U](f: FlatMapFunction[T, U], encoder: Encoder[U]): Dataset[U] =
    super.flatMap(f, encoder)

  /** @inheritdoc */
  override def foreachPartition(func: ForeachPartitionFunction[T]): Unit =
    super.foreachPartition(func)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def repartition(numPartitions: Int, partitionExprs: Column*): Dataset[T] =
    super.repartition(numPartitions, partitionExprs: _*)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def repartition(partitionExprs: Column*): Dataset[T] =
    super.repartition(partitionExprs: _*)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def repartitionByRange(numPartitions: Int, partitionExprs: Column*): Dataset[T] =
    super.repartitionByRange(numPartitions, partitionExprs: _*)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def repartitionByRange(partitionExprs: Column*): Dataset[T] =
    super.repartitionByRange(partitionExprs: _*)

  /** @inheritdoc */
  override def distinct(): Dataset[T] = super.distinct()

  /** @inheritdoc */
  @scala.annotation.varargs
  override def groupBy(col1: String, cols: String*): RelationalGroupedDataset =
    super.groupBy(col1, cols: _*)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def rollup(col1: String, cols: String*): RelationalGroupedDataset =
    super.rollup(col1, cols: _*)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def cube(col1: String, cols: String*): RelationalGroupedDataset =
    super.cube(col1, cols: _*)

  /** @inheritdoc */
  override def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame =
    super.agg(aggExpr, aggExprs: _*)

  /** @inheritdoc */
  override def agg(exprs: Map[String, String]): DataFrame = super.agg(exprs)

  /** @inheritdoc */
  override def agg(exprs: java.util.Map[String, String]): DataFrame = super.agg(exprs)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def agg(expr: Column, exprs: Column*): DataFrame = super.agg(expr, exprs: _*)

  /** @inheritdoc */
  override def groupByKey[K](
      func: MapFunction[T, K],
      encoder: Encoder[K]): KeyValueGroupedDataset[K, T] =
    super.groupByKey(func, encoder).asInstanceOf[KeyValueGroupedDataset[K, T]]
}
