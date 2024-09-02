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

import java.io.{ByteArrayOutputStream, CharArrayWriter, DataOutputStream}
import java.util

import scala.collection.mutable.{ArrayBuffer, HashSet}
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.control.NonFatal

import org.apache.commons.lang3.StringUtils
import org.apache.commons.text.StringEscapeUtils

import org.apache.spark.TaskContext
import org.apache.spark.annotation.{DeveloperApi, Stable, Unstable}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function._
import org.apache.spark.api.python.{PythonRDD, SerDeUtil}
import org.apache.spark.api.r.RRDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow, QueryPlanningTracker, ScalaReflection, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.json.{JacksonGenerator, JSONOptions}
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserUtils}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.{TreeNodeTag, TreePattern}
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, IntervalUtils}
import org.apache.spark.sql.catalyst.util.TypeUtils.toSQLId
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.TypedAggregateExpression
import org.apache.spark.sql.execution.arrow.{ArrowBatchStreamWriter, ArrowConverters}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation, FileTable}
import org.apache.spark.sql.execution.python.EvaluatePython
import org.apache.spark.sql.execution.stat.StatFunctions
import org.apache.spark.sql.internal.ExpressionUtils.column
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.TypedAggUtils.withInputType
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

private[sql] object Dataset {
  val curId = new java.util.concurrent.atomic.AtomicLong()
  val DATASET_ID_KEY = "__dataset_id"
  val COL_POS_KEY = "__col_position"
  val DATASET_ID_TAG = TreeNodeTag[HashSet[Long]]("dataset_id")

  def apply[T: Encoder](sparkSession: SparkSession, logicalPlan: LogicalPlan): Dataset[T] = {
    val dataset = new Dataset(sparkSession, logicalPlan, implicitly[Encoder[T]])
    // Eagerly bind the encoder so we verify that the encoder matches the underlying
    // schema. The user will get an error if this is not the case.
    // optimization: it is guaranteed that [[InternalRow]] can be converted to [[Row]] so
    // do not do this check in that case. this check can be expensive since it requires running
    // the whole [[Analyzer]] to resolve the deserializer
    if (dataset.exprEnc.clsTag.runtimeClass != classOf[Row]) {
      dataset.resolvedEnc
    }
    dataset
  }

  def ofRows(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame =
    sparkSession.withActive {
      val qe = sparkSession.sessionState.executePlan(logicalPlan)
      qe.assertAnalyzed()
      new Dataset[Row](qe, ExpressionEncoder(qe.analyzed.schema))
  }

  def ofRows(
      sparkSession: SparkSession,
      logicalPlan: LogicalPlan,
      shuffleCleanupMode: ShuffleCleanupMode): DataFrame =
    sparkSession.withActive {
      val qe = new QueryExecution(
        sparkSession, logicalPlan, shuffleCleanupMode = shuffleCleanupMode)
      qe.assertAnalyzed()
      new Dataset[Row](qe, ExpressionEncoder(qe.analyzed.schema))
    }

  /** A variant of ofRows that allows passing in a tracker so we can track query parsing time. */
  def ofRows(
      sparkSession: SparkSession,
      logicalPlan: LogicalPlan,
      tracker: QueryPlanningTracker,
      shuffleCleanupMode: ShuffleCleanupMode = DoNotCleanup)
    : DataFrame = sparkSession.withActive {
    val qe = new QueryExecution(
      sparkSession, logicalPlan, tracker, shuffleCleanupMode = shuffleCleanupMode)
    qe.assertAnalyzed()
    new Dataset[Row](qe, ExpressionEncoder(qe.analyzed.schema))
  }
}

/**
 * A Dataset is a strongly typed collection of domain-specific objects that can be transformed
 * in parallel using functional or relational operations. Each Dataset also has an untyped view
 * called a `DataFrame`, which is a Dataset of [[Row]].
 *
 * Operations available on Datasets are divided into transformations and actions. Transformations
 * are the ones that produce new Datasets, and actions are the ones that trigger computation and
 * return results. Example transformations include map, filter, select, and aggregate (`groupBy`).
 * Example actions count, show, or writing data out to file systems.
 *
 * Datasets are "lazy", i.e. computations are only triggered when an action is invoked. Internally,
 * a Dataset represents a logical plan that describes the computation required to produce the data.
 * When an action is invoked, Spark's query optimizer optimizes the logical plan and generates a
 * physical plan for efficient execution in a parallel and distributed manner. To explore the
 * logical plan as well as optimized physical plan, use the `explain` function.
 *
 * To efficiently support domain-specific objects, an [[Encoder]] is required. The encoder maps
 * the domain specific type `T` to Spark's internal type system. For example, given a class `Person`
 * with two fields, `name` (string) and `age` (int), an encoder is used to tell Spark to generate
 * code at runtime to serialize the `Person` object into a binary structure. This binary structure
 * often has much lower memory footprint as well as are optimized for efficiency in data processing
 * (e.g. in a columnar format). To understand the internal binary representation for data, use the
 * `schema` function.
 *
 * There are typically two ways to create a Dataset. The most common way is by pointing Spark
 * to some files on storage systems, using the `read` function available on a `SparkSession`.
 * {{{
 *   val people = spark.read.parquet("...").as[Person]  // Scala
 *   Dataset<Person> people = spark.read().parquet("...").as(Encoders.bean(Person.class)); // Java
 * }}}
 *
 * Datasets can also be created through transformations available on existing Datasets. For example,
 * the following creates a new Dataset by applying a filter on the existing one:
 * {{{
 *   val names = people.map(_.name)  // in Scala; names is a Dataset[String]
 *   Dataset<String> names = people.map(
 *     (MapFunction<Person, String>) p -> p.name, Encoders.STRING()); // Java
 * }}}
 *
 * Dataset operations can also be untyped, through various domain-specific-language (DSL)
 * functions defined in: Dataset (this class), [[Column]], and [[functions]]. These operations
 * are very similar to the operations available in the data frame abstraction in R or Python.
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
 * @since 1.6.0
 */
@Stable
class Dataset[T] private[sql](
    @DeveloperApi @Unstable @transient val queryExecution: QueryExecution,
    @DeveloperApi @Unstable @transient val encoder: Encoder[T])
  extends api.Dataset[T, Dataset] {
  type RGD = RelationalGroupedDataset

  @transient lazy val sparkSession: SparkSession = {
    if (queryExecution == null || queryExecution.sparkSession == null) {
      throw QueryExecutionErrors.transformationsAndActionsNotInvokedByDriverError()
    }
    queryExecution.sparkSession
  }

  import sparkSession.RichColumn

  // A globally unique id of this Dataset.
  private[sql] val id = Dataset.curId.getAndIncrement()

  queryExecution.assertAnalyzed()

  // Note for Spark contributors: if adding or updating any action in `Dataset`, please make sure
  // you wrap it with `withNewExecutionId` if this actions doesn't call other action.

  def this(sparkSession: SparkSession, logicalPlan: LogicalPlan, encoder: Encoder[T]) = {
    this(sparkSession.sessionState.executePlan(logicalPlan), encoder)
  }

  def this(sqlContext: SQLContext, logicalPlan: LogicalPlan, encoder: Encoder[T]) = {
    this(sqlContext.sparkSession, logicalPlan, encoder)
  }

  @transient private[sql] val logicalPlan: LogicalPlan = {
    val plan = queryExecution.commandExecuted
    if (sparkSession.conf.get(SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED)) {
      val dsIds = plan.getTagValue(Dataset.DATASET_ID_TAG).getOrElse(new HashSet[Long])
      dsIds.add(id)
      plan.setTagValue(Dataset.DATASET_ID_TAG, dsIds)
    }
    plan
  }

  /**
   * Currently [[ExpressionEncoder]] is the only implementation of [[Encoder]], here we turn the
   * passed in encoder to [[ExpressionEncoder]] explicitly, and mark it implicit so that we can use
   * it when constructing new Dataset objects that have the same object type (that will be
   * possibly resolved to a different schema).
   */
  private[sql] implicit val exprEnc: ExpressionEncoder[T] = encoderFor(encoder)

  // The resolved `ExpressionEncoder` which can be used to turn rows to objects of type T, after
  // collecting rows to the driver side.
  private lazy val resolvedEnc = {
    exprEnc.resolveAndBind(logicalPlan.output, sparkSession.sessionState.analyzer)
  }

  private implicit def classTag: ClassTag[T] = exprEnc.clsTag

  // sqlContext must be val because a stable identifier is expected when you import implicits
  @transient lazy val sqlContext: SQLContext = sparkSession.sqlContext

  private[sql] def resolve(colName: String): NamedExpression = {
    val resolver = sparkSession.sessionState.analyzer.resolver
    queryExecution.analyzed.resolveQuoted(colName, resolver)
      .getOrElse(throw QueryCompilationErrors.unresolvedColumnError(colName, schema.fieldNames))
  }

  private[sql] def numericColumns: Seq[Expression] = {
    schema.fields.filter(_.dataType.isInstanceOf[NumericType]).map { n =>
      queryExecution.analyzed.resolveQuoted(n.name, sparkSession.sessionState.analyzer.resolver).get
    }.toImmutableArraySeq
  }

  /**
   * Get rows represented in Sequence by specific truncate and vertical requirement.
   *
   * @param numRows Number of rows to return
   * @param truncate If set to more than 0, truncates strings to `truncate` characters and
   *                   all cells will be aligned right.
   */
  private[sql] def getRows(
      numRows: Int,
      truncate: Int): Seq[Seq[String]] = {
    val newDf = commandResultOptimized.toDF()
    val castCols = newDf.logicalPlan.output.map { col =>
      column(ToPrettyString(col))
    }
    val data = newDf.select(castCols: _*).take(numRows + 1)

    // For array values, replace Seq and Array with square brackets
    // For cells that are beyond `truncate` characters, replace it with the
    // first `truncate-3` and "..."
    (schema.fieldNames
      .map(SchemaUtils.escapeMetaCharacters).toImmutableArraySeq +: data.map { row =>
      row.toSeq.map { cell =>
        assert(cell != null, "ToPrettyString is not nullable and should not return null value")
        // Escapes meta-characters not to break the `showString` format
        val str = SchemaUtils.escapeMetaCharacters(cell.toString)
        if (truncate > 0 && str.length > truncate) {
          // do not show ellipses for strings shorter than 4 characters.
          if (truncate < 4) str.substring(0, truncate)
          else str.substring(0, truncate - 3) + "..."
        } else {
          str
        }
      }: Seq[String]
    }).toImmutableArraySeq
  }

  /**
   * Compose the string representing rows for output
   *
   * @param _numRows Number of rows to show
   * @param truncate If set to more than 0, truncates strings to `truncate` characters and
   *                   all cells will be aligned right.
   * @param vertical If set to true, prints output rows vertically (one line per column value).
   */
  private[sql] def showString(
      _numRows: Int,
      truncate: Int = 20,
      vertical: Boolean = false): String = {
    val numRows = _numRows.max(0).min(ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH - 1)
    // Get rows represented by Seq[Seq[String]], we may get one more line if it has more data.
    val tmpRows = getRows(numRows, truncate)

    val hasMoreData = tmpRows.length - 1 > numRows
    val rows = tmpRows.take(numRows + 1)

    val sb = new StringBuilder
    val numCols = schema.fieldNames.length
    // We set a minimum column width at '3'
    val minimumColWidth = 3

    if (!vertical) {
      // Initialise the width of each column to a minimum value
      val colWidths = Array.fill(numCols)(minimumColWidth)

      // Compute the width of each column
      for (row <- rows) {
        for ((cell, i) <- row.zipWithIndex) {
          colWidths(i) = math.max(colWidths(i), Utils.stringHalfWidth(cell))
        }
      }

      val paddedRows = rows.map { row =>
        row.zipWithIndex.map { case (cell, i) =>
          if (truncate > 0) {
            StringUtils.leftPad(cell, colWidths(i) - Utils.stringHalfWidth(cell) + cell.length)
          } else {
            StringUtils.rightPad(cell, colWidths(i) - Utils.stringHalfWidth(cell) + cell.length)
          }
        }
      }

      // Create SeparateLine
      val sep: String = colWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString()

      // column names
      paddedRows.head.addString(sb, "|", "|", "|\n")
      sb.append(sep)

      // data
      paddedRows.tail.foreach(_.addString(sb, "|", "|", "|\n"))
      sb.append(sep)
    } else {
      // Extended display mode enabled
      val fieldNames = rows.head
      val dataRows = rows.tail

      // Compute the width of field name and data columns
      val fieldNameColWidth = fieldNames.foldLeft(minimumColWidth) { case (curMax, fieldName) =>
        math.max(curMax, Utils.stringHalfWidth(fieldName))
      }
      val dataColWidth = dataRows.foldLeft(minimumColWidth) { case (curMax, row) =>
        math.max(curMax, row.map(cell => Utils.stringHalfWidth(cell)).max)
      }

      dataRows.zipWithIndex.foreach { case (row, i) =>
        // "+ 5" in size means a character length except for padded names and data
        val rowHeader = StringUtils.rightPad(
          s"-RECORD $i", fieldNameColWidth + dataColWidth + 5, "-")
        sb.append(rowHeader).append("\n")
        row.zipWithIndex.map { case (cell, j) =>
          val fieldName = StringUtils.rightPad(fieldNames(j),
            fieldNameColWidth - Utils.stringHalfWidth(fieldNames(j)) + fieldNames(j).length)
          val data = StringUtils.rightPad(cell,
            dataColWidth - Utils.stringHalfWidth(cell) + cell.length)
          s" $fieldName | $data "
        }.addString(sb, "", "\n", "\n")
      }
    }

    // Print a footer
    if (vertical && rows.tail.isEmpty) {
      // In a vertical mode, print an empty row set explicitly
      sb.append("(0 rows)\n")
    } else if (hasMoreData) {
      // For Data that has more than "numRows" records
      val rowsString = if (numRows == 1) "row" else "rows"
      sb.append(s"only showing top $numRows $rowsString\n")
    }

    sb.toString()
  }

  /**
   * Compose the HTML representing rows for output
   *
   * @param _numRows Number of rows to show
   * @param truncate If set to more than 0, truncates strings to `truncate` characters and
   *                   all cells will be aligned right.
   */
  private[sql] def htmlString(
      _numRows: Int,
      truncate: Int = 20): String = {
    val numRows = _numRows.max(0).min(ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH - 1)
    // Get rows represented by Seq[Seq[String]], we may get one more line if it has more data.
    val tmpRows = getRows(numRows, truncate)

    val hasMoreData = tmpRows.length - 1 > numRows
    val rows = tmpRows.take(numRows + 1)

    val sb = new StringBuilder

    sb.append("<table border='1'>\n")

    sb.append(rows.head.map(StringEscapeUtils.escapeHtml4)
      .mkString("<tr><th>", "</th><th>", "</th></tr>\n"))
    rows.tail.foreach { row =>
      sb.append(row.map(StringEscapeUtils.escapeHtml4)
        .mkString("<tr><td>", "</td><td>", "</td></tr>\n"))
    }

    sb.append("</table>\n")

    if (hasMoreData) {
      sb.append(s"only showing top $numRows ${if (numRows == 1) "row" else "rows"}\n")
    }

    sb.toString()
  }

  override def toString: String = {
    try {
      val builder = new StringBuilder
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
        s"Invalid tree; ${e.getMessage}:\n$queryExecution"
    }
  }

  /** @inheritdoc */
  // This is declared with parentheses to prevent the Scala compiler from treating
  // `ds.toDF("1")` as invoking this toDF and then apply on the returned DataFrame.
  def toDF(): DataFrame = new Dataset[Row](queryExecution, ExpressionEncoder(schema))

  /** @inheritdoc */
  def as[U : Encoder]: Dataset[U] = Dataset[U](sparkSession, logicalPlan)

  /** @inheritdoc */
  def to(schema: StructType): DataFrame = withPlan {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(schema).asInstanceOf[StructType]
    Project.matchSchema(logicalPlan, replaced, sparkSession.sessionState.conf)
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  def toDF(colNames: String*): DataFrame = {
    require(schema.size == colNames.size,
      "The number of columns doesn't match.\n" +
        s"Old column names (${schema.size}): " + schema.fields.map(_.name).mkString(", ") + "\n" +
        s"New column names (${colNames.size}): " + colNames.mkString(", "))

    val newCols = logicalPlan.output.zip(colNames).map { case (oldAttribute, newName) =>
      column(oldAttribute).as(newName)
    }
    select(newCols : _*)
  }

  /** @inheritdoc */
  def schema: StructType = sparkSession.withActive {
    queryExecution.analyzed.schema
  }

  /** @inheritdoc */
  def explain(mode: String): Unit = sparkSession.withActive {
    // Because temporary views are resolved during analysis when we create a Dataset, and
    // `ExplainCommand` analyzes input query plan and resolves temporary views again. Using
    // `ExplainCommand` here will probably output different query plans, compared to the results
    // of evaluation of the Dataset. So just output QueryExecution's query plans here.

    // scalastyle:off println
    println(queryExecution.explainString(ExplainMode.fromString(mode)))
    // scalastyle:on println
  }

  /** @inheritdoc */
  def isLocal: Boolean = logicalPlan.isInstanceOf[LocalRelation] ||
    logicalPlan.isInstanceOf[CommandResult]

  /** @inheritdoc */
  def isEmpty: Boolean = withAction("isEmpty",
      commandResultOptimized.select().limit(1).queryExecution) { plan =>
    plan.executeTake(1).isEmpty
  }

  /** @inheritdoc */
  def isStreaming: Boolean = logicalPlan.isStreaming

  /** @inheritdoc */
  protected[sql] def checkpoint(eager: Boolean, reliableCheckpoint: Boolean): Dataset[T] = {
    val actionName = if (reliableCheckpoint) "checkpoint" else "localCheckpoint"
    withAction(actionName, queryExecution) { physicalPlan =>
      val internalRdd = physicalPlan.execute().map(_.copy())
      if (reliableCheckpoint) {
        internalRdd.checkpoint()
      } else {
        internalRdd.localCheckpoint()
      }

      if (eager) {
        internalRdd.doCheckpoint()
      }

      withTypedPlan[T] {
        LogicalRDD.fromDataset(rdd = internalRdd, originDataset = this, isStreaming = isStreaming)
      }
    }
  }

  /** @inheritdoc */
  // We only accept an existing column name, not a derived column here as a watermark that is
  // defined on a derived column cannot referenced elsewhere in the plan.
  def withWatermark(eventTime: String, delayThreshold: String): Dataset[T] = withTypedPlan {
    val parsedDelay = IntervalUtils.fromIntervalString(delayThreshold)
    require(!IntervalUtils.isNegative(parsedDelay),
      s"delay threshold ($delayThreshold) should not be negative.")
    EliminateEventTimeWatermark(
      EventTimeWatermark(UnresolvedAttribute(eventTime), parsedDelay, logicalPlan))
  }

  /** @inheritdoc */
  // scalastyle:off println
  def show(numRows: Int, truncate: Boolean): Unit = if (truncate) {
    println(showString(numRows, truncate = 20))
  } else {
    println(showString(numRows, truncate = 0))
  }

  /** @inheritdoc */
  // scalastyle:off println
  def show(numRows: Int, truncate: Int, vertical: Boolean): Unit =
    println(showString(numRows, truncate, vertical))
  // scalastyle:on println

  /**
   * Returns a [[DataFrameNaFunctions]] for working with missing data.
   * {{{
   *   // Dropping rows containing any null values.
   *   ds.na.drop()
   * }}}
   *
   * @group untypedrel
   * @since 1.6.0
   */
  def na: DataFrameNaFunctions = new DataFrameNaFunctions(toDF())

  /**
   * Returns a [[DataFrameStatFunctions]] for working statistic functions support.
   * {{{
   *   // Finding frequent items in column with name 'a'.
   *   ds.stat.freqItems(Seq("a"))
   * }}}
   *
   * @group untypedrel
   * @since 1.6.0
   */
  def stat: DataFrameStatFunctions = new DataFrameStatFunctions(toDF())

  /** @inheritdoc */
  def join(right: Dataset[_]): DataFrame = withPlan {
    Join(logicalPlan, right.logicalPlan, joinType = Inner, None, JoinHint.NONE)
  }

  /** @inheritdoc */
  def join(right: Dataset[_], usingColumns: Seq[String], joinType: String): DataFrame = {
    // Analyze the self join. The assumption is that the analyzer will disambiguate left vs right
    // by creating a new instance for one of the branch.
    val joined = sparkSession.sessionState.executePlan(
      Join(logicalPlan, right.logicalPlan, joinType = JoinType(joinType), None, JoinHint.NONE))
      .analyzed.asInstanceOf[Join]

    withPlan {
      Join(
        joined.left,
        joined.right,
        UsingJoin(JoinType(joinType), usingColumns.toIndexedSeq),
        None,
        JoinHint.NONE)
    }
  }

  /**
   * find the trivially true predicates and automatically resolves them to both sides.
   */
  private def resolveSelfJoinCondition(
      right: Dataset[_],
      joinExprs: Option[Column],
      joinType: String): Join = {
    // Note that in this function, we introduce a hack in the case of self-join to automatically
    // resolve ambiguous join conditions into ones that might make sense [SPARK-6231].
    // Consider this case: df.join(df, df("key") === df("key"))
    // Since df("key") === df("key") is a trivially true condition, this actually becomes a
    // cartesian join. However, most likely users expect to perform a self join using "key".
    // With that assumption, this hack turns the trivially true condition into equality on join
    // keys that are resolved to both sides.

    // Trigger analysis so in the case of self-join, the analyzer will clone the plan.
    // After the cloning, left and right side will have distinct expression ids.
    val plan = withPlan(
      Join(logicalPlan, right.logicalPlan,
        JoinType(joinType), joinExprs.map(_.expr), JoinHint.NONE))
      .queryExecution.analyzed.asInstanceOf[Join]

    // If auto self join alias is disabled, return the plan.
    if (!sparkSession.sessionState.conf.dataFrameSelfJoinAutoResolveAmbiguity) {
      return plan
    }

    // If left/right have no output set intersection, return the plan.
    val lanalyzed = this.queryExecution.analyzed
    val ranalyzed = right.queryExecution.analyzed
    if (lanalyzed.outputSet.intersect(ranalyzed.outputSet).isEmpty) {
      return plan
    }

    // Otherwise, find the trivially true predicates and automatically resolves them to both sides.
    // By the time we get here, since we have already run analysis, all attributes should've been
    // resolved and become AttributeReference.

    JoinWith.resolveSelfJoinCondition(sparkSession.sessionState.analyzer.resolver, plan)
  }

  /** @inheritdoc */
  def join(right: Dataset[_], joinExprs: Column, joinType: String): DataFrame = {
    withPlan {
      resolveSelfJoinCondition(right, Some(joinExprs), joinType)
    }
  }

  /** @inheritdoc */
  def crossJoin(right: Dataset[_]): DataFrame = withPlan {
    Join(logicalPlan, right.logicalPlan, joinType = Cross, None, JoinHint.NONE)
  }

  /** @inheritdoc */
  def joinWith[U](other: Dataset[U], condition: Column, joinType: String): Dataset[(T, U)] = {
    // Creates a Join node and resolve it first, to get join condition resolved, self-join resolved,
    // etc.
    val joined = sparkSession.sessionState.executePlan(
      Join(
        this.logicalPlan,
        other.logicalPlan,
        JoinType(joinType),
        Some(condition.expr),
        JoinHint.NONE)).analyzed.asInstanceOf[Join]

    implicit val tuple2Encoder: Encoder[(T, U)] =
      ExpressionEncoder
        .tuple(Seq(this.exprEnc, other.exprEnc), useNullSafeDeserializer = true)
        .asInstanceOf[Encoder[(T, U)]]

    withTypedPlan(JoinWith.typedJoinWith(
      joined,
      sparkSession.sessionState.conf.dataFrameSelfJoinAutoResolveAmbiguity,
      sparkSession.sessionState.analyzer.resolver,
      this.exprEnc.isSerializedAsStructForTopLevel,
      other.exprEnc.isSerializedAsStructForTopLevel))
  }

  // TODO(SPARK-22947): Fix the DataFrame API.
  private[sql] def joinAsOf(
      other: Dataset[_],
      leftAsOf: Column,
      rightAsOf: Column,
      usingColumns: Seq[String],
      joinType: String,
      tolerance: Column,
      allowExactMatches: Boolean,
      direction: String): DataFrame = {
    val joinConditions = usingColumns.map { name =>
      this(name) === other(name)
    }
    val joinCondition = joinConditions.reduceOption(_ && _).orNull
    joinAsOf(other, leftAsOf, rightAsOf, joinCondition, joinType,
      tolerance, allowExactMatches, direction)
  }

  // TODO(SPARK-22947): Fix the DataFrame API.
  private[sql] def joinAsOf(
      other: Dataset[_],
      leftAsOf: Column,
      rightAsOf: Column,
      joinExprs: Column,
      joinType: String,
      tolerance: Column,
      allowExactMatches: Boolean,
      direction: String): DataFrame = {
    val joined = resolveSelfJoinCondition(other, Option(joinExprs), joinType)
    val leftAsOfExpr = leftAsOf.expr.transformUp {
      case a: AttributeReference if logicalPlan.outputSet.contains(a) =>
        val index = logicalPlan.output.indexWhere(_.exprId == a.exprId)
        joined.left.output(index)
    }
    val rightAsOfExpr = rightAsOf.expr.transformUp {
      case a: AttributeReference if other.logicalPlan.outputSet.contains(a) =>
        val index = other.logicalPlan.output.indexWhere(_.exprId == a.exprId)
        joined.right.output(index)
    }
    withPlan {
      AsOfJoin(
        joined.left, joined.right,
        leftAsOfExpr, rightAsOfExpr,
        joined.condition,
        joined.joinType,
        Option(tolerance).map(_.expr),
        allowExactMatches,
        AsOfJoinDirection(direction)
      )
    }
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  def hint(name: String, parameters: Any*): Dataset[T] = withTypedPlan {
    val exprs = parameters.map {
      case c: Column => c.expr
      case s: Symbol => Column(s.name).expr
      case e: Expression => e
      case literal => Literal(literal)
    }
    UnresolvedHint(name, exprs, logicalPlan)
  }

  /** @inheritdoc */
  def col(colName: String): Column = colName match {
    case "*" =>
      column(ResolvedStar(queryExecution.analyzed.output))
    case _ =>
      if (sparkSession.sessionState.conf.supportQuotedRegexColumnName) {
        colRegex(colName)
      } else {
        column(addDataFrameIdToCol(resolve(colName)))
      }
  }

  /** @inheritdoc */
  def metadataColumn(colName: String): Column =
    column(queryExecution.analyzed.getMetadataAttributeByName(colName))

  // Attach the dataset id and column position to the column reference, so that we can detect
  // ambiguous self-join correctly. See the rule `DetectAmbiguousSelfJoin`.
  // This must be called before we return a `Column` that contains `AttributeReference`.
  // Note that, the metadata added here are only available in the analyzer, as the analyzer rule
  // `DetectAmbiguousSelfJoin` will remove it.
  private def addDataFrameIdToCol(expr: NamedExpression): NamedExpression = {
    val newExpr = expr transform {
      case a: AttributeReference
        if sparkSession.conf.get(SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED) =>
        val metadata = new MetadataBuilder()
          .withMetadata(a.metadata)
          .putLong(Dataset.DATASET_ID_KEY, id)
          .putLong(Dataset.COL_POS_KEY, logicalPlan.output.indexWhere(a.semanticEquals))
          .build()
        a.withMetadata(metadata)
    }
    newExpr.asInstanceOf[NamedExpression]
  }

  /** @inheritdoc */
  def colRegex(colName: String): Column = {
    val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
    colName match {
      case ParserUtils.escapedIdentifier(columnNameRegex) =>
        column(UnresolvedRegex(columnNameRegex, None, caseSensitive))
      case ParserUtils.qualifiedEscapedIdentifier(nameParts, columnNameRegex) =>
        column(UnresolvedRegex(columnNameRegex, Some(nameParts), caseSensitive))
      case _ =>
        column(addDataFrameIdToCol(resolve(colName)))
    }
  }

  /** @inheritdoc */
  def as(alias: String): Dataset[T] = withTypedPlan {
    SubqueryAlias(alias, logicalPlan)
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  def select(cols: Column*): DataFrame = withPlan {
    val untypedCols = cols.map {
      case typedCol: TypedColumn[_, _] =>
        // Checks if a `TypedColumn` has been inserted with
        // specific input type and schema by `withInputType`.
        val needInputType = typedCol.expr.exists {
          case ta: TypedAggregateExpression if ta.inputDeserializer.isEmpty => true
          case _ => false
        }

        if (!needInputType) {
          typedCol
        } else {
          throw QueryCompilationErrors.cannotPassTypedColumnInUntypedSelectError(typedCol.toString)
        }

      case other => other
    }
    Project(untypedCols.map(_.named), logicalPlan)
  }

  /** @inheritdoc */
  def select[U1](c1: TypedColumn[T, U1]): Dataset[U1] = {
    implicit val encoder: ExpressionEncoder[U1] = encoderFor(c1.encoder)
    val tc1 = withInputType(c1.named, exprEnc, logicalPlan.output)
    val project = Project(tc1 :: Nil, logicalPlan)

    if (!encoder.isSerializedAsStructForTopLevel) {
      new Dataset[U1](sparkSession, project, encoder)
    } else {
      // Flattens inner fields of U1
      new Dataset[Tuple1[U1]](sparkSession, project, ExpressionEncoder.tuple(encoder)).map(_._1)
    }
  }

  /** @inheritdoc */
  protected def selectUntyped(columns: TypedColumn[_, _]*): Dataset[_] = {
    val encoders = columns.map(c => encoderFor(c.encoder))
    val namedColumns = columns.map(c => withInputType(c.named, exprEnc, logicalPlan.output))
    val execution = new QueryExecution(sparkSession, Project(namedColumns, logicalPlan))
    new Dataset(execution, ExpressionEncoder.tuple(encoders))
  }

  /** @inheritdoc */
  def filter(condition: Column): Dataset[T] = withTypedPlan {
    Filter(condition.expr, logicalPlan)
  }

  /**
   * Groups the Dataset using the specified columns, so we can run aggregation on them. See
   * [[RelationalGroupedDataset]] for all the available aggregate functions.
   *
   * {{{
   *   // Compute the average for all numeric columns grouped by department.
   *   ds.groupBy($"department").avg()
   *
   *   // Compute the max age and average salary, grouped by department and gender.
   *   ds.groupBy($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def groupBy(cols: Column*): RelationalGroupedDataset = {
    RelationalGroupedDataset(toDF(), cols.map(_.expr), RelationalGroupedDataset.GroupByType)
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  def rollup(cols: Column*): RelationalGroupedDataset = {
    RelationalGroupedDataset(toDF(), cols.map(_.expr), RelationalGroupedDataset.RollupType)
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  def cube(cols: Column*): RelationalGroupedDataset = {
    RelationalGroupedDataset(toDF(), cols.map(_.expr), RelationalGroupedDataset.CubeType)
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  def groupingSets(groupingSets: Seq[Seq[Column]], cols: Column*): RelationalGroupedDataset = {
    RelationalGroupedDataset(
      toDF(),
      cols.map(_.expr),
      RelationalGroupedDataset.GroupingSetsType(groupingSets.map(_.map(_.expr))))
  }

  /** @inheritdoc */
  def reduce(func: (T, T) => T): T = withNewRDDExecutionId("reduce") {
    rdd.reduce(func)
  }

  /**
   * (Scala-specific)
   * Returns a [[KeyValueGroupedDataset]] where the data is grouped by the given key `func`.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def groupByKey[K: Encoder](func: T => K): KeyValueGroupedDataset[K, T] = {
    val withGroupingKey = AppendColumns(func, logicalPlan)
    val executed = sparkSession.sessionState.executePlan(withGroupingKey)

    new KeyValueGroupedDataset(
      encoderFor[K],
      encoderFor[T],
      executed,
      logicalPlan.output,
      withGroupingKey.newColumns)
  }

  /**
   * (Java-specific)
   * Returns a [[KeyValueGroupedDataset]] where the data is grouped by the given key `func`.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def groupByKey[K](func: MapFunction[T, K], encoder: Encoder[K]): KeyValueGroupedDataset[K, T] =
    groupByKey(func.call(_))(encoder)

  /** @inheritdoc */
  def unpivot(
      ids: Array[Column],
      values: Array[Column],
      variableColumnName: String,
      valueColumnName: String): DataFrame = withPlan {
    Unpivot(
      Some(ids.map(_.named).toImmutableArraySeq),
      Some(values.map(v => Seq(v.named)).toImmutableArraySeq),
      None,
      variableColumnName,
      Seq(valueColumnName),
      logicalPlan
    )
  }

  /** @inheritdoc */
  def unpivot(
      ids: Array[Column],
      variableColumnName: String,
      valueColumnName: String): DataFrame = withPlan {
    Unpivot(
      Some(ids.map(_.named).toImmutableArraySeq),
      None,
      None,
      variableColumnName,
      Seq(valueColumnName),
      logicalPlan
    )
  }

  /**
   * Called from Python as Seq[Column] are easier to create via py4j than Array[Column].
   * We use Array[Column] for unpivot rather than Seq[Column] as those are Java-friendly.
   */
  private[sql] def unpivotWithSeq(
      ids: Seq[Column],
      values: Seq[Column],
      variableColumnName: String,
      valueColumnName: String): DataFrame =
    unpivot(ids.toArray, values.toArray, variableColumnName, valueColumnName)

  /**
   * Called from Python as Seq[Column] are easier to create via py4j than Array[Column].
   * We use Array[Column] for unpivot rather than Seq[Column] as those are Java-friendly.
   */
  private[sql] def unpivotWithSeq(
      ids: Seq[Column],
      variableColumnName: String,
      valueColumnName: String): DataFrame =
    unpivot(ids.toArray, variableColumnName, valueColumnName)

  /** @inheritdoc */
  @scala.annotation.varargs
  def observe(name: String, expr: Column, exprs: Column*): Dataset[T] = withTypedPlan {
    CollectMetrics(name, (expr +: exprs).map(_.named), logicalPlan, id)
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  def observe(observation: Observation, expr: Column, exprs: Column*): Dataset[T] = {
    sparkSession.observationManager.register(observation, this)
    observe(observation.name, expr, exprs: _*)
  }

  /** @inheritdoc */
  def limit(n: Int): Dataset[T] = withTypedPlan {
    Limit(Literal(n), logicalPlan)
  }

  /** @inheritdoc */
  def offset(n: Int): Dataset[T] = withTypedPlan {
    Offset(Literal(n), logicalPlan)
  }

  // This breaks caching, but it's usually ok because it addresses a very specific use case:
  // using union to union many files or partitions.
  private def combineUnions(plan: LogicalPlan): LogicalPlan = {
    plan.transformDownWithPruning(_.containsPattern(TreePattern.UNION)) {
      case Distinct(u: Union) =>
        Distinct(flattenUnion(u, isUnionDistinct = true))
      // Only handle distinct-like 'Deduplicate', where the keys == output
      case Deduplicate(keys: Seq[Attribute], u: Union) if AttributeSet(keys) == u.outputSet =>
        Deduplicate(keys, flattenUnion(u, isUnionDistinct = true))
      case u: Union =>
        flattenUnion(u, isUnionDistinct = false)
    }
  }

  private def flattenUnion(u: Union, isUnionDistinct: Boolean): Union = {
    var changed = false
    // We only need to look at the direct children of Union, as the nested adjacent Unions should
    // have been combined already by previous `Dataset#union` transformations.
    val newChildren = u.children.flatMap {
      case Distinct(Union(children, byName, allowMissingCol))
          if isUnionDistinct && byName == u.byName && allowMissingCol == u.allowMissingCol =>
        changed = true
        children
      // Only handle distinct-like 'Deduplicate', where the keys == output
      case Deduplicate(keys: Seq[Attribute], child @ Union(children, byName, allowMissingCol))
          if AttributeSet(keys) == child.outputSet && isUnionDistinct && byName == u.byName &&
            allowMissingCol == u.allowMissingCol =>
        changed = true
        children
      case Union(children, byName, allowMissingCol)
          if !isUnionDistinct && byName == u.byName && allowMissingCol == u.allowMissingCol =>
        changed = true
        children
      case other =>
        Seq(other)
    }
    if (changed) {
      val newUnion = Union(newChildren)
      newUnion.copyTagsFrom(u)
      newUnion
    } else {
      u
    }
  }

  /** @inheritdoc */
  def union(other: Dataset[T]): Dataset[T] = withSetOperator {
    combineUnions(Union(logicalPlan, other.logicalPlan))
  }

  /** @inheritdoc */
  def unionByName(other: Dataset[T], allowMissingColumns: Boolean): Dataset[T] = {
    withSetOperator {
      // We need to resolve the by-name Union first, as the underlying Unions are already resolved
      // and we can only combine adjacent Unions if they are all resolved.
      val resolvedUnion = sparkSession.sessionState.executePlan(
        Union(logicalPlan :: other.logicalPlan :: Nil, byName = true, allowMissingColumns))
      combineUnions(resolvedUnion.analyzed)
    }
  }

  /** @inheritdoc */
  def intersect(other: Dataset[T]): Dataset[T] = withSetOperator {
    Intersect(logicalPlan, other.logicalPlan, isAll = false)
  }

  /** @inheritdoc */
  def intersectAll(other: Dataset[T]): Dataset[T] = withSetOperator {
    Intersect(logicalPlan, other.logicalPlan, isAll = true)
  }

  /** @inheritdoc */
  def except(other: Dataset[T]): Dataset[T] = withSetOperator {
    Except(logicalPlan, other.logicalPlan, isAll = false)
  }

  /** @inheritdoc */
  def exceptAll(other: Dataset[T]): Dataset[T] = withSetOperator {
    Except(logicalPlan, other.logicalPlan, isAll = true)
  }

  /** @inheritdoc */
  def sample(withReplacement: Boolean, fraction: Double, seed: Long): Dataset[T] = {
    withTypedPlan {
      Sample(0.0, fraction, withReplacement, seed, logicalPlan)
    }
  }

  /** @inheritdoc */
  def randomSplit(weights: Array[Double], seed: Long): Array[Dataset[T]] = {
    require(weights.forall(_ >= 0),
      s"Weights must be nonnegative, but got ${weights.mkString("[", ",", "]")}")
    require(weights.sum > 0,
      s"Sum of weights must be positive, but got ${weights.mkString("[", ",", "]")}")

    // It is possible that the underlying dataframe doesn't guarantee the ordering of rows in its
    // constituent partitions each time a split is materialized which could result in
    // overlapping splits. To prevent this, we explicitly sort each input partition to make the
    // ordering deterministic. Note that MapTypes cannot be sorted and are explicitly pruned out
    // from the sort order.
    val sortOrder = logicalPlan.output
      .filter(attr => RowOrdering.isOrderable(attr.dataType))
      .map(SortOrder(_, Ascending))
    val plan = if (sortOrder.nonEmpty) {
      Sort(sortOrder, global = false, logicalPlan)
    } else {
      // SPARK-12662: If sort order is empty, we materialize the dataset to guarantee determinism
      cache()
      logicalPlan
    }
    val sum = weights.sum
    val normalizedCumWeights = weights.map(_ / sum).scanLeft(0.0d)(_ + _)
    normalizedCumWeights.sliding(2).map { x =>
      new Dataset[T](
        sparkSession, Sample(x(0), x(1), withReplacement = false, seed, plan), encoder)
    }.toArray
  }

  /** @inheritdoc */
  override def randomSplit(weights: Array[Double]): Array[Dataset[T]] =
    randomSplit(weights, Utils.random.nextLong())

  /**
   * Randomly splits this Dataset with the provided weights. Provided for the Python Api.
   *
   * @param weights weights for splits, will be normalized if they don't sum to 1.
   * @param seed Seed for sampling.
   */
  private[spark] def randomSplit(weights: List[Double], seed: Long): Array[Dataset[T]] = {
    randomSplit(weights.toArray, seed)
  }

  /** @inheritdoc */
  override def randomSplitAsList(weights: Array[Double], seed: Long): util.List[Dataset[T]] =
    util.Arrays.asList(randomSplit(weights, seed): _*)

  /** @inheritdoc */
  @deprecated("use flatMap() or select() with functions.explode() instead", "2.0.0")
  def explode[A <: Product : TypeTag](input: Column*)(f: Row => IterableOnce[A]): DataFrame = {
    val elementSchema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]

    val convert = CatalystTypeConverters.createToCatalystConverter(elementSchema)

    val rowFunction =
      f.andThen(_.map(convert(_).asInstanceOf[InternalRow]))
    val generator = UserDefinedGenerator(elementSchema, rowFunction, input.map(_.expr))

    withPlan {
      Generate(generator, unrequiredChildIndex = Nil, outer = false,
        qualifier = None, generatorOutput = Nil, logicalPlan)
    }
  }

  /** @inheritdoc */
  @deprecated("use flatMap() or select() with functions.explode() instead", "2.0.0")
  def explode[A, B : TypeTag](inputColumn: String, outputColumn: String)(f: A => IterableOnce[B])
    : DataFrame = {
    val dataType = ScalaReflection.schemaFor[B].dataType
    val attributes = AttributeReference(outputColumn, dataType)() :: Nil
    // TODO handle the metadata?
    val elementSchema = attributes.toStructType

    def rowFunction(row: Row): IterableOnce[InternalRow] = {
      val convert = CatalystTypeConverters.createToCatalystConverter(dataType)
      f(row(0).asInstanceOf[A]).map(o => InternalRow(convert(o)))
    }
    val generator = UserDefinedGenerator(elementSchema, rowFunction, apply(inputColumn).expr :: Nil)

    withPlan {
      Generate(generator, unrequiredChildIndex = Nil, outer = false,
        qualifier = None, generatorOutput = Nil, logicalPlan)
    }
  }

  /** @inheritdoc */
  protected[spark] def withColumns(colNames: Seq[String], cols: Seq[Column]): DataFrame = {
    require(colNames.size == cols.size,
      s"The size of column names: ${colNames.size} isn't equal to " +
        s"the size of columns: ${cols.size}")
    SchemaUtils.checkColumnNameDuplication(
      colNames,
      sparkSession.sessionState.conf.caseSensitiveAnalysis)

    val resolver = sparkSession.sessionState.analyzer.resolver
    val output = queryExecution.analyzed.output

    val columnSeq = colNames.zip(cols)

    val replacedAndExistingColumns = output.map { field =>
      columnSeq.find { case (colName, _) =>
        resolver(field.name, colName)
      } match {
        case Some((colName: String, col: Column)) => col.as(colName)
        case _ => column(field)
      }
    }

    val newColumns = columnSeq.filter { case (colName, col) =>
      !output.exists(f => resolver(f.name, colName))
    }.map { case (colName, col) => col.as(colName) }

    select(replacedAndExistingColumns ++ newColumns : _*)
  }

  /** @inheritdoc */
  private[spark] def withColumns(
      colNames: Seq[String],
      cols: Seq[Column],
      metadata: Seq[Metadata]): DataFrame = {
    require(colNames.size == metadata.size,
      s"The size of column names: ${colNames.size} isn't equal to " +
        s"the size of metadata elements: ${metadata.size}")
    val newCols = colNames.zip(cols).zip(metadata).map { case ((colName, col), metadata) =>
      col.as(colName, metadata)
    }
    withColumns(colNames, newCols)
  }

  /** @inheritdoc */
  private[spark] def withColumn(colName: String, col: Column, metadata: Metadata): DataFrame =
    withColumns(Seq(colName), Seq(col), Seq(metadata))

  protected[spark] def withColumnsRenamed(
      colNames: Seq[String],
      newColNames: Seq[String]): DataFrame = {
    require(colNames.size == newColNames.size,
      s"The size of existing column names: ${colNames.size} isn't equal to " +
        s"the size of new column names: ${newColNames.size}")

    val resolver = sparkSession.sessionState.analyzer.resolver
    val output: Seq[NamedExpression] = queryExecution.analyzed.output
    var shouldRename = false

    val projectList = colNames.zip(newColNames).foldLeft(output) {
      case (attrs, (existingName, newName)) =>
        attrs.map(attr =>
          if (resolver(attr.name, existingName)) {
            shouldRename = true
            Alias(attr, newName)()
          } else {
            attr
          }
        )
    }
    if (shouldRename) {
      withPlan(Project(projectList, logicalPlan))
    } else {
      toDF()
    }
  }

  /** @inheritdoc */
  def withMetadata(columnName: String, metadata: Metadata): DataFrame = {
    withColumn(columnName, col(columnName), metadata)
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  def drop(colNames: String*): DataFrame = {
    val resolver = sparkSession.sessionState.analyzer.resolver
    val allColumns = queryExecution.analyzed.output
    val remainingCols = allColumns.filter { attribute =>
      colNames.forall(n => !resolver(attribute.name, n))
    }.map(attribute => column(attribute))
    if (remainingCols.size == allColumns.size) {
      toDF()
    } else {
      this.select(remainingCols: _*)
    }
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  def drop(col: Column, cols: Column*): DataFrame = withPlan {
    DataFrameDropColumns((col +: cols).map(_.expr), logicalPlan)
  }

  /** @inheritdoc */
  def dropDuplicates(): Dataset[T] = dropDuplicates(this.columns)

  /** @inheritdoc */
  def dropDuplicates(colNames: Seq[String]): Dataset[T] = withTypedPlan {
    val groupCols = groupColsFromDropDuplicates(colNames)
    Deduplicate(groupCols, logicalPlan)
  }

  /** @inheritdoc */
  def dropDuplicatesWithinWatermark(): Dataset[T] = {
    dropDuplicatesWithinWatermark(this.columns)
  }

  /** @inheritdoc */
  def dropDuplicatesWithinWatermark(colNames: Seq[String]): Dataset[T] = withTypedPlan {
    val groupCols = groupColsFromDropDuplicates(colNames)
    // UnsupportedOperationChecker will fail the query if this is called with batch Dataset.
    DeduplicateWithinWatermark(groupCols, logicalPlan)
  }

  private def groupColsFromDropDuplicates(colNames: Seq[String]): Seq[Attribute] = {
    val resolver = sparkSession.sessionState.analyzer.resolver
    val allColumns = queryExecution.analyzed.output
    // SPARK-31990: We must keep `toSet.toSeq` here because of the backward compatibility issue
    // (the Streaming's state store depends on the `groupCols` order).
    colNames.toSet.toSeq.flatMap { (colName: String) =>
      // It is possibly there are more than one columns with the same name,
      // so we call filter instead of find.
      val cols = allColumns.filter(col => resolver(col.name, colName))
      if (cols.isEmpty) {
        throw QueryCompilationErrors.cannotResolveColumnNameAmongAttributesError(
          colName, schema.fieldNames.mkString(", "))
      }
      cols
    }
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  def summary(statistics: String*): DataFrame = StatFunctions.summary(this, statistics)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def describe(cols: String*): DataFrame = {
    val selected = if (cols.isEmpty) this else select(cols.head, cols.tail: _*)
    selected.summary("count", "mean", "stddev", "min", "max")
  }

  /** @inheritdoc */
  def head(n: Int): Array[T] = withAction("head", limit(n).queryExecution)(collectFromPlan)

  /** @inheritdoc */
  def filter(func: T => Boolean): Dataset[T] = {
    withTypedPlan(TypedFilter(func, logicalPlan))
  }

  /** @inheritdoc */
  def filter(func: FilterFunction[T]): Dataset[T] = {
    withTypedPlan(TypedFilter(func, logicalPlan))
  }

  /** @inheritdoc */
  def map[U : Encoder](func: T => U): Dataset[U] = {
    withTypedPlan(MapElements[T, U](func, logicalPlan))
  }

  /** @inheritdoc */
  def map[U](func: MapFunction[T, U], encoder: Encoder[U]): Dataset[U] = {
    implicit val uEnc: Encoder[U] = encoder
    withTypedPlan(MapElements[T, U](func, logicalPlan))
  }

  /** @inheritdoc */
  def mapPartitions[U : Encoder](func: Iterator[T] => Iterator[U]): Dataset[U] = {
    new Dataset[U](
      sparkSession,
      MapPartitions[T, U](func, logicalPlan),
      implicitly[Encoder[U]])
  }

  /** @inheritdoc */
  def mapPartitions[U](f: MapPartitionsFunction[T, U], encoder: Encoder[U]): Dataset[U] = {
    val func: (Iterator[T]) => Iterator[U] = x => f.call(x.asJava).asScala
    mapPartitions(func)(encoder)
  }

  /**
   * Returns a new `DataFrame` that contains the result of applying a serialized R function
   * `func` to each partition.
   */
  private[sql] def mapPartitionsInR(
      func: Array[Byte],
      packageNames: Array[Byte],
      broadcastVars: Array[Broadcast[Object]],
      schema: StructType): DataFrame = {
    val rowEncoder = encoder.asInstanceOf[ExpressionEncoder[Row]]
    Dataset.ofRows(
      sparkSession,
      MapPartitionsInR(func, packageNames, broadcastVars, schema, rowEncoder, logicalPlan))
  }

  /**
   * Applies a Scalar iterator Pandas UDF to each partition. The user-defined function
   * defines a transformation: `iter(pandas.DataFrame)` -> `iter(pandas.DataFrame)`.
   * Each partition is each iterator consisting of DataFrames as batches.
   *
   * This function uses Apache Arrow as serialization format between Java executors and Python
   * workers.
   */
  private[sql] def mapInPandas(
      funcCol: Column,
      isBarrier: Boolean = false,
      profile: ResourceProfile = null): DataFrame = {
    val func = funcCol.expr
    Dataset.ofRows(
      sparkSession,
      MapInPandas(
        func,
        toAttributes(func.dataType.asInstanceOf[StructType]),
        logicalPlan,
        isBarrier,
        Option(profile)))
  }

  /**
   * Applies a function to each partition in Arrow format. The user-defined function
   * defines a transformation: `iter(pyarrow.RecordBatch)` -> `iter(pyarrow.RecordBatch)`.
   * Each partition is each iterator consisting of `pyarrow.RecordBatch`s as batches.
   */
  private[sql] def mapInArrow(
      funcCol: Column,
      isBarrier: Boolean = false,
      profile: ResourceProfile = null): DataFrame = {
    val func = funcCol.expr
    Dataset.ofRows(
      sparkSession,
      MapInArrow(
        func,
        toAttributes(func.dataType.asInstanceOf[StructType]),
        logicalPlan,
        isBarrier,
        Option(profile)))
  }

  /** @inheritdoc */
  def foreach(f: T => Unit): Unit = withNewRDDExecutionId("foreach") {
    rdd.foreach(f)
  }

  /** @inheritdoc */
  def foreachPartition(f: Iterator[T] => Unit): Unit = withNewRDDExecutionId("foreachPartition") {
    rdd.foreachPartition(f)
  }

  /** @inheritdoc */
  def tail(n: Int): Array[T] = withAction(
    "tail", withTypedPlan(Tail(Literal(n), logicalPlan)).queryExecution)(collectFromPlan)

  /** @inheritdoc */
  def collect(): Array[T] = withAction("collect", queryExecution)(collectFromPlan)

  /** @inheritdoc */
  def collectAsList(): java.util.List[T] = withAction("collectAsList", queryExecution) { plan =>
    val values = collectFromPlan(plan)
    java.util.Arrays.asList(values : _*)
  }

  /** @inheritdoc */
  def toLocalIterator(): java.util.Iterator[T] = {
    withAction("toLocalIterator", queryExecution) { plan =>
      val fromRow = resolvedEnc.createDeserializer()
      plan.executeToIterator().map(fromRow).asJava
    }
  }

  /** @inheritdoc */
  def count(): Long = withAction("count", groupBy().count().queryExecution) { plan =>
    plan.executeCollect().head.getLong(0)
  }

  /** @inheritdoc */
  def repartition(numPartitions: Int): Dataset[T] = withTypedPlan {
    Repartition(numPartitions, shuffle = true, logicalPlan)
  }

  protected def repartitionByExpression(
      numPartitions: Option[Int],
      partitionExprs: Seq[Column]): Dataset[T] = {
    // The underlying `LogicalPlan` operator special-cases all-`SortOrder` arguments.
    // However, we don't want to complicate the semantics of this API method.
    // Instead, let's give users a friendly error message, pointing them to the new method.
    val sortOrders = partitionExprs.filter(_.expr.isInstanceOf[SortOrder])
    if (sortOrders.nonEmpty) throw new IllegalArgumentException(
      s"""Invalid partitionExprs specified: $sortOrders
         |For range partitioning use repartitionByRange(...) instead.
       """.stripMargin)
    withTypedPlan {
      RepartitionByExpression(partitionExprs.map(_.expr), logicalPlan, numPartitions)
    }
  }

  protected def repartitionByRange(
      numPartitions: Option[Int],
      partitionExprs: Seq[Column]): Dataset[T] = {
    require(partitionExprs.nonEmpty, "At least one partition-by expression must be specified.")
    val sortOrder: Seq[SortOrder] = partitionExprs.map(_.expr match {
      case expr: SortOrder => expr
      case expr: Expression => SortOrder(expr, Ascending)
    })
    withTypedPlan {
      RepartitionByExpression(sortOrder, logicalPlan, numPartitions)
    }
  }

  /** @inheritdoc */
  def coalesce(numPartitions: Int): Dataset[T] = withTypedPlan {
    Repartition(numPartitions, shuffle = false, logicalPlan)
  }

  /** @inheritdoc */
  def persist(): this.type = persist(sparkSession.sessionState.conf.defaultCacheStorageLevel)

  /** @inheritdoc */
  override def cache(): this.type = persist()

  /** @inheritdoc */
  def persist(newLevel: StorageLevel): this.type = {
    sparkSession.sharedState.cacheManager.cacheQuery(this, None, newLevel)
    this
  }

  /** @inheritdoc */
  def storageLevel: StorageLevel = {
    sparkSession.sharedState.cacheManager.lookupCachedData(this).map { cachedData =>
      cachedData.cachedRepresentation.cacheBuilder.storageLevel
    }.getOrElse(StorageLevel.NONE)
  }

  /** @inheritdoc */
  def unpersist(blocking: Boolean): this.type = {
    sparkSession.sharedState.cacheManager.uncacheQuery(this, cascade = false, blocking)
    this
  }

  /** @inheritdoc */
  override def unpersist(): this.type = unpersist(blocking = false)

  // Represents the `QueryExecution` used to produce the content of the Dataset as an `RDD`.
  @transient private lazy val rddQueryExecution: QueryExecution = {
    val deserialized = CatalystSerde.deserialize[T](logicalPlan)
    sparkSession.sessionState.executePlan(deserialized)
  }

  /**
   * Represents the content of the Dataset as an `RDD` of `T`.
   *
   * @group basic
   * @since 1.6.0
   */
  lazy val rdd: RDD[T] = {
    val objectType = exprEnc.deserializer.dataType
    rddQueryExecution.toRdd.mapPartitions { rows =>
      rows.map(_.get(0, objectType).asInstanceOf[T])
    }
  }

  /**
   * Returns the content of the Dataset as a `JavaRDD` of `T`s.
   * @group basic
   * @since 1.6.0
   */
  def toJavaRDD: JavaRDD[T] = rdd.toJavaRDD()

  /**
   * Returns the content of the Dataset as a `JavaRDD` of `T`s.
   * @group basic
   * @since 1.6.0
   */
  def javaRDD: JavaRDD[T] = toJavaRDD

  protected def createTempView(
      viewName: String,
      replace: Boolean,
      global: Boolean): Unit = sparkSession.withActive {
    val viewType = if (global) GlobalTempView else LocalTempView

    val identifier = try {
      sparkSession.sessionState.sqlParser.parseMultipartIdentifier(viewName)
    } catch {
      case _: ParseException => throw QueryCompilationErrors.invalidViewNameError(viewName)
    }

    if (!SQLConf.get.allowsTempViewCreationWithMultipleNameparts && identifier.size > 1) {
      // Temporary view names should NOT contain database prefix like "database.table"
      throw new AnalysisException(
        errorClass = "TEMP_VIEW_NAME_TOO_MANY_NAME_PARTS",
        messageParameters = Map("actualName" -> viewName))
    }

    withPlan {
      CreateViewCommand(
        name = TableIdentifier(identifier.last),
        userSpecifiedColumns = Nil,
        comment = None,
        properties = Map.empty,
        originalText = None,
        plan = logicalPlan,
        allowExisting = false,
        replace = replace,
        viewType = viewType,
        isAnalyzed = true)
    }
  }

  /** @inheritdoc */
  def write: DataFrameWriter[T] = {
    if (isStreaming) {
      logicalPlan.failAnalysis(
        errorClass = "CALL_ON_STREAMING_DATASET_UNSUPPORTED",
        messageParameters = Map("methodName" -> toSQLId("write")))
    }
    new DataFrameWriter[T](this)
  }

  /**
   * Create a write configuration builder for v2 sources.
   *
   * This builder is used to configure and execute write operations. For example, to append to an
   * existing table, run:
   *
   * {{{
   *   df.writeTo("catalog.db.table").append()
   * }}}
   *
   * This can also be used to create or replace existing tables:
   *
   * {{{
   *   df.writeTo("catalog.db.table").partitionedBy($"col").createOrReplace()
   * }}}
   *
   * @group basic
   * @since 3.0.0
   */
  def writeTo(table: String): DataFrameWriterV2[T] = {
    // TODO: streaming could be adapted to use this interface
    if (isStreaming) {
      logicalPlan.failAnalysis(
        errorClass = "CALL_ON_STREAMING_DATASET_UNSUPPORTED",
        messageParameters = Map("methodName" -> toSQLId("writeTo")))
    }
    new DataFrameWriterV2[T](table, this)
  }

  /**
   * Merges a set of updates, insertions, and deletions based on a source table into
   * a target table.
   *
   * Scala Examples:
   * {{{
   *   spark.table("source")
   *     .mergeInto("target", $"source.id" === $"target.id")
   *     .whenMatched($"salary" === 100)
   *     .delete()
   *     .whenNotMatched()
   *     .insertAll()
   *     .whenNotMatchedBySource($"salary" === 100)
   *     .update(Map(
   *       "salary" -> lit(200)
   *     ))
   *     .merge()
   * }}}
   *
   * @group basic
   * @since 4.0.0
   */
  def mergeInto(table: String, condition: Column): MergeIntoWriter[T] = {
    if (isStreaming) {
      logicalPlan.failAnalysis(
        errorClass = "CALL_ON_STREAMING_DATASET_UNSUPPORTED",
        messageParameters = Map("methodName" -> toSQLId("mergeInto")))
    }

    new MergeIntoWriter[T](table, this, condition)
  }

  /**
   * Interface for saving the content of the streaming Dataset out into external storage.
   *
   * @group basic
   * @since 2.0.0
   */
  def writeStream: DataStreamWriter[T] = {
    if (!isStreaming) {
      logicalPlan.failAnalysis(
        errorClass = "WRITE_STREAM_NOT_ALLOWED",
        messageParameters = Map.empty)
    }
    new DataStreamWriter[T](this)
  }

  /** @inheritdoc */
  override def toJSON: Dataset[String] = {
    val rowSchema = this.schema
    val sessionLocalTimeZone = sparkSession.sessionState.conf.sessionLocalTimeZone
    mapPartitions { iter =>
      val writer = new CharArrayWriter()
      // create the Generator without separator inserted between 2 records
      val gen = new JacksonGenerator(rowSchema, writer,
        new JSONOptions(Map.empty[String, String], sessionLocalTimeZone))

      new Iterator[String] {
        private val toRow = exprEnc.createSerializer()
        override def hasNext: Boolean = iter.hasNext
        override def next(): String = {
          gen.write(toRow(iter.next()))
          gen.flush()

          val json = writer.toString
          if (hasNext) {
            writer.reset()
          } else {
            gen.close()
          }

          json
        }
      }
    } (Encoders.STRING)
  }

  /** @inheritdoc */
  def inputFiles: Array[String] = {
    val files: Seq[String] = queryExecution.optimizedPlan.collect {
      case LogicalRelation(fsBasedRelation: FileRelation, _, _, _) =>
        fsBasedRelation.inputFiles
      case fr: FileRelation =>
        fr.inputFiles
      case r: HiveTableRelation =>
        r.tableMeta.storage.locationUri.map(_.toString).toArray
      case DataSourceV2ScanRelation(DataSourceV2Relation(table: FileTable, _, _, _, _),
          _, _, _, _) =>
        table.fileIndex.inputFiles
    }.flatten
    files.toSet.toArray
  }

  /** @inheritdoc */
  @DeveloperApi
  def sameSemantics(other: Dataset[T]): Boolean = {
    queryExecution.analyzed.sameResult(other.queryExecution.analyzed)
  }

  /** @inheritdoc */
  @DeveloperApi
  def semanticHash(): Int = {
    queryExecution.analyzed.semanticHash()
  }

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
  override def join(
      right: Dataset[_],
      usingColumns: Array[String],
      joinType: String): DataFrame =
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
      c3: TypedColumn[T, U3]): Dataset[(U1, U2, U3)] = super.select(c1, c2, c3)

  /** @inheritdoc */
  override def select[U1, U2, U3, U4](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3],
      c4: TypedColumn[T, U4]): Dataset[(U1, U2, U3, U4)] = super.select(c1, c2, c3, c4)

  /** @inheritdoc */
  override def select[U1, U2, U3, U4, U5](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3],
      c4: TypedColumn[T, U4],
      c5: TypedColumn[T, U5]): Dataset[(U1, U2, U3, U4, U5)] = super.select(c1, c2, c3, c4, c5)

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
  override def dropDuplicates(colNames: Array[String]): Dataset[T] = super.dropDuplicates(colNames)

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
  override def flatMap[U: Encoder](func: T => IterableOnce[U]): Dataset[U] = super.flatMap(func)

  /** @inheritdoc */
  override def flatMap[U](f: FlatMapFunction[T, U], encoder: Encoder[U]): Dataset[U] =
    super.flatMap(f, encoder)

  /** @inheritdoc */
  override def foreach(func: ForeachFunction[T]): Unit = super.foreach(func)

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

  ////////////////////////////////////////////////////////////////////////////
  // For Python API
  ////////////////////////////////////////////////////////////////////////////

  /**
   * It adds a new long column with the name `name` that increases one by one.
   * This is for 'distributed-sequence' default index in pandas API on Spark.
   */
  private[sql] def withSequenceColumn(name: String) = {
    select(column(DistributedSequenceID()).alias(name), col("*"))
  }

  /**
   * Converts a JavaRDD to a PythonRDD.
   */
  private[sql] def javaToPython: JavaRDD[Array[Byte]] = {
    val structType = schema  // capture it for closure
    val rdd = queryExecution.toRdd.map(EvaluatePython.toJava(_, structType))
    EvaluatePython.javaToPython(rdd)
  }

  private[sql] def collectToPython(): Array[Any] = {
    EvaluatePython.registerPicklers()
    withAction("collectToPython", queryExecution) { plan =>
      val toJava: (Any) => Any = EvaluatePython.toJava(_, schema)
      val iter: Iterator[Array[Byte]] = new SerDeUtil.AutoBatchedPickler(
        plan.executeCollect().iterator.map(toJava))
      PythonRDD.serveIterator(iter, "serve-DataFrame")
    }
  }

  private[sql] def tailToPython(n: Int): Array[Any] = {
    EvaluatePython.registerPicklers()
    withAction("tailToPython", queryExecution) { plan =>
      val toJava: (Any) => Any = EvaluatePython.toJava(_, schema)
      val iter: Iterator[Array[Byte]] = new SerDeUtil.AutoBatchedPickler(
        plan.executeTail(n).iterator.map(toJava))
      PythonRDD.serveIterator(iter, "serve-DataFrame")
    }
  }

  private[sql] def getRowsToPython(
      _numRows: Int,
      truncate: Int): Array[Any] = {
    EvaluatePython.registerPicklers()
    val numRows = _numRows.max(0).min(ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH - 1)
    val rows = getRows(numRows, truncate).map(_.toArray).toArray
    val toJava: (Any) => Any = EvaluatePython.toJava(_, ArrayType(ArrayType(StringType)))
    val iter: Iterator[Array[Byte]] = new SerDeUtil.AutoBatchedPickler(
      rows.iterator.map(toJava))
    PythonRDD.serveIterator(iter, "serve-GetRows")
  }

  /**
   * Collect a Dataset as Arrow batches and serve stream to SparkR. It sends
   * arrow batches in an ordered manner with buffering. This is inevitable
   * due to missing R API that reads batches from socket directly. See ARROW-4512.
   * Eventually, this code should be deduplicated by `collectAsArrowToPython`.
   */
  private[sql] def collectAsArrowToR(): Array[Any] = {
    val timeZoneId = sparkSession.sessionState.conf.sessionLocalTimeZone

    RRDD.serveToStream("serve-Arrow") { outputStream =>
      withAction("collectAsArrowToR", queryExecution) { plan =>
        val buffer = new ByteArrayOutputStream()
        val out = new DataOutputStream(outputStream)
        val batchWriter =
          new ArrowBatchStreamWriter(schema, buffer, timeZoneId, errorOnDuplicatedFieldNames = true)
        val arrowBatchRdd = toArrowBatchRdd(plan)
        val numPartitions = arrowBatchRdd.partitions.length

        // Store collection results for worst case of 1 to N-1 partitions
        val results = new Array[Array[Array[Byte]]](Math.max(0, numPartitions - 1))
        var lastIndex = -1  // index of last partition written

        // Handler to eagerly write partitions to Python in order
        def handlePartitionBatches(index: Int, arrowBatches: Array[Array[Byte]]): Unit = {
          // If result is from next partition in order
          if (index - 1 == lastIndex) {
            batchWriter.writeBatches(arrowBatches.iterator)
            lastIndex += 1
            // Write stored partitions that come next in order
            while (lastIndex < results.length && results(lastIndex) != null) {
              batchWriter.writeBatches(results(lastIndex).iterator)
              results(lastIndex) = null
              lastIndex += 1
            }
            // After last batch, end the stream
            if (lastIndex == results.length) {
              batchWriter.end()
              val batches = buffer.toByteArray
              out.writeInt(batches.length)
              out.write(batches)
            }
          } else {
            // Store partitions received out of order
            results(index - 1) = arrowBatches
          }
        }

        sparkSession.sparkContext.runJob(
          arrowBatchRdd,
          (ctx: TaskContext, it: Iterator[Array[Byte]]) => it.toArray,
          0 until numPartitions,
          handlePartitionBatches)
      }
    }
  }

  /**
   * Collect a Dataset as Arrow batches and serve stream to PySpark. It sends
   * arrow batches in an un-ordered manner without buffering, and then batch order
   * information at the end. The batches should be reordered at Python side.
   */
  private[sql] def collectAsArrowToPython: Array[Any] = {
    val timeZoneId = sparkSession.sessionState.conf.sessionLocalTimeZone
    val errorOnDuplicatedFieldNames =
      sparkSession.sessionState.conf.pandasStructHandlingMode == "legacy"

    PythonRDD.serveToStream("serve-Arrow") { outputStream =>
      withAction("collectAsArrowToPython", queryExecution) { plan =>
        val out = new DataOutputStream(outputStream)
        val batchWriter =
          new ArrowBatchStreamWriter(schema, out, timeZoneId, errorOnDuplicatedFieldNames)

        // Batches ordered by (index of partition, batch index in that partition) tuple
        val batchOrder = ArrayBuffer.empty[(Int, Int)]

        // Handler to eagerly write batches to Python as they arrive, un-ordered
        val handlePartitionBatches = (index: Int, arrowBatches: Array[Array[Byte]]) =>
          if (arrowBatches.nonEmpty) {
            // Write all batches (can be more than 1) in the partition, store the batch order tuple
            batchWriter.writeBatches(arrowBatches.iterator)
            arrowBatches.indices.foreach {
              partitionBatchIndex => batchOrder.append((index, partitionBatchIndex))
            }
          }

        Utils.tryWithSafeFinally {
          val arrowBatchRdd = toArrowBatchRdd(plan)
          sparkSession.sparkContext.runJob(
            arrowBatchRdd,
            (it: Iterator[Array[Byte]]) => it.toArray,
            handlePartitionBatches)
        } {
          // After processing all partitions, end the batch stream
          batchWriter.end()

          // Write batch order indices
          out.writeInt(batchOrder.length)
          // Sort by (index of partition, batch index in that partition) tuple to get the
          // overall_batch_index from 0 to N-1 batches, which can be used to put the
          // transferred batches in the correct order
          batchOrder.zipWithIndex.sortBy(_._1).foreach { case (_, overallBatchIndex) =>
            out.writeInt(overallBatchIndex)
          }
        }
      }
    }
  }

  private[sql] def toPythonIterator(prefetchPartitions: Boolean = false): Array[Any] = {
    withNewExecutionId {
      PythonRDD.toLocalIteratorAndServe(javaToPython.rdd, prefetchPartitions)
    }
  }

  ////////////////////////////////////////////////////////////////////////////
  // Private Helpers
  ////////////////////////////////////////////////////////////////////////////

  /**
   * Wrap a Dataset action to track all Spark jobs in the body so that we can connect them with
   * an execution.
   */
  private def withNewExecutionId[U](body: => U): U = {
    SQLExecution.withNewExecutionId(queryExecution)(body)
  }

  /**
   * Wrap an action of the Dataset's RDD to track all Spark jobs in the body so that we can connect
   * them with an execution. Before performing the action, the metrics of the executed plan will be
   * reset.
   */
  private def withNewRDDExecutionId[U](name: String)(body: => U): U = {
    SQLExecution.withNewExecutionId(rddQueryExecution, Some(name)) {
      rddQueryExecution.executedPlan.resetMetrics()
      body
    }
  }

  /**
   * Wrap a Dataset action to track the QueryExecution and time cost, then report to the
   * user-registered callback functions, and also to convert asserts/NPE to
   * the internal error exception.
   */
  private def withAction[U](name: String, qe: QueryExecution)(action: SparkPlan => U) = {
    SQLExecution.withNewExecutionId(qe, Some(name)) {
      QueryExecution.withInternalError(s"""The "$name" action failed.""") {
        qe.executedPlan.resetMetrics()
        action(qe.executedPlan)
      }
    }
  }

  /**
   * Collect all elements from a spark plan.
   */
  private def collectFromPlan(plan: SparkPlan): Array[T] = {
    val fromRow = resolvedEnc.createDeserializer()
    plan.executeCollect().map(fromRow)
  }

  protected def sortInternal(global: Boolean, sortExprs: Seq[Column]): Dataset[T] = {
    val sortOrder: Seq[SortOrder] = sortExprs.map { col =>
      col.expr match {
        case expr: SortOrder =>
          expr
        case expr: Expression =>
          SortOrder(expr, Ascending)
      }
    }
    withTypedPlan {
      Sort(sortOrder, global = global, logicalPlan)
    }
  }

  /** A convenient function to wrap a logical plan and produce a DataFrame. */
  @inline private def withPlan(logicalPlan: LogicalPlan): DataFrame = {
    Dataset.ofRows(sparkSession, logicalPlan)
  }

  /** A convenient function to wrap a logical plan and produce a Dataset. */
  @inline private def withTypedPlan[U : Encoder](logicalPlan: LogicalPlan): Dataset[U] = {
    Dataset(sparkSession, logicalPlan)
  }

  /** A convenient function to wrap a set based logical plan and produce a Dataset. */
  @inline private def withSetOperator[U : Encoder](logicalPlan: LogicalPlan): Dataset[U] = {
    if (classTag.runtimeClass.isAssignableFrom(classOf[Row])) {
      // Set operators widen types (change the schema), so we cannot reuse the row encoder.
      Dataset.ofRows(sparkSession, logicalPlan).asInstanceOf[Dataset[U]]
    } else {
      Dataset(sparkSession, logicalPlan)
    }
  }

  /** Returns a optimized plan for CommandResult, convert to `LocalRelation`. */
  private def commandResultOptimized: Dataset[T] = {
    logicalPlan match {
      case c: CommandResult =>
        // Convert to `LocalRelation` and let `ConvertToLocalRelation` do the casting locally to
        // avoid triggering a job
        Dataset(sparkSession, LocalRelation(c.output, c.rows))
      case _ => this
    }
  }

  /** Convert to an RDD of serialized ArrowRecordBatches. */
  private[sql] def toArrowBatchRdd(plan: SparkPlan): RDD[Array[Byte]] = {
    val schemaCaptured = this.schema
    val maxRecordsPerBatch = sparkSession.sessionState.conf.arrowMaxRecordsPerBatch
    val timeZoneId = sparkSession.sessionState.conf.sessionLocalTimeZone
    val errorOnDuplicatedFieldNames =
      sparkSession.sessionState.conf.pandasStructHandlingMode == "legacy"
    plan.execute().mapPartitionsInternal { iter =>
      val context = TaskContext.get()
      ArrowConverters.toBatchIterator(
        iter, schemaCaptured, maxRecordsPerBatch, timeZoneId, errorOnDuplicatedFieldNames, context)
    }
  }

  // This is only used in tests, for now.
  private[sql] def toArrowBatchRdd: RDD[Array[Byte]] = {
    toArrowBatchRdd(queryExecution.executedPlan)
  }
}
