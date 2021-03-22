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

import java.io.CharArrayWriter

import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag
import scala.util.control.NonFatal

import org.apache.commons.lang3.StringUtils

import org.apache.spark.TaskContext
import org.apache.spark.annotation.{DeveloperApi, Experimental, InterfaceStability}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function._
import org.apache.spark.api.python.{PythonRDD, SerDeUtil}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateSafeProjection
import org.apache.spark.sql.catalyst.json.{JacksonGenerator, JSONOptions}
import org.apache.spark.sql.catalyst.optimizer.CombineUnions
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserUtils}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, PartitioningCollection}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.arrow.{ArrowBatchStreamWriter, ArrowConverters}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.python.EvaluatePython
import org.apache.spark.sql.execution.stat.StatFunctions
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.util.Utils

private[sql] object Dataset {
  def apply[T: Encoder](sparkSession: SparkSession, logicalPlan: LogicalPlan): Dataset[T] = {
    val dataset = new Dataset(sparkSession, logicalPlan, implicitly[Encoder[T]])
    // Eagerly bind the encoder so we verify that the encoder matches the underlying
    // schema. The user will get an error if this is not the case.
    // optimization: it is guaranteed that [[InternalRow]] can be converted to [[Row]] so
    // do not do this check in that case. this check can be expensive since it requires running
    // the whole [[Analyzer]] to resolve the deserializer
    if (dataset.exprEnc.clsTag.runtimeClass != classOf[Row]) {
      dataset.deserializer
    }
    dataset
  }

  def ofRows(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame = {
    val qe = sparkSession.sessionState.executePlan(logicalPlan)
    qe.assertAnalyzed()
    new Dataset[Row](sparkSession, qe, RowEncoder(qe.analyzed.schema))
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
 *   Dataset<String> names = people.map((Person p) -> p.name, Encoders.STRING));
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
@InterfaceStability.Stable
class Dataset[T] private[sql](
    @transient val sparkSession: SparkSession,
    @DeveloperApi @InterfaceStability.Unstable @transient val queryExecution: QueryExecution,
    encoder: Encoder[T])
  extends Serializable {

  queryExecution.assertAnalyzed()

  // Note for Spark contributors: if adding or updating any action in `Dataset`, please make sure
  // you wrap it with `withNewExecutionId` if this actions doesn't call other action.

  def this(sparkSession: SparkSession, logicalPlan: LogicalPlan, encoder: Encoder[T]) = {
    this(sparkSession, sparkSession.sessionState.executePlan(logicalPlan), encoder)
  }

  def this(sqlContext: SQLContext, logicalPlan: LogicalPlan, encoder: Encoder[T]) = {
    this(sqlContext.sparkSession, logicalPlan, encoder)
  }

  @transient private[sql] val logicalPlan: LogicalPlan = {
    // For various commands (like DDL) and queries with side effects, we force query execution
    // to happen right away to let these side effects take place eagerly.
    queryExecution.analyzed match {
      case c: Command =>
        LocalRelation(c.output, withAction("command", queryExecution)(_.executeCollect()))
      case u @ Union(children) if children.forall(_.isInstanceOf[Command]) =>
        LocalRelation(u.output, withAction("command", queryExecution)(_.executeCollect()))
      case _ =>
        queryExecution.analyzed
    }
  }

  /**
   * Currently [[ExpressionEncoder]] is the only implementation of [[Encoder]], here we turn the
   * passed in encoder to [[ExpressionEncoder]] explicitly, and mark it implicit so that we can use
   * it when constructing new Dataset objects that have the same object type (that will be
   * possibly resolved to a different schema).
   */
  private[sql] implicit val exprEnc: ExpressionEncoder[T] = encoderFor(encoder)

  // The deserializer expression which can be used to build a projection and turn rows to objects
  // of type T, after collecting rows to the driver side.
  private lazy val deserializer =
    exprEnc.resolveAndBind(logicalPlan.output, sparkSession.sessionState.analyzer).deserializer

  private implicit def classTag = exprEnc.clsTag

  // sqlContext must be val because a stable identifier is expected when you import implicits
  @transient lazy val sqlContext: SQLContext = sparkSession.sqlContext

  private[sql] def resolve(colName: String): NamedExpression = {
    queryExecution.analyzed.resolveQuoted(colName, sparkSession.sessionState.analyzer.resolver)
      .getOrElse {
        throw new AnalysisException(
          s"""Cannot resolve column name "$colName" among (${schema.fieldNames.mkString(", ")})""")
      }
  }

  private[sql] def numericColumns: Seq[Expression] = {
    schema.fields.filter(_.dataType.isInstanceOf[NumericType]).map { n =>
      queryExecution.analyzed.resolveQuoted(n.name, sparkSession.sessionState.analyzer.resolver).get
    }
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
    val newDf = toDF()
    val castCols = newDf.logicalPlan.output.map { col =>
      // Since binary types in top-level schema fields have a specific format to print,
      // so we do not cast them to strings here.
      if (col.dataType == BinaryType) {
        Column(col)
      } else {
        Column(col).cast(StringType)
      }
    }
    val data = newDf.select(castCols: _*).take(numRows + 1)

    // For array values, replace Seq and Array with square brackets
    // For cells that are beyond `truncate` characters, replace it with the
    // first `truncate-3` and "..."
    schema.fieldNames.toSeq +: data.map { row =>
      row.toSeq.map { cell =>
        val str = cell match {
          case null => "null"
          case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
          case _ => cell.toString
        }
        if (truncate > 0 && str.length > truncate) {
          // do not show ellipses for strings shorter than 4 characters.
          if (truncate < 4) str.substring(0, truncate)
          else str.substring(0, truncate - 3) + "..."
        } else {
          str
        }
      }: Seq[String]
    }
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

  override def toString: String = {
    try {
      val builder = new StringBuilder
      val fields = schema.take(2).map {
        case f => s"${f.name}: ${f.dataType.simpleString(2)}"
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

  /**
   * Converts this strongly typed collection of data to generic Dataframe. In contrast to the
   * strongly typed objects that Dataset operations work on, a Dataframe returns generic [[Row]]
   * objects that allow fields to be accessed by ordinal or name.
   *
   * @group basic
   * @since 1.6.0
   */
  // This is declared with parentheses to prevent the Scala compiler from treating
  // `ds.toDF("1")` as invoking this toDF and then apply on the returned DataFrame.
  def toDF(): DataFrame = new Dataset[Row](sparkSession, queryExecution, RowEncoder(schema))

  /**
   * :: Experimental ::
   * Returns a new Dataset where each record has been mapped on to the specified type. The
   * method used to map columns depend on the type of `U`:
   *  - When `U` is a class, fields for the class will be mapped to columns of the same name
   *    (case sensitivity is determined by `spark.sql.caseSensitive`).
   *  - When `U` is a tuple, the columns will be mapped by ordinal (i.e. the first column will
   *    be assigned to `_1`).
   *  - When `U` is a primitive type (i.e. String, Int, etc), then the first column of the
   *    `DataFrame` will be used.
   *
   * If the schema of the Dataset does not match the desired `U` type, you can use `select`
   * along with `alias` or `as` to rearrange or rename as required.
   *
   * Note that `as[]` only changes the view of the data that is passed into typed operations,
   * such as `map()`, and does not eagerly project away any columns that are not present in
   * the specified class.
   *
   * @group basic
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def as[U : Encoder]: Dataset[U] = Dataset[U](sparkSession, logicalPlan)

  /**
   * Converts this strongly typed collection of data to generic `DataFrame` with columns renamed.
   * This can be quite convenient in conversion from an RDD of tuples into a `DataFrame` with
   * meaningful names. For example:
   * {{{
   *   val rdd: RDD[(Int, String)] = ...
   *   rdd.toDF()  // this implicit conversion creates a DataFrame with column name `_1` and `_2`
   *   rdd.toDF("id", "name")  // this creates a DataFrame with column name "id" and "name"
   * }}}
   *
   * @group basic
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def toDF(colNames: String*): DataFrame = {
    require(schema.size == colNames.size,
      "The number of columns doesn't match.\n" +
        s"Old column names (${schema.size}): " + schema.fields.map(_.name).mkString(", ") + "\n" +
        s"New column names (${colNames.size}): " + colNames.mkString(", "))

    val newCols = logicalPlan.output.zip(colNames).map { case (oldAttribute, newName) =>
      Column(oldAttribute).as(newName)
    }
    select(newCols : _*)
  }

  /**
   * Returns the schema of this Dataset.
   *
   * @group basic
   * @since 1.6.0
   */
  def schema: StructType = queryExecution.analyzed.schema

  /**
   * Prints the schema to the console in a nice tree format.
   *
   * @group basic
   * @since 1.6.0
   */
  // scalastyle:off println
  def printSchema(): Unit = println(schema.treeString)
  // scalastyle:on println

  /**
   * Prints the plans (logical and physical) to the console for debugging purposes.
   *
   * @group basic
   * @since 1.6.0
   */
  def explain(extended: Boolean): Unit = {
    val explain = ExplainCommand(queryExecution.logical, extended = extended)
    sparkSession.sessionState.executePlan(explain).executedPlan.executeCollect().foreach {
      // scalastyle:off println
      r => println(r.getString(0))
      // scalastyle:on println
    }
  }

  /**
   * Prints the physical plan to the console for debugging purposes.
   *
   * @group basic
   * @since 1.6.0
   */
  def explain(): Unit = explain(extended = false)

  /**
   * Returns all column names and their data types as an array.
   *
   * @group basic
   * @since 1.6.0
   */
  def dtypes: Array[(String, String)] = schema.fields.map { field =>
    (field.name, field.dataType.toString)
  }

  /**
   * Returns all column names as an array.
   *
   * @group basic
   * @since 1.6.0
   */
  def columns: Array[String] = schema.fields.map(_.name)

  /**
   * Returns true if the `collect` and `take` methods can be run locally
   * (without any Spark executors).
   *
   * @group basic
   * @since 1.6.0
   */
  def isLocal: Boolean = logicalPlan.isInstanceOf[LocalRelation]

  /**
   * Returns true if the `Dataset` is empty.
   *
   * @group basic
   * @since 2.4.0
   */
  def isEmpty: Boolean = withAction("isEmpty", limit(1).groupBy().count().queryExecution) { plan =>
    plan.executeCollect().head.getLong(0) == 0
  }

  /**
   * Returns true if this Dataset contains one or more sources that continuously
   * return data as it arrives. A Dataset that reads data from a streaming source
   * must be executed as a `StreamingQuery` using the `start()` method in
   * `DataStreamWriter`. Methods that return a single answer, e.g. `count()` or
   * `collect()`, will throw an [[AnalysisException]] when there is a streaming
   * source present.
   *
   * @group streaming
   * @since 2.0.0
   */
  @InterfaceStability.Evolving
  def isStreaming: Boolean = logicalPlan.isStreaming

  /**
   * Eagerly checkpoint a Dataset and return the new Dataset. Checkpointing can be used to truncate
   * the logical plan of this Dataset, which is especially useful in iterative algorithms where the
   * plan may grow exponentially. It will be saved to files inside the checkpoint
   * directory set with `SparkContext#setCheckpointDir`.
   *
   * @group basic
   * @since 2.1.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def checkpoint(): Dataset[T] = checkpoint(eager = true, reliableCheckpoint = true)

  /**
   * Returns a checkpointed version of this Dataset. Checkpointing can be used to truncate the
   * logical plan of this Dataset, which is especially useful in iterative algorithms where the
   * plan may grow exponentially. It will be saved to files inside the checkpoint
   * directory set with `SparkContext#setCheckpointDir`.
   *
   * @group basic
   * @since 2.1.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def checkpoint(eager: Boolean): Dataset[T] = checkpoint(eager = eager, reliableCheckpoint = true)

  /**
   * Eagerly locally checkpoints a Dataset and return the new Dataset. Checkpointing can be
   * used to truncate the logical plan of this Dataset, which is especially useful in iterative
   * algorithms where the plan may grow exponentially. Local checkpoints are written to executor
   * storage and despite potentially faster they are unreliable and may compromise job completion.
   *
   * @group basic
   * @since 2.3.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def localCheckpoint(): Dataset[T] = checkpoint(eager = true, reliableCheckpoint = false)

  /**
   * Locally checkpoints a Dataset and return the new Dataset. Checkpointing can be used to truncate
   * the logical plan of this Dataset, which is especially useful in iterative algorithms where the
   * plan may grow exponentially. Local checkpoints are written to executor storage and despite
   * potentially faster they are unreliable and may compromise job completion.
   *
   * @group basic
   * @since 2.3.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def localCheckpoint(eager: Boolean): Dataset[T] = checkpoint(
    eager = eager,
    reliableCheckpoint = false
  )

  /**
   * Returns a checkpointed version of this Dataset.
   *
   * @param eager Whether to checkpoint this dataframe immediately
   * @param reliableCheckpoint Whether to create a reliable checkpoint saved to files inside the
   *                           checkpoint directory. If false creates a local checkpoint using
   *                           the caching subsystem
   */
  private def checkpoint(eager: Boolean, reliableCheckpoint: Boolean): Dataset[T] = {
    val internalRdd = queryExecution.toRdd.map(_.copy())
    if (reliableCheckpoint) {
      internalRdd.checkpoint()
    } else {
      internalRdd.localCheckpoint()
    }

    if (eager) {
      internalRdd.count()
    }

    val physicalPlan = queryExecution.executedPlan

    // Takes the first leaf partitioning whenever we see a `PartitioningCollection`. Otherwise the
    // size of `PartitioningCollection` may grow exponentially for queries involving deep inner
    // joins.
    def firstLeafPartitioning(partitioning: Partitioning): Partitioning = {
      partitioning match {
        case p: PartitioningCollection => firstLeafPartitioning(p.partitionings.head)
        case p => p
      }
    }

    val outputPartitioning = firstLeafPartitioning(physicalPlan.outputPartitioning)

    Dataset.ofRows(
      sparkSession,
      LogicalRDD(
        logicalPlan.output,
        internalRdd,
        outputPartitioning,
        physicalPlan.outputOrdering,
        isStreaming
      )(sparkSession)).as[T]
  }

  /**
   * Defines an event time watermark for this [[Dataset]]. A watermark tracks a point in time
   * before which we assume no more late data is going to arrive.
   *
   * Spark will use this watermark for several purposes:
   *  - To know when a given time window aggregation can be finalized and thus can be emitted when
   *    using output modes that do not allow updates.
   *  - To minimize the amount of state that we need to keep for on-going aggregations,
   *    `mapGroupsWithState` and `dropDuplicates` operators.
   *
   *  The current watermark is computed by looking at the `MAX(eventTime)` seen across
   *  all of the partitions in the query minus a user specified `delayThreshold`.  Due to the cost
   *  of coordinating this value across partitions, the actual watermark used is only guaranteed
   *  to be at least `delayThreshold` behind the actual event time.  In some cases we may still
   *  process records that arrive more than `delayThreshold` late.
   *
   * @param eventTime the name of the column that contains the event time of the row.
   * @param delayThreshold the minimum delay to wait to data to arrive late, relative to the latest
   *                       record that has been processed in the form of an interval
   *                       (e.g. "1 minute" or "5 hours"). NOTE: This should not be negative.
   *
   * @group streaming
   * @since 2.1.0
   */
  @InterfaceStability.Evolving
  // We only accept an existing column name, not a derived column here as a watermark that is
  // defined on a derived column cannot referenced elsewhere in the plan.
  def withWatermark(eventTime: String, delayThreshold: String): Dataset[T] = withTypedPlan {
    val parsedDelay =
      try {
        CalendarInterval.fromCaseInsensitiveString(delayThreshold)
      } catch {
        case e: IllegalArgumentException =>
          throw new AnalysisException(
            s"Unable to parse time delay '$delayThreshold'",
            cause = Some(e))
      }
    require(parsedDelay.milliseconds >= 0 && parsedDelay.months >= 0,
      s"delay threshold ($delayThreshold) should not be negative.")
    EliminateEventTimeWatermark(
      EventTimeWatermark(UnresolvedAttribute(eventTime), parsedDelay, logicalPlan))
  }

  /**
   * Displays the Dataset in a tabular form. Strings more than 20 characters will be truncated,
   * and all cells will be aligned right. For example:
   * {{{
   *   year  month AVG('Adj Close) MAX('Adj Close)
   *   1980  12    0.503218        0.595103
   *   1981  01    0.523289        0.570307
   *   1982  02    0.436504        0.475256
   *   1983  03    0.410516        0.442194
   *   1984  04    0.450090        0.483521
   * }}}
   *
   * @param numRows Number of rows to show
   *
   * @group action
   * @since 1.6.0
   */
  def show(numRows: Int): Unit = show(numRows, truncate = true)

  /**
   * Displays the top 20 rows of Dataset in a tabular form. Strings more than 20 characters
   * will be truncated, and all cells will be aligned right.
   *
   * @group action
   * @since 1.6.0
   */
  def show(): Unit = show(20)

  /**
   * Displays the top 20 rows of Dataset in a tabular form.
   *
   * @param truncate Whether truncate long strings. If true, strings more than 20 characters will
   *                 be truncated and all cells will be aligned right
   *
   * @group action
   * @since 1.6.0
   */
  def show(truncate: Boolean): Unit = show(20, truncate)

  /**
   * Displays the Dataset in a tabular form. For example:
   * {{{
   *   year  month AVG('Adj Close) MAX('Adj Close)
   *   1980  12    0.503218        0.595103
   *   1981  01    0.523289        0.570307
   *   1982  02    0.436504        0.475256
   *   1983  03    0.410516        0.442194
   *   1984  04    0.450090        0.483521
   * }}}
   * @param numRows Number of rows to show
   * @param truncate Whether truncate long strings. If true, strings more than 20 characters will
   *              be truncated and all cells will be aligned right
   *
   * @group action
   * @since 1.6.0
   */
  // scalastyle:off println
  def show(numRows: Int, truncate: Boolean): Unit = if (truncate) {
    println(showString(numRows, truncate = 20))
  } else {
    println(showString(numRows, truncate = 0))
  }

  /**
   * Displays the Dataset in a tabular form. For example:
   * {{{
   *   year  month AVG('Adj Close) MAX('Adj Close)
   *   1980  12    0.503218        0.595103
   *   1981  01    0.523289        0.570307
   *   1982  02    0.436504        0.475256
   *   1983  03    0.410516        0.442194
   *   1984  04    0.450090        0.483521
   * }}}
   *
   * @param numRows Number of rows to show
   * @param truncate If set to more than 0, truncates strings to `truncate` characters and
   *                    all cells will be aligned right.
   * @group action
   * @since 1.6.0
   */
  def show(numRows: Int, truncate: Int): Unit = show(numRows, truncate, vertical = false)

  /**
   * Displays the Dataset in a tabular form. For example:
   * {{{
   *   year  month AVG('Adj Close) MAX('Adj Close)
   *   1980  12    0.503218        0.595103
   *   1981  01    0.523289        0.570307
   *   1982  02    0.436504        0.475256
   *   1983  03    0.410516        0.442194
   *   1984  04    0.450090        0.483521
   * }}}
   *
   * If `vertical` enabled, this command prints output rows vertically (one line per column value)?
   *
   * {{{
   * -RECORD 0-------------------
   *  year            | 1980
   *  month           | 12
   *  AVG('Adj Close) | 0.503218
   *  AVG('Adj Close) | 0.595103
   * -RECORD 1-------------------
   *  year            | 1981
   *  month           | 01
   *  AVG('Adj Close) | 0.523289
   *  AVG('Adj Close) | 0.570307
   * -RECORD 2-------------------
   *  year            | 1982
   *  month           | 02
   *  AVG('Adj Close) | 0.436504
   *  AVG('Adj Close) | 0.475256
   * -RECORD 3-------------------
   *  year            | 1983
   *  month           | 03
   *  AVG('Adj Close) | 0.410516
   *  AVG('Adj Close) | 0.442194
   * -RECORD 4-------------------
   *  year            | 1984
   *  month           | 04
   *  AVG('Adj Close) | 0.450090
   *  AVG('Adj Close) | 0.483521
   * }}}
   *
   * @param numRows Number of rows to show
   * @param truncate If set to more than 0, truncates strings to `truncate` characters and
   *                    all cells will be aligned right.
   * @param vertical If set to true, prints output rows vertically (one line per column value).
   * @group action
   * @since 2.3.0
   */
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

  /**
   * Join with another `DataFrame`.
   *
   * Behaves as an INNER JOIN and requires a subsequent join predicate.
   *
   * @param right Right side of the join operation.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def join(right: Dataset[_]): DataFrame = withPlan {
    Join(logicalPlan, right.logicalPlan, joinType = Inner, None)
  }

  /**
   * Inner equi-join with another `DataFrame` using the given column.
   *
   * Different from other join functions, the join column will only appear once in the output,
   * i.e. similar to SQL's `JOIN USING` syntax.
   *
   * {{{
   *   // Joining df1 and df2 using the column "user_id"
   *   df1.join(df2, "user_id")
   * }}}
   *
   * @param right Right side of the join operation.
   * @param usingColumn Name of the column to join on. This column must exist on both sides.
   *
   * @note If you perform a self-join using this function without aliasing the input
   * `DataFrame`s, you will NOT be able to reference any columns after the join, since
   * there is no way to disambiguate which side of the join you would like to reference.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def join(right: Dataset[_], usingColumn: String): DataFrame = {
    join(right, Seq(usingColumn))
  }

  /**
   * Inner equi-join with another `DataFrame` using the given columns.
   *
   * Different from other join functions, the join columns will only appear once in the output,
   * i.e. similar to SQL's `JOIN USING` syntax.
   *
   * {{{
   *   // Joining df1 and df2 using the columns "user_id" and "user_name"
   *   df1.join(df2, Seq("user_id", "user_name"))
   * }}}
   *
   * @param right Right side of the join operation.
   * @param usingColumns Names of the columns to join on. This columns must exist on both sides.
   *
   * @note If you perform a self-join using this function without aliasing the input
   * `DataFrame`s, you will NOT be able to reference any columns after the join, since
   * there is no way to disambiguate which side of the join you would like to reference.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def join(right: Dataset[_], usingColumns: Seq[String]): DataFrame = {
    join(right, usingColumns, "inner")
  }

  /**
   * Equi-join with another `DataFrame` using the given columns. A cross join with a predicate
   * is specified as an inner join. If you would explicitly like to perform a cross join use the
   * `crossJoin` method.
   *
   * Different from other join functions, the join columns will only appear once in the output,
   * i.e. similar to SQL's `JOIN USING` syntax.
   *
   * @param right Right side of the join operation.
   * @param usingColumns Names of the columns to join on. This columns must exist on both sides.
   * @param joinType Type of join to perform. Default `inner`. Must be one of:
   *                 `inner`, `cross`, `outer`, `full`, `full_outer`, `left`, `left_outer`,
   *                 `right`, `right_outer`, `left_semi`, `left_anti`.
   *
   * @note If you perform a self-join using this function without aliasing the input
   * `DataFrame`s, you will NOT be able to reference any columns after the join, since
   * there is no way to disambiguate which side of the join you would like to reference.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def join(right: Dataset[_], usingColumns: Seq[String], joinType: String): DataFrame = {
    // Analyze the self join. The assumption is that the analyzer will disambiguate left vs right
    // by creating a new instance for one of the branch.
    val joined = sparkSession.sessionState.executePlan(
      Join(logicalPlan, right.logicalPlan, joinType = JoinType(joinType), None))
      .analyzed.asInstanceOf[Join]

    withPlan {
      Join(
        joined.left,
        joined.right,
        UsingJoin(JoinType(joinType), usingColumns),
        None)
    }
  }

  /**
   * Inner join with another `DataFrame`, using the given join expression.
   *
   * {{{
   *   // The following two are equivalent:
   *   df1.join(df2, $"df1Key" === $"df2Key")
   *   df1.join(df2).where($"df1Key" === $"df2Key")
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def join(right: Dataset[_], joinExprs: Column): DataFrame = join(right, joinExprs, "inner")

  /**
   * Join with another `DataFrame`, using the given join expression. The following performs
   * a full outer join between `df1` and `df2`.
   *
   * {{{
   *   // Scala:
   *   import org.apache.spark.sql.functions._
   *   df1.join(df2, $"df1Key" === $"df2Key", "outer")
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   df1.join(df2, col("df1Key").equalTo(col("df2Key")), "outer");
   * }}}
   *
   * @param right Right side of the join.
   * @param joinExprs Join expression.
   * @param joinType Type of join to perform. Default `inner`. Must be one of:
   *                 `inner`, `cross`, `outer`, `full`, `full_outer`, `left`, `left_outer`,
   *                 `right`, `right_outer`, `left_semi`, `left_anti`.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def join(right: Dataset[_], joinExprs: Column, joinType: String): DataFrame = {
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
      Join(logicalPlan, right.logicalPlan, JoinType(joinType), Some(joinExprs.expr)))
      .queryExecution.analyzed.asInstanceOf[Join]

    // If auto self join alias is disabled, return the plan.
    if (!sparkSession.sessionState.conf.dataFrameSelfJoinAutoResolveAmbiguity) {
      return withPlan(plan)
    }

    // If left/right have no output set intersection, return the plan.
    val lanalyzed = withPlan(this.logicalPlan).queryExecution.analyzed
    val ranalyzed = withPlan(right.logicalPlan).queryExecution.analyzed
    if (lanalyzed.outputSet.intersect(ranalyzed.outputSet).isEmpty) {
      return withPlan(plan)
    }

    // Otherwise, find the trivially true predicates and automatically resolves them to both sides.
    // By the time we get here, since we have already run analysis, all attributes should've been
    // resolved and become AttributeReference.
    val cond = plan.condition.map { _.transform {
      case catalyst.expressions.EqualTo(a: AttributeReference, b: AttributeReference)
          if a.sameRef(b) =>
        catalyst.expressions.EqualTo(
          withPlan(plan.left).resolve(a.name),
          withPlan(plan.right).resolve(b.name))
      case catalyst.expressions.EqualNullSafe(a: AttributeReference, b: AttributeReference)
        if a.sameRef(b) =>
        catalyst.expressions.EqualNullSafe(
          withPlan(plan.left).resolve(a.name),
          withPlan(plan.right).resolve(b.name))
    }}

    withPlan {
      plan.copy(condition = cond)
    }
  }

  /**
   * Explicit cartesian join with another `DataFrame`.
   *
   * @param right Right side of the join operation.
   *
   * @note Cartesian joins are very expensive without an extra filter that can be pushed down.
   *
   * @group untypedrel
   * @since 2.1.0
   */
  def crossJoin(right: Dataset[_]): DataFrame = withPlan {
    Join(logicalPlan, right.logicalPlan, joinType = Cross, None)
  }

  /**
   * :: Experimental ::
   * Joins this Dataset returning a `Tuple2` for each pair where `condition` evaluates to
   * true.
   *
   * This is similar to the relation `join` function with one important difference in the
   * result schema. Since `joinWith` preserves objects present on either side of the join, the
   * result schema is similarly nested into a tuple under the column names `_1` and `_2`.
   *
   * This type of join can be useful both for preserving type-safety with the original object
   * types as well as working with relational data where either side of the join has column
   * names in common.
   *
   * @param other Right side of the join.
   * @param condition Join expression.
   * @param joinType Type of join to perform. Default `inner`. Must be one of:
   *                 `inner`, `cross`, `outer`, `full`, `full_outer`, `left`, `left_outer`,
   *                 `right`, `right_outer`.
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def joinWith[U](other: Dataset[U], condition: Column, joinType: String): Dataset[(T, U)] = {
    // Creates a Join node and resolve it first, to get join condition resolved, self-join resolved,
    // etc.
    val joined = sparkSession.sessionState.executePlan(
      Join(
        this.logicalPlan,
        other.logicalPlan,
        JoinType(joinType),
        Some(condition.expr))).analyzed.asInstanceOf[Join]

    if (joined.joinType == LeftSemi || joined.joinType == LeftAnti) {
      throw new AnalysisException("Invalid join type in joinWith: " + joined.joinType.sql)
    }

    // For both join side, combine all outputs into a single column and alias it with "_1" or "_2",
    // to match the schema for the encoder of the join result.
    // Note that we do this before joining them, to enable the join operator to return null for one
    // side, in cases like outer-join.
    val left = {
      val combined = if (this.exprEnc.flat) {
        assert(joined.left.output.length == 1)
        Alias(joined.left.output.head, "_1")()
      } else {
        Alias(CreateStruct(joined.left.output), "_1")()
      }
      Project(combined :: Nil, joined.left)
    }

    val right = {
      val combined = if (other.exprEnc.flat) {
        assert(joined.right.output.length == 1)
        Alias(joined.right.output.head, "_2")()
      } else {
        Alias(CreateStruct(joined.right.output), "_2")()
      }
      Project(combined :: Nil, joined.right)
    }

    // Rewrites the join condition to make the attribute point to correct column/field, after we
    // combine the outputs of each join side.
    val conditionExpr = joined.condition.get transformUp {
      case a: Attribute if joined.left.outputSet.contains(a) =>
        if (this.exprEnc.flat) {
          left.output.head
        } else {
          val index = joined.left.output.indexWhere(_.exprId == a.exprId)
          GetStructField(left.output.head, index)
        }
      case a: Attribute if joined.right.outputSet.contains(a) =>
        if (other.exprEnc.flat) {
          right.output.head
        } else {
          val index = joined.right.output.indexWhere(_.exprId == a.exprId)
          GetStructField(right.output.head, index)
        }
    }

    implicit val tuple2Encoder: Encoder[(T, U)] =
      ExpressionEncoder.tuple(this.exprEnc, other.exprEnc)

    withTypedPlan(Join(left, right, joined.joinType, Some(conditionExpr)))
  }

  /**
   * :: Experimental ::
   * Using inner equi-join to join this Dataset returning a `Tuple2` for each pair
   * where `condition` evaluates to true.
   *
   * @param other Right side of the join.
   * @param condition Join expression.
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def joinWith[U](other: Dataset[U], condition: Column): Dataset[(T, U)] = {
    joinWith(other, condition, "inner")
  }

  /**
   * Returns a new Dataset with each partition sorted by the given expressions.
   *
   * This is the same operation as "SORT BY" in SQL (Hive QL).
   *
   * @group typedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def sortWithinPartitions(sortCol: String, sortCols: String*): Dataset[T] = {
    sortWithinPartitions((sortCol +: sortCols).map(Column(_)) : _*)
  }

  /**
   * Returns a new Dataset with each partition sorted by the given expressions.
   *
   * This is the same operation as "SORT BY" in SQL (Hive QL).
   *
   * @group typedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def sortWithinPartitions(sortExprs: Column*): Dataset[T] = {
    sortInternal(global = false, sortExprs)
  }

  /**
   * Returns a new Dataset sorted by the specified column, all in ascending order.
   * {{{
   *   // The following 3 are equivalent
   *   ds.sort("sortcol")
   *   ds.sort($"sortcol")
   *   ds.sort($"sortcol".asc)
   * }}}
   *
   * @group typedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def sort(sortCol: String, sortCols: String*): Dataset[T] = {
    sort((sortCol +: sortCols).map(Column(_)) : _*)
  }

  /**
   * Returns a new Dataset sorted by the given expressions. For example:
   * {{{
   *   ds.sort($"col1", $"col2".desc)
   * }}}
   *
   * @group typedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def sort(sortExprs: Column*): Dataset[T] = {
    sortInternal(global = true, sortExprs)
  }

  /**
   * Returns a new Dataset sorted by the given expressions.
   * This is an alias of the `sort` function.
   *
   * @group typedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def orderBy(sortCol: String, sortCols: String*): Dataset[T] = sort(sortCol, sortCols : _*)

  /**
   * Returns a new Dataset sorted by the given expressions.
   * This is an alias of the `sort` function.
   *
   * @group typedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def orderBy(sortExprs: Column*): Dataset[T] = sort(sortExprs : _*)

  /**
   * Selects column based on the column name and returns it as a [[Column]].
   *
   * @note The column name can also reference to a nested column like `a.b`.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def apply(colName: String): Column = col(colName)

  /**
   * Specifies some hint on the current Dataset. As an example, the following code specifies
   * that one of the plan can be broadcasted:
   *
   * {{{
   *   df1.join(df2.hint("broadcast"))
   * }}}
   *
   * @group basic
   * @since 2.2.0
   */
  @scala.annotation.varargs
  def hint(name: String, parameters: Any*): Dataset[T] = withTypedPlan {
    UnresolvedHint(name, parameters, logicalPlan)
  }

  /**
   * Selects column based on the column name and returns it as a [[Column]].
   *
   * @note The column name can also reference to a nested column like `a.b`.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def col(colName: String): Column = colName match {
    case "*" =>
      Column(ResolvedStar(queryExecution.analyzed.output))
    case _ =>
      if (sqlContext.conf.supportQuotedRegexColumnName) {
        colRegex(colName)
      } else {
        val expr = resolve(colName)
        Column(expr)
      }
  }

  /**
   * Selects column based on the column name specified as a regex and returns it as [[Column]].
   * @group untypedrel
   * @since 2.3.0
   */
  def colRegex(colName: String): Column = {
    val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
    colName match {
      case ParserUtils.escapedIdentifier(columnNameRegex) =>
        Column(UnresolvedRegex(columnNameRegex, None, caseSensitive))
      case ParserUtils.qualifiedEscapedIdentifier(nameParts, columnNameRegex) =>
        Column(UnresolvedRegex(columnNameRegex, Some(nameParts), caseSensitive))
      case _ =>
        Column(resolve(colName))
    }
  }

  /**
   * Returns a new Dataset with an alias set.
   *
   * @group typedrel
   * @since 1.6.0
   */
  def as(alias: String): Dataset[T] = withTypedPlan {
    SubqueryAlias(alias, logicalPlan)
  }

  /**
   * (Scala-specific) Returns a new Dataset with an alias set.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def as(alias: Symbol): Dataset[T] = as(alias.name)

  /**
   * Returns a new Dataset with an alias set. Same as `as`.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def alias(alias: String): Dataset[T] = as(alias)

  /**
   * (Scala-specific) Returns a new Dataset with an alias set. Same as `as`.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def alias(alias: Symbol): Dataset[T] = as(alias)

  /**
   * Selects a set of column based expressions.
   * {{{
   *   ds.select($"colA", $"colB" + 1)
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def select(cols: Column*): DataFrame = withPlan {
    Project(cols.map(_.named), logicalPlan)
  }

  /**
   * Selects a set of columns. This is a variant of `select` that can only select
   * existing columns using column names (i.e. cannot construct expressions).
   *
   * {{{
   *   // The following two are equivalent:
   *   ds.select("colA", "colB")
   *   ds.select($"colA", $"colB")
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def select(col: String, cols: String*): DataFrame = select((col +: cols).map(Column(_)) : _*)

  /**
   * Selects a set of SQL expressions. This is a variant of `select` that accepts
   * SQL expressions.
   *
   * {{{
   *   // The following are equivalent:
   *   ds.selectExpr("colA", "colB as newName", "abs(colC)")
   *   ds.select(expr("colA"), expr("colB as newName"), expr("abs(colC)"))
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def selectExpr(exprs: String*): DataFrame = {
    select(exprs.map { expr =>
      Column(sparkSession.sessionState.sqlParser.parseExpression(expr))
    }: _*)
  }

  /**
   * :: Experimental ::
   * Returns a new Dataset by computing the given [[Column]] expression for each element.
   *
   * {{{
   *   val ds = Seq(1, 2, 3).toDS()
   *   val newDS = ds.select(expr("value + 1").as[Int])
   * }}}
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def select[U1](c1: TypedColumn[T, U1]): Dataset[U1] = {
    implicit val encoder = c1.encoder
    val project = Project(c1.withInputType(exprEnc, logicalPlan.output).named :: Nil, logicalPlan)

    if (encoder.flat) {
      new Dataset[U1](sparkSession, project, encoder)
    } else {
      // Flattens inner fields of U1
      new Dataset[Tuple1[U1]](sparkSession, project, ExpressionEncoder.tuple(encoder)).map(_._1)
    }
  }

  /**
   * Internal helper function for building typed selects that return tuples. For simplicity and
   * code reuse, we do this without the help of the type system and then use helper functions
   * that cast appropriately for the user facing interface.
   */
  protected def selectUntyped(columns: TypedColumn[_, _]*): Dataset[_] = {
    val encoders = columns.map(_.encoder)
    val namedColumns =
      columns.map(_.withInputType(exprEnc, logicalPlan.output).named)
    val execution = new QueryExecution(sparkSession, Project(namedColumns, logicalPlan))
    new Dataset(sparkSession, execution, ExpressionEncoder.tuple(encoders))
  }

  /**
   * :: Experimental ::
   * Returns a new Dataset by computing the given [[Column]] expressions for each element.
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def select[U1, U2](c1: TypedColumn[T, U1], c2: TypedColumn[T, U2]): Dataset[(U1, U2)] =
    selectUntyped(c1, c2).asInstanceOf[Dataset[(U1, U2)]]

  /**
   * :: Experimental ::
   * Returns a new Dataset by computing the given [[Column]] expressions for each element.
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def select[U1, U2, U3](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3]): Dataset[(U1, U2, U3)] =
    selectUntyped(c1, c2, c3).asInstanceOf[Dataset[(U1, U2, U3)]]

  /**
   * :: Experimental ::
   * Returns a new Dataset by computing the given [[Column]] expressions for each element.
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def select[U1, U2, U3, U4](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3],
      c4: TypedColumn[T, U4]): Dataset[(U1, U2, U3, U4)] =
    selectUntyped(c1, c2, c3, c4).asInstanceOf[Dataset[(U1, U2, U3, U4)]]

  /**
   * :: Experimental ::
   * Returns a new Dataset by computing the given [[Column]] expressions for each element.
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def select[U1, U2, U3, U4, U5](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3],
      c4: TypedColumn[T, U4],
      c5: TypedColumn[T, U5]): Dataset[(U1, U2, U3, U4, U5)] =
    selectUntyped(c1, c2, c3, c4, c5).asInstanceOf[Dataset[(U1, U2, U3, U4, U5)]]

  /**
   * Filters rows using the given condition.
   * {{{
   *   // The following are equivalent:
   *   peopleDs.filter($"age" > 15)
   *   peopleDs.where($"age" > 15)
   * }}}
   *
   * @group typedrel
   * @since 1.6.0
   */
  def filter(condition: Column): Dataset[T] = withTypedPlan {
    Filter(condition.expr, logicalPlan)
  }

  /**
   * Filters rows using the given SQL expression.
   * {{{
   *   peopleDs.filter("age > 15")
   * }}}
   *
   * @group typedrel
   * @since 1.6.0
   */
  def filter(conditionExpr: String): Dataset[T] = {
    filter(Column(sparkSession.sessionState.sqlParser.parseExpression(conditionExpr)))
  }

  /**
   * Filters rows using the given condition. This is an alias for `filter`.
   * {{{
   *   // The following are equivalent:
   *   peopleDs.filter($"age" > 15)
   *   peopleDs.where($"age" > 15)
   * }}}
   *
   * @group typedrel
   * @since 1.6.0
   */
  def where(condition: Column): Dataset[T] = filter(condition)

  /**
   * Filters rows using the given SQL expression.
   * {{{
   *   peopleDs.where("age > 15")
   * }}}
   *
   * @group typedrel
   * @since 1.6.0
   */
  def where(conditionExpr: String): Dataset[T] = {
    filter(Column(sparkSession.sessionState.sqlParser.parseExpression(conditionExpr)))
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

  /**
   * Create a multi-dimensional rollup for the current Dataset using the specified columns,
   * so we can run aggregation on them.
   * See [[RelationalGroupedDataset]] for all the available aggregate functions.
   *
   * {{{
   *   // Compute the average for all numeric columns rolluped by department and group.
   *   ds.rollup($"department", $"group").avg()
   *
   *   // Compute the max age and average salary, rolluped by department and gender.
   *   ds.rollup($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def rollup(cols: Column*): RelationalGroupedDataset = {
    RelationalGroupedDataset(toDF(), cols.map(_.expr), RelationalGroupedDataset.RollupType)
  }

  /**
   * Create a multi-dimensional cube for the current Dataset using the specified columns,
   * so we can run aggregation on them.
   * See [[RelationalGroupedDataset]] for all the available aggregate functions.
   *
   * {{{
   *   // Compute the average for all numeric columns cubed by department and group.
   *   ds.cube($"department", $"group").avg()
   *
   *   // Compute the max age and average salary, cubed by department and gender.
   *   ds.cube($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def cube(cols: Column*): RelationalGroupedDataset = {
    RelationalGroupedDataset(toDF(), cols.map(_.expr), RelationalGroupedDataset.CubeType)
  }

  /**
   * Groups the Dataset using the specified columns, so that we can run aggregation on them.
   * See [[RelationalGroupedDataset]] for all the available aggregate functions.
   *
   * This is a variant of groupBy that can only group by existing columns using column names
   * (i.e. cannot construct expressions).
   *
   * {{{
   *   // Compute the average for all numeric columns grouped by department.
   *   ds.groupBy("department").avg()
   *
   *   // Compute the max age and average salary, grouped by department and gender.
   *   ds.groupBy($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   * @group untypedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def groupBy(col1: String, cols: String*): RelationalGroupedDataset = {
    val colNames: Seq[String] = col1 +: cols
    RelationalGroupedDataset(
      toDF(), colNames.map(colName => resolve(colName)), RelationalGroupedDataset.GroupByType)
  }

  /**
   * :: Experimental ::
   * (Scala-specific)
   * Reduces the elements of this Dataset using the specified binary function. The given `func`
   * must be commutative and associative or the result may be non-deterministic.
   *
   * @group action
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def reduce(func: (T, T) => T): T = withNewRDDExecutionId {
    rdd.reduce(func)
  }

  /**
   * :: Experimental ::
   * (Java-specific)
   * Reduces the elements of this Dataset using the specified binary function. The given `func`
   * must be commutative and associative or the result may be non-deterministic.
   *
   * @group action
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def reduce(func: ReduceFunction[T]): T = reduce(func.call(_, _))

  /**
   * :: Experimental ::
   * (Scala-specific)
   * Returns a [[KeyValueGroupedDataset]] where the data is grouped by the given key `func`.
   *
   * @group typedrel
   * @since 2.0.0
   */
  @Experimental
  @InterfaceStability.Evolving
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
   * :: Experimental ::
   * (Java-specific)
   * Returns a [[KeyValueGroupedDataset]] where the data is grouped by the given key `func`.
   *
   * @group typedrel
   * @since 2.0.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def groupByKey[K](func: MapFunction[T, K], encoder: Encoder[K]): KeyValueGroupedDataset[K, T] =
    groupByKey(func.call(_))(encoder)

  /**
   * Create a multi-dimensional rollup for the current Dataset using the specified columns,
   * so we can run aggregation on them.
   * See [[RelationalGroupedDataset]] for all the available aggregate functions.
   *
   * This is a variant of rollup that can only group by existing columns using column names
   * (i.e. cannot construct expressions).
   *
   * {{{
   *   // Compute the average for all numeric columns rolluped by department and group.
   *   ds.rollup("department", "group").avg()
   *
   *   // Compute the max age and average salary, rolluped by department and gender.
   *   ds.rollup($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def rollup(col1: String, cols: String*): RelationalGroupedDataset = {
    val colNames: Seq[String] = col1 +: cols
    RelationalGroupedDataset(
      toDF(), colNames.map(colName => resolve(colName)), RelationalGroupedDataset.RollupType)
  }

  /**
   * Create a multi-dimensional cube for the current Dataset using the specified columns,
   * so we can run aggregation on them.
   * See [[RelationalGroupedDataset]] for all the available aggregate functions.
   *
   * This is a variant of cube that can only group by existing columns using column names
   * (i.e. cannot construct expressions).
   *
   * {{{
   *   // Compute the average for all numeric columns cubed by department and group.
   *   ds.cube("department", "group").avg()
   *
   *   // Compute the max age and average salary, cubed by department and gender.
   *   ds.cube($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   * @group untypedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def cube(col1: String, cols: String*): RelationalGroupedDataset = {
    val colNames: Seq[String] = col1 +: cols
    RelationalGroupedDataset(
      toDF(), colNames.map(colName => resolve(colName)), RelationalGroupedDataset.CubeType)
  }

  /**
   * (Scala-specific) Aggregates on the entire Dataset without groups.
   * {{{
   *   // ds.agg(...) is a shorthand for ds.groupBy().agg(...)
   *   ds.agg("age" -> "max", "salary" -> "avg")
   *   ds.groupBy().agg("age" -> "max", "salary" -> "avg")
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame = {
    groupBy().agg(aggExpr, aggExprs : _*)
  }

  /**
   * (Scala-specific) Aggregates on the entire Dataset without groups.
   * {{{
   *   // ds.agg(...) is a shorthand for ds.groupBy().agg(...)
   *   ds.agg(Map("age" -> "max", "salary" -> "avg"))
   *   ds.groupBy().agg(Map("age" -> "max", "salary" -> "avg"))
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def agg(exprs: Map[String, String]): DataFrame = groupBy().agg(exprs)

  /**
   * (Java-specific) Aggregates on the entire Dataset without groups.
   * {{{
   *   // ds.agg(...) is a shorthand for ds.groupBy().agg(...)
   *   ds.agg(Map("age" -> "max", "salary" -> "avg"))
   *   ds.groupBy().agg(Map("age" -> "max", "salary" -> "avg"))
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def agg(exprs: java.util.Map[String, String]): DataFrame = groupBy().agg(exprs)

  /**
   * Aggregates on the entire Dataset without groups.
   * {{{
   *   // ds.agg(...) is a shorthand for ds.groupBy().agg(...)
   *   ds.agg(max($"age"), avg($"salary"))
   *   ds.groupBy().agg(max($"age"), avg($"salary"))
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def agg(expr: Column, exprs: Column*): DataFrame = groupBy().agg(expr, exprs : _*)

  /**
   * Returns a new Dataset by taking the first `n` rows. The difference between this function
   * and `head` is that `head` is an action and returns an array (by triggering query execution)
   * while `limit` returns a new Dataset.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def limit(n: Int): Dataset[T] = withTypedPlan {
    Limit(Literal(n), logicalPlan)
  }

  /**
   * Returns a new Dataset containing union of rows in this Dataset and another Dataset.
   *
   * This is equivalent to `UNION ALL` in SQL. To do a SQL-style set union (that does
   * deduplication of elements), use this function followed by a [[distinct]].
   *
   * Also as standard in SQL, this function resolves columns by position (not by name).
   *
   * @group typedrel
   * @since 2.0.0
   */
  @deprecated("use union()", "2.0.0")
  def unionAll(other: Dataset[T]): Dataset[T] = union(other)

  /**
   * Returns a new Dataset containing union of rows in this Dataset and another Dataset.
   *
   * This is equivalent to `UNION ALL` in SQL. To do a SQL-style set union (that does
   * deduplication of elements), use this function followed by a [[distinct]].
   *
   * Also as standard in SQL, this function resolves columns by position (not by name):
   *
   * {{{
   *   val df1 = Seq((1, 2, 3)).toDF("col0", "col1", "col2")
   *   val df2 = Seq((4, 5, 6)).toDF("col1", "col2", "col0")
   *   df1.union(df2).show
   *
   *   // output:
   *   // +----+----+----+
   *   // |col0|col1|col2|
   *   // +----+----+----+
   *   // |   1|   2|   3|
   *   // |   4|   5|   6|
   *   // +----+----+----+
   * }}}
   *
   * Notice that the column positions in the schema aren't necessarily matched with the
   * fields in the strongly typed objects in a Dataset. This function resolves columns
   * by their positions in the schema, not the fields in the strongly typed objects. Use
   * [[unionByName]] to resolve columns by field name in the typed objects.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def union(other: Dataset[T]): Dataset[T] = withSetOperator {
    // This breaks caching, but it's usually ok because it addresses a very specific use case:
    // using union to union many files or partitions.
    CombineUnions(Union(logicalPlan, other.logicalPlan))
  }

  /**
   * Returns a new Dataset containing union of rows in this Dataset and another Dataset.
   *
   * This is different from both `UNION ALL` and `UNION DISTINCT` in SQL. To do a SQL-style set
   * union (that does deduplication of elements), use this function followed by a [[distinct]].
   *
   * The difference between this function and [[union]] is that this function
   * resolves columns by name (not by position):
   *
   * {{{
   *   val df1 = Seq((1, 2, 3)).toDF("col0", "col1", "col2")
   *   val df2 = Seq((4, 5, 6)).toDF("col1", "col2", "col0")
   *   df1.unionByName(df2).show
   *
   *   // output:
   *   // +----+----+----+
   *   // |col0|col1|col2|
   *   // +----+----+----+
   *   // |   1|   2|   3|
   *   // |   6|   4|   5|
   *   // +----+----+----+
   * }}}
   *
   * @group typedrel
   * @since 2.3.0
   */
  def unionByName(other: Dataset[T]): Dataset[T] = withSetOperator {
    // Check column name duplication
    val resolver = sparkSession.sessionState.analyzer.resolver
    val leftOutputAttrs = logicalPlan.output
    val rightOutputAttrs = other.logicalPlan.output

    SchemaUtils.checkColumnNameDuplication(
      leftOutputAttrs.map(_.name),
      "in the left attributes",
      sparkSession.sessionState.conf.caseSensitiveAnalysis)
    SchemaUtils.checkColumnNameDuplication(
      rightOutputAttrs.map(_.name),
      "in the right attributes",
      sparkSession.sessionState.conf.caseSensitiveAnalysis)

    // Builds a project list for `other` based on `logicalPlan` output names
    val rightProjectList = leftOutputAttrs.map { lattr =>
      rightOutputAttrs.find { rattr => resolver(lattr.name, rattr.name) }.getOrElse {
        throw new AnalysisException(
          s"""Cannot resolve column name "${lattr.name}" among """ +
            s"""(${rightOutputAttrs.map(_.name).mkString(", ")})""")
      }
    }

    // Delegates failure checks to `CheckAnalysis`
    val notFoundAttrs = rightOutputAttrs.diff(rightProjectList)
    val rightChild = Project(rightProjectList ++ notFoundAttrs, other.logicalPlan)

    // This breaks caching, but it's usually ok because it addresses a very specific use case:
    // using union to union many files or partitions.
    CombineUnions(Union(logicalPlan, rightChild))
  }

  /**
   * Returns a new Dataset containing rows only in both this Dataset and another Dataset.
   * This is equivalent to `INTERSECT` in SQL.
   *
   * @note Equality checking is performed directly on the encoded representation of the data
   * and thus is not affected by a custom `equals` function defined on `T`.
   *
   * @group typedrel
   * @since 1.6.0
   */
  def intersect(other: Dataset[T]): Dataset[T] = withSetOperator {
    Intersect(logicalPlan, other.logicalPlan, isAll = false)
  }

  /**
   * Returns a new Dataset containing rows only in both this Dataset and another Dataset while
   * preserving the duplicates.
   * This is equivalent to `INTERSECT ALL` in SQL.
   *
   * @note Equality checking is performed directly on the encoded representation of the data
   * and thus is not affected by a custom `equals` function defined on `T`. Also as standard
   * in SQL, this function resolves columns by position (not by name).
   *
   * @group typedrel
   * @since 2.4.0
   */
  def intersectAll(other: Dataset[T]): Dataset[T] = withSetOperator {
    Intersect(logicalPlan, other.logicalPlan, isAll = true)
  }


  /**
   * Returns a new Dataset containing rows in this Dataset but not in another Dataset.
   * This is equivalent to `EXCEPT DISTINCT` in SQL.
   *
   * @note Equality checking is performed directly on the encoded representation of the data
   * and thus is not affected by a custom `equals` function defined on `T`.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def except(other: Dataset[T]): Dataset[T] = withSetOperator {
    Except(logicalPlan, other.logicalPlan, isAll = false)
  }

  /**
   * Returns a new Dataset containing rows in this Dataset but not in another Dataset while
   * preserving the duplicates.
   * This is equivalent to `EXCEPT ALL` in SQL.
   *
   * @note Equality checking is performed directly on the encoded representation of the data
   * and thus is not affected by a custom `equals` function defined on `T`. Also as standard in
   * SQL, this function resolves columns by position (not by name).
   *
   * @group typedrel
   * @since 2.4.0
   */
  def exceptAll(other: Dataset[T]): Dataset[T] = withSetOperator {
    Except(logicalPlan, other.logicalPlan, isAll = true)
  }

  /**
   * Returns a new [[Dataset]] by sampling a fraction of rows (without replacement),
   * using a user-supplied seed.
   *
   * @param fraction Fraction of rows to generate, range [0.0, 1.0].
   * @param seed Seed for sampling.
   *
   * @note This is NOT guaranteed to provide exactly the fraction of the count
   * of the given [[Dataset]].
   *
   * @group typedrel
   * @since 2.3.0
   */
  def sample(fraction: Double, seed: Long): Dataset[T] = {
    sample(withReplacement = false, fraction = fraction, seed = seed)
  }

  /**
   * Returns a new [[Dataset]] by sampling a fraction of rows (without replacement),
   * using a random seed.
   *
   * @param fraction Fraction of rows to generate, range [0.0, 1.0].
   *
   * @note This is NOT guaranteed to provide exactly the fraction of the count
   * of the given [[Dataset]].
   *
   * @group typedrel
   * @since 2.3.0
   */
  def sample(fraction: Double): Dataset[T] = {
    sample(withReplacement = false, fraction = fraction)
  }

  /**
   * Returns a new [[Dataset]] by sampling a fraction of rows, using a user-supplied seed.
   *
   * @param withReplacement Sample with replacement or not.
   * @param fraction Fraction of rows to generate, range [0.0, 1.0].
   * @param seed Seed for sampling.
   *
   * @note This is NOT guaranteed to provide exactly the fraction of the count
   * of the given [[Dataset]].
   *
   * @group typedrel
   * @since 1.6.0
   */
  def sample(withReplacement: Boolean, fraction: Double, seed: Long): Dataset[T] = {
    withTypedPlan {
      Sample(0.0, fraction, withReplacement, seed, logicalPlan)
    }
  }

  /**
   * Returns a new [[Dataset]] by sampling a fraction of rows, using a random seed.
   *
   * @param withReplacement Sample with replacement or not.
   * @param fraction Fraction of rows to generate, range [0.0, 1.0].
   *
   * @note This is NOT guaranteed to provide exactly the fraction of the total count
   * of the given [[Dataset]].
   *
   * @group typedrel
   * @since 1.6.0
   */
  def sample(withReplacement: Boolean, fraction: Double): Dataset[T] = {
    sample(withReplacement, fraction, Utils.random.nextLong)
  }

  /**
   * Randomly splits this Dataset with the provided weights.
   *
   * @param weights weights for splits, will be normalized if they don't sum to 1.
   * @param seed Seed for sampling.
   *
   * For Java API, use [[randomSplitAsList]].
   *
   * @group typedrel
   * @since 2.0.0
   */
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

  /**
   * Returns a Java list that contains randomly split Dataset with the provided weights.
   *
   * @param weights weights for splits, will be normalized if they don't sum to 1.
   * @param seed Seed for sampling.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def randomSplitAsList(weights: Array[Double], seed: Long): java.util.List[Dataset[T]] = {
    val values = randomSplit(weights, seed)
    java.util.Arrays.asList(values : _*)
  }

  /**
   * Randomly splits this Dataset with the provided weights.
   *
   * @param weights weights for splits, will be normalized if they don't sum to 1.
   * @group typedrel
   * @since 2.0.0
   */
  def randomSplit(weights: Array[Double]): Array[Dataset[T]] = {
    randomSplit(weights, Utils.random.nextLong)
  }

  /**
   * Randomly splits this Dataset with the provided weights. Provided for the Python Api.
   *
   * @param weights weights for splits, will be normalized if they don't sum to 1.
   * @param seed Seed for sampling.
   */
  private[spark] def randomSplit(weights: List[Double], seed: Long): Array[Dataset[T]] = {
    randomSplit(weights.toArray, seed)
  }

  /**
   * (Scala-specific) Returns a new Dataset where each row has been expanded to zero or more
   * rows by the provided function. This is similar to a `LATERAL VIEW` in HiveQL. The columns of
   * the input row are implicitly joined with each row that is output by the function.
   *
   * Given that this is deprecated, as an alternative, you can explode columns either using
   * `functions.explode()` or `flatMap()`. The following example uses these alternatives to count
   * the number of books that contain a given word:
   *
   * {{{
   *   case class Book(title: String, words: String)
   *   val ds: Dataset[Book]
   *
   *   val allWords = ds.select('title, explode(split('words, " ")).as("word"))
   *
   *   val bookCountPerWord = allWords.groupBy("word").agg(countDistinct("title"))
   * }}}
   *
   * Using `flatMap()` this can similarly be exploded as:
   *
   * {{{
   *   ds.flatMap(_.words.split(" "))
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  @deprecated("use flatMap() or select() with functions.explode() instead", "2.0.0")
  def explode[A <: Product : TypeTag](input: Column*)(f: Row => TraversableOnce[A]): DataFrame = {
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

  /**
   * (Scala-specific) Returns a new Dataset where a single column has been expanded to zero
   * or more rows by the provided function. This is similar to a `LATERAL VIEW` in HiveQL. All
   * columns of the input row are implicitly joined with each value that is output by the function.
   *
   * Given that this is deprecated, as an alternative, you can explode columns either using
   * `functions.explode()`:
   *
   * {{{
   *   ds.select(explode(split('words, " ")).as("word"))
   * }}}
   *
   * or `flatMap()`:
   *
   * {{{
   *   ds.flatMap(_.words.split(" "))
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  @deprecated("use flatMap() or select() with functions.explode() instead", "2.0.0")
  def explode[A, B : TypeTag](inputColumn: String, outputColumn: String)(f: A => TraversableOnce[B])
    : DataFrame = {
    val dataType = ScalaReflection.schemaFor[B].dataType
    val attributes = AttributeReference(outputColumn, dataType)() :: Nil
    // TODO handle the metadata?
    val elementSchema = attributes.toStructType

    def rowFunction(row: Row): TraversableOnce[InternalRow] = {
      val convert = CatalystTypeConverters.createToCatalystConverter(dataType)
      f(row(0).asInstanceOf[A]).map(o => InternalRow(convert(o)))
    }
    val generator = UserDefinedGenerator(elementSchema, rowFunction, apply(inputColumn).expr :: Nil)

    withPlan {
      Generate(generator, unrequiredChildIndex = Nil, outer = false,
        qualifier = None, generatorOutput = Nil, logicalPlan)
    }
  }

  /**
   * Returns a new Dataset by adding a column or replacing the existing column that has
   * the same name.
   *
   * `column`'s expression must only refer to attributes supplied by this Dataset. It is an
   * error to add a column that refers to some other Dataset.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def withColumn(colName: String, col: Column): DataFrame = withColumns(Seq(colName), Seq(col))

  /**
   * Returns a new Dataset by adding columns or replacing the existing columns that has
   * the same names.
   */
  private[spark] def withColumns(colNames: Seq[String], cols: Seq[Column]): DataFrame = {
    require(colNames.size == cols.size,
      s"The size of column names: ${colNames.size} isn't equal to " +
        s"the size of columns: ${cols.size}")
    SchemaUtils.checkColumnNameDuplication(
      colNames,
      "in given column names",
      sparkSession.sessionState.conf.caseSensitiveAnalysis)

    val resolver = sparkSession.sessionState.analyzer.resolver
    val output = queryExecution.analyzed.output

    val columnMap = colNames.zip(cols).toMap

    val replacedAndExistingColumns = output.map { field =>
      columnMap.find { case (colName, _) =>
        resolver(field.name, colName)
      } match {
        case Some((colName: String, col: Column)) => col.as(colName)
        case _ => Column(field)
      }
    }

    val newColumns = columnMap.filter { case (colName, col) =>
      !output.exists(f => resolver(f.name, colName))
    }.map { case (colName, col) => col.as(colName) }

    select(replacedAndExistingColumns ++ newColumns : _*)
  }

  /**
   * Returns a new Dataset by adding columns with metadata.
   */
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

  /**
   * Returns a new Dataset by adding a column with metadata.
   */
  private[spark] def withColumn(colName: String, col: Column, metadata: Metadata): DataFrame =
    withColumns(Seq(colName), Seq(col), Seq(metadata))

  /**
   * Returns a new Dataset with a column renamed.
   * This is a no-op if schema doesn't contain existingName.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def withColumnRenamed(existingName: String, newName: String): DataFrame = {
    val resolver = sparkSession.sessionState.analyzer.resolver
    val output = queryExecution.analyzed.output
    val shouldRename = output.exists(f => resolver(f.name, existingName))
    if (shouldRename) {
      val columns = output.map { col =>
        if (resolver(col.name, existingName)) {
          Column(col).as(newName)
        } else {
          Column(col)
        }
      }
      select(columns : _*)
    } else {
      toDF()
    }
  }

  /**
   * Returns a new Dataset with a column dropped. This is a no-op if schema doesn't contain
   * column name.
   *
   * This method can only be used to drop top level columns. the colName string is treated
   * literally without further interpretation.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def drop(colName: String): DataFrame = {
    drop(Seq(colName) : _*)
  }

  /**
   * Returns a new Dataset with columns dropped.
   * This is a no-op if schema doesn't contain column name(s).
   *
   * This method can only be used to drop top level columns. the colName string is treated literally
   * without further interpretation.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def drop(colNames: String*): DataFrame = {
    val resolver = sparkSession.sessionState.analyzer.resolver
    val allColumns = queryExecution.analyzed.output
    val remainingCols = allColumns.filter { attribute =>
      colNames.forall(n => !resolver(attribute.name, n))
    }.map(attribute => Column(attribute))
    if (remainingCols.size == allColumns.size) {
      toDF()
    } else {
      this.select(remainingCols: _*)
    }
  }

  /**
   * Returns a new Dataset with a column dropped.
   * This version of drop accepts a [[Column]] rather than a name.
   * This is a no-op if the Dataset doesn't have a column
   * with an equivalent expression.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def drop(col: Column): DataFrame = {
    val expression = col match {
      case Column(u: UnresolvedAttribute) =>
        queryExecution.analyzed.resolveQuoted(
          u.name, sparkSession.sessionState.analyzer.resolver).getOrElse(u)
      case Column(expr: Expression) => expr
    }
    val attrs = this.logicalPlan.output
    val colsAfterDrop = attrs.filter { attr =>
      attr != expression
    }.map(attr => Column(attr))
    select(colsAfterDrop : _*)
  }

  /**
   * Returns a new Dataset that contains only the unique rows from this Dataset.
   * This is an alias for `distinct`.
   *
   * For a static batch [[Dataset]], it just drops duplicate rows. For a streaming [[Dataset]], it
   * will keep all data across triggers as intermediate state to drop duplicates rows. You can use
   * [[withWatermark]] to limit how late the duplicate data can be and system will accordingly limit
   * the state. In addition, too late data older than watermark will be dropped to avoid any
   * possibility of duplicates.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def dropDuplicates(): Dataset[T] = dropDuplicates(this.columns)

  /**
   * (Scala-specific) Returns a new Dataset with duplicate rows removed, considering only
   * the subset of columns.
   *
   * For a static batch [[Dataset]], it just drops duplicate rows. For a streaming [[Dataset]], it
   * will keep all data across triggers as intermediate state to drop duplicates rows. You can use
   * [[withWatermark]] to limit how late the duplicate data can be and system will accordingly limit
   * the state. In addition, too late data older than watermark will be dropped to avoid any
   * possibility of duplicates.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def dropDuplicates(colNames: Seq[String]): Dataset[T] = withTypedPlan {
    val resolver = sparkSession.sessionState.analyzer.resolver
    val allColumns = queryExecution.analyzed.output
    val groupCols = colNames.toSet.toSeq.flatMap { (colName: String) =>
      // It is possibly there are more than one columns with the same name,
      // so we call filter instead of find.
      val cols = allColumns.filter(col => resolver(col.name, colName))
      if (cols.isEmpty) {
        throw new AnalysisException(
          s"""Cannot resolve column name "$colName" among (${schema.fieldNames.mkString(", ")})""")
      }
      cols
    }
    Deduplicate(groupCols, logicalPlan)
  }

  /**
   * Returns a new Dataset with duplicate rows removed, considering only
   * the subset of columns.
   *
   * For a static batch [[Dataset]], it just drops duplicate rows. For a streaming [[Dataset]], it
   * will keep all data across triggers as intermediate state to drop duplicates rows. You can use
   * [[withWatermark]] to limit how late the duplicate data can be and system will accordingly limit
   * the state. In addition, too late data older than watermark will be dropped to avoid any
   * possibility of duplicates.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def dropDuplicates(colNames: Array[String]): Dataset[T] = dropDuplicates(colNames.toSeq)

  /**
   * Returns a new [[Dataset]] with duplicate rows removed, considering only
   * the subset of columns.
   *
   * For a static batch [[Dataset]], it just drops duplicate rows. For a streaming [[Dataset]], it
   * will keep all data across triggers as intermediate state to drop duplicates rows. You can use
   * [[withWatermark]] to limit how late the duplicate data can be and system will accordingly limit
   * the state. In addition, too late data older than watermark will be dropped to avoid any
   * possibility of duplicates.
   *
   * @group typedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def dropDuplicates(col1: String, cols: String*): Dataset[T] = {
    val colNames: Seq[String] = col1 +: cols
    dropDuplicates(colNames)
  }

  /**
   * Computes basic statistics for numeric and string columns, including count, mean, stddev, min,
   * and max. If no columns are given, this function computes statistics for all numerical or
   * string columns.
   *
   * This function is meant for exploratory data analysis, as we make no guarantee about the
   * backward compatibility of the schema of the resulting Dataset. If you want to
   * programmatically compute summary statistics, use the `agg` function instead.
   *
   * {{{
   *   ds.describe("age", "height").show()
   *
   *   // output:
   *   // summary age   height
   *   // count   10.0  10.0
   *   // mean    53.3  178.05
   *   // stddev  11.6  15.7
   *   // min     18.0  163.0
   *   // max     92.0  192.0
   * }}}
   *
   * Use [[summary]] for expanded statistics and control over which statistics to compute.
   *
   * @param cols Columns to compute statistics on.
   *
   * @group action
   * @since 1.6.0
   */
  @scala.annotation.varargs
  def describe(cols: String*): DataFrame = {
    val selected = if (cols.isEmpty) this else select(cols.head, cols.tail: _*)
    selected.summary("count", "mean", "stddev", "min", "max")
  }

  /**
   * Computes specified statistics for numeric and string columns. Available statistics are:
   *
   * - count
   * - mean
   * - stddev
   * - min
   * - max
   * - arbitrary approximate percentiles specified as a percentage (eg, 75%)
   *
   * If no statistics are given, this function computes count, mean, stddev, min,
   * approximate quartiles (percentiles at 25%, 50%, and 75%), and max.
   *
   * This function is meant for exploratory data analysis, as we make no guarantee about the
   * backward compatibility of the schema of the resulting Dataset. If you want to
   * programmatically compute summary statistics, use the `agg` function instead.
   *
   * {{{
   *   ds.summary().show()
   *
   *   // output:
   *   // summary age   height
   *   // count   10.0  10.0
   *   // mean    53.3  178.05
   *   // stddev  11.6  15.7
   *   // min     18.0  163.0
   *   // 25%     24.0  176.0
   *   // 50%     24.0  176.0
   *   // 75%     32.0  180.0
   *   // max     92.0  192.0
   * }}}
   *
   * {{{
   *   ds.summary("count", "min", "25%", "75%", "max").show()
   *
   *   // output:
   *   // summary age   height
   *   // count   10.0  10.0
   *   // min     18.0  163.0
   *   // 25%     24.0  176.0
   *   // 75%     32.0  180.0
   *   // max     92.0  192.0
   * }}}
   *
   * To do a summary for specific columns first select them:
   *
   * {{{
   *   ds.select("age", "height").summary().show()
   * }}}
   *
   * See also [[describe]] for basic statistics.
   *
   * @param statistics Statistics from above list to be computed.
   *
   * @group action
   * @since 2.3.0
   */
  @scala.annotation.varargs
  def summary(statistics: String*): DataFrame = StatFunctions.summary(this, statistics.toSeq)

  /**
   * Returns the first `n` rows.
   *
   * @note this method should only be used if the resulting array is expected to be small, as
   * all the data is loaded into the driver's memory.
   *
   * @group action
   * @since 1.6.0
   */
  def head(n: Int): Array[T] = withAction("head", limit(n).queryExecution)(collectFromPlan)

  /**
   * Returns the first row.
   * @group action
   * @since 1.6.0
   */
  def head(): T = head(1).head

  /**
   * Returns the first row. Alias for head().
   * @group action
   * @since 1.6.0
   */
  def first(): T = head()

  /**
   * Concise syntax for chaining custom transformations.
   * {{{
   *   def featurize(ds: Dataset[T]): Dataset[U] = ...
   *
   *   ds
   *     .transform(featurize)
   *     .transform(...)
   * }}}
   *
   * @group typedrel
   * @since 1.6.0
   */
  def transform[U](t: Dataset[T] => Dataset[U]): Dataset[U] = t(this)

  /**
   * :: Experimental ::
   * (Scala-specific)
   * Returns a new Dataset that only contains elements where `func` returns `true`.
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def filter(func: T => Boolean): Dataset[T] = {
    withTypedPlan(TypedFilter(func, logicalPlan))
  }

  /**
   * :: Experimental ::
   * (Java-specific)
   * Returns a new Dataset that only contains elements where `func` returns `true`.
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def filter(func: FilterFunction[T]): Dataset[T] = {
    withTypedPlan(TypedFilter(func, logicalPlan))
  }

  /**
   * :: Experimental ::
   * (Scala-specific)
   * Returns a new Dataset that contains the result of applying `func` to each element.
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def map[U : Encoder](func: T => U): Dataset[U] = withTypedPlan {
    MapElements[T, U](func, logicalPlan)
  }

  /**
   * :: Experimental ::
   * (Java-specific)
   * Returns a new Dataset that contains the result of applying `func` to each element.
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def map[U](func: MapFunction[T, U], encoder: Encoder[U]): Dataset[U] = {
    implicit val uEnc = encoder
    withTypedPlan(MapElements[T, U](func, logicalPlan))
  }

  /**
   * :: Experimental ::
   * (Scala-specific)
   * Returns a new Dataset that contains the result of applying `func` to each partition.
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def mapPartitions[U : Encoder](func: Iterator[T] => Iterator[U]): Dataset[U] = {
    new Dataset[U](
      sparkSession,
      MapPartitions[T, U](func, logicalPlan),
      implicitly[Encoder[U]])
  }

  /**
   * :: Experimental ::
   * (Java-specific)
   * Returns a new Dataset that contains the result of applying `f` to each partition.
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
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
   * :: Experimental ::
   * (Scala-specific)
   * Returns a new Dataset by first applying a function to all elements of this Dataset,
   * and then flattening the results.
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def flatMap[U : Encoder](func: T => TraversableOnce[U]): Dataset[U] =
    mapPartitions(_.flatMap(func))

  /**
   * :: Experimental ::
   * (Java-specific)
   * Returns a new Dataset by first applying a function to all elements of this Dataset,
   * and then flattening the results.
   *
   * @group typedrel
   * @since 1.6.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def flatMap[U](f: FlatMapFunction[T, U], encoder: Encoder[U]): Dataset[U] = {
    val func: (T) => Iterator[U] = x => f.call(x).asScala
    flatMap(func)(encoder)
  }

  /**
   * Applies a function `f` to all rows.
   *
   * @group action
   * @since 1.6.0
   */
  def foreach(f: T => Unit): Unit = withNewRDDExecutionId {
    rdd.foreach(f)
  }

  /**
   * (Java-specific)
   * Runs `func` on each element of this Dataset.
   *
   * @group action
   * @since 1.6.0
   */
  def foreach(func: ForeachFunction[T]): Unit = foreach(func.call(_))

  /**
   * Applies a function `f` to each partition of this Dataset.
   *
   * @group action
   * @since 1.6.0
   */
  def foreachPartition(f: Iterator[T] => Unit): Unit = withNewRDDExecutionId {
    rdd.foreachPartition(f)
  }

  /**
   * (Java-specific)
   * Runs `func` on each partition of this Dataset.
   *
   * @group action
   * @since 1.6.0
   */
  def foreachPartition(func: ForeachPartitionFunction[T]): Unit = {
    foreachPartition((it: Iterator[T]) => func.call(it.asJava))
  }

  /**
   * Returns the first `n` rows in the Dataset.
   *
   * Running take requires moving data into the application's driver process, and doing so with
   * a very large `n` can crash the driver process with OutOfMemoryError.
   *
   * @group action
   * @since 1.6.0
   */
  def take(n: Int): Array[T] = head(n)

  /**
   * Returns the first `n` rows in the Dataset as a list.
   *
   * Running take requires moving data into the application's driver process, and doing so with
   * a very large `n` can crash the driver process with OutOfMemoryError.
   *
   * @group action
   * @since 1.6.0
   */
  def takeAsList(n: Int): java.util.List[T] = java.util.Arrays.asList(take(n) : _*)

  /**
   * Returns an array that contains all rows in this Dataset.
   *
   * Running collect requires moving all the data into the application's driver process, and
   * doing so on a very large dataset can crash the driver process with OutOfMemoryError.
   *
   * For Java API, use [[collectAsList]].
   *
   * @group action
   * @since 1.6.0
   */
  def collect(): Array[T] = withAction("collect", queryExecution)(collectFromPlan)

  /**
   * Returns a Java list that contains all rows in this Dataset.
   *
   * Running collect requires moving all the data into the application's driver process, and
   * doing so on a very large dataset can crash the driver process with OutOfMemoryError.
   *
   * @group action
   * @since 1.6.0
   */
  def collectAsList(): java.util.List[T] = withAction("collectAsList", queryExecution) { plan =>
    val values = collectFromPlan(plan)
    java.util.Arrays.asList(values : _*)
  }

  /**
   * Returns an iterator that contains all rows in this Dataset.
   *
   * The iterator will consume as much memory as the largest partition in this Dataset.
   *
   * @note this results in multiple Spark jobs, and if the input Dataset is the result
   * of a wide transformation (e.g. join with different partitioners), to avoid
   * recomputing the input Dataset should be cached first.
   *
   * @group action
   * @since 2.0.0
   */
  def toLocalIterator(): java.util.Iterator[T] = {
    withAction("toLocalIterator", queryExecution) { plan =>
      // This projection writes output to a `InternalRow`, which means applying this projection is
      // not thread-safe. Here we create the projection inside this method to make `Dataset`
      // thread-safe.
      val objProj = GenerateSafeProjection.generate(deserializer :: Nil)
      plan.executeToIterator().map { row =>
        // The row returned by SafeProjection is `SpecificInternalRow`, which ignore the data type
        // parameter of its `get` method, so it's safe to use null here.
        objProj(row).get(0, null).asInstanceOf[T]
      }.asJava
    }
  }

  /**
   * Returns the number of rows in the Dataset.
   * @group action
   * @since 1.6.0
   */
  def count(): Long = withAction("count", groupBy().count().queryExecution) { plan =>
    plan.executeCollect().head.getLong(0)
  }

  /**
   * Returns a new Dataset that has exactly `numPartitions` partitions.
   *
   * @group typedrel
   * @since 1.6.0
   */
  def repartition(numPartitions: Int): Dataset[T] = withTypedPlan {
    Repartition(numPartitions, shuffle = true, logicalPlan)
  }

  /**
   * Returns a new Dataset partitioned by the given partitioning expressions into
   * `numPartitions`. The resulting Dataset is hash partitioned.
   *
   * This is the same operation as "DISTRIBUTE BY" in SQL (Hive QL).
   *
   * @group typedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def repartition(numPartitions: Int, partitionExprs: Column*): Dataset[T] = {
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

  /**
   * Returns a new Dataset partitioned by the given partitioning expressions, using
   * `spark.sql.shuffle.partitions` as number of partitions.
   * The resulting Dataset is hash partitioned.
   *
   * This is the same operation as "DISTRIBUTE BY" in SQL (Hive QL).
   *
   * @group typedrel
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def repartition(partitionExprs: Column*): Dataset[T] = {
    repartition(sparkSession.sessionState.conf.numShufflePartitions, partitionExprs: _*)
  }

  /**
   * Returns a new Dataset partitioned by the given partitioning expressions into
   * `numPartitions`. The resulting Dataset is range partitioned.
   *
   * At least one partition-by expression must be specified.
   * When no explicit sort order is specified, "ascending nulls first" is assumed.
   * Note, the rows are not sorted in each partition of the resulting Dataset.
   *
   * @group typedrel
   * @since 2.3.0
   */
  @scala.annotation.varargs
  def repartitionByRange(numPartitions: Int, partitionExprs: Column*): Dataset[T] = {
    require(partitionExprs.nonEmpty, "At least one partition-by expression must be specified.")
    val sortOrder: Seq[SortOrder] = partitionExprs.map(_.expr match {
      case expr: SortOrder => expr
      case expr: Expression => SortOrder(expr, Ascending)
    })
    withTypedPlan {
      RepartitionByExpression(sortOrder, logicalPlan, numPartitions)
    }
  }

  /**
   * Returns a new Dataset partitioned by the given partitioning expressions, using
   * `spark.sql.shuffle.partitions` as number of partitions.
   * The resulting Dataset is range partitioned.
   *
   * At least one partition-by expression must be specified.
   * When no explicit sort order is specified, "ascending nulls first" is assumed.
   * Note, the rows are not sorted in each partition of the resulting Dataset.
   *
   * @group typedrel
   * @since 2.3.0
   */
  @scala.annotation.varargs
  def repartitionByRange(partitionExprs: Column*): Dataset[T] = {
    repartitionByRange(sparkSession.sessionState.conf.numShufflePartitions, partitionExprs: _*)
  }

  /**
   * Returns a new Dataset that has exactly `numPartitions` partitions, when the fewer partitions
   * are requested. If a larger number of partitions is requested, it will stay at the current
   * number of partitions. Similar to coalesce defined on an `RDD`, this operation results in
   * a narrow dependency, e.g. if you go from 1000 partitions to 100 partitions, there will not
   * be a shuffle, instead each of the 100 new partitions will claim 10 of the current partitions.
   *
   * However, if you're doing a drastic coalesce, e.g. to numPartitions = 1,
   * this may result in your computation taking place on fewer nodes than
   * you like (e.g. one node in the case of numPartitions = 1). To avoid this,
   * you can call repartition. This will add a shuffle step, but means the
   * current upstream partitions will be executed in parallel (per whatever
   * the current partitioning is).
   *
   * @group typedrel
   * @since 1.6.0
   */
  def coalesce(numPartitions: Int): Dataset[T] = withTypedPlan {
    Repartition(numPartitions, shuffle = false, logicalPlan)
  }

  /**
   * Returns a new Dataset that contains only the unique rows from this Dataset.
   * This is an alias for `dropDuplicates`.
   *
   * @note Equality checking is performed directly on the encoded representation of the data
   * and thus is not affected by a custom `equals` function defined on `T`.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def distinct(): Dataset[T] = dropDuplicates()

  /**
   * Persist this Dataset with the default storage level (`MEMORY_AND_DISK`).
   *
   * @group basic
   * @since 1.6.0
   */
  def persist(): this.type = {
    sparkSession.sharedState.cacheManager.cacheQuery(this)
    this
  }

  /**
   * Persist this Dataset with the default storage level (`MEMORY_AND_DISK`).
   *
   * @group basic
   * @since 1.6.0
   */
  def cache(): this.type = persist()

  /**
   * Persist this Dataset with the given storage level.
   * @param newLevel One of: `MEMORY_ONLY`, `MEMORY_AND_DISK`, `MEMORY_ONLY_SER`,
   *                 `MEMORY_AND_DISK_SER`, `DISK_ONLY`, `MEMORY_ONLY_2`,
   *                 `MEMORY_AND_DISK_2`, etc.
   *
   * @group basic
   * @since 1.6.0
   */
  def persist(newLevel: StorageLevel): this.type = {
    sparkSession.sharedState.cacheManager.cacheQuery(this, None, newLevel)
    this
  }

  /**
   * Get the Dataset's current storage level, or StorageLevel.NONE if not persisted.
   *
   * @group basic
   * @since 2.1.0
   */
  def storageLevel: StorageLevel = {
    sparkSession.sharedState.cacheManager.lookupCachedData(this).map { cachedData =>
      cachedData.cachedRepresentation.cacheBuilder.storageLevel
    }.getOrElse(StorageLevel.NONE)
  }

  /**
   * Mark the Dataset as non-persistent, and remove all blocks for it from memory and disk.
   * This will not un-persist any cached data that is built upon this Dataset.
   *
   * @param blocking Whether to block until all blocks are deleted.
   *
   * @group basic
   * @since 1.6.0
   */
  def unpersist(blocking: Boolean): this.type = {
    sparkSession.sharedState.cacheManager.uncacheQuery(this, cascade = false, blocking)
    this
  }

  /**
   * Mark the Dataset as non-persistent, and remove all blocks for it from memory and disk.
   * This will not un-persist any cached data that is built upon this Dataset.
   *
   * @group basic
   * @since 1.6.0
   */
  def unpersist(): this.type = unpersist(blocking = false)

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

  /**
   * Registers this Dataset as a temporary table using the given name. The lifetime of this
   * temporary table is tied to the [[SparkSession]] that was used to create this Dataset.
   *
   * @group basic
   * @since 1.6.0
   */
  @deprecated("Use createOrReplaceTempView(viewName) instead.", "2.0.0")
  def registerTempTable(tableName: String): Unit = {
    createOrReplaceTempView(tableName)
  }

  /**
   * Creates a local temporary view using the given name. The lifetime of this
   * temporary view is tied to the [[SparkSession]] that was used to create this Dataset.
   *
   * Local temporary view is session-scoped. Its lifetime is the lifetime of the session that
   * created it, i.e. it will be automatically dropped when the session terminates. It's not
   * tied to any databases, i.e. we can't use `db1.view1` to reference a local temporary view.
   *
   * @throws AnalysisException if the view name is invalid or already exists
   *
   * @group basic
   * @since 2.0.0
   */
  @throws[AnalysisException]
  def createTempView(viewName: String): Unit = withPlan {
    createTempViewCommand(viewName, replace = false, global = false)
  }



  /**
   * Creates a local temporary view using the given name. The lifetime of this
   * temporary view is tied to the [[SparkSession]] that was used to create this Dataset.
   *
   * @group basic
   * @since 2.0.0
   */
  def createOrReplaceTempView(viewName: String): Unit = withPlan {
    createTempViewCommand(viewName, replace = true, global = false)
  }

  /**
   * Creates a global temporary view using the given name. The lifetime of this
   * temporary view is tied to this Spark application.
   *
   * Global temporary view is cross-session. Its lifetime is the lifetime of the Spark application,
   * i.e. it will be automatically dropped when the application terminates. It's tied to a system
   * preserved database `global_temp`, and we must use the qualified name to refer a global temp
   * view, e.g. `SELECT * FROM global_temp.view1`.
   *
   * @throws AnalysisException if the view name is invalid or already exists
   *
   * @group basic
   * @since 2.1.0
   */
  @throws[AnalysisException]
  def createGlobalTempView(viewName: String): Unit = withPlan {
    createTempViewCommand(viewName, replace = false, global = true)
  }

  /**
   * Creates or replaces a global temporary view using the given name. The lifetime of this
   * temporary view is tied to this Spark application.
   *
   * Global temporary view is cross-session. Its lifetime is the lifetime of the Spark application,
   * i.e. it will be automatically dropped when the application terminates. It's tied to a system
   * preserved database `global_temp`, and we must use the qualified name to refer a global temp
   * view, e.g. `SELECT * FROM global_temp.view1`.
   *
   * @group basic
   * @since 2.2.0
   */
  def createOrReplaceGlobalTempView(viewName: String): Unit = withPlan {
    createTempViewCommand(viewName, replace = true, global = true)
  }

  private def createTempViewCommand(
      viewName: String,
      replace: Boolean,
      global: Boolean): CreateViewCommand = {
    val viewType = if (global) GlobalTempView else LocalTempView

    val tableIdentifier = try {
      sparkSession.sessionState.sqlParser.parseTableIdentifier(viewName)
    } catch {
      case _: ParseException => throw new AnalysisException(s"Invalid view name: $viewName")
    }
    CreateViewCommand(
      name = tableIdentifier,
      userSpecifiedColumns = Nil,
      comment = None,
      properties = Map.empty,
      originalText = None,
      child = logicalPlan,
      allowExisting = false,
      replace = replace,
      viewType = viewType)
  }

  /**
   * Interface for saving the content of the non-streaming Dataset out into external storage.
   *
   * @group basic
   * @since 1.6.0
   */
  def write: DataFrameWriter[T] = {
    if (isStreaming) {
      logicalPlan.failAnalysis(
        "'write' can not be called on streaming Dataset/DataFrame")
    }
    new DataFrameWriter[T](this)
  }

  /**
   * Interface for saving the content of the streaming Dataset out into external storage.
   *
   * @group basic
   * @since 2.0.0
   */
  @InterfaceStability.Evolving
  def writeStream: DataStreamWriter[T] = {
    if (!isStreaming) {
      logicalPlan.failAnalysis(
        "'writeStream' can be called only on streaming Dataset/DataFrame")
    }
    new DataStreamWriter[T](this)
  }


  /**
   * Returns the content of the Dataset as a Dataset of JSON strings.
   * @since 2.0.0
   */
  def toJSON: Dataset[String] = {
    val rowSchema = this.schema
    val sessionLocalTimeZone = sparkSession.sessionState.conf.sessionLocalTimeZone
    mapPartitions { iter =>
      val writer = new CharArrayWriter()
      // create the Generator without separator inserted between 2 records
      val gen = new JacksonGenerator(rowSchema, writer,
        new JSONOptions(Map.empty[String, String], sessionLocalTimeZone))

      new Iterator[String] {
        override def hasNext: Boolean = iter.hasNext
        override def next(): String = {
          gen.write(exprEnc.toRow(iter.next()))
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

  /**
   * Returns a best-effort snapshot of the files that compose this Dataset. This method simply
   * asks each constituent BaseRelation for its respective files and takes the union of all results.
   * Depending on the source relations, this may not find all input files. Duplicates are removed.
   *
   * @group basic
   * @since 2.0.0
   */
  def inputFiles: Array[String] = {
    val files: Seq[String] = queryExecution.optimizedPlan.collect {
      case LogicalRelation(fsBasedRelation: FileRelation, _, _, _) =>
        fsBasedRelation.inputFiles
      case fr: FileRelation =>
        fr.inputFiles
      case r: HiveTableRelation =>
        r.tableMeta.storage.locationUri.map(_.toString).toArray
    }.flatten
    files.toSet.toArray
  }

  ////////////////////////////////////////////////////////////////////////////
  // For Python API
  ////////////////////////////////////////////////////////////////////////////

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
    val iter = withAction("collectToPython", queryExecution) { plan =>
      val toJava: (Any) => Any = EvaluatePython.toJava(_, schema)
      new SerDeUtil.AutoBatchedPickler(plan.executeCollect().iterator.map(toJava))
    }
    PythonRDD.serveIterator(iter, "serve-DataFrame")
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
   * Collect a Dataset as Arrow batches and serve stream to PySpark.
   */
  private[sql] def collectAsArrowToPython(): Array[Any] = {
    val timeZoneId = sparkSession.sessionState.conf.sessionLocalTimeZone

    PythonRDD.serveToStreamWithSync("serve-Arrow") { out =>
      withAction("collectAsArrowToPython", queryExecution) { plan =>
        val batchWriter = new ArrowBatchStreamWriter(schema, out, timeZoneId)
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

  private[sql] def toPythonIterator(): Array[Any] = {
    withNewExecutionId {
      PythonRDD.toLocalIteratorAndServe(javaToPython.rdd)
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
    SQLExecution.withNewExecutionId(sparkSession, queryExecution)(body)
  }

  /**
   * Wrap an action of the Dataset's RDD to track all Spark jobs in the body so that we can connect
   * them with an execution. Before performing the action, the metrics of the executed plan will be
   * reset.
   */
  private def withNewRDDExecutionId[U](body: => U): U = {
    SQLExecution.withNewExecutionId(sparkSession, rddQueryExecution) {
      rddQueryExecution.executedPlan.foreach { plan =>
        plan.resetMetrics()
      }
      body
    }
  }

  /**
   * Wrap a Dataset action to track the QueryExecution and time cost, then report to the
   * user-registered callback functions.
   */
  private def withAction[U](name: String, qe: QueryExecution)(action: SparkPlan => U) = {
    try {
      qe.executedPlan.foreach { plan =>
        plan.resetMetrics()
      }
      val start = System.nanoTime()
      val result = SQLExecution.withNewExecutionId(sparkSession, qe) {
        action(qe.executedPlan)
      }
      val end = System.nanoTime()
      sparkSession.listenerManager.onSuccess(name, qe, end - start)
      result
    } catch {
      case e: Throwable =>
        sparkSession.listenerManager.onFailure(name, qe, e)
        throw e
    }
  }

  /**
   * Collect all elements from a spark plan.
   */
  private def collectFromPlan(plan: SparkPlan): Array[T] = {
    // This projection writes output to a `InternalRow`, which means applying this projection is not
    // thread-safe. Here we create the projection inside this method to make `Dataset` thread-safe.
    val objProj = GenerateSafeProjection.generate(deserializer :: Nil)
    plan.executeCollect().map { row =>
      // The row returned by SafeProjection is `SpecificInternalRow`, which ignore the data type
      // parameter of its `get` method, so it's safe to use null here.
      objProj(row).get(0, null).asInstanceOf[T]
    }
  }

  private def sortInternal(global: Boolean, sortExprs: Seq[Column]): Dataset[T] = {
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

  /** Convert to an RDD of serialized ArrowRecordBatches. */
  private[sql] def toArrowBatchRdd(plan: SparkPlan): RDD[Array[Byte]] = {
    val schemaCaptured = this.schema
    val maxRecordsPerBatch = sparkSession.sessionState.conf.arrowMaxRecordsPerBatch
    val timeZoneId = sparkSession.sessionState.conf.sessionLocalTimeZone
    plan.execute().mapPartitionsInternal { iter =>
      val context = TaskContext.get()
      ArrowConverters.toBatchIterator(
        iter, schemaCaptured, maxRecordsPerBatch, timeZoneId, context)
    }
  }

  // This is only used in tests, for now.
  private[sql] def toArrowBatchRdd: RDD[Array[Byte]] = {
    toArrowBatchRdd(queryExecution.executedPlan)
  }
}
