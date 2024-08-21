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

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.analysis.{CannotReplaceMissingTableException, NoSuchTableException, TableAlreadyExistsException, UnresolvedFunction, UnresolvedIdentifier, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, CreateTableAsSelect, LogicalPlan, OptionList, OverwriteByExpression, OverwritePartitionsDynamic, ReplaceTableAsSelect, UnresolvedTableSpec}
import org.apache.spark.sql.connector.expressions.{ClusterByTransform, FieldReference, LogicalExpressions, NamedReference, Transform}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.types.IntegerType

/**
 * Interface used to write a [[org.apache.spark.sql.Dataset]] to external storage using the v2 API.
 *
 * @since 3.0.0
 */
@Experimental
final class DataFrameWriterV2[T] private[sql](table: String, ds: Dataset[T])
    extends CreateTableWriter[T] {

  private val df: DataFrame = ds.toDF()

  private val sparkSession = ds.sparkSession
  import sparkSession.expression

  private val tableName = sparkSession.sessionState.sqlParser.parseMultipartIdentifier(table)

  private val logicalPlan = df.queryExecution.logical

  private var provider: Option[String] = None

  private val options = new mutable.HashMap[String, String]()

  private val properties = new mutable.HashMap[String, String]()

  private var partitioning: Option[Seq[Transform]] = None

  private var clustering: Option[ClusterByTransform] = None

  override def using(provider: String): CreateTableWriter[T] = {
    this.provider = Some(provider)
    this
  }

  override def option(key: String, value: String): DataFrameWriterV2[T] = {
    this.options.put(key, value)
    this
  }

  override def options(options: scala.collection.Map[String, String]): DataFrameWriterV2[T] = {
    options.foreach {
      case (key, value) =>
        this.options.put(key, value)
    }
    this
  }

  override def options(options: java.util.Map[String, String]): DataFrameWriterV2[T] = {
    this.options(options.asScala)
    this
  }

  override def tableProperty(property: String, value: String): CreateTableWriter[T] = {
    this.properties.put(property, value)
    this
  }

  @scala.annotation.varargs
  override def partitionedBy(column: Column, columns: Column*): CreateTableWriter[T] = {
    def ref(name: String): NamedReference = LogicalExpressions.parseReference(name)

    val asTransforms = (column +: columns).map(expression).map {
      case PartitionTransform.YEARS(Seq(attr: Attribute)) =>
        LogicalExpressions.years(ref(attr.name))
      case PartitionTransform.MONTHS(Seq(attr: Attribute)) =>
        LogicalExpressions.months(ref(attr.name))
      case PartitionTransform.DAYS(Seq(attr: Attribute)) =>
        LogicalExpressions.days(ref(attr.name))
      case PartitionTransform.HOURS(Seq(attr: Attribute)) =>
        LogicalExpressions.hours(ref(attr.name))
      case PartitionTransform.BUCKET(Seq(Literal(numBuckets: Int, IntegerType), attr: Attribute)) =>
        LogicalExpressions.bucket(numBuckets, Array(ref(attr.name)))
      case PartitionTransform.BUCKET(Seq(numBuckets, e)) =>
        throw QueryCompilationErrors.invalidBucketsNumberError(numBuckets.toString, e.toString)
      case attr: Attribute =>
        LogicalExpressions.identity(ref(attr.name))
      case expr =>
        throw QueryCompilationErrors.invalidPartitionTransformationError(expr)
    }

    this.partitioning = Some(asTransforms)
    validatePartitioning()
    this
  }

  @scala.annotation.varargs
  override def clusterBy(colName: String, colNames: String*): CreateTableWriter[T] = {
    this.clustering =
      Some(ClusterByTransform((colName +: colNames).map(col => FieldReference(col))))
    validatePartitioning()
    this
  }

  /**
   * Validate that clusterBy is not used with partitionBy.
   */
  private def validatePartitioning(): Unit = {
    if (partitioning.nonEmpty && clustering.nonEmpty) {
      throw QueryCompilationErrors.clusterByWithPartitionedBy()
    }
  }

  override def create(): Unit = {
    val tableSpec = UnresolvedTableSpec(
      properties = properties.toMap,
      provider = provider,
      optionExpression = OptionList(Seq.empty),
      location = None,
      comment = None,
      serde = None,
      external = false)
    runCommand(
      CreateTableAsSelect(
        UnresolvedIdentifier(tableName),
        partitioning.getOrElse(Seq.empty) ++ clustering,
        logicalPlan,
        tableSpec,
        options.toMap,
        false))
  }

  override def replace(): Unit = {
    internalReplace(orCreate = false)
  }

  override def createOrReplace(): Unit = {
    internalReplace(orCreate = true)
  }


  /**
   * Append the contents of the data frame to the output table.
   *
   * If the output table does not exist, this operation will fail with
   * [[org.apache.spark.sql.catalyst.analysis.NoSuchTableException]]. The data frame will be
   * validated to ensure it is compatible with the existing table.
   *
   * @throws org.apache.spark.sql.catalyst.analysis.NoSuchTableException If the table does not exist
   */
  @throws(classOf[NoSuchTableException])
  def append(): Unit = {
    val append = AppendData.byName(UnresolvedRelation(tableName), logicalPlan, options.toMap)
    runCommand(append)
  }

  /**
   * Overwrite rows matching the given filter condition with the contents of the data frame in
   * the output table.
   *
   * If the output table does not exist, this operation will fail with
   * [[org.apache.spark.sql.catalyst.analysis.NoSuchTableException]].
   * The data frame will be validated to ensure it is compatible with the existing table.
   *
   * @throws org.apache.spark.sql.catalyst.analysis.NoSuchTableException If the table does not exist
   */
  @throws(classOf[NoSuchTableException])
  def overwrite(condition: Column): Unit = {
    val overwrite = OverwriteByExpression.byName(
      UnresolvedRelation(tableName), logicalPlan, expression(condition), options.toMap)
    runCommand(overwrite)
  }

  /**
   * Overwrite all partition for which the data frame contains at least one row with the contents
   * of the data frame in the output table.
   *
   * This operation is equivalent to Hive's `INSERT OVERWRITE ... PARTITION`, which replaces
   * partitions dynamically depending on the contents of the data frame.
   *
   * If the output table does not exist, this operation will fail with
   * [[org.apache.spark.sql.catalyst.analysis.NoSuchTableException]]. The data frame will be
   * validated to ensure it is compatible with the existing table.
   *
   * @throws org.apache.spark.sql.catalyst.analysis.NoSuchTableException If the table does not exist
   */
  @throws(classOf[NoSuchTableException])
  def overwritePartitions(): Unit = {
    val dynamicOverwrite = OverwritePartitionsDynamic.byName(
      UnresolvedRelation(tableName), logicalPlan, options.toMap)
    runCommand(dynamicOverwrite)
  }

  /**
   * Wrap an action to track the QueryExecution and time cost, then report to the user-registered
   * callback functions.
   */
  private def runCommand(command: LogicalPlan): Unit = {
    val qe = new QueryExecution(sparkSession, command, df.queryExecution.tracker)
    qe.assertCommandExecuted()
  }

  private def internalReplace(orCreate: Boolean): Unit = {
    val tableSpec = UnresolvedTableSpec(
      properties = properties.toMap,
      provider = provider,
      optionExpression = OptionList(Seq.empty),
      location = None,
      comment = None,
      serde = None,
      external = false)
    runCommand(ReplaceTableAsSelect(
      UnresolvedIdentifier(tableName),
      partitioning.getOrElse(Seq.empty) ++ clustering,
      logicalPlan,
      tableSpec,
      writeOptions = options.toMap,
      orCreate = orCreate))
  }
}

private object PartitionTransform {
  class ExtractTransform(name: String) {
    private val NAMES = Seq(name)

    def unapply(e: Expression): Option[Seq[Expression]] = e match {
      case UnresolvedFunction(NAMES, children, false, None, false, Nil, true) => Option(children)
      case _ => None
    }
  }

  val HOURS = new ExtractTransform("hours")
  val DAYS = new ExtractTransform("days")
  val MONTHS = new ExtractTransform("months")
  val YEARS = new ExtractTransform("years")
  val BUCKET = new ExtractTransform("bucket")
}

/**
 * Configuration methods common to create/replace operations and insert/overwrite operations.
 * @tparam R builder type to return
 * @since 3.0.0
 */
trait WriteConfigMethods[R] {
  /**
   * Add a write option.
   *
   * @since 3.0.0
   */
  def option(key: String, value: String): R

  /**
   * Add a boolean output option.
   *
   * @since 3.0.0
   */
  def option(key: String, value: Boolean): R = option(key, value.toString)

  /**
   * Add a long output option.
   *
   * @since 3.0.0
   */
  def option(key: String, value: Long): R = option(key, value.toString)

  /**
   * Add a double output option.
   *
   * @since 3.0.0
   */
  def option(key: String, value: Double): R = option(key, value.toString)

  /**
   * Add write options from a Scala Map.
   *
   * @since 3.0.0
   */
  def options(options: scala.collection.Map[String, String]): R

  /**
   * Add write options from a Java Map.
   *
   * @since 3.0.0
   */
  def options(options: java.util.Map[String, String]): R
}

/**
 * Trait to restrict calls to create and replace operations.
 *
 * @since 3.0.0
 */
trait CreateTableWriter[T] extends WriteConfigMethods[CreateTableWriter[T]] {
  /**
   * Create a new table from the contents of the data frame.
   *
   * The new table's schema, partition layout, properties, and other configuration will be
   * based on the configuration set on this writer.
   *
   * If the output table exists, this operation will fail with
   * [[org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException]].
   *
   * @throws org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
   *         If the table already exists
   */
  @throws(classOf[TableAlreadyExistsException])
  def create(): Unit

  /**
   * Replace an existing table with the contents of the data frame.
   *
   * The existing table's schema, partition layout, properties, and other configuration will be
   * replaced with the contents of the data frame and the configuration set on this writer.
   *
   * If the output table does not exist, this operation will fail with
   * [[org.apache.spark.sql.catalyst.analysis.CannotReplaceMissingTableException]].
   *
   * @throws org.apache.spark.sql.catalyst.analysis.CannotReplaceMissingTableException
   *         If the table does not exist
   */
  @throws(classOf[CannotReplaceMissingTableException])
  def replace(): Unit

  /**
   * Create a new table or replace an existing table with the contents of the data frame.
   *
   * The output table's schema, partition layout, properties, and other configuration will be based
   * on the contents of the data frame and the configuration set on this writer. If the table
   * exists, its configuration and data will be replaced.
   */
  def createOrReplace(): Unit

  /**
   * Partition the output table created by `create`, `createOrReplace`, or `replace` using
   * the given columns or transforms.
   *
   * When specified, the table data will be stored by these values for efficient reads.
   *
   * For example, when a table is partitioned by day, it may be stored in a directory layout like:
   * <ul>
   * <li>`table/day=2019-06-01/`</li>
   * <li>`table/day=2019-06-02/`</li>
   * </ul>
   *
   * Partitioning is one of the most widely used techniques to optimize physical data layout.
   * It provides a coarse-grained index for skipping unnecessary data reads when queries have
   * predicates on the partitioned columns. In order for partitioning to work well, the number
   * of distinct values in each column should typically be less than tens of thousands.
   *
   * @since 3.0.0
   */
  @scala.annotation.varargs
  def partitionedBy(column: Column, columns: Column*): CreateTableWriter[T]

  /**
   * Clusters the output by the given columns on the storage. The rows with matching values in
   * the specified clustering columns will be consolidated within the same group.
   *
   * For instance, if you cluster a dataset by date, the data sharing the same date will be stored
   * together in a file. This arrangement improves query efficiency when you apply selective
   * filters to these clustering columns, thanks to data skipping.
   *
   * @since 4.0.0
   */
  @scala.annotation.varargs
  def clusterBy(colName: String, colNames: String*): CreateTableWriter[T]

  /**
   * Specifies a provider for the underlying output data source. Spark's default catalog supports
   * "parquet", "json", etc.
   *
   * @since 3.0.0
   */
  def using(provider: String): CreateTableWriter[T]

  /**
   * Add a table property.
   */
  def tableProperty(property: String, value: String): CreateTableWriter[T]
}
