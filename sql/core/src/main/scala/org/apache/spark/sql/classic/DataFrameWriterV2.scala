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

package org.apache.spark.sql.classic

import java.util

import scala.collection.mutable
import scala.jdk.CollectionConverters.MapHasAsScala

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, UnresolvedFunction, UnresolvedIdentifier, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog.TableWritePrivilege._
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.types.IntegerType

/**
 * Interface used to write a [[org.apache.spark.sql.classic.Dataset]] to external storage using
 * the v2 API.
 *
 * @since 3.0.0
 */
@Experimental
final class DataFrameWriterV2[T] private[sql](table: String, ds: Dataset[T])
    extends sql.DataFrameWriterV2[T] {

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

  /** @inheritdoc */
  override def using(provider: String): this.type = {
    this.provider = Some(provider)
    this
  }

  /** @inheritdoc */
  override def option(key: String, value: String): this.type = {
    this.options.put(key, value)
    this
  }

  /** @inheritdoc */
  override def options(options: scala.collection.Map[String, String]): this.type = {
    options.foreach {
      case (key, value) =>
        this.options.put(key, value)
    }
    this
  }

  /** @inheritdoc */
  override def options(options: util.Map[String, String]): this.type = {
    this.options(options.asScala)
    this
  }

  /** @inheritdoc */
  override def tableProperty(property: String, value: String): this.type = {
    this.properties.put(property, value)
    this
  }


  /** @inheritdoc */
  @scala.annotation.varargs
  override def partitionedBy(column: Column, columns: Column*): this.type = {
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

  /** @inheritdoc */
  @scala.annotation.varargs
  override def clusterBy(colName: String, colNames: String*): this.type = {
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

  /** @inheritdoc */
  override def create(): Unit = {
    val tableSpec = UnresolvedTableSpec(
      properties = properties.toMap,
      provider = provider,
      optionExpression = OptionList(Seq.empty),
      location = None,
      comment = None,
      collation = None,
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

  /** @inheritdoc */
  override def replace(): Unit = {
    internalReplace(orCreate = false)
  }

  /** @inheritdoc */
  override def createOrReplace(): Unit = {
    internalReplace(orCreate = true)
  }

  /** @inheritdoc */
  @throws(classOf[NoSuchTableException])
  def append(): Unit = {
    val append = AppendData.byName(
      UnresolvedRelation(tableName).requireWritePrivileges(Seq(INSERT)),
      logicalPlan, options.toMap)
    runCommand(append)
  }

  /** @inheritdoc */
  @throws(classOf[NoSuchTableException])
  def overwrite(condition: Column): Unit = {
    val overwrite = OverwriteByExpression.byName(
      UnresolvedRelation(tableName).requireWritePrivileges(Seq(INSERT, DELETE)),
      logicalPlan, expression(condition), options.toMap)
    runCommand(overwrite)
  }

  /** @inheritdoc */
  @throws(classOf[NoSuchTableException])
  def overwritePartitions(): Unit = {
    val dynamicOverwrite = OverwritePartitionsDynamic.byName(
      UnresolvedRelation(tableName).requireWritePrivileges(Seq(INSERT, DELETE)),
      logicalPlan, options.toMap)
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
      collation = None,
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
