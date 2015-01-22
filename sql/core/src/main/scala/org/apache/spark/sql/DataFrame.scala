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

import scala.language.implicitConversions
import scala.reflect.ClassTag

import com.fasterxml.jackson.core.JsonFactory

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.{Literal => LiteralExpr}
import org.apache.spark.sql.catalyst.plans.{JoinType, Inner}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.json.JsonRDD
import org.apache.spark.sql.types.{NumericType, StructType}


class DataFrame(
    val sqlContext: SQLContext,
    val baseLogicalPlan: LogicalPlan,
    operatorsEnabled: Boolean)
  extends DataFrameSpecificApi with RDDApi[Row] {

  def this(sqlContext: Option[SQLContext], plan: Option[LogicalPlan]) =
    this(sqlContext.orNull, plan.orNull, sqlContext.isDefined && plan.isDefined)

  def this(sqlContext: SQLContext, plan: LogicalPlan) = this(sqlContext, plan, true)

  @transient
  protected[sql] lazy val queryExecution = sqlContext.executePlan(baseLogicalPlan)

  @transient protected[sql] val logicalPlan: LogicalPlan = baseLogicalPlan match {
    // For various commands (like DDL) and queries with side effects, we force query optimization to
    // happen right away to let these side effects take place eagerly.
    case _: Command | _: InsertIntoTable | _: CreateTableAsSelect[_] |_: WriteToFile =>
      LogicalRDD(queryExecution.analyzed.output, queryExecution.toRdd)(sqlContext)
    case _ =>
      baseLogicalPlan
  }

  private[this] implicit def toDataFrame(logicalPlan: LogicalPlan): DataFrame = {
    new DataFrame(sqlContext, logicalPlan, true)
  }

  protected[sql] def numericColumns: Seq[Expression] = {
    schema.fields.filter(_.dataType.isInstanceOf[NumericType]).map { n =>
      logicalPlan.resolve(n.name, sqlContext.analyzer.resolver).get
    }
  }

  protected[sql] def resolve(colName: String): NamedExpression = {
    logicalPlan.resolve(colName, sqlContext.analyzer.resolver).getOrElse(
      throw new RuntimeException(s"""Cannot resolve column name "$colName""""))
  }

  def toSchemaRDD: DataFrame = this

  override def schema: StructType = queryExecution.analyzed.schema

  override def dtypes: Array[(String, String)] = schema.fields.map { field =>
    (field.name, field.dataType.toString)
  }

  override def columns: Array[String] = schema.fields.map(_.name)

  override def show(): Unit = {
    ???
  }

  override def join(right: DataFrame): DataFrame = {
    Join(logicalPlan, right.logicalPlan, joinType = Inner, None)
  }

  override def join(right: DataFrame, joinExprs: Column): DataFrame = {
    Join(logicalPlan, right.logicalPlan, Inner, Some(joinExprs.expr))
  }

  override def join(right: DataFrame, joinType: String, joinExprs: Column): DataFrame = {
    Join(logicalPlan, right.logicalPlan, JoinType(joinType), Some(joinExprs.expr))
  }

  override def sort(colName: String): DataFrame = {
    Sort(Seq(SortOrder(apply(colName).expr, Ascending)), global = true, logicalPlan)
  }

  @scala.annotation.varargs
  override def sort(sortExpr: Column, sortExprs: Column*): DataFrame = {
    val sortOrder: Seq[SortOrder] = (sortExpr +: sortExprs).map { col =>
      col.expr match {
        case expr: SortOrder =>
          expr
        case expr: Expression =>
          SortOrder(expr, Ascending)
      }
    }
    Sort(sortOrder, global = true, logicalPlan)
  }

  override def tail(n: Int): Array[Row] = ???

  /** Selecting a single column */
  override def apply(colName: String): Column = {
    val expr = resolve(colName)
    new Column(Some(sqlContext), Some(Project(Seq(expr), logicalPlan)), expr)
  }

  /** Projection */
  override def apply(projection: Product): DataFrame = {
    require(projection.productArity >= 1)
    select(projection.productIterator.map {
      case c: Column => c
      case o: Any => new Column(Some(sqlContext), None, LiteralExpr(o))
    }.toSeq :_*)
  }

  override def as(name: String): DataFrame = Subquery(name, logicalPlan)

  @scala.annotation.varargs
  override def select(cols: Column*): DataFrame = {
    val exprs = cols.zipWithIndex.map {
      case (Column(expr: NamedExpression), _) =>
        expr
      case (Column(expr: Expression), _) =>
        Alias(expr, expr.toString)()
    }
    Project(exprs.toSeq, logicalPlan)
  }

  /** Filtering */
  override def filter(condition: Column): DataFrame = {
    Filter(condition.expr, logicalPlan)
  }

  @scala.annotation.varargs
  override def groupby(cols: Column*): GroupedDataFrame = {
    new GroupedDataFrame(this, cols.map(_.expr))
  }

  @scala.annotation.varargs
  override def groupby(col1: String, cols: String*): GroupedDataFrame = {
    val colNames: Seq[String] = col1 +: cols
    new GroupedDataFrame(this, colNames.map(colName => resolve(colName)))
  }

  override def agg(exprs: Map[String, String]): DataFrame = groupby().agg(exprs)

  @scala.annotation.varargs
  override def agg(expr: Column, exprs: Column*): DataFrame = groupby().agg(expr, exprs :_*)

  override def limit(n: Int): DataFrame = Limit(LiteralExpr(n), logicalPlan)

  override def unionAll(other: DataFrame): DataFrame = Union(logicalPlan, other.logicalPlan)

  def intersect(other: DataFrame): DataFrame = Intersect(logicalPlan, other.logicalPlan)

  def except(other: DataFrame): DataFrame = Except(logicalPlan, other.logicalPlan)

  /////////////////////////////////////////////////////////////////////////////

  override def addColumn(colName: String, col: Column): DataFrame = ???

  override def updateColumn(colName: String, col: Column): DataFrame = ???

  override def head(n: Int): Array[Row] = limit(n).collect()

  override def removeColumn(colName: String, col: Column): DataFrame = ???

  override def map[R: ClassTag](f: Row => R): RDD[R] = {
    rdd.map(f)
  }

  override def mapPartitions[R: ClassTag](f: Iterator[Row] => Iterator[R]): RDD[R] = {
    rdd.mapPartitions(f)
  }

  override def take(n: Int): Array[Row] = head(n)

  override def collect(): Array[Row] = rdd.collect()

  override def collectAsList(): java.util.List[Row] = java.util.Arrays.asList(rdd.collect() :_*)

  override def count(): Long = groupby().count().rdd.collect().head.getLong(0)

  override def persist(): this.type = {
    sqlContext.cacheQuery(this)
    this
  }

  override def persist(newLevel: StorageLevel): this.type = {
    sqlContext.cacheQuery(this, None, newLevel)
    this
  }

  override def unpersist(blocking: Boolean): this.type = {
    sqlContext.tryUncacheQuery(this, blocking)
    this
  }

  /////////////////////////////////////////////////////////////////////////////
  // I/O
  /////////////////////////////////////////////////////////////////////////////

  override def rdd: RDD[Row] = {
    val schema = this.schema
    queryExecution.executedPlan.execute().map(ScalaReflection.convertRowToScala(_, schema))
  }

  /**
   * Registers this RDD as a temporary table using the given name.  The lifetime of this temporary
   * table is tied to the [[SQLContext]] that was used to create this SchemaRDD.
   *
   * @group schema
   */
  override def registerTempTable(tableName: String): Unit = {
    sqlContext.registerRDDAsTable(this, tableName)
  }

  override def saveAsParquetFile(path: String): Unit = {
    sqlContext.executePlan(WriteToFile(path, logicalPlan)).toRdd
  }

  override def toJSON: RDD[String] = {
    val rowSchema = this.schema
    this.mapPartitions { iter =>
      val jsonFactory = new JsonFactory()
      iter.map(JsonRDD.rowToJSON(rowSchema, jsonFactory))
    }
  }
}
