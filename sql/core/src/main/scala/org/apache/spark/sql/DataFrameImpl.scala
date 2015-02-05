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
import scala.collection.JavaConversions._

import com.fasterxml.jackson.core.JsonFactory

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.python.SerDeUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.catalyst.{SqlParser, ScalaReflection}
import org.apache.spark.sql.catalyst.analysis.{ResolvedStar, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{JoinType, Inner}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{LogicalRDD, EvaluatePython}
import org.apache.spark.sql.json.JsonRDD
import org.apache.spark.sql.sources.{ResolvedDataSource, CreateTableUsingAsLogicalPlan}
import org.apache.spark.sql.types.{NumericType, StructType}


/**
 * Internal implementation of [[DataFrame]]. Users of the API should use [[DataFrame]] directly.
 */
private[sql] class DataFrameImpl protected[sql](
    override val sqlContext: SQLContext,
    val queryExecution: SQLContext#QueryExecution)
  extends DataFrame {

  /**
   * A constructor that automatically analyzes the logical plan. This reports error eagerly
   * as the [[DataFrame]] is constructed.
   */
  def this(sqlContext: SQLContext, logicalPlan: LogicalPlan) = {
    this(sqlContext, {
      val qe = sqlContext.executePlan(logicalPlan)
      qe.analyzed  // This should force analysis and throw errors if there are any
      qe
    })
  }

  @transient protected[sql] override val logicalPlan: LogicalPlan = queryExecution.logical match {
    // For various commands (like DDL) and queries with side effects, we force query optimization to
    // happen right away to let these side effects take place eagerly.
    case _: Command | _: InsertIntoTable | _: CreateTableAsSelect[_] |_: WriteToFile =>
      LogicalRDD(queryExecution.analyzed.output, queryExecution.toRdd)(sqlContext)
    case _ =>
      queryExecution.logical
  }

  /**
   * An implicit conversion function internal to this class for us to avoid doing
   * "new DataFrameImpl(...)" everywhere.
   */
  @inline private implicit def logicalPlanToDataFrame(logicalPlan: LogicalPlan): DataFrame = {
    new DataFrameImpl(sqlContext, logicalPlan)
  }

  protected[sql] def resolve(colName: String): NamedExpression = {
    queryExecution.analyzed.resolve(colName, sqlContext.analyzer.resolver).getOrElse {
      throw new RuntimeException(
        s"""Cannot resolve column name "$colName" among (${schema.fieldNames.mkString(", ")})""")
    }
  }

  protected[sql] def numericColumns: Seq[Expression] = {
    schema.fields.filter(_.dataType.isInstanceOf[NumericType]).map { n =>
      queryExecution.analyzed.resolve(n.name, sqlContext.analyzer.resolver).get
    }
  }

  override def toDataFrame(colName: String, colNames: String*): DataFrame = {
    val newNames = colName +: colNames
    require(schema.size == newNames.size,
      "The number of columns doesn't match.\n" +
        "Old column names: " + schema.fields.map(_.name).mkString(", ") + "\n" +
        "New column names: " + newNames.mkString(", "))

    val newCols = schema.fieldNames.zip(newNames).map { case (oldName, newName) =>
      apply(oldName).as(newName)
    }
    select(newCols :_*)
  }

  override def schema: StructType = queryExecution.analyzed.schema

  override def dtypes: Array[(String, String)] = schema.fields.map { field =>
    (field.name, field.dataType.toString)
  }

  override def columns: Array[String] = schema.fields.map(_.name)

  override def printSchema(): Unit = println(schema.treeString)

  override def join(right: DataFrame): DataFrame = {
    Join(logicalPlan, right.logicalPlan, joinType = Inner, None)
  }

  override def join(right: DataFrame, joinExprs: Column): DataFrame = {
    Join(logicalPlan, right.logicalPlan, Inner, Some(joinExprs.expr))
  }

  override def join(right: DataFrame, joinExprs: Column, joinType: String): DataFrame = {
    Join(logicalPlan, right.logicalPlan, JoinType(joinType), Some(joinExprs.expr))
  }

  override def sort(sortCol: String, sortCols: String*): DataFrame = {
    sort((sortCol +: sortCols).map(apply) :_*)
  }

  override def sort(sortExprs: Column*): DataFrame = {
    val sortOrder: Seq[SortOrder] = sortExprs.map { col =>
      col.expr match {
        case expr: SortOrder =>
          expr
        case expr: Expression =>
          SortOrder(expr, Ascending)
      }
    }
    Sort(sortOrder, global = true, logicalPlan)
  }

  override def orderBy(sortCol: String, sortCols: String*): DataFrame = {
    sort(sortCol, sortCols :_*)
  }

  override def orderBy(sortExprs: Column*): DataFrame = {
    sort(sortExprs :_*)
  }

  override def col(colName: String): Column = colName match {
    case "*" =>
      Column(ResolvedStar(schema.fieldNames.map(resolve)))
    case _ =>
      val expr = resolve(colName)
      Column(sqlContext, Project(Seq(expr), logicalPlan), expr)
  }

  override def apply(projection: Product): DataFrame = {
    require(projection.productArity >= 1)
    select(projection.productIterator.map {
      case c: Column => c
      case o: Any => Column(Literal(o))
    }.toSeq :_*)
  }

  override def as(name: String): DataFrame = Subquery(name, logicalPlan)

  override def select(cols: Column*): DataFrame = {
    val exprs = cols.zipWithIndex.map {
      case (Column(expr: NamedExpression), _) =>
        expr
      case (Column(expr: Expression), _) =>
        Alias(expr, expr.toString)()
    }
    Project(exprs.toSeq, logicalPlan)
  }

  override def select(col: String, cols: String*): DataFrame = {
    select((col +: cols).map(Column(_)) :_*)
  }

  override def selectExpr(exprs: String*): DataFrame = {
    select(exprs.map { expr =>
      Column(new SqlParser().parseExpression(expr))
    } :_*)
  }

  override def filter(condition: Column): DataFrame = {
    Filter(condition.expr, logicalPlan)
  }

  override def filter(conditionExpr: String): DataFrame = {
    filter(Column(new SqlParser().parseExpression(conditionExpr)))
  }

  override def where(condition: Column): DataFrame = {
    filter(condition)
  }

  override def apply(condition: Column): DataFrame = {
    filter(condition)
  }

  override def groupBy(cols: Column*): GroupedData = {
    new GroupedData(this, cols.map(_.expr))
  }

  override def groupBy(col1: String, cols: String*): GroupedData = {
    val colNames: Seq[String] = col1 +: cols
    new GroupedData(this, colNames.map(colName => resolve(colName)))
  }

  override def limit(n: Int): DataFrame = {
    Limit(Literal(n), logicalPlan)
  }

  override def unionAll(other: DataFrame): DataFrame = {
    Union(logicalPlan, other.logicalPlan)
  }

  override def intersect(other: DataFrame): DataFrame = {
    Intersect(logicalPlan, other.logicalPlan)
  }

  override def except(other: DataFrame): DataFrame = {
    Except(logicalPlan, other.logicalPlan)
  }

  override def sample(withReplacement: Boolean, fraction: Double, seed: Long): DataFrame = {
    Sample(fraction, withReplacement, seed, logicalPlan)
  }

  /////////////////////////////////////////////////////////////////////////////

  override def addColumn(colName: String, col: Column): DataFrame = {
    select(Column("*"), col.as(colName))
  }

  override def head(n: Int): Array[Row] = limit(n).collect()

  override def head(): Row = head(1).head

  override def first(): Row = head()

  override def map[R: ClassTag](f: Row => R): RDD[R] = rdd.map(f)

  override def flatMap[R: ClassTag](f: Row => TraversableOnce[R]): RDD[R] = rdd.flatMap(f)

  override def mapPartitions[R: ClassTag](f: Iterator[Row] => Iterator[R]): RDD[R] = {
    rdd.mapPartitions(f)
  }

  override def foreach(f: Row => Unit): Unit = rdd.foreach(f)

  override def foreachPartition(f: Iterator[Row] => Unit): Unit = rdd.foreachPartition(f)

  override def take(n: Int): Array[Row] = head(n)

  override def collect(): Array[Row] = queryExecution.executedPlan.executeCollect()

  override def collectAsList(): java.util.List[Row] = java.util.Arrays.asList(rdd.collect() :_*)

  override def count(): Long = groupBy().count().rdd.collect().head.getLong(0)

  override def repartition(numPartitions: Int): DataFrame = {
    sqlContext.applySchema(rdd.repartition(numPartitions), schema)
  }

  override def persist(): this.type = {
    sqlContext.cacheManager.cacheQuery(this)
    this
  }

  override def persist(newLevel: StorageLevel): this.type = {
    sqlContext.cacheManager.cacheQuery(this, None, newLevel)
    this
  }

  override def unpersist(blocking: Boolean): this.type = {
    sqlContext.cacheManager.tryUncacheQuery(this, blocking)
    this
  }

  /////////////////////////////////////////////////////////////////////////////
  // I/O
  /////////////////////////////////////////////////////////////////////////////

  override def rdd: RDD[Row] = {
    val schema = this.schema
    queryExecution.executedPlan.execute().map(ScalaReflection.convertRowToScala(_, schema))
  }

  override def registerTempTable(tableName: String): Unit = {
    sqlContext.registerRDDAsTable(this, tableName)
  }

  override def saveAsParquetFile(path: String): Unit = {
    sqlContext.executePlan(WriteToFile(path, logicalPlan)).toRdd
  }

  override def saveAsTable(tableName: String): Unit = {
    val dataSourceName = sqlContext.conf.defaultDataSourceName
    val cmd =
      CreateTableUsingAsLogicalPlan(
        tableName,
        dataSourceName,
        temporary = false,
        Map.empty,
        allowExisting = false,
        logicalPlan)

    sqlContext.executePlan(cmd).toRdd
  }

  override def saveAsTable(
      tableName: String,
      dataSourceName: String,
      option: (String, String),
      options: (String, String)*): Unit = {
    val cmd =
      CreateTableUsingAsLogicalPlan(
        tableName,
        dataSourceName,
        temporary = false,
        (option +: options).toMap,
        allowExisting = false,
        logicalPlan)

    sqlContext.executePlan(cmd).toRdd
  }

  override def saveAsTable(
      tableName: String,
      dataSourceName: String,
      options: java.util.Map[String, String]): Unit = {
    val opts = options.toSeq
    saveAsTable(tableName, dataSourceName, opts.head, opts.tail:_*)
  }

  override def save(path: String): Unit = {
    val dataSourceName = sqlContext.conf.defaultDataSourceName
    save(dataSourceName, "path" -> path)
  }

  override def save(
      dataSourceName: String,
      option: (String, String),
      options: (String, String)*): Unit = {
    ResolvedDataSource(sqlContext, dataSourceName, (option +: options).toMap, this)
  }

  override def save(
      dataSourceName: String,
      options: java.util.Map[String, String]): Unit = {
    val opts = options.toSeq
    save(dataSourceName, opts.head, opts.tail:_*)
  }

  override def insertInto(tableName: String, overwrite: Boolean): Unit = {
    sqlContext.executePlan(InsertIntoTable(UnresolvedRelation(Seq(tableName)),
      Map.empty, logicalPlan, overwrite)).toRdd
  }

  override def toJSON: RDD[String] = {
    val rowSchema = this.schema
    this.mapPartitions { iter =>
      val jsonFactory = new JsonFactory()
      iter.map(JsonRDD.rowToJSON(rowSchema, jsonFactory))
    }
  }

  ////////////////////////////////////////////////////////////////////////////
  // for Python API
  ////////////////////////////////////////////////////////////////////////////
  protected[sql] override def javaToPython: JavaRDD[Array[Byte]] = {
    val fieldTypes = schema.fields.map(_.dataType)
    val jrdd = rdd.map(EvaluatePython.rowToArray(_, fieldTypes)).toJavaRDD()
    SerDeUtil.javaToPython(jrdd)
  }
}
