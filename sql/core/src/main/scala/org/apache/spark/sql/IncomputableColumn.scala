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

import scala.reflect.ClassTag

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.sources.SaveMode
import org.apache.spark.sql.types.StructType

private[sql] class IncomputableColumn(protected[sql] val expr: Expression) extends Column {

  def this(name: String) = this(name match {
    case "*" => UnresolvedStar(None)
    case _ if name.endsWith(".*") => UnresolvedStar(Some(name.substring(0, name.length - 2)))
    case _ => UnresolvedAttribute(name)
  })

  private def err[T](): T = {
    throw new UnsupportedOperationException("Cannot run this method on an UncomputableColumn")
  }

  override def toString = expr.prettyString

  override def isComputable: Boolean = false

  override val sqlContext: SQLContext = null

  override def queryExecution = err()

  protected[sql] override def logicalPlan: LogicalPlan = err()

  override def toDataFrame(colNames: String*): DataFrame = err()

  override def schema: StructType = err()

  override def dtypes: Array[(String, String)] = err()

  override def columns: Array[String] = err()

  override def printSchema(): Unit = err()

  override def show(): Unit = err()

  override def isLocal: Boolean = false

  override def join(right: DataFrame): DataFrame = err()

  override def join(right: DataFrame, joinExprs: Column): DataFrame = err()

  override def join(right: DataFrame, joinExprs: Column, joinType: String): DataFrame = err()

  override def sort(sortCol: String, sortCols: String*): DataFrame = err()

  override def sort(sortExprs: Column*): DataFrame = err()

  override def orderBy(sortCol: String, sortCols: String*): DataFrame = err()

  override def orderBy(sortExprs: Column*): DataFrame = err()

  override def col(colName: String): Column = err()

  override def select(cols: Column*): DataFrame = err()

  override def select(col: String, cols: String*): DataFrame = err()

  override def selectExpr(exprs: String*): DataFrame = err()

  override def addColumn(colName: String, col: Column): DataFrame = err()

  override def renameColumn(existingName: String, newName: String): DataFrame = err()

  override def filter(condition: Column): DataFrame = err()

  override def filter(conditionExpr: String): DataFrame = err()

  override def where(condition: Column): DataFrame = err()

  override def groupBy(cols: Column*): GroupedData = err()

  override def groupBy(col1: String, cols: String*): GroupedData = err()

  override def limit(n: Int): DataFrame = err()

  override def unionAll(other: DataFrame): DataFrame = err()

  override def intersect(other: DataFrame): DataFrame = err()

  override def except(other: DataFrame): DataFrame = err()

  override def sample(withReplacement: Boolean, fraction: Double, seed: Long): DataFrame = err()

  /////////////////////////////////////////////////////////////////////////////

  override def head(n: Int): Array[Row] = err()

  override def head(): Row = err()

  override def first(): Row = err()

  override def map[R: ClassTag](f: Row => R): RDD[R] = err()

  override def flatMap[R: ClassTag](f: Row => TraversableOnce[R]): RDD[R] = err()

  override def mapPartitions[R: ClassTag](f: Iterator[Row] => Iterator[R]): RDD[R] = err()

  override def foreach(f: Row => Unit): Unit = err()

  override def foreachPartition(f: Iterator[Row] => Unit): Unit = err()

  override def take(n: Int): Array[Row] = err()

  override def collect(): Array[Row] = err()

  override def collectAsList(): java.util.List[Row] = err()

  override def count(): Long = err()

  override def repartition(numPartitions: Int): DataFrame = err()

  override def distinct: DataFrame = err()

  override def persist(): this.type = err()

  override def persist(newLevel: StorageLevel): this.type = err()

  override def unpersist(blocking: Boolean): this.type = err()

  override def rdd: RDD[Row] = err()

  override def registerTempTable(tableName: String): Unit = err()

  override def saveAsParquetFile(path: String): Unit = err()

  override def saveAsTable(
      tableName: String,
      source: String,
      mode: SaveMode,
      options: Map[String, String]): Unit = err()

  override def save(
      source: String,
      mode: SaveMode,
      options: Map[String, String]): Unit = err()

  override def insertInto(tableName: String, overwrite: Boolean): Unit = err()

  override def toJSON: RDD[String] = err()

  protected[sql] override def javaToPython: JavaRDD[Array[Byte]] = err()
}
