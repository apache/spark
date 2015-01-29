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

import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.storage.StorageLevel


/**
 * An internal interface defining the RDD-like methods for [[DataFrame]].
 * Please use [[DataFrame]] directly, and do NOT use this.
 */
private[sql] trait RDDApi[T] {

  def cache(): this.type = persist()

  def persist(): this.type

  def persist(newLevel: StorageLevel): this.type

  def unpersist(): this.type = unpersist(blocking = false)

  def unpersist(blocking: Boolean): this.type

  def map[R: ClassTag](f: T => R): RDD[R]

  def flatMap[R: ClassTag](f: T => TraversableOnce[R]): RDD[R]

  def mapPartitions[R: ClassTag](f: Iterator[T] => Iterator[R]): RDD[R]

  def foreach(f: T => Unit): Unit

  def foreachPartition(f: Iterator[T] => Unit): Unit

  def take(n: Int): Array[T]

  def collect(): Array[T]

  def collectAsList(): java.util.List[T]

  def count(): Long

  def first(): T

  def repartition(numPartitions: Int): DataFrame
}


/**
 * An internal interface defining data frame related methods in [[DataFrame]].
 * Please use [[DataFrame]] directly, and do NOT use this.
 */
private[sql] trait DataFrameSpecificApi {

  def schema: StructType

  def printSchema(): Unit

  def dtypes: Array[(String, String)]

  def columns: Array[String]

  def head(): Row

  def head(n: Int): Array[Row]

  /////////////////////////////////////////////////////////////////////////////
  // Relational operators
  /////////////////////////////////////////////////////////////////////////////
  def apply(colName: String): Column

  def apply(projection: Product): DataFrame

  @scala.annotation.varargs
  def select(cols: Column*): DataFrame

  @scala.annotation.varargs
  def select(col: String, cols: String*): DataFrame

  def apply(condition: Column): DataFrame

  def as(name: String): DataFrame

  def filter(condition: Column): DataFrame

  def where(condition: Column): DataFrame

  @scala.annotation.varargs
  def groupBy(cols: Column*): GroupedDataFrame

  @scala.annotation.varargs
  def groupBy(col1: String, cols: String*): GroupedDataFrame

  def agg(exprs: Map[String, String]): DataFrame

  def agg(exprs: java.util.Map[String, String]): DataFrame

  @scala.annotation.varargs
  def agg(expr: Column, exprs: Column*): DataFrame

  @scala.annotation.varargs
  def sort(sortExpr: Column, sortExprs: Column*): DataFrame

  @scala.annotation.varargs
  def sort(sortCol: String, sortCols: String*): DataFrame

  @scala.annotation.varargs
  def orderBy(sortExpr: Column, sortExprs: Column*): DataFrame

  @scala.annotation.varargs
  def orderBy(sortCol: String, sortCols: String*): DataFrame

  def join(right: DataFrame): DataFrame

  def join(right: DataFrame, joinExprs: Column): DataFrame

  def join(right: DataFrame, joinExprs: Column, joinType: String): DataFrame

  def limit(n: Int): DataFrame

  def unionAll(other: DataFrame): DataFrame

  def intersect(other: DataFrame): DataFrame

  def except(other: DataFrame): DataFrame

  def sample(withReplacement: Boolean, fraction: Double, seed: Long): DataFrame

  def sample(withReplacement: Boolean, fraction: Double): DataFrame

  /////////////////////////////////////////////////////////////////////////////
  // Column mutation
  /////////////////////////////////////////////////////////////////////////////
  def addColumn(colName: String, col: Column): DataFrame

  /////////////////////////////////////////////////////////////////////////////
  // I/O and interaction with other frameworks
  /////////////////////////////////////////////////////////////////////////////

  def rdd: RDD[Row]

  def toJavaRDD: JavaRDD[Row] = rdd.toJavaRDD()

  def toJSON: RDD[String]

  def registerTempTable(tableName: String): Unit

  def saveAsParquetFile(path: String): Unit

  @Experimental
  def saveAsTable(tableName: String): Unit

  @Experimental
  def insertInto(tableName: String, overwrite: Boolean): Unit

  @Experimental
  def insertInto(tableName: String): Unit = insertInto(tableName, overwrite = false)

  /////////////////////////////////////////////////////////////////////////////
  // Stat functions
  /////////////////////////////////////////////////////////////////////////////
//  def describe(): Unit
//
//  def mean(): Unit
//
//  def max(): Unit
//
//  def min(): Unit
}


/**
 * An internal interface defining expression APIs for [[DataFrame]].
 * Please use [[DataFrame]] and [[Column]] directly, and do NOT use this.
 */
private[sql] trait ExpressionApi {

  def isComputable: Boolean

  def unary_- : Column
  def unary_! : Column
  def unary_~ : Column

  def + (other: Column): Column
  def + (other: Any): Column
  def - (other: Column): Column
  def - (other: Any): Column
  def * (other: Column): Column
  def * (other: Any): Column
  def / (other: Column): Column
  def / (other: Any): Column
  def % (other: Column): Column
  def % (other: Any): Column
  def & (other: Column): Column
  def & (other: Any): Column
  def | (other: Column): Column
  def | (other: Any): Column
  def ^ (other: Column): Column
  def ^ (other: Any): Column

  def && (other: Column): Column
  def && (other: Boolean): Column
  def || (other: Column): Column
  def || (other: Boolean): Column

  def < (other: Column): Column
  def < (other: Any): Column
  def <= (other: Column): Column
  def <= (other: Any): Column
  def > (other: Column): Column
  def > (other: Any): Column
  def >= (other: Column): Column
  def >= (other: Any): Column
  def === (other: Column): Column
  def === (other: Any): Column
  def equalTo(other: Column): Column
  def equalTo(other: Any): Column
  def <=> (other: Column): Column
  def <=> (other: Any): Column
  def !== (other: Column): Column
  def !== (other: Any): Column

  @scala.annotation.varargs
  def in(list: Column*): Column

  def like(other: String): Column
  def rlike(other: String): Column

  def contains(other: Column): Column
  def contains(other: Any): Column
  def startsWith(other: Column): Column
  def startsWith(other: String): Column
  def endsWith(other: Column): Column
  def endsWith(other: String): Column

  def substr(startPos: Column, len: Column): Column
  def substr(startPos: Int, len: Int): Column

  def isNull: Column
  def isNotNull: Column

  def getItem(ordinal: Int): Column
  def getField(fieldName: String): Column

  def cast(to: DataType): Column
  def cast(to: String): Column

  def asc: Column
  def desc: Column

  def as(alias: String): Column
}


/**
 * An internal interface defining aggregation APIs for [[DataFrame]].
 * Please use [[DataFrame]] and [[GroupedDataFrame]] directly, and do NOT use this.
 */
private[sql] trait GroupedDataFrameApi {

  def agg(exprs: Map[String, String]): DataFrame

  @scala.annotation.varargs
  def agg(expr: Column, exprs: Column*): DataFrame

  def avg(): DataFrame

  def mean(): DataFrame

  def min(): DataFrame

  def max(): DataFrame

  def sum(): DataFrame

  def count(): DataFrame

  // TODO: Add var, std
}
