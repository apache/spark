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

import org.apache.spark.api.java.JavaRDD

import scala.reflect.ClassTag

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils


trait RDDApi[T] {

  def cache(): this.type = persist()

  def persist(): this.type

  def persist(newLevel: StorageLevel): this.type

  def unpersist(): this.type = unpersist(blocking = false)

  def unpersist(blocking: Boolean): this.type

  def map[R: ClassTag](f: T => R): RDD[R]

  def mapPartitions[R: ClassTag](f: Iterator[T] => Iterator[R]): RDD[R]

  def take(n: Int): Array[T]

  def collect(): Array[T]

  def collectAsList(): java.util.List[T]

  def count(): Long

}


trait DataFrameSpecificApi {

  /**
   * Returns the schema of this SchemaRDD (represented by a [[StructType]]).
   *
   * @group schema
   */
  def schema: StructType

  def printSchema(): Unit

  /////////////////////////////////////////////////////////////////////////////
  // Metadata
  /////////////////////////////////////////////////////////////////////////////
  def dtypes: Array[(String, String)]

  def columns: Array[String]

  def head(): Row = head(5).head

  def head(n: Int): Array[Row]

  def tail(n: Int = 5): Array[Row]

  def show(): Unit

  /////////////////////////////////////////////////////////////////////////////
  // Relational operators
  /////////////////////////////////////////////////////////////////////////////
  /** Selecting a single column */
  def apply(colName: String): Column

  /** Projection */
  def apply(projection: Product): DataFrame

  @scala.annotation.varargs
  def select(cols: Column*): DataFrame

  @scala.annotation.varargs
  def select(col: String, cols: String*): DataFrame

  /** Filtering */
  def apply(condition: Column): DataFrame = filter(condition)

  def as(name: String): DataFrame

  def filter(condition: Column): DataFrame

  def where(condition: Column): DataFrame = filter(condition)

  @scala.annotation.varargs
  def groupby(cols: Column*): GroupedDataFrame

  @scala.annotation.varargs
  def groupby(col1: String, cols: String*): GroupedDataFrame

  def agg(exprs: Map[String, String]): DataFrame

  @scala.annotation.varargs
  def agg(expr: Column, exprs: Column*): DataFrame

  def sort(colName: String): DataFrame

  @scala.annotation.varargs
  def orderBy(sortExpr: Column, sortExprs: Column*): DataFrame = sort(sortExpr, sortExprs :_*)

  @scala.annotation.varargs
  def sort(sortExpr: Column, sortExprs: Column*): DataFrame

  def join(right: DataFrame): DataFrame

  def join(right: DataFrame, joinExprs: Column): DataFrame

  def join(right: DataFrame, joinType: String, joinExprs: Column): DataFrame

  def limit(n: Int): DataFrame

  def unionAll(other: DataFrame): DataFrame

  def intersect(other: DataFrame): DataFrame

  def except(other: DataFrame): DataFrame

  def sample(withReplacement: Boolean, fraction: Double, seed: Long): DataFrame

  def sample(withReplacement: Boolean, fraction: Double): DataFrame = {
    sample(withReplacement, fraction, Utils.random.nextLong)
  }

  /////////////////////////////////////////////////////////////////////////////
  // Column mutation
  /////////////////////////////////////////////////////////////////////////////
  def addColumn(colName: String, col: Column): DataFrame

  def removeColumn(colName: String, col: Column): DataFrame

  def updateColumn(colName: String, col: Column): DataFrame

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


trait ExpressionApi[ExprType] {
  def unary_- : ExprType
  def unary_! : ExprType
  def unary_~ : ExprType

  def + (other: ExprType): ExprType
  def - (other: ExprType): ExprType
  def * (other: ExprType): ExprType
  def / (other: ExprType): ExprType
  def % (other: ExprType): ExprType
  def & (other: ExprType): ExprType
  def | (other: ExprType): ExprType
  def ^ (other: ExprType): ExprType

  def && (other: ExprType): ExprType
  def || (other: ExprType): ExprType

  def < (other: ExprType): ExprType
  def <= (other: ExprType): ExprType
  def > (other: ExprType): ExprType
  def >= (other: ExprType): ExprType
  def === (other: ExprType): ExprType
  def equalTo(other: ExprType): ExprType
  def <=> (other: ExprType): ExprType
  def !== (other: ExprType): ExprType

  def in(list: ExprType*): ExprType

  def like(other: ExprType): ExprType
  def rlike(other: ExprType): ExprType

  def contains(other: ExprType): ExprType
  def startsWith(other: ExprType): ExprType
  def endsWith(other: ExprType): ExprType

  def substr(startPos: ExprType, len: ExprType): ExprType
  def substring(startPos: ExprType, len: ExprType): ExprType = substr(startPos, len)

  def isNull: ExprType
  def isNotNull: ExprType

  def getItem(ordinal: ExprType): ExprType
  def getItem(ordinal: Int): Column
  def getField(fieldName: String): ExprType

  def cast(to: DataType): ExprType

  def asc: ExprType
  def desc: ExprType

  def as(alias: String): ExprType
}


trait GroupedDataFrameApi {

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
