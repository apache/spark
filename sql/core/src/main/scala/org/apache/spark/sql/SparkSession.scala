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

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ExecutionListenerManager

/**
 * An entry point to Spark.
 */
abstract class SparkSession {

  def sparkContext: SparkContext

  def isRootSession: Boolean

  def conf: RuntimeConfig

  def newSession(): SparkSession

  def listenerManager: ExecutionListenerManager

  def experimental: ExperimentalMethods

  def emptyDataFrame: DataFrame

  def udf: UDFRegistration



  def streams: ContinuousQueryManager



  def range(end: Long): DataFrame

  def range(start: Long, end: Long): DataFrame

  def range(start: Long, end: Long, step: Long): DataFrame

  def range(start: Long, end: Long, step: Long, numPartitions: Int): DataFrame

  def sql(sqlText: String): DataFrame

  def table(tableName: String): DataFrame



  def createDataFrame[A <: Product : TypeTag](rdd: RDD[A]): DataFrame

  def createDataFrame[A <: Product : TypeTag](data: Seq[A]): DataFrame

  def baseRelationToDataFrame(baseRelation: BaseRelation): DataFrame

  def createDataFrame(rowRDD: RDD[Row], schema: StructType): DataFrame

  def createDataset[T : Encoder](data: Seq[T]): Dataset[T]

  def createDataset[T : Encoder](data: RDD[T]): Dataset[T]

  def createDataset[T : Encoder](data: java.util.List[T]): Dataset[T]

  def createDataFrame(rowRDD: JavaRDD[Row], schema: StructType): DataFrame

  def createDataFrame(rows: java.util.List[Row], schema: StructType): DataFrame

  def createDataFrame(rdd: RDD[_], beanClass: Class[_]): DataFrame

  def createDataFrame(rdd: JavaRDD[_], beanClass: Class[_]): DataFrame

  def createDataFrame(data: java.util.List[_], beanClass: Class[_]): DataFrame

  def read: DataFrameReader



  def isCached(tableName: String): Boolean

  def cacheTable(tableName: String): Unit

  def uncacheTable(tableName: String): Unit

  def clearCache(): Unit

  def createExternalTable(tableName: String, path: String): DataFrame

  def createExternalTable(
    tableName: String,
    path: String,
    source: String): DataFrame

  def createExternalTable(
    tableName: String,
    source: String,
    options: java.util.Map[String, String]): DataFrame

  def createExternalTable(
    tableName: String,
    source: String,
    options: Map[String, String]): DataFrame

  def createExternalTable(
    tableName: String,
    source: String,
    schema: StructType,
    options: java.util.Map[String, String]): DataFrame

  def createExternalTable(
    tableName: String,
    source: String,
    schema: StructType,
    options: Map[String, String]): DataFrame

  def dropTempTable(tableName: String): Unit

  def tables(): DataFrame

  def tables(databaseName: String): DataFrame

}
