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

import scala.jdk.CollectionConverters.PropertiesHasAsScala
import scala.reflect.runtime.universe.TypeTag

import _root_.java.util.{List => JavaList, Map => JavaMap, Properties}

import org.apache.spark.annotation.Stable
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalog.Table
import org.apache.spark.sql.connect.ConnectConversions._
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.streaming.{DataStreamReader, StreamingQueryManager}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ExecutionListenerManager

@Stable
class SQLContext private[sql] (override val sparkSession: SparkSession)
    extends api.SQLContext(sparkSession) {

  /** @inheritdoc */
  def newSession(): SQLContext = sparkSession.newSession().sqlContext

  /** @inheritdoc */
  def listenerManager: ExecutionListenerManager = sparkSession.listenerManager

  /** @inheritdoc */
  def setConf(props: Properties): Unit = sparkSession.conf.synchronized {
    props.asScala.foreach { case (k, v) => sparkSession.conf.set(k, v) }
  }

  /** @inheritdoc */
  def experimental: ExperimentalMethods = sparkSession.experimental

  /** @inheritdoc */
  def udf: UDFRegistration = sparkSession.udf

  /** @inheritdoc */
  def read: DataFrameReader = sparkSession.read

  /** @inheritdoc */
  def readStream: DataStreamReader = sparkSession.readStream

  /** @inheritdoc */
  def tables(): Dataset[Row] = {
    sparkSession.catalog
      .listTables()
      .map(ListTableRow.fromTable)(Encoders.product[ListTableRow])
      .toDF()
  }

  /** @inheritdoc */
  def tables(databaseName: String): Dataset[Row] = {
    sparkSession.catalog
      .listTables(databaseName)
      .map(ListTableRow.fromTable)(Encoders.product[ListTableRow])
      .toDF()
  }

  /** @inheritdoc */
  def streams: StreamingQueryManager = sparkSession.streams

  /** @inheritdoc */
  def tableNames(databaseName: String): Array[String] = {
    sparkSession.catalog.listTables(databaseName).map(_.name)(Encoders.STRING).collect()
  }

  /** @inheritdoc */
  override def emptyDataFrame: Dataset[Row] = {
    super.emptyDataFrame
  }

  /** @inheritdoc */
  override def createDataFrame[A <: Product: TypeTag](rdd: RDD[A]): Dataset[Row] =
    super.createDataFrame(rdd)

  /** @inheritdoc */
  override def createDataFrame[A <: Product: TypeTag](data: Seq[A]): Dataset[Row] =
    super.createDataFrame(data)

  /** @inheritdoc */
  override def baseRelationToDataFrame(baseRelation: BaseRelation): Dataset[Row] =
    super.baseRelationToDataFrame(baseRelation)

  /** @inheritdoc */
  override def createDataFrame(rowRDD: RDD[Row], schema: StructType): Dataset[Row] =
    super.createDataFrame(rowRDD, schema)

  /** @inheritdoc */
  override def createDataset[T: Encoder](data: Seq[T]): Dataset[T] = super.createDataset(data)

  /** @inheritdoc */
  override def createDataset[T: Encoder](data: RDD[T]): Dataset[T] = super.createDataset(data)

  /** @inheritdoc */
  override def createDataset[T: Encoder](data: JavaList[T]): Dataset[T] =
    super.createDataset(data)

  /** @inheritdoc */
  override def createDataFrame(rowRDD: JavaRDD[Row], schema: StructType): Dataset[Row] =
    super.createDataFrame(rowRDD, schema)

  /** @inheritdoc */
  override def createDataFrame(rows: JavaList[Row], schema: StructType): Dataset[Row] =
    super.createDataFrame(rows, schema)

  /** @inheritdoc */
  override def createDataFrame(rdd: RDD[_], beanClass: Class[_]): Dataset[Row] =
    super.createDataFrame(rdd, beanClass)

  /** @inheritdoc */
  override def createDataFrame(rdd: JavaRDD[_], beanClass: Class[_]): Dataset[Row] =
    super.createDataFrame(rdd, beanClass)

  /** @inheritdoc */
  override def createDataFrame(data: JavaList[_], beanClass: Class[_]): Dataset[Row] =
    super.createDataFrame(data, beanClass)

  /** @inheritdoc */
  override def createExternalTable(tableName: String, path: String): Dataset[Row] =
    super.createExternalTable(tableName, path)

  /** @inheritdoc */
  override def createExternalTable(
      tableName: String,
      path: String,
      source: String): Dataset[Row] = {
    super.createExternalTable(tableName, path, source)
  }

  /** @inheritdoc */
  override def createExternalTable(
      tableName: String,
      source: String,
      options: JavaMap[String, String]): Dataset[Row] = {
    super.createExternalTable(tableName, source, options)
  }

  /** @inheritdoc */
  override def createExternalTable(
      tableName: String,
      source: String,
      options: Map[String, String]): Dataset[Row] = {
    super.createExternalTable(tableName, source, options)
  }

  /** @inheritdoc */
  override def createExternalTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: JavaMap[String, String]): Dataset[Row] = {
    super.createExternalTable(tableName, source, schema, options)
  }

  /** @inheritdoc */
  override def createExternalTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: Map[String, String]): Dataset[Row] = {
    super.createExternalTable(tableName, source, schema, options)
  }

  /** @inheritdoc */
  override def range(end: Long): Dataset[Row] = super.range(end)

  /** @inheritdoc */
  override def range(start: Long, end: Long): Dataset[Row] = super.range(start, end)

  /** @inheritdoc */
  override def range(start: Long, end: Long, step: Long): Dataset[Row] =
    super.range(start, end, step)

  /** @inheritdoc */
  override def range(start: Long, end: Long, step: Long, numPartitions: Int): Dataset[Row] =
    super.range(start, end, step, numPartitions)

  /** @inheritdoc */
  override def sql(sqlText: String): Dataset[Row] = super.sql(sqlText)

  /** @inheritdoc */
  override def table(tableName: String): Dataset[Row] = super.table(tableName)

  /** @inheritdoc */
  override def applySchema(rowRDD: RDD[Row], schema: StructType): Dataset[Row] =
    super.applySchema(rowRDD, schema)

  /** @inheritdoc */
  override def applySchema(rowRDD: JavaRDD[Row], schema: StructType): Dataset[Row] =
    super.applySchema(rowRDD, schema)

  /** @inheritdoc */
  override def applySchema(rdd: RDD[_], beanClass: Class[_]): Dataset[Row] =
    super.applySchema(rdd, beanClass)

  /** @inheritdoc */
  override def applySchema(rdd: JavaRDD[_], beanClass: Class[_]): Dataset[Row] =
    super.applySchema(rdd, beanClass)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def parquetFile(paths: String*): Dataset[Row] = super.parquetFile(paths: _*)

  /** @inheritdoc */
  override def jsonFile(path: String): Dataset[Row] = super.jsonFile(path)

  /** @inheritdoc */
  override def jsonFile(path: String, schema: StructType): Dataset[Row] =
    super.jsonFile(path, schema)

  /** @inheritdoc */
  override def jsonFile(path: String, samplingRatio: Double): Dataset[Row] =
    super.jsonFile(path, samplingRatio)

  /** @inheritdoc */
  override def jsonRDD(json: RDD[String]): Dataset[Row] = super.jsonRDD(json)

  /** @inheritdoc */
  override def jsonRDD(json: JavaRDD[String]): Dataset[Row] = super.jsonRDD(json)

  /** @inheritdoc */
  override def jsonRDD(json: RDD[String], schema: StructType): Dataset[Row] =
    super.jsonRDD(json, schema)

  /** @inheritdoc */
  override def jsonRDD(json: JavaRDD[String], schema: StructType): Dataset[Row] =
    super.jsonRDD(json, schema)

  /** @inheritdoc */
  override def jsonRDD(json: RDD[String], samplingRatio: Double): Dataset[Row] =
    super.jsonRDD(json, samplingRatio)

  /** @inheritdoc */
  override def jsonRDD(json: JavaRDD[String], samplingRatio: Double): Dataset[Row] =
    super.jsonRDD(json, samplingRatio)

  /** @inheritdoc */
  override def load(path: String): Dataset[Row] = super.load(path)

  /** @inheritdoc */
  override def load(path: String, source: String): Dataset[Row] = super.load(path, source)

  /** @inheritdoc */
  override def load(source: String, options: JavaMap[String, String]): Dataset[Row] =
    super.load(source, options)

  /** @inheritdoc */
  override def load(source: String, options: Map[String, String]): Dataset[Row] =
    super.load(source, options)

  /** @inheritdoc */
  override def load(
      source: String,
      schema: StructType,
      options: JavaMap[String, String]): Dataset[Row] = {
    super.load(source, schema, options)
  }

  /** @inheritdoc */
  override def load(
      source: String,
      schema: StructType,
      options: Map[String, String]): Dataset[Row] = {
    super.load(source, schema, options)
  }

  /** @inheritdoc */
  override def jdbc(url: String, table: String): Dataset[Row] = super.jdbc(url, table)

  /** @inheritdoc */
  override def jdbc(
      url: String,
      table: String,
      columnName: String,
      lowerBound: Long,
      upperBound: Long,
      numPartitions: Int): Dataset[Row] = {
    super.jdbc(url, table, columnName, lowerBound, upperBound, numPartitions)
  }

  /** @inheritdoc */
  override def jdbc(url: String, table: String, theParts: Array[String]): Dataset[Row] = {
    super.jdbc(url, table, theParts)
  }
}

protected sealed case class ListTableRow(
    database: String,
    tableName: String,
    isTemporary: Boolean)

protected object ListTableRow {
  def fromTable(table: Table): ListTableRow = {
    ListTableRow(table.database, table.name, table.isTemporary)
  }
}
