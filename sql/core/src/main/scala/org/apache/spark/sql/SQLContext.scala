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

import java.util.{List => JList, Map => JMap, Properties}

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.annotation.{DeveloperApi, Experimental, Stable, Unstable}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.internal.{SessionState, SharedState, SQLConf}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.streaming.{DataStreamReader, StreamingQueryManager}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ExecutionListenerManager

/**
 * The entry point for working with structured data (rows and columns) in Spark 1.x.
 *
 * As of Spark 2.0, this is replaced by [[SparkSession]]. However, we are keeping the class here
 * for backward compatibility.
 *
 * @groupname basic Basic Operations
 * @groupname ddl_ops Persistent Catalog DDL
 * @groupname cachemgmt Cached Table Management
 * @groupname genericdata Generic Data Sources
 * @groupname specificdata Specific Data Sources
 * @groupname config Configuration
 * @groupname dataframes Custom DataFrame Creation
 * @groupname dataset Custom Dataset Creation
 * @groupname Ungrouped Support functions for language integrated queries
 * @since 1.0.0
 */
@Stable
class SQLContext private[sql] (override val sparkSession: SparkSession)
    extends api.SQLContext(sparkSession) {

  self =>

  sparkSession.sparkContext.assertNotStopped()

  // Note: Since Spark 2.0 this class has become a wrapper of SparkSession, where the
  // real functionality resides. This class remains mainly for backward compatibility.

  @deprecated("Use SparkSession.builder instead", "2.0.0")
  def this(sc: SparkContext) = {
    this(SparkSession.builder().sparkContext(sc).getOrCreate())
  }

  @deprecated("Use SparkSession.builder instead", "2.0.0")
  def this(sparkContext: JavaSparkContext) = this(sparkContext.sc)

  // TODO: move this logic into SparkSession

  private[sql] def sessionState: SessionState = sparkSession.sessionState

  private[sql] def sharedState: SharedState = sparkSession.sharedState

  @deprecated("Use SparkSession.sessionState.conf instead", "4.0.0")
  private[sql] def conf: SQLConf = sessionState.conf

  /** @inheritdoc */
  def listenerManager: ExecutionListenerManager = sparkSession.listenerManager

  /** @inheritdoc */
  def setConf(props: Properties): Unit = {
    sessionState.conf.setConf(props)
  }

  private[sql] def setConf[T](entry: ConfigEntry[T], value: T): Unit = {
    sessionState.conf.setConf(entry, value)
  }

  /** @inheritdoc */
  @Experimental
  @transient
  @Unstable
  def experimental: ExperimentalMethods = sparkSession.experimental

  /** @inheritdoc */
  def udf: UDFRegistration = sparkSession.udf

  // scalastyle:off
  // Disable style checker so "implicits" object can start with lowercase i

  /** @inheritdoc */
  object implicits extends SQLImplicits {
    /** @inheritdoc */
    override protected def session: SparkSession = sparkSession
  }

  // scalastyle:on

  /**
   * Creates a DataFrame from an RDD[Row]. User can specify whether the input rows should be
   * converted to Catalyst rows.
   */
  private[sql] def internalCreateDataFrame(
      catalystRows: RDD[InternalRow],
      schema: StructType,
      isStreaming: Boolean = false): DataFrame = {
    sparkSession.internalCreateDataFrame(catalystRows, schema, isStreaming)
  }

  /** @inheritdoc */
  def read: DataFrameReader = sparkSession.read

  /** @inheritdoc */
  def readStream: DataStreamReader = sparkSession.readStream

  /**
   * Registers the given `DataFrame` as a temporary table in the catalog. Temporary tables exist
   * only during the lifetime of this instance of SQLContext.
   */
  private[sql] def registerDataFrameAsTable(df: DataFrame, tableName: String): Unit = {
    df.createOrReplaceTempView(tableName)
  }

  /**
   * Returns a `StreamingQueryManager` that allows managing all the
   * [[org.apache.spark.sql.streaming.StreamingQuery StreamingQueries]] active on `this` context.
   *
   * @since 2.0.0
   */
  def streams: StreamingQueryManager = sparkSession.streams

  /** @inheritdoc */
  override def sparkContext: SparkContext = super.sparkContext

  /** @inheritdoc */
  override def newSession(): SQLContext = sparkSession.newSession().sqlContext

  /** @inheritdoc */
  override def emptyDataFrame: Dataset[Row] = super.emptyDataFrame

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
  @DeveloperApi
  override def createDataFrame(rowRDD: RDD[Row], schema: StructType): Dataset[Row] =
    super.createDataFrame(rowRDD, schema)

  /** @inheritdoc */
  override def createDataset[T: Encoder](data: Seq[T]): Dataset[T] = super.createDataset(data)

  /** @inheritdoc */
  override def createDataset[T: Encoder](data: RDD[T]): Dataset[T] = super.createDataset(data)

  /** @inheritdoc */
  override def createDataset[T: Encoder](data: JList[T]): Dataset[T] =
    super.createDataset(data)

  /** @inheritdoc */
  @DeveloperApi
  override def createDataFrame(rowRDD: JavaRDD[Row], schema: StructType): Dataset[Row] =
    super.createDataFrame(rowRDD, schema)

  /** @inheritdoc */
  @DeveloperApi
  override def createDataFrame(rows: JList[Row], schema: StructType): Dataset[Row] =
    super.createDataFrame(rows, schema)

  /** @inheritdoc */
  override def createDataFrame(rdd: RDD[_], beanClass: Class[_]): Dataset[Row] =
    super.createDataFrame(rdd, beanClass)

  /** @inheritdoc */
  override def createDataFrame(rdd: JavaRDD[_], beanClass: Class[_]): Dataset[Row] =
    super.createDataFrame(rdd, beanClass)

  /** @inheritdoc */
  override def createDataFrame(data: JList[_], beanClass: Class[_]): Dataset[Row] =
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
      options: JMap[String, String]): Dataset[Row] = {
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
      options: JMap[String, String]): Dataset[Row] = {
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
  override def tables(): DataFrame = super.tables()

  /** @inheritdoc */
  override def tables(databaseName: String): DataFrame = super.tables(databaseName)

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
  override def jsonRDD(json: RDD[String]): Dataset[Row] = read.json(json)

  /** @inheritdoc */
  override def jsonRDD(json: JavaRDD[String]): Dataset[Row] = read.json(json)

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
  override def load(source: String, options: JMap[String, String]): Dataset[Row] =
    super.load(source, options)

  /** @inheritdoc */
  override def load(source: String, options: Map[String, String]): Dataset[Row] =
    super.load(source, options)

  /** @inheritdoc */
  override def load(
      source: String,
      schema: StructType,
      options: JMap[String, String]): Dataset[Row] = {
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
  override def jdbc(url: String, table: String, theParts: Array[String]): Dataset[Row] =
    super.jdbc(url, table, theParts)
}

object SQLContext extends api.SQLContextCompanion {

  override private[sql] type SQLContextImpl = SQLContext
  override private[sql] type SparkContextImpl = SparkContext

  /** @inheritdoc */
  def getOrCreate(sparkContext: SparkContext): SQLContext = {
    SparkSession.builder().sparkContext(sparkContext).getOrCreate().sqlContext
  }

  /** @inheritdoc */
  override def setActive(sqlContext: SQLContext): Unit = super.setActive(sqlContext)

  /**
   * Converts an iterator of Java Beans to InternalRow using the provided bean info & schema. This
   * is not related to the singleton, but is a static method for internal use.
   */
  private[sql] def beansToRows(
      data: Iterator[_],
      beanClass: Class[_],
      attrs: Seq[AttributeReference]): Iterator[InternalRow] = {
    def createStructConverter(cls: Class[_], fieldTypes: Seq[DataType]): Any => InternalRow = {
      val methodConverters =
        JavaTypeInference
          .getJavaBeanReadableProperties(cls)
          .zip(fieldTypes)
          .map { case (property, fieldType) =>
            val method = property.getReadMethod
            method -> createConverter(method.getReturnType, fieldType)
          }
      value =>
        if (value == null) {
          null
        } else {
          new GenericInternalRow(methodConverters.map { case (method, converter) =>
            converter(method.invoke(value))
          })
        }
    }

    def createConverter(cls: Class[_], dataType: DataType): Any => Any = dataType match {
      case struct: StructType => createStructConverter(cls, struct.map(_.dataType))
      case _ => CatalystTypeConverters.createToCatalystConverter(dataType)
    }

    val dataConverter = createStructConverter(beanClass, attrs.map(_.dataType))
    data.map(dataConverter)
  }

  /**
   * Extract `spark.sql.*` properties from the conf and return them as a [[Properties]].
   */
  private[sql] def getSQLProperties(sparkConf: SparkConf): Properties = {
    val properties = new Properties
    sparkConf.getAll.foreach { case (key, value) =>
      if (key.startsWith("spark.sql")) {
        properties.setProperty(key, value)
      }
    }
    properties
  }

}
