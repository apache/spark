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

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.annotation.{Experimental, Stable, Unstable}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.analysis.{CurrentNamespace, UnresolvedNamespace}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.ShowTables
import org.apache.spark.sql.internal.{SessionState, SharedState, SQLConf}
import org.apache.spark.sql.streaming.{DataStreamReader, StreamingQueryManager}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ExecutionListenerManager

/**
 * The entry point for working with structured data (rows and columns) in Spark 1.x.
 *
 * As of Spark 2.0, this is replaced by [[SparkSession]]. However, we are keeping the class
 * here for backward compatibility.
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
class SQLContext private[sql](override val sparkSession: SparkSession)
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
  override def sparkContext: SparkContext = sparkSession.sparkContext

  /** @inheritdoc */
  override def newSession(): SQLContext = sparkSession.newSession().sqlContext

  /** @inheritdoc */
  override def listenerManager: ExecutionListenerManager = sparkSession.listenerManager

  /** @inheritdoc */
  override def setConf(props: Properties): Unit = {
    sessionState.conf.setConf(props)
  }

  private[sql] def setConf[T](entry: ConfigEntry[T], value: T): Unit = {
    sessionState.conf.setConf(entry, value)
  }

  @Experimental
  @transient
  @Unstable
  /** @inheritdoc */
  override def experimental: ExperimentalMethods = sparkSession.experimental

  /** @inheritdoc */
  override def udf: UDFRegistration = sparkSession.udf

  // scalastyle:off
  // Disable style checker so "implicits" object can start with lowercase i
  /**
   * (Scala-specific) Implicit methods available in Scala for converting
   * common Scala objects into `DataFrame`s.
   *
   * {{{
   *   val sqlContext = new SQLContext(sc)
   *   import sqlContext.implicits._
   * }}}
   *
   * @group basic
   * @since 1.3.0
   */
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
  override def read: DataFrameReader = sparkSession.read

  /** @inheritdoc */
  override def readStream: DataStreamReader = sparkSession.readStream

  /**
   * Registers the given `DataFrame` as a temporary table in the catalog. Temporary tables exist
   * only during the lifetime of this instance of SQLContext.
   */
  private[sql] def registerDataFrameAsTable(df: DataFrame, tableName: String): Unit = {
    df.createOrReplaceTempView(tableName)
  }

  /** @inheritdoc */
  override def tables(): DataFrame = {
    Dataset.ofRows(sparkSession, ShowTables(CurrentNamespace, None))
  }

  /** @inheritdoc */
  override def tables(databaseName: String): DataFrame = {
    Dataset.ofRows(sparkSession, ShowTables(UnresolvedNamespace(Seq(databaseName)), None))
  }

  /** @inheritdoc */
  override def streams: StreamingQueryManager = sparkSession.streams

  /** @inheritdoc */
  override def tableNames(databaseName: String): Array[String] = {
    sessionState.catalog.listTables(databaseName).map(_.table).toArray
  }
}

object SQLContext extends api.SQLContextCompanion {

  override private[sql] type SQLContextImpl = SQLContext
  override private[sql] type SparkContextImpl = SparkContext

  /** @inheritdoc */
  override def getOrCreate(sparkContext: SparkContext): SQLContext = {
    SparkSession.builder().sparkContext(sparkContext).getOrCreate().sqlContext
  }

  /**
   * Converts an iterator of Java Beans to InternalRow using the provided
   * bean info & schema. This is not related to the singleton, but is a static
   * method for internal use.
   */
  private[sql] def beansToRows(
      data: Iterator[_],
      beanClass: Class[_],
      attrs: Seq[AttributeReference]): Iterator[InternalRow] = {
    def createStructConverter(cls: Class[_], fieldTypes: Seq[DataType]): Any => InternalRow = {
      val methodConverters =
        JavaTypeInference.getJavaBeanReadableProperties(cls).zip(fieldTypes)
          .map { case (property, fieldType) =>
            val method = property.getReadMethod
            method -> createConverter(method.getReturnType, fieldType)
          }
      value =>
        if (value == null) {
          null
        } else {
          new GenericInternalRow(
            methodConverters.map { case (method, converter) =>
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
