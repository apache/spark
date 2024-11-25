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
import org.apache.spark.annotation.Stable
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.analysis.{CurrentNamespace, UnresolvedNamespace}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.ShowTables
import org.apache.spark.sql.internal.{SessionState, SharedState, SQLConf}
import org.apache.spark.sql.types._

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
class SQLContext private[sql](sparkSession: SparkSession)
  extends api.SQLContext(sparkSession) {

  self =>

  sparkSession.sparkContext.assertNotStopped()

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

  /**
   * Returns a [[SQLContext]] as new session, with separated SQL configurations, temporary
   * tables, registered functions, but sharing the same `SparkContext`, cached data and
   * other things.
   *
   * @since 1.6.0
   */
  override def newSession(): SQLContext = sparkSession.newSession().sqlContext

  /**
   * Set Spark SQL configuration properties.
   *
   * @group config
   * @since 1.0.0
   */
  override def setConf(props: Properties): Unit = {
    sessionState.conf.setConf(props)
  }

  /**
   * Set the given Spark SQL configuration property.
   */
  private[sql] def setConf[T](entry: ConfigEntry[T], value: T): Unit = {
    sessionState.conf.setConf(entry, value)
  }

  /**
   * Creates a DataFrame from an RDD[Row]. User can specify whether the input rows should be
   * converted to Catalyst rows.
   */
  private[sql]
  def internalCreateDataFrame(
      catalystRows: RDD[InternalRow],
      schema: StructType,
      isStreaming: Boolean = false) = {
    sparkSession.internalCreateDataFrame(catalystRows, schema, isStreaming)
  }

  /**
   * Returns a `DataFrame` containing names of existing tables in the current database.
   * The returned DataFrame has three columns, database, tableName and isTemporary (a Boolean
   * indicating if a table is a temporary one or not).
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  override def tables(): DataFrame = {
    Dataset.ofRows(sparkSession, ShowTables(CurrentNamespace, None))
  }

  /**
   * Returns a `DataFrame` containing names of existing tables in the given database.
   * The returned DataFrame has three columns, database, tableName and isTemporary (a Boolean
   * indicating if a table is a temporary one or not).
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  def tables(databaseName: String): DataFrame = {
    Dataset.ofRows(sparkSession, ShowTables(UnresolvedNamespace(Seq(databaseName)), None))
  }

  /**
   * Returns the names of tables in the given database as an array.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  def tableNames(databaseName: String): Array[String] = {
    sessionState.catalog.listTables(databaseName).map(_.table).toArray
  }
}

/**
 * This SQLContext object contains utility functions to create a singleton SQLContext instance,
 * or to get the created SQLContext instance.
 *
 * It also provides utility functions to support preference for threads in multiple sessions
 * scenario, setActive could set a SQLContext for current thread, which will be returned by
 * getOrCreate instead of the global one.
 */
object SQLContext extends api.SQLContextCompanion[SQLContext] {

  /**
   * Get the singleton SQLContext if it exists or create a new one using the given SparkContext.
   *
   * This function can be used to create a singleton SQLContext object that can be shared across
   * the JVM.
   *
   * If there is an active SQLContext for current thread, it will be returned instead of the global
   * one.
   *
   * @since 1.5.0
   */
  @deprecated("Use SparkSession.builder instead", "2.0.0")
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
