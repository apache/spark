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

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCPartition, JDBCPartitioningInfo, JDBCRelation}
import org.apache.spark.sql.execution.datasources.json.JSONRelation
import org.apache.spark.sql.execution.datasources.parquet.ParquetRelation
import org.apache.spark.sql.execution.datasources.{LogicalRelation, ResolvedDataSource}
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Logging, Partition}

/**
 * :: Experimental ::
 * Interface used to load a [[DataFrame]] from external storage systems (e.g. file systems,
 * key-value stores, etc). Use [[SQLContext.read]] to access this.
 *
 * @since 1.4.0
 */
@Experimental
class DataFrameReader private[sql](sqlContext: SQLContext) extends Logging {

  /**
   * Specifies the input data source format.
   *
   * @since 1.4.0
   */
  def format(source: String): DataFrameReader = {
    this.source = source
    this
  }

  /**
   * Specifies the input schema. Some data sources (e.g. JSON) can infer the input schema
   * automatically from data. By specifying the schema here, the underlying data source can
   * skip the schema inference step, and thus speed up data loading.
   *
   * @since 1.4.0
   */
  def schema(schema: StructType): DataFrameReader = {
    this.userSpecifiedSchema = Option(schema)
    this
  }

  /**
   * Adds an input option for the underlying data source.
   *
   * @since 1.4.0
   */
  def option(key: String, value: String): DataFrameReader = {
    this.extraOptions += (key -> value)
    this
  }

  /**
   * (Scala-specific) Adds input options for the underlying data source.
   *
   * @since 1.4.0
   */
  def options(options: scala.collection.Map[String, String]): DataFrameReader = {
    this.extraOptions ++= options
    this
  }

  /**
   * Adds input options for the underlying data source.
   *
   * @since 1.4.0
   */
  def options(options: java.util.Map[String, String]): DataFrameReader = {
    this.options(scala.collection.JavaConversions.mapAsScalaMap(options))
    this
  }

  /**
   * Loads input in as a [[DataFrame]], for data sources that require a path (e.g. data backed by
   * a local or distributed file system).
   *
   * @since 1.4.0
   */
  def load(path: String): DataFrame = {
    option("path", path).load()
  }

  /**
   * Loads input in as a [[DataFrame]], for data sources that don't require a path (e.g. external
   * key-value stores).
   *
   * @since 1.4.0
   */
  def load(): DataFrame = {
    val resolved = ResolvedDataSource(
      sqlContext,
      userSpecifiedSchema = userSpecifiedSchema,
      partitionColumns = Array.empty[String],
      provider = source,
      options = extraOptions.toMap)
    DataFrame(sqlContext, LogicalRelation(resolved.relation))
  }

  /**
   * Construct a [[DataFrame]] representing the database table accessible via JDBC URL
   * url named table and connection properties.
   *
   * @since 1.4.0
   */
  def jdbc(url: String, table: String, properties: Properties): DataFrame = {
    jdbc(url, table, JDBCRelation.columnPartition(null), properties)
  }

  /**
   * Construct a [[DataFrame]] representing the database table accessible via JDBC URL
   * url named table. Partitions of the table will be retrieved in parallel based on the parameters
   * passed to this function.
   *
   * Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash
   * your external database systems.
   *
   * @param url JDBC database url of the form `jdbc:subprotocol:subname`
   * @param table Name of the table in the external database.
   * @param columnName the name of a column of integral type that will be used for partitioning.
   * @param lowerBound the minimum value of `columnName` used to decide partition stride
   * @param upperBound the maximum value of `columnName` used to decide partition stride
   * @param numPartitions the number of partitions.  the range `minValue`-`maxValue` will be split
   *                      evenly into this many partitions
   * @param connectionProperties JDBC database connection arguments, a list of arbitrary string
   *                             tag/value. Normally at least a "user" and "password" property
   *                             should be included.
   *
   * @since 1.4.0
   */
  def jdbc(
      url: String,
      table: String,
      columnName: String,
      lowerBound: Long,
      upperBound: Long,
      numPartitions: Int,
      connectionProperties: Properties): DataFrame = {
    val partitioning = JDBCPartitioningInfo(columnName, lowerBound, upperBound, numPartitions)
    val parts = JDBCRelation.columnPartition(partitioning)
    jdbc(url, table, parts, connectionProperties)
  }

  /**
   * Construct a [[DataFrame]] representing the database table accessible via JDBC URL
   * url named table using connection properties. The `predicates` parameter gives a list
   * expressions suitable for inclusion in WHERE clauses; each one defines one partition
   * of the [[DataFrame]].
   *
   * Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash
   * your external database systems.
   *
   * @param url JDBC database url of the form `jdbc:subprotocol:subname`
   * @param table Name of the table in the external database.
   * @param predicates Condition in the where clause for each partition.
   * @param connectionProperties JDBC database connection arguments, a list of arbitrary string
   *                             tag/value. Normally at least a "user" and "password" property
   *                             should be included.
   * @since 1.4.0
   */
  def jdbc(
      url: String,
      table: String,
      predicates: Array[String],
      connectionProperties: Properties): DataFrame = {
    val parts: Array[Partition] = predicates.zipWithIndex.map { case (part, i) =>
      JDBCPartition(part, i) : Partition
    }
    jdbc(url, table, parts, connectionProperties)
  }

  private def jdbc(
      url: String,
      table: String,
      parts: Array[Partition],
      connectionProperties: Properties): DataFrame = {
    val props = new Properties()
    extraOptions.foreach { case (key, value) =>
      props.put(key, value)
    }
    // connectionProperties should override settings in extraOptions
    props.putAll(connectionProperties)
    val relation = JDBCRelation(url, table, parts, props)(sqlContext)
    sqlContext.baseRelationToDataFrame(relation)
  }

  /**
   * Loads a JSON file (one object per line) and returns the result as a [[DataFrame]].
   *
   * This function goes through the input once to determine the input schema. If you know the
   * schema in advance, use the version that specifies the schema to avoid the extra scan.
   *
   * @param path input path
   * @since 1.4.0
   */
  def json(path: String): DataFrame = format("json").load(path)

  /**
   * Loads an `JavaRDD[String]` storing JSON objects (one object per record) and
   * returns the result as a [[DataFrame]].
   *
   * Unless the schema is specified using [[schema]] function, this function goes through the
   * input once to determine the input schema.
   *
   * @param jsonRDD input RDD with one JSON object per record
   * @since 1.4.0
   */
  def json(jsonRDD: JavaRDD[String]): DataFrame = json(jsonRDD.rdd)

  /**
   * Loads an `RDD[String]` storing JSON objects (one object per record) and
   * returns the result as a [[DataFrame]].
   *
   * Unless the schema is specified using [[schema]] function, this function goes through the
   * input once to determine the input schema.
   *
   * @param jsonRDD input RDD with one JSON object per record
   * @since 1.4.0
   */
  def json(jsonRDD: RDD[String]): DataFrame = {
    val samplingRatio = extraOptions.getOrElse("samplingRatio", "1.0").toDouble
    sqlContext.baseRelationToDataFrame(
      new JSONRelation(Some(jsonRDD), samplingRatio, userSpecifiedSchema, None, None)(sqlContext))
  }

  /**
   * Loads a Parquet file, returning the result as a [[DataFrame]]. This function returns an empty
   * [[DataFrame]] if no paths are passed in.
   *
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def parquet(paths: String*): DataFrame = {
    if (paths.isEmpty) {
      sqlContext.emptyDataFrame
    } else {
      val globbedPaths = paths.flatMap { path =>
        val hdfsPath = new Path(path)
        val fs = hdfsPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
        val qualified = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
        SparkHadoopUtil.get.globPathIfNecessary(qualified)
      }.toArray

      sqlContext.baseRelationToDataFrame(
        new ParquetRelation(
          globbedPaths.map(_.toString), userSpecifiedSchema, None, extraOptions.toMap)(sqlContext))
    }
  }

  /**
   * Loads an ORC file and returns the result as a [[DataFrame]].
   *
   * @param path input path
   * @since 1.5.0
   * @note Currently, this method can only be used together with `HiveContext`.
   */
  def orc(path: String): DataFrame = format("orc").load(path)

  /**
   * Returns the specified table as a [[DataFrame]].
   *
   * @since 1.4.0
   */
  def table(tableName: String): DataFrame = {
    DataFrame(sqlContext, sqlContext.catalog.lookupRelation(Seq(tableName)))
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Builder pattern config options
  ///////////////////////////////////////////////////////////////////////////////////////

  private var source: String = sqlContext.conf.defaultDataSourceName

  private var userSpecifiedSchema: Option[StructType] = None

  private var extraOptions = new scala.collection.mutable.HashMap[String, String]

}
