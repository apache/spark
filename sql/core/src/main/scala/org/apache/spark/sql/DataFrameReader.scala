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

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.json.{JsonRDD, JSONRelation}
import org.apache.spark.sql.parquet.ParquetRelation2
import org.apache.spark.sql.sources.{LogicalRelation, ResolvedDataSource}
import org.apache.spark.sql.types.StructType

/**
 * :: Experimental ::
 * Interface used to load a [[DataFrame]] from external storage systems (e.g. file systems,
 * key-value stores, etc).
 *
 * @since 1.4.0
 */
@Experimental
class DataFrameReader private[sql](sqlContext: SQLContext) {

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
   * Specifies the input partitioning. If specified, the underlying data source does not need to
   * discover the data partitioning scheme, and thus can speed up very large inputs.
   *
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def partitionBy(colNames: String*): DataFrameReader = {
    this.partitioningColumns = Option(colNames)
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
      partitionColumns = partitioningColumns.map(_.toArray).getOrElse(Array.empty[String]),
      provider = source,
      options = extraOptions.toMap)
    DataFrame(sqlContext, LogicalRelation(resolved.relation))
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
    if (sqlContext.conf.useJacksonStreamingAPI) {
      sqlContext.baseRelationToDataFrame(
        new JSONRelation(() => jsonRDD, None, samplingRatio, userSpecifiedSchema)(sqlContext))
    } else {
      val columnNameOfCorruptJsonRecord = sqlContext.conf.columnNameOfCorruptRecord
      val appliedSchema = userSpecifiedSchema.getOrElse(
        JsonRDD.nullTypeToStringType(
          JsonRDD.inferSchema(jsonRDD, 1.0, columnNameOfCorruptJsonRecord)))
      val rowRDD = JsonRDD.jsonStringToRow(jsonRDD, appliedSchema, columnNameOfCorruptJsonRecord)
      sqlContext.createDataFrame(rowRDD, appliedSchema, needsConversion = false)
    }
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
      val globbedPaths = paths.map(new Path(_)).flatMap(SparkHadoopUtil.get.globPath).toArray
      sqlContext.baseRelationToDataFrame(
        new ParquetRelation2(
          globbedPaths.map(_.toString), None, None, Map.empty[String, String])(sqlContext))
    }
  }

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

  private var partitioningColumns: Option[Seq[String]] = None

}
