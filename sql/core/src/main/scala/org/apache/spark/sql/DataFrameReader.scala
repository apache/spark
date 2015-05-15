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
   * Loads a JSON file (one object per line) and returns the result as a [[DataFrame]].
   *
   * This function goes through the input once to determine the input schema. If you know the
   * schema in advance, use the version that specifies the schema to avoid the extra scan.
   *
   * @param path input path
   * @since 1.4.0
   */
  def json(path: String): DataFrame = json(path, 1.0)

  /**
   * Loads a JSON file (one object per line) and returns the result as a [[DataFrame]].
   *
   * This function goes through the input once to determine the input schema. If you know the
   * schema in advance, use the version that specifies the schema to avoid the extra scan.
   *
   * @param path input path
   * @param samplingRatio ratio of data to go through to determine the input schema.
   * @since 1.4.0
   */
  def json(path: String, samplingRatio: Double): DataFrame = {
    sqlContext.load("json", Map("path" -> path, "samplingRatio" -> samplingRatio.toString))
  }

  /**
   * Loads a JSON file (one object per line) and returns the result as a [[DataFrame]].
   *
   * @param path input path
   * @param schema schema of the JSON objects.
   * @since 1.4.0
   */
  def json(path: String, schema: StructType): DataFrame = {
    sqlContext.load("json", schema, Map("path" -> path))
  }

  /**
   * Loads an `RDD[String]` storing JSON objects (one object per record) and
   * returns the result as a [[DataFrame]].
   *
   * This function goes through the input once to determine the input schema. If you know the
   * schema in advance, use the version that specifies the schema to avoid the extra scan.
   *
   * @param jsonRDD input RDD with one JSON object per record
   * @since 1.4.0
   */
  def json(jsonRDD: RDD[String]): DataFrame = json(jsonRDD, 1.0)

  /**
   * Loads an `JavaRDD[String]` storing JSON objects (one object per record) and
   * returns the result as a [[DataFrame]].
   *
   * This function goes through the input once to determine the input schema. If you know the
   * schema in advance, use the version that specifies the schema to avoid the extra scan.
   *
   * @param jsonRDD input RDD with one JSON object per record
   * @since 1.4.0
   */
  def json(jsonRDD: JavaRDD[String]): DataFrame = json(jsonRDD, 1.0)

  /**
   * Loads an `RDD[String]` storing JSON objects (one object per record) and
   * returns the result as a [[DataFrame]].
   *
   * This function goes through the input once to determine the input schema. If you know the
   * schema in advance, use the version that specifies the schema to avoid the extra scan.
   *
   * @param jsonRDD input RDD with one JSON object per record
   * @param samplingRatio ratio of data to go through to determine the input schema.
   * @since 1.4.0
   */
  def json(jsonRDD: RDD[String], samplingRatio: Double): DataFrame = {
    if (sqlContext.conf.useJacksonStreamingAPI) {
      sqlContext.baseRelationToDataFrame(
        new JSONRelation(() => jsonRDD, None, samplingRatio, None)(sqlContext))
    } else {
      val columnNameOfCorruptJsonRecord = sqlContext.conf.columnNameOfCorruptRecord
      val appliedSchema =
        JsonRDD.nullTypeToStringType(
          JsonRDD.inferSchema(jsonRDD, samplingRatio, columnNameOfCorruptJsonRecord))
      val rowRDD = JsonRDD.jsonStringToRow(jsonRDD, appliedSchema, columnNameOfCorruptJsonRecord)
      sqlContext.createDataFrame(rowRDD, appliedSchema, needsConversion = false)
    }
  }

  /**
   * Loads an `JavaRDD[String]` storing JSON objects (one object per record) and
   * returns the result as a [[DataFrame]].
   *
   * This function goes through the input once to determine the input schema. If you know the
   * schema in advance, use the version that specifies the schema to avoid the extra scan.
   *
   * @param jsonRDD input RDD with one JSON object per record
   * @param samplingRatio ratio of data to go through to determine the input schema.
   * @since 1.4.0
   */
  def json(jsonRDD: JavaRDD[String], samplingRatio: Double): DataFrame = {
    json(jsonRDD.rdd, samplingRatio)
  }

  /**
   * Loads an `JavaRDD[String]` storing JSON objects (one object per record) and
   * returns the result as a [[DataFrame]].
   *
   * @param jsonRDD input RDD with one JSON object per record
   * @since 1.4.0
   */
  def json(jsonRDD: RDD[String], schema: StructType): DataFrame = {
    if (sqlContext.conf.useJacksonStreamingAPI) {
      sqlContext.baseRelationToDataFrame(
        new JSONRelation(() => jsonRDD, None, 1.0, Some(schema))(sqlContext))
    } else {
      val columnNameOfCorruptJsonRecord = sqlContext.conf.columnNameOfCorruptRecord
      val appliedSchema =
        Option(schema).getOrElse(
          JsonRDD.nullTypeToStringType(
            JsonRDD.inferSchema(jsonRDD, 1.0, columnNameOfCorruptJsonRecord)))
      val rowRDD = JsonRDD.jsonStringToRow(jsonRDD, appliedSchema, columnNameOfCorruptJsonRecord)
      sqlContext.createDataFrame(rowRDD, appliedSchema, needsConversion = false)
    }
  }

  /**
   * Loads an `JavaRDD[String]` storing JSON objects (one object per record) and
   * returns the result as a [[DataFrame]].
   *
   * @param jsonRDD input RDD with one JSON object per record
   * @since 1.4.0
   */
  def json(jsonRDD: JavaRDD[String], schema: StructType): DataFrame = {
    json(jsonRDD.rdd, schema)
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

}
