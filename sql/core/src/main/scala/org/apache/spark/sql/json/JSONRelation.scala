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

package org.apache.spark.sql.json

import java.io.IOException

import org.apache.hadoop.fs.Path

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Expression, Attribute, Row}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}


private[sql] class DefaultSource
  extends RelationProvider
  with SchemaRelationProvider
  with CreatableRelationProvider {

  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path", sys.error("'path' must be specified for json data."))
  }

  /** Returns a new base relation with the parameters. */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val path = checkPath(parameters)
    val samplingRatio = parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)

    new JSONRelation(path, samplingRatio, None, sqlContext)
  }

  /** Returns a new base relation with the given schema and parameters. */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    val path = checkPath(parameters)
    val samplingRatio = parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)

    new JSONRelation(path, samplingRatio, Some(schema), sqlContext)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val path = checkPath(parameters)
    val filesystemPath = new Path(path)
    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
    val doSave = if (fs.exists(filesystemPath)) {
      mode match {
        case SaveMode.Append =>
          sys.error(s"Append mode is not supported by ${this.getClass.getCanonicalName}")
        case SaveMode.Overwrite => {
          var success: Boolean = false
          try {
            success = fs.delete(filesystemPath, true)
          } catch {
            case e: IOException =>
              throw new IOException(
                s"Unable to clear output directory ${filesystemPath.toString} prior"
                  + s" to writing to JSON table:\n${e.toString}")
          }
          if (!success) {
            throw new IOException(
              s"Unable to clear output directory ${filesystemPath.toString} prior"
                + s" to writing to JSON table.")
          }
          true
        }
        case SaveMode.ErrorIfExists =>
          sys.error(s"path $path already exists.")
        case SaveMode.Ignore => false
      }
    } else {
      true
    }
    if (doSave) {
      // Only save data when the save mode is not ignore.
      data.toJSON.saveAsTextFile(path)
    }

    createRelation(sqlContext, parameters, data.schema)
  }
}

private[sql] class JSONRelation(
    // baseRDD is not immutable with respect to INSERT OVERWRITE
    // and so it must be recreated at least as often as the
    // underlying inputs are modified. To be safe, a function is
    // used instead of a regular RDD value to ensure a fresh RDD is
    // recreated for each and every operation.
    baseRDD: () => RDD[String],
    val path: Option[String],
    val samplingRatio: Double,
    userSpecifiedSchema: Option[StructType])(
    @transient val sqlContext: SQLContext)
  extends BaseRelation
  with TableScan
  with InsertableRelation
  with CatalystScan {

  def this(
      path: String,
      samplingRatio: Double,
      userSpecifiedSchema: Option[StructType],
      sqlContext: SQLContext) =
    this(
      () => sqlContext.sparkContext.textFile(path),
      Some(path),
      samplingRatio,
      userSpecifiedSchema)(sqlContext)

  private val useJacksonStreamingAPI: Boolean = sqlContext.conf.useJacksonStreamingAPI

  override val needConversion: Boolean = false

  override lazy val schema = userSpecifiedSchema.getOrElse {
    if (useJacksonStreamingAPI) {
      InferSchema(
        baseRDD(),
        samplingRatio,
        sqlContext.conf.columnNameOfCorruptRecord)
    } else {
      JsonRDD.nullTypeToStringType(
        JsonRDD.inferSchema(
          baseRDD(),
          samplingRatio,
          sqlContext.conf.columnNameOfCorruptRecord))
    }
  }

  override def buildScan(): RDD[Row] = {
    if (useJacksonStreamingAPI) {
      JacksonParser(
        baseRDD(),
        schema,
        sqlContext.conf.columnNameOfCorruptRecord)
    } else {
      JsonRDD.jsonStringToRow(
        baseRDD(),
        schema,
        sqlContext.conf.columnNameOfCorruptRecord)
    }
  }

  override def buildScan(requiredColumns: Seq[Attribute], filters: Seq[Expression]): RDD[Row] = {
    if (useJacksonStreamingAPI) {
      JacksonParser(
        baseRDD(),
        StructType.fromAttributes(requiredColumns),
        sqlContext.conf.columnNameOfCorruptRecord)
    } else {
      JsonRDD.jsonStringToRow(
        baseRDD(),
        StructType.fromAttributes(requiredColumns),
        sqlContext.conf.columnNameOfCorruptRecord)
    }
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val filesystemPath = path match {
      case Some(p) => new Path(p)
      case None =>
        throw new IOException(s"Cannot INSERT into table with no path defined")
    }

    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)

    if (overwrite) {
      if (fs.exists(filesystemPath)) {
        var success: Boolean = false
        try {
          success = fs.delete(filesystemPath, true)
        } catch {
          case e: IOException =>
            throw new IOException(
              s"Unable to clear output directory ${filesystemPath.toString} prior"
                + s" to writing to JSON table:\n${e.toString}")
        }
        if (!success) {
          throw new IOException(
            s"Unable to clear output directory ${filesystemPath.toString} prior"
              + s" to writing to JSON table.")
        }
      }
      // Write the data.
      data.toJSON.saveAsTextFile(filesystemPath.toString)
      // Right now, we assume that the schema is not changed. We will not update the schema.
      // schema = data.schema
    } else {
      // TODO: Support INSERT INTO
      sys.error("JSON table only support INSERT OVERWRITE for now.")
    }
  }

  override def hashCode(): Int = 41 * (41 + path.hashCode) + schema.hashCode()

  override def equals(other: Any): Boolean = other match {
    case that: JSONRelation =>
      (this.path == that.path) && this.schema.sameType(that.schema)
    case _ => false
  }
}
