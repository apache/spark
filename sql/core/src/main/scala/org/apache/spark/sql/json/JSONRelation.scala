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

import org.apache.spark.sql.{SaveMode, DataFrame, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataType, StructType}


private[sql] class DefaultSource
  extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {

  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path", sys.error("'path' must be specified for json data."))
  }

  /** Returns a new base relation with the parameters. */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val path = checkPath(parameters)
    val samplingRatio = parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)

    JSONRelation(path, samplingRatio, None)(sqlContext)
  }

  /** Returns a new base relation with the given schema and parameters. */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    val path = checkPath(parameters)
    val samplingRatio = parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)

    JSONRelation(path, samplingRatio, Some(schema))(sqlContext)
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
        case SaveMode.Overwrite =>
          fs.delete(filesystemPath, true)
          true
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

private[sql] case class JSONRelation(
    path: String,
    samplingRatio: Double,
    userSpecifiedSchema: Option[StructType])(
    @transient val sqlContext: SQLContext)
  extends TableScan with InsertableRelation {
  // TODO: Support partitioned JSON relation.
  private def baseRDD = sqlContext.sparkContext.textFile(path)

  override val schema = userSpecifiedSchema.getOrElse(
    JsonRDD.nullTypeToStringType(
      JsonRDD.inferSchema(
        baseRDD,
        samplingRatio,
        sqlContext.conf.columnNameOfCorruptRecord)))

  override def buildScan() =
    JsonRDD.jsonStringToRow(baseRDD, schema, sqlContext.conf.columnNameOfCorruptRecord)

  override def insert(data: DataFrame, overwrite: Boolean) = {
    val filesystemPath = new Path(path)
    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)

    if (overwrite) {
      try {
        fs.delete(filesystemPath, true)
      } catch {
        case e: IOException =>
          throw new IOException(
            s"Unable to clear output directory ${filesystemPath.toString} prior"
              + s" to INSERT OVERWRITE a JSON table:\n${e.toString}")
      }
      // Write the data.
      data.toJSON.saveAsTextFile(path)
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
