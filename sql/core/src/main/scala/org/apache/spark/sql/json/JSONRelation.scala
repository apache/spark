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
import org.apache.spark.sql.types.StructType


private[sql] class DefaultSource
  extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {

  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path", sys.error("'path' must be specified for json data."))
  }

  /** Returns a new base relation with the parameters. */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val path = parameters.getOrElse("path", sys.error("Option 'path' not specified"))
    val samplingRatio = parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)

    JSONRelation(path, samplingRatio, None)(sqlContext)
  }

  /** Returns a new base relation with the given schema and parameters. */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    val path = parameters.getOrElse("path", sys.error("Option 'path' not specified"))
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
          //fs.delete(filesystemPath, true)
          true
        case SaveMode.ErrorIfExists =>
          sys.error(s"path $path already exists.")
        case SaveMode.Ignore => false
      }
    } else {
      true
    }
    val relation = if (doSave) {
      // Only save data when the save mode is not ignore.
      //data.toJSON.saveAsTextFile(path)
      val createdRelation = createRelation(sqlContext,parameters, data.schema)
      createdRelation.asInstanceOf[JSONRelation].insert(data, true)

      createdRelation
    } else {
      createRelation(sqlContext, parameters, data.schema)
    }

    relation
  }
}

private[sql] case class JSONRelation(
    path: String,
    samplingRatio: Double,
    userSpecifiedSchema: Option[StructType])(
    @transient val sqlContext: SQLContext)
  extends TableScan with InsertableRelation {
  // TODO: Support partitioned JSON relation.
  val filePath = new Path(path,"*").toUri.toString
  private def baseRDD = sqlContext.sparkContext.textFile(filePath)

  override val schema = userSpecifiedSchema.getOrElse(
    JsonRDD.nullTypeToStringType(
      JsonRDD.inferSchema(
        baseRDD,
        samplingRatio,
        sqlContext.conf.columnNameOfCorruptRecord)))

  override def buildScan() =
    JsonRDD.jsonStringToRow(baseRDD, schema, sqlContext.conf.columnNameOfCorruptRecord)

  private def isTemporaryFile(file: Path): Boolean = {
    file.getName == "_temporary"
  }

  override def insert(data: DataFrame, overwrite: Boolean) = {

    val filesystemPath = new Path(path)
    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)

    // If the path exists, it must be a directory.
    // Otherwise we create a directory with the path name.
    if (fs.exists(filesystemPath) && !fs.getFileStatus(filesystemPath).isDirectory) {
      sys.error("a CREATE [TEMPORARY] TABLE AS SELECT statement need the path must be directory")
    }

    if (overwrite) {
      val temporaryPath = new Path(path, "_temporary")
      val dataPath = new Path(path, "data")
      // Write the data.
      data.toJSON.saveAsTextFile(temporaryPath.toUri.toString)
      val pathsToDelete = fs.listStatus(filesystemPath).filter(
        f => !isTemporaryFile(f.getPath)).map(_.getPath)

      try {
        pathsToDelete.foreach(fs.delete(_,true))
      } catch {
        case e: IOException =>
          throw new IOException(
            s"Unable to delete original data in directory ${filesystemPath.toString} when"
              + s" run INSERT OVERWRITE a JSON table:\n${e.toString}")
      }
      fs.rename(temporaryPath,dataPath)
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
      (this.path == that.path) && (this.schema == that.schema)
    case _ => false
  }
}
