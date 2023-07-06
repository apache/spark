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
package org.apache.spark.sql.execution.datasources.xml

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.execution.datasources.xml.util.XmlFile
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

/**
 * Provides access to XML data from pure SQL statements (i.e. for users of the
 * JDBC server).
 */
class DefaultSource
  extends RelationProvider
  with SchemaRelationProvider
  with CreatableRelationProvider
  with DataSourceRegister {

  /**
   * Short alias for spark-xml data source.
   */
  override def shortName(): String = "xml"

  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path",
      throw new IllegalArgumentException("'path' must be specified for XML data."))
  }

  /**
   * Creates a new relation for data store in XML given parameters.
   * Parameters have to include 'path'.
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  /**
   * Creates a new relation for data store in XML given parameters and user supported schema.
   * Parameters have to include 'path'.
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): XmlRelation = {
    val path = checkPath(parameters)
    // We need the `charset` and `rowTag` before creating the relation.
    val (charset, rowTag) = {
      val options = XmlOptions(parameters)
      (options.charset, options.rowTag)
    }

    val paramsWithTZ =
      sqlContext.sparkContext.getConf.getOption("spark.sql.session.timeZone") match {
        case Some(tz) => parameters.updated("timezone", tz)
        case None => parameters
      }

    XmlRelation(
      () => XmlFile.withCharset(sqlContext.sparkContext, path, charset, rowTag),
      Some(path),
      paramsWithTZ,
      schema)(sqlContext)
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
          throw new IllegalArgumentException(
            s"Append mode is not supported by ${this.getClass.getCanonicalName}")
        case SaveMode.Overwrite =>
          fs.delete(filesystemPath, true)
          true
        case SaveMode.ErrorIfExists =>
          throw new IllegalArgumentException(s"path $path already exists.")
        case SaveMode.Ignore => false
      }
    } else {
      true
    }
    if (doSave) {
      // Only save data when the save mode is not ignore.
      XmlFile.saveAsXmlFile(data, filesystemPath.toString, parameters)
    }
    createRelation(sqlContext, parameters, data.schema)
  }
}
