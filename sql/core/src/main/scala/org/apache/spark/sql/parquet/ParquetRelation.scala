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

package org.apache.spark.sql.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.plans.physical.{TableFormat, TableLocation}
import parquet.hadoop.ParquetOutputFormat
import parquet.hadoop.metadata.CompressionCodecName
import parquet.schema.MessageType

import org.apache.spark.sql.{HadoopDirectory, HadoopDirectoryLike, SQLContext}
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical._

/**
 * Relation that consists of data stored in a Parquet columnar format.
 *
 * Users should interact with parquet files though a SchemaRDD, created by a [[SQLContext]] instead
 * of using this class directly.
 *
 * {{{
 *   val parquetRDD = sqlContext.parquetFile("path/to/parquet.file")
 * }}}
 *
 * @param location The path to the Parquet file.
 */
private[sql] case class ParquetRelation(
    location: HadoopDirectory,
    @transient conf: Configuration,
    @transient sqlContext: SQLContext)
  extends LeafNode with MultiInstanceRelation {

  self: Product =>

  /** Schema derived from ParquetFile */
  def parquetSchema: MessageType =
    ParquetTypesConverter
      .readMetaData(location.asPath, conf)
      .getFileMetaData
      .getSchema

  /** Attributes */
  override val output = ParquetTypesConverter.readSchemaFromFile(location.asPath, conf)

  override def newInstance = ParquetRelation(location, conf, sqlContext).asInstanceOf[this.type]

  // Equals must also take into account the output attributes so that we can distinguish between
  // different instances of the same relation,
  override def equals(other: Any) = other match {
    case p: ParquetRelation =>
      p.location == location && p.output == output
    case _ => false
  }

  // TODO: Use data from the footers.
  override lazy val statistics = Statistics(sizeInBytes = sqlContext.defaultSizeInBytes)
}

private[sql] object ParquetRelation {

  def enableLogForwarding() {
    // Note: Logger.getLogger("parquet") has a default logger
    // that appends to Console which needs to be cleared.
    val parquetLogger = java.util.logging.Logger.getLogger("parquet")
    parquetLogger.getHandlers.foreach(parquetLogger.removeHandler)
    // TODO(witgo): Need to set the log level ?
    // if(parquetLogger.getLevel != null) parquetLogger.setLevel(null)
    if (!parquetLogger.getUseParentHandlers) parquetLogger.setUseParentHandlers(true)
  }

  // The element type for the RDDs that this relation maps to.
  type RowType = org.apache.spark.sql.catalyst.expressions.GenericMutableRow

  // The compression type
  type CompressionType = parquet.hadoop.metadata.CompressionCodecName

  // The default compression
  val defaultCompression = CompressionCodecName.GZIP

  /**
   * Creates an empty ParquetRelation and underlying Parquetfile that only
   * consists of the Metadata for the given schema.
   *
   * @param path The directory the Parquetfile will be stored in.
   * @param attributes The schema of the relation.
   * @param conf A configuration to be used.
   * @return An empty ParquetRelation.
   */
  def createEmpty(path: HadoopDirectory,
                  attributes: Seq[Attribute],
                  conf: Configuration,
                  sqlContext: SQLContext): ParquetRelation = {
    if (conf.get(ParquetOutputFormat.COMPRESSION) == null) {
      conf.set(ParquetOutputFormat.COMPRESSION, ParquetRelation.defaultCompression.name())
    }
    ParquetRelation.enableLogForwarding()
    ParquetTypesConverter.writeMetaData(attributes, path, conf)
    new ParquetRelation(path, conf, sqlContext) {
      override val output = attributes
    }
  }
}

/**
 * Format for creating and loading Parquet tables.
 * Parquet tables can currently only be created within a single Hadoop directory.
 */
class ParquetFormat(sqlContext: SQLContext, conf: Configuration) extends TableFormat {

  /**
   * Creates a new ParquetRelation and underlying Parquet file for the given LogicalPlan. Note that
   * this is used inside [[org.apache.spark.sql.execution.SparkStrategies SparkStrategies]] to
   * create a resolved relation as a data sink for writing to a Parquetfile. The relation is empty
   * but is initialized with ParquetMetadata and can be inserted into.
   *
   * @return An empty ParquetRelation with inferred metadata.
   */
  override def createEmptyRelation(
      location: TableLocation, schema: Seq[Attribute]): ParquetRelation = {
    location match {
      case dir: HadoopDirectoryLike =>
        ParquetRelation.createEmpty(dir.asHadoopDirectory, schema, conf, sqlContext)
      case _ =>
        sys.error(s"Parquet relation only supports Hadoop directories, found: $location")
    }
  }

  override def loadExistingRelation(location: TableLocation): ParquetRelation = {
    location match {
      case dir: HadoopDirectoryLike =>
        new ParquetRelation(dir.asHadoopDirectory, conf, sqlContext)
      case _ =>
        sys.error(s"Parquet relation only supports Hadoop directories, found: $location")
    }
  }
}
