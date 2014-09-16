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

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsAction

import parquet.hadoop.ParquetOutputFormat
import parquet.hadoop.metadata.CompressionCodecName
import parquet.schema.MessageType

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, UnresolvedException}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, LeafNode}

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
 * @param path The path to the Parquet file.
 */
private[sql] case class ParquetRelation(
    path: String,
    @transient conf: Option[Configuration],
    @transient sqlContext: SQLContext,
    partitioningAttributes: Seq[Attribute] = Nil)
  extends LeafNode with MultiInstanceRelation {

  self: Product =>

  /** Schema derived from ParquetFile */
  def parquetSchema: MessageType =
    ParquetTypesConverter
      .readMetaData(new Path(path), conf)
      .getFileMetaData
      .getSchema

  /** Attributes */
  override val output =
    partitioningAttributes ++
    ParquetTypesConverter.readSchemaFromFile(
      new Path(path.split(",").head),
      conf,
      sqlContext.isParquetBinaryAsString)

  override def newInstance() = ParquetRelation(path, conf, sqlContext).asInstanceOf[this.type]

  // Equals must also take into account the output attributes so that we can distinguish between
  // different instances of the same relation,
  override def equals(other: Any) = other match {
    case p: ParquetRelation =>
      p.path == path && p.output == output
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

  // The parquet compression short names
  val shortParquetCompressionCodecNames = Map(
    "NONE"         -> CompressionCodecName.UNCOMPRESSED,
    "UNCOMPRESSED" -> CompressionCodecName.UNCOMPRESSED,
    "SNAPPY"       -> CompressionCodecName.SNAPPY,
    "GZIP"         -> CompressionCodecName.GZIP,
    "LZO"          -> CompressionCodecName.LZO)

  /**
   * Creates a new ParquetRelation and underlying Parquetfile for the given LogicalPlan. Note that
   * this is used inside [[org.apache.spark.sql.execution.SparkStrategies SparkStrategies]] to
   * create a resolved relation as a data sink for writing to a Parquetfile. The relation is empty
   * but is initialized with ParquetMetadata and can be inserted into.
   *
   * @param pathString The directory the Parquetfile will be stored in.
   * @param child The child node that will be used for extracting the schema.
   * @param conf A configuration to be used.
   * @return An empty ParquetRelation with inferred metadata.
   */
  def create(pathString: String,
             child: LogicalPlan,
             conf: Configuration,
             sqlContext: SQLContext): ParquetRelation = {
    if (!child.resolved) {
      throw new UnresolvedException[LogicalPlan](
        child,
        "Attempt to create Parquet table from unresolved child (when schema is not available)")
    }
    createEmpty(pathString, child.output, false, conf, sqlContext)
  }

  /**
   * Creates an empty ParquetRelation and underlying Parquetfile that only
   * consists of the Metadata for the given schema.
   *
   * @param pathString The directory the Parquetfile will be stored in.
   * @param attributes The schema of the relation.
   * @param conf A configuration to be used.
   * @return An empty ParquetRelation.
   */
  def createEmpty(pathString: String,
                  attributes: Seq[Attribute],
                  allowExisting: Boolean,
                  conf: Configuration,
                  sqlContext: SQLContext): ParquetRelation = {
    val path = checkPath(pathString, allowExisting, conf)
    conf.set(ParquetOutputFormat.COMPRESSION, shortParquetCompressionCodecNames.getOrElse(
      sqlContext.parquetCompressionCodec.toUpperCase, CompressionCodecName.UNCOMPRESSED).name())
    ParquetRelation.enableLogForwarding()
    ParquetTypesConverter.writeMetaData(attributes, path, conf)
    new ParquetRelation(path.toString, Some(conf), sqlContext) {
      override val output = attributes
    }
  }

  private def checkPath(pathStr: String, allowExisting: Boolean, conf: Configuration): Path = {
    if (pathStr == null) {
      throw new IllegalArgumentException("Unable to create ParquetRelation: path is null")
    }
    val origPath = new Path(pathStr)
    val fs = origPath.getFileSystem(conf)
    if (fs == null) {
      throw new IllegalArgumentException(
        s"Unable to create ParquetRelation: incorrectly formatted path $pathStr")
    }
    val path = origPath.makeQualified(fs)
    if (!allowExisting && fs.exists(path)) {
      sys.error(s"File $pathStr already exists.")
    }

    if (fs.exists(path) &&
        !fs.getFileStatus(path)
        .getPermission
        .getUserAction
        .implies(FsAction.READ_WRITE)) {
      throw new IOException(
        s"Unable to create ParquetRelation: path $path not read-writable")
    }
    path
  }
}
