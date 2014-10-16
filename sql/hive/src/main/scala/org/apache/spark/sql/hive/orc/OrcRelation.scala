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

package org.apache.spark.sql.hive.orc

import java.util.Properties
import java.io.IOException
import scala.collection.mutable

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.hive.ql.io.orc._

import org.apache.spark.sql.parquet.FileSystemHelper
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, LeafNode}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedException, MultiInstanceRelation}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.hive.HiveMetastoreTypes

private[sql] case class OrcRelation(
    path: String,
    @transient conf: Option[Configuration],
    @transient sqlContext: SQLContext,
    partitioningAttributes: Seq[Attribute] = Nil)
  extends LeafNode with MultiInstanceRelation {
  self: Product =>

  val prop: Properties = new Properties

  var rowClass: Class[_] = null

  override val output = orcSchema

  // TODO: use statistics in ORC file
  override lazy val statistics = Statistics(sizeInBytes = sqlContext.defaultSizeInBytes)

  private def orcSchema: Seq[Attribute] = {
    // get the schema info through ORC Reader
    val origPath = new Path(path)
    val reader = OrcFileOperator.getMetaDataReader(origPath, conf)
    val inspector = reader.getObjectInspector.asInstanceOf[StructObjectInspector]
    // data types that is inspected by this inspector
    val schema = inspector.getTypeName

    HiveMetastoreTypes.toDataType(schema).asInstanceOf[StructType].toAttributes
  }

  override def newInstance() = OrcRelation(path, conf, sqlContext).asInstanceOf[this.type]
}

private[sql] object OrcRelation {
  /**
   * Creates a new OrcRelation and underlying Orcfile for the given LogicalPlan. Note that
   * this is used inside [[org.apache.spark.sql.execution.SparkStrategies]] to
   * create a resolved relation as a data sink for writing to a Orcfile.
   *
   * @param pathString The directory the ORCfile will be stored in.
   * @param child The child node that will be used for extracting the schema.
   * @param conf A configuration to be used.
   * @return An empty OrcRelation with inferred metadata.
   */
  def create(
      pathString: String,
      child: LogicalPlan,
      conf: Configuration,
      sqlContext: SQLContext): OrcRelation = {
    if (!child.resolved) {
      throw new UnresolvedException[LogicalPlan](
        child,
        "Attempt to create Orc table from unresolved child")
    }
    createEmpty(pathString, child.output, false, conf, sqlContext)
  }

  /**
   * Creates an empty OrcRelation and underlying Orcfile that only
   * consists of the Metadata for the given schema.
   *
   * @param pathString The directory the Orcfile will be stored in.
   * @param attributes The schema of the relation.
   * @param conf A configuration to be used.
   * @return An empty OrcRelation.
   */
  def createEmpty(
      pathString: String,
      attributes: Seq[Attribute],
      allowExisting: Boolean,
      conf: Configuration,
      sqlContext: SQLContext): OrcRelation = {
    val path = checkPath(pathString, allowExisting, conf)

    /** TODO: set compression kind in hive 0.13.1
      * conf.set(
      *   HiveConf.ConfVars.OHIVE_ORC_DEFAULT_COMPRESS.varname,
      *   shortOrcCompressionCodecNames.getOrElse(
      *    sqlContext.orcCompressionCodec.toUpperCase, CompressionKind.NONE).name)
      */
    new OrcRelation(path.toString, Some(conf), sqlContext)
  }

  private def checkPath(pathStr: String, allowExisting: Boolean, conf: Configuration): Path = {
    require(pathStr != null, "Unable to create OrcRelation: path is null")
    val origPath = new Path(pathStr)
    val fs = origPath.getFileSystem(conf)
    require(fs != null, s"Unable to create OrcRelation: incorrectly formatted path $pathStr")
    val path = origPath.makeQualified(fs)
    if (!allowExisting) {
      require(!fs.exists(path), s"File $pathStr already exists.")
    }
    if (fs.exists(path)) {
      require(fs.getFileStatus(path).getPermission.getUserAction.implies(FsAction.READ_WRITE),
        s"Unable to create OrcRelation: path $path not read-writable")
    }
    path
  }
}

private[sql] object OrcFileOperator {
  def getMetaDataReader(origPath: Path, configuration: Option[Configuration]): Reader = {
    val conf = configuration.getOrElse(new Configuration())
    val fs: FileSystem = origPath.getFileSystem(conf)
    val orcFiles = FileSystemHelper.listFiles(origPath, conf, ".orc")
    require(orcFiles != Seq.empty, "orcFiles is empty")
    if (fs.exists(origPath)) {
      OrcFile.createReader(fs, orcFiles(0))
    } else {
      throw new IOException(s"File not found: $origPath")
    }
  }


  def writeMetaData(attributes: Seq[Attribute], origPath: Path, conf: Configuration) {
    require(origPath != null, "Unable to write ORC metadata: path is null")
    val fs = origPath.getFileSystem(conf)
    if (fs == null) {
      throw new IllegalArgumentException(
        s"Unable to write Orc metadata: path $origPath is incorrectly formatted")
    }

    val path = origPath.makeQualified(fs)
    if (fs.exists(path) && !fs.getFileStatus(path).isDir) {
      throw new IllegalArgumentException(s"Expected to write to directory $path but found file")
    }
  }
}
