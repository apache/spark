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
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.hive.ql.io.orc._

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, LeafNode}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedException, MultiInstanceRelation}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.hive.HiveMetastoreTypes
import org.apache.spark.sql.parquet.FileSystemHelper
import org.apache.spark.sql.SQLContext

import scala.collection.JavaConversions._

private[sql] case class OrcRelation(
    attributes: Seq[Attribute],
    path: String,
    @transient conf: Option[Configuration],
    @transient sqlContext: SQLContext,
    partitioningAttributes: Seq[Attribute] = Nil)
  extends LeafNode with MultiInstanceRelation {
  self: Product =>

  val prop: Properties = new Properties

  override lazy val output = attributes ++ OrcFileOperator.orcSchema(path, conf, prop)

  // TODO: use statistics in ORC file
  override lazy val statistics = Statistics(sizeInBytes = sqlContext.defaultSizeInBytes)

  override def newInstance() =
    OrcRelation(attributes, path, conf, sqlContext).asInstanceOf[this.type]
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
    val path = checkPath(pathString, false, conf)
    new OrcRelation(child.output, path.toString, Some(conf), sqlContext)
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

private[sql] object OrcFileOperator{
  def getMetaDataReader(origPath: Path, configuration: Option[Configuration]): Reader = {
    val conf = configuration.getOrElse(new Configuration())
    val fs: FileSystem = origPath.getFileSystem(conf)
    val orcFiles = FileSystemHelper.listFiles(origPath, conf, ".orc")
    if (orcFiles == Seq.empty) {
      // should return null when write to orc file
      return null
    }
    OrcFile.createReader(fs, orcFiles(0))
  }

  def orcSchema(
      path: String,
      conf: Option[Configuration],
      prop: Properties): Seq[Attribute] = {
    // get the schema info through ORC Reader
    val origPath = new Path(path)
    val reader = getMetaDataReader(origPath, conf)
    if (reader == null) {
      // return empty seq when saveAsOrcFile
      return Seq.empty
    }
    val inspector = reader.getObjectInspector.asInstanceOf[StructObjectInspector]
    // data types that is inspected by this inspector
    val schema = inspector.getTypeName
    // set prop here, initial OrcSerde need it
    val fields = inspector.getAllStructFieldRefs
    val (columns, columnTypes) = fields.map { f =>
      f.getFieldName -> f.getFieldObjectInspector.getTypeName
    }.unzip
    prop.setProperty("columns", columns.mkString(","))
    prop.setProperty("columns.types", columnTypes.mkString(":"))

    HiveMetastoreTypes.toDataType(schema).asInstanceOf[StructType].toAttributes
  }
}
