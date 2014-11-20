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

import java.util.{Locale, Properties}
import scala.collection.JavaConversions._

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils
import org.apache.hadoop.io.{NullWritable, Writable}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileStatus, FileSystem}
import org.apache.hadoop.hive.ql.io.orc.Reader
import org.apache.hadoop.hive.ql.io.orc.{OrcFile, OrcInputFormat, OrcSerde}
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector

import org.apache.spark.sql.sources.{CatalystScan, BaseRelation, RelationProvider}
import org.apache.spark.sql.SQLContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.expressions.{SpecificMutableRow, Attribute, Expression, Row}
import org.apache.spark.sql.hive.{HadoopTableReader, HiveMetastoreTypes}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.parquet.FileSystemHelper

/**
 * Allows creation of orc based tables using the syntax
 * `CREATE TEMPORARY TABLE ... USING org.apache.spark.sql.orc`.
 * Currently the only option required is `path`, which should be the location of a collection of,
 * optionally partitioned, parquet files.
 */
class DefaultSource extends RelationProvider {
  /** Returns a new base relation with the given parameters. */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val path =
      parameters.getOrElse("path", sys.error("'path' must be specified for orc tables."))

    OrcRelation2(path)(sqlContext)
  }
}
private[orc] case class Partition(partitionValues: Map[String, Any], files: Seq[FileStatus])

/**
 * Partitioning: Partitions are auto discovered and must be in the form of directories `key=value/`
 * located at `path`.  Currently only a single partitioning column is supported and it must
 * be an integer.  This class supports both fully self-describing data, which contains the partition
 * key, and data where the partition key is only present in the folder structure.  The presence
 * of the partitioning key in the data is also auto-detected.  The `null` partition is not yet
 * supported.
 *
 * Metadata: The metadata is automatically discovered by reading the first orc file present.
 * There is currently no support for working with files that have different schema.  Additionally,
 * when parquet metadata caching is turned on, the FileStatus objects for all data will be cached
 * to improve the speed of interactive querying.  When data is added to a table it must be dropped
 * and recreated to pick up any changes.
 *
 * Statistics: Statistics for the size of the table are automatically populated during metadata
 * discovery.
 */
@DeveloperApi
case class OrcRelation2(path: String)(@transient val sqlContext: SQLContext)
  extends CatalystScan with Logging {

  // used for what?
  val prop: Properties = new Properties

  @transient
  lazy val serde: OrcSerde = initSerde

  private def initSerde(): OrcSerde = {
    val serde = new OrcSerde
    serde.initialize(null, prop)
    serde
  }

  def sparkContext = sqlContext.sparkContext

  // Minor Hack: scala doesnt seem to respect @transient for vals declared via extraction
  @transient
  private var partitionKeys: Seq[String] = _
  @transient
  private var partitions: Seq[Partition] = _
  discoverPartitions()

  // TODO: Only finds the first partition, assumes the key is of type Integer...
  // Why key type is Int?
  private def discoverPartitions() = {
    val fs = FileSystem.get(new java.net.URI(path), sparkContext.hadoopConfiguration)
    val partValue = "([^=]+)=([^=]+)".r

    val childrenOfPath = fs.listStatus(new Path(path))
      .filterNot(_.getPath.getName.startsWith("_"))
      .filterNot(_.getPath.getName.startsWith("."))

    val childDirs = childrenOfPath.filter(s => s.isDir)

    if (childDirs.size > 0) {
      val partitionPairs = childDirs.map(_.getPath.getName).map {
        case partValue(key, value) => (key, value)
      }

      val foundKeys = partitionPairs.map(_._1).distinct
      if (foundKeys.size > 1) {
        sys.error(s"Too many distinct partition keys: $foundKeys")
      }

      // Do a parallel lookup of partition metadata.
      val partitionFiles =
        childDirs.par.map { d =>
          fs.listStatus(d.getPath)
            // TODO: Is there a standard hadoop function for this?
            .filterNot(_.getPath.getName.startsWith("_"))
            .filterNot(_.getPath.getName.startsWith("."))
        }.seq

      partitionKeys = foundKeys.toSeq
      partitions = partitionFiles.zip(partitionPairs).map { case (files, (key, value)) =>
        Partition(Map(key -> value.toInt), files)
      }.toSeq
    } else {
      partitionKeys = Nil
      partitions = Partition(Map.empty, childrenOfPath) :: Nil
    }
  }

  override val sizeInBytes = partitions.flatMap(_.files).map(_.getLen).sum

  private def getMetaDataReader(origPath: Path, configuration: Option[Configuration]): Reader = {
    val conf = configuration.getOrElse(new Configuration())
    val fs: FileSystem = origPath.getFileSystem(conf)
    val orcFiles = FileSystemHelper.listFiles(origPath, conf, ".orc")
    if (orcFiles == Seq.empty) {
      // should return null when write to orc file
      return null
    }
    OrcFile.createReader(fs, orcFiles(0))
  }

  private def orcSchema(
      path: Path,
      conf: Option[Configuration],
      prop: Properties): StructType = {
    // get the schema info through ORC Reader
    val reader = getMetaDataReader(path, conf)
    if (reader == null) {
      // return empty seq when saveAsOrcFile
      return StructType(Seq.empty)
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

    HiveMetastoreTypes.toDataType(schema).asInstanceOf[StructType]
  }

  val dataSchema = orcSchema(
      partitions.head.files.head.getPath,
      Some(sparkContext.hadoopConfiguration),
      prop)

  val dataIncludesKey =
    partitionKeys.headOption.map(dataSchema.fieldNames.contains(_)).getOrElse(true)

  override val schema =
    if (dataIncludesKey) {
      dataSchema
    } else {
      StructType(dataSchema.fields :+ StructField(partitionKeys.head, IntegerType))
    }

  override def buildScan(output: Seq[Attribute], predicates: Seq[Expression]): RDD[Row] = {
    val sc = sparkContext
    val job = new Job(sc.hadoopConfiguration)
    val conf: Configuration = job.getConfiguration
    // todo: predicates push down
    path.split(",").foreach { curPath =>
      val qualifiedPath = {
        val path = new Path(curPath)
        path.getFileSystem(conf).makeQualified(path)
      }
      FileInputFormat.addInputPath(job, qualifiedPath)
    }

    addColumnIds(output, schema.toAttributes, conf)
    val inputClass = classOf[OrcInputFormat].asInstanceOf[
      Class[_ <: org.apache.hadoop.mapred.InputFormat[NullWritable, Writable]]]

    // use SpecificMutableRow to decrease GC garbage
    val mutableRow = new SpecificMutableRow(output.map(_.dataType))
    val attrsWithIndex = output.zipWithIndex
    val rowRdd = sc.hadoopRDD[NullWritable, Writable](conf.asInstanceOf[JobConf], inputClass,
      classOf[NullWritable], classOf[Writable]).map(_._2).mapPartitions { iter =>
      val deserializer = serde
      HadoopTableReader.fillObject(iter, deserializer, attrsWithIndex, mutableRow)
    }
    rowRdd
  }

  /**
   * add column ids and names
   * @param output
   * @param relationOutput
   * @param conf
   */
  def addColumnIds(output: Seq[Attribute], relationOutput: Seq[Attribute], conf: Configuration) {
    val names = output.map(_.name)
    val fieldIdMap = relationOutput.map(_.name).zipWithIndex.toMap
    val ids = output.map { att =>
      val realName = att.name.toLowerCase(Locale.ENGLISH)
      fieldIdMap.getOrElse(realName, -1)
    }.filter(_ >= 0).map(_.asInstanceOf[Integer])

    assert(ids.size == names.size, "columns id and name length does not match!")
    if (ids != null && !ids.isEmpty) {
      ColumnProjectionUtils.appendReadColumns(conf, ids, names)
    }
  }
}
