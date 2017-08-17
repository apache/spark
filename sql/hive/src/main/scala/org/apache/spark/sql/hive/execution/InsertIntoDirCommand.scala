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

package org.apache.spark.sql.hive.execution

import java.io.File
import java.net.URI
import java.util.Properties

import scala.language.existentials

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe
import org.apache.hadoop.mapred._

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.HiveSerDe
import org.apache.spark.util.{SerializableJobConf, Utils}


case class InsertIntoDirCommand(path: String,
                                isLocal: Boolean,
                                rowStorage: CatalogStorageFormat,
                                fileStorage: CatalogStorageFormat,
                                query: LogicalPlan) extends SaveAsHiveFile {

  override def children: Seq[LogicalPlan] = query :: Nil

  override def run(sparkSession: SparkSession, children: Seq[SparkPlan]): Seq[Row] = {
    assert(children.length == 1)

    val Array(cols, types) = children.head.output.foldLeft(Array("", "")) { case (r, a) =>
      r(0) = r(0) + a.name + ","
      r(1) = r(1) + a.dataType.catalogString + ":"
      r
    }

    val properties = new Properties()
    properties.put("columns", cols.dropRight(1))
    properties.put("columns.types", types.dropRight(1))

    val sqlContext = sparkSession.sqlContext

    val defaultStorage: CatalogStorageFormat = {
      val defaultStorageType =
        sqlContext.conf.getConfString("hive.default.fileformat", "textfile")
      val defaultHiveSerde = HiveSerDe.sourceToSerDe(defaultStorageType)
      CatalogStorageFormat(
        locationUri = None,
        inputFormat = defaultHiveSerde.flatMap(_.inputFormat)
          .orElse(Some("org.apache.hadoop.mapred.TextInputFormat")),
        outputFormat = defaultHiveSerde.flatMap(_.outputFormat)
          .orElse(Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")),
        serde = defaultHiveSerde.flatMap(_.serde),
        compressed = false,
        properties = Map())
    }

    val storage = CatalogStorageFormat(
      locationUri = Some(new URI(path)),
      inputFormat = fileStorage.inputFormat.orElse(defaultStorage.inputFormat),
      outputFormat = fileStorage.outputFormat.orElse(defaultStorage.outputFormat),
      serde = rowStorage.serde.orElse(fileStorage.serde).orElse(defaultStorage.serde),
      compressed = false,
      properties = rowStorage.properties ++ fileStorage.properties)

    properties.put(serdeConstants.SERIALIZATION_LIB,
      storage.serde.getOrElse(classOf[LazySimpleSerDe].getName))

    import scala.collection.JavaConverters._
    properties.putAll(rowStorage.properties.asJava)
    properties.putAll(fileStorage.properties.asJava)

    var tableDesc = new TableDesc(
      Utils.classForName(storage.inputFormat.get).asInstanceOf[Class[_ <: InputFormat[_, _]]],
      Utils.classForName(storage.outputFormat.get),
      properties
    )

    val targetPath = new Path(path)
    val fileSinkConf = new org.apache.spark.sql.hive.HiveShim.ShimFileSinkDesc(
      targetPath.toString, tableDesc, false)

    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    val jobConf = new JobConf(hadoopConf)
    jobConf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    val jobConfSer = new SerializableJobConf(jobConf)

    FileSystem.get(jobConf).delete(targetPath, true)

    saveAsHiveFile(
      sparkSession = sparkSession,
      plan = children.head,
      fileSinkConf = fileSinkConf,
      outputLocation = path)

//    val outputPath = FileOutputFormat.getOutputPath(jobConf)
//    if( isLocal ) {
//      Utils.deleteRecursively(new File(path))
//      outputPath.getFileSystem(hadoopConf).copyToLocalFile(true, outputPath, targetPath)
//      log.info(s"Copied results from ${outputPath} to local dir ${path}")
//    } else {
//      log.info(s"Results available at path ${outputPath}")
//    }

    Seq.empty[Row]
  }
}

