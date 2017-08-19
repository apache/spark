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

import java.util.Properties

import scala.language.existentials

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.common.FileUtils
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe
import org.apache.hadoop.mapred._

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.util.Utils


case class InsertIntoDirCommand(path: String,
                                isLocal: Boolean,
                                storage: CatalogStorageFormat,
                                query: LogicalPlan) extends SaveAsHiveFile {

  override def children: Seq[LogicalPlan] = query :: Nil

  override def run(sparkSession: SparkSession, children: Seq[SparkPlan]): Seq[Row] = {
    assert(children.length == 1)
    assert(!storage.locationUri.isEmpty)

    val Array(cols, types) = children.head.output.foldLeft(Array("", "")) { case (r, a) =>
      r(0) = r(0) + a.name + ","
      r(1) = r(1) + a.dataType.catalogString + ":"
      r
    }

    val properties = new Properties()
    properties.put("columns", cols.dropRight(1))
    properties.put("columns.types", types.dropRight(1))

    val sqlContext = sparkSession.sqlContext

    properties.put(serdeConstants.SERIALIZATION_LIB,
      storage.serde.getOrElse(classOf[LazySimpleSerDe].getName))

    import scala.collection.JavaConverters._
    properties.putAll(storage.properties.asJava)

    var tableDesc = new TableDesc(
      Utils.classForName(storage.inputFormat.get).asInstanceOf[Class[_ <: InputFormat[_, _]]],
      Utils.classForName(storage.outputFormat.get),
      properties
    )

    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    val jobConf = new JobConf(hadoopConf)

    val targetPath = new Path(storage.locationUri.get)
    val writeToPath =
      if (isLocal) {
        val localFileSystem = FileSystem.getLocal(jobConf)
        val localPath = localFileSystem.makeQualified(targetPath)
        if (localFileSystem.exists(localPath)) {
          localFileSystem.delete(localPath, true)
        }
        localPath
      } else {
        val qualifiedPath = FileUtils.makeQualified(targetPath, hadoopConf)
        val dfs = qualifiedPath.getFileSystem(jobConf)
        if (dfs.exists(qualifiedPath)) {
          dfs.delete(qualifiedPath, true)
        } else {
          dfs.mkdirs(qualifiedPath.getParent)
        }
        qualifiedPath
      }

    val fileSinkConf = new org.apache.spark.sql.hive.HiveShim.ShimFileSinkDesc(
      writeToPath.toString, tableDesc, false)

    saveAsHiveFile(
      sparkSession = sparkSession,
      plan = children.head,
      hadoopConf = hadoopConf,
      fileSinkConf = fileSinkConf,
      outputLocation = path)

    Seq.empty[Row]
  }
}

