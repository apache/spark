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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.orc.{OrcFile, Reader}
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector

import org.apache.spark.Logging
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.hive.HiveMetastoreTypes
import org.apache.spark.sql.types.StructType

private[orc] object OrcFileOperator extends Logging{
  def getFileReader(pathStr: String, config: Option[Configuration] = None ): Reader = {
    val conf = config.getOrElse(new Configuration)
    val fspath = new Path(pathStr)
    val fs = fspath.getFileSystem(conf)
    val orcFiles = listOrcFiles(pathStr, conf)

    // TODO Need to consider all files when schema evolution is taken into account.
    OrcFile.createReader(fs, orcFiles.head)
  }

  def readSchema(path: String, conf: Option[Configuration]): StructType = {
    val reader = getFileReader(path, conf)
    val readerInspector = reader.getObjectInspector.asInstanceOf[StructObjectInspector]
    val schema = readerInspector.getTypeName
    HiveMetastoreTypes.toDataType(schema).asInstanceOf[StructType]
  }

  def getObjectInspector(path: String, conf: Option[Configuration]): StructObjectInspector = {
    getFileReader(path, conf).getObjectInspector.asInstanceOf[StructObjectInspector]
  }

  def listOrcFiles(pathStr: String, conf: Configuration): Seq[Path] = {
    val origPath = new Path(pathStr)
    val fs = origPath.getFileSystem(conf)
    val path = origPath.makeQualified(fs)
    val paths = SparkHadoopUtil.get.listLeafStatuses(fs, origPath)
      .filterNot(_.isDir)
      .map(_.getPath)
      .filterNot(_.getName.startsWith("_"))
      .filterNot(_.getName.startsWith("."))

    if (paths == null || paths.size == 0) {
      throw new IllegalArgumentException(
        s"orcFileOperator: path $path does not have valid orc files matching the pattern")
    }

    paths
  }
}
