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

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.common.FileUtils
import org.apache.hadoop.hive.ql.plan.FileSinkDesc
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe

import org.apache.spark.SparkException
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.util.SchemaUtils

/**
 * Command for writing the results of `query` to file system.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   INSERT OVERWRITE [LOCAL] DIRECTORY
 *   path
 *   [ROW FORMAT row_format]
 *   [STORED AS file_format]
 *   SELECT ...
 * }}}
 *
 * @param isLocal whether the path specified in `storage` is a local directory
 * @param storage storage format used to describe how the query result is stored.
 * @param query the logical plan representing data to write to
 * @param overwrite whether overwrites existing directory
 */
case class InsertIntoHiveDirCommand(
    isLocal: Boolean,
    storage: CatalogStorageFormat,
    query: LogicalPlan,
    overwrite: Boolean,
    outputColumnNames: Seq[String]) extends SaveAsHiveFile with V1WritesHiveUtils {

  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    assert(storage.locationUri.nonEmpty)
    SchemaUtils.checkColumnNameDuplication(
      outputColumnNames,
      sparkSession.sessionState.conf.caseSensitiveAnalysis)

    val table = CatalogTable(
      identifier = TableIdentifier(storage.locationUri.get.toString, Some("default")),
      provider = Some(DDLUtils.HIVE_PROVIDER),
      tableType = org.apache.spark.sql.catalyst.catalog.CatalogTableType.VIEW,
      storage = storage,
      schema = outputColumns.toStructType
    )
    DDLUtils.checkTableColumns(table)

    val hiveTable = HiveClientImpl.toHiveTable(table)
    hiveTable.getMetadata.put(serdeConstants.SERIALIZATION_LIB,
      storage.serde.getOrElse(classOf[LazySimpleSerDe].getName))

    val tableDesc = new TableDesc(
      hiveTable.getInputFormatClass,
      hiveTable.getOutputFormatClass,
      hiveTable.getMetadata
    )

    val hadoopConf = sparkSession.sessionState.newHadoopConf()

    val targetPath = new Path(storage.locationUri.get)
    val qualifiedPath = FileUtils.makeQualified(targetPath, hadoopConf)
    val (writeToPath: Path, fs: FileSystem) =
      if (isLocal) {
        val localFileSystem = FileSystem.getLocal(hadoopConf)
        (localFileSystem.makeQualified(targetPath), localFileSystem)
      } else {
        val dfs = qualifiedPath.getFileSystem(hadoopConf)
        (qualifiedPath, dfs)
      }
    if (!fs.exists(writeToPath)) {
      fs.mkdirs(writeToPath)
    }

    // The temporary path must be a HDFS path, not a local path.
    val hiveTempPath = new HiveTempPath(sparkSession, hadoopConf, qualifiedPath)
    val tmpPath = hiveTempPath.externalTempPath
    val fileSinkConf = new FileSinkDesc(tmpPath, tableDesc, false)
    setupHadoopConfForCompression(fileSinkConf, hadoopConf, sparkSession)
    hiveTempPath.createTmpPath()

    try {
      saveAsHiveFile(
        sparkSession = sparkSession,
        plan = child,
        hadoopConf = hadoopConf,
        fileFormat = new HiveFileFormat(fileSinkConf),
        outputLocation = tmpPath.toString)

      if (overwrite && fs.exists(writeToPath)) {
        fs.listStatus(writeToPath).foreach { existFile =>
          hiveTempPath.deleteIfNotStagingDir(existFile.getPath, fs)
        }
      }

      val dfs = tmpPath.getFileSystem(hadoopConf)
      dfs.listStatus(tmpPath).foreach {
        tmpFile =>
          if (isLocal) {
            dfs.copyToLocalFile(tmpFile.getPath, writeToPath)
          } else {
            dfs.rename(tmpFile.getPath, writeToPath)
          }
      }
    } catch {
      case e: Throwable =>
        throw new SparkException(
          "Failed inserting overwrite directory " + storage.locationUri.get, e)
    } finally {
      hiveTempPath.deleteTmpPath()
    }

    Seq.empty[Row]
  }

  override protected def withNewChildInternal(
    newChild: LogicalPlan): InsertIntoHiveDirCommand = copy(query = newChild)
}

