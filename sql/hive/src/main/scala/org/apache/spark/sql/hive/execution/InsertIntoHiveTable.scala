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

import java.io.{File, IOException}
import java.net.URI
import java.text.SimpleDateFormat
import java.util.{Date, Locale, Random}

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.common.FileUtils
import org.apache.hadoop.hive.ql.exec.TaskRunner
import org.apache.hadoop.hive.ql.ErrorMsg
import org.apache.hadoop.hive.ql.plan.TableDesc

import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.{AnalysisException, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.FileFormatWriter
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.HiveShim.{ShimFileSinkDesc => FileSinkDesc}
import org.apache.spark.sql.hive.client.{HiveClientImpl, HiveVersion}
import org.apache.spark.SparkException


/**
 * Command for writing data out to a Hive table.
 *
 * This class is mostly a mess, for legacy reasons (since it evolved in organic ways and had to
 * follow Hive's internal implementations closely, which itself was a mess too). Please don't
 * blame Reynold for this! He was just moving code around!
 *
 * In the future we should converge the write path for Hive with the normal data source write path,
 * as defined in `org.apache.spark.sql.execution.datasources.FileFormatWriter`.
 *
 * @param table the metadata of the table.
 * @param partition a map from the partition key to the partition value (optional). If the partition
 *                  value is optional, dynamic partition insert will be performed.
 *                  As an example, `INSERT INTO tbl PARTITION (a=1, b=2) AS ...` would have
 *
 *                  {{{
 *                  Map('a' -> Some('1'), 'b' -> Some('2'))
 *                  }}}
 *
 *                  and `INSERT INTO tbl PARTITION (a=1, b) AS ...`
 *                  would have
 *
 *                  {{{
 *                  Map('a' -> Some('1'), 'b' -> None)
 *                  }}}.
 * @param query the logical plan representing data to write to.
 * @param overwrite overwrite existing table or partitions.
 * @param ifPartitionNotExists If true, only write if the partition does not exist.
 *                                   Only valid for static partitions.
 */
case class InsertIntoHiveTable(
    table: CatalogTable,
    partition: Map[String, Option[String]],
    query: LogicalPlan,
    overwrite: Boolean,
    ifPartitionNotExists: Boolean) extends RunnableCommand {

  override protected def innerChildren: Seq[LogicalPlan] = query :: Nil

  var createdTempDir: Option[Path] = None

  private def executionId: String = {
    val rand: Random = new Random
    val format = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss_SSS", Locale.US)
    "hive_" + format.format(new Date) + "_" + Math.abs(rand.nextLong)
  }

  private def getStagingDir(
      inputPath: Path,
      hadoopConf: Configuration,
      stagingDir: String): Path = {
    val inputPathUri: URI = inputPath.toUri
    val inputPathName: String = inputPathUri.getPath
    val fs: FileSystem = inputPath.getFileSystem(hadoopConf)
    var stagingPathName: String =
      if (inputPathName.indexOf(stagingDir) == -1) {
        new Path(inputPathName, stagingDir).toString
      } else {
        inputPathName.substring(0, inputPathName.indexOf(stagingDir) + stagingDir.length)
      }

    // SPARK-20594: This is a walk-around fix to resolve a Hive bug. Hive requires that the
    // staging directory needs to avoid being deleted when users set hive.exec.stagingdir
    // under the table directory.
    if (FileUtils.isSubDir(new Path(stagingPathName), inputPath, fs) &&
      !stagingPathName.stripPrefix(inputPathName).stripPrefix(File.separator).startsWith(".")) {
      logDebug(s"The staging dir '$stagingPathName' should be a child directory starts " +
        "with '.' to avoid being deleted if we set hive.exec.stagingdir under the table " +
        "directory.")
      stagingPathName = new Path(inputPathName, ".hive-staging").toString
    }

    val dir: Path =
      fs.makeQualified(
        new Path(stagingPathName + "_" + executionId + "-" + TaskRunner.getTaskRunnerID))
    logDebug("Created staging dir = " + dir + " for path = " + inputPath)
    try {
      if (!FileUtils.mkdir(fs, dir, true, hadoopConf)) {
        throw new IllegalStateException("Cannot create staging directory  '" + dir.toString + "'")
      }
      createdTempDir = Some(dir)
      fs.deleteOnExit(dir)
    } catch {
      case e: IOException =>
        throw new RuntimeException(
          "Cannot create staging directory '" + dir.toString + "': " + e.getMessage, e)
    }
    dir
  }

  private def getExternalScratchDir(
      extURI: URI,
      hadoopConf: Configuration,
      stagingDir: String): Path = {
    getStagingDir(
      new Path(extURI.getScheme, extURI.getAuthority, extURI.getPath),
      hadoopConf,
      stagingDir)
  }

  def getExternalTmpPath(
      path: Path,
      hiveVersion: HiveVersion,
      hadoopConf: Configuration,
      stagingDir: String,
      scratchDir: String): Path = {
    import org.apache.spark.sql.hive.client.hive._

    // Before Hive 1.1, when inserting into a table, Hive will create the staging directory under
    // a common scratch directory. After the writing is finished, Hive will simply empty the table
    // directory and move the staging directory to it.
    // After Hive 1.1, Hive will create the staging directory under the table directory, and when
    // moving staging directory to table directory, Hive will still empty the table directory, but
    // will exclude the staging directory there.
    // We have to follow the Hive behavior here, to avoid troubles. For example, if we create
    // staging directory under the table director for Hive prior to 1.1, the staging directory will
    // be removed by Hive when Hive is trying to empty the table directory.
    val hiveVersionsUsingOldExternalTempPath: Set[HiveVersion] = Set(v12, v13, v14, v1_0)
    val hiveVersionsUsingNewExternalTempPath: Set[HiveVersion] = Set(v1_1, v1_2, v2_0, v2_1)

    // Ensure all the supported versions are considered here.
    assert(hiveVersionsUsingNewExternalTempPath ++ hiveVersionsUsingOldExternalTempPath ==
      allSupportedHiveVersions)

    if (hiveVersionsUsingOldExternalTempPath.contains(hiveVersion)) {
      oldVersionExternalTempPath(path, hadoopConf, scratchDir)
    } else if (hiveVersionsUsingNewExternalTempPath.contains(hiveVersion)) {
      newVersionExternalTempPath(path, hadoopConf, stagingDir)
    } else {
      throw new IllegalStateException("Unsupported hive version: " + hiveVersion.fullVersion)
    }
  }

  // Mostly copied from Context.java#getExternalTmpPath of Hive 0.13
  def oldVersionExternalTempPath(
      path: Path,
      hadoopConf: Configuration,
      scratchDir: String): Path = {
    val extURI: URI = path.toUri
    val scratchPath = new Path(scratchDir, executionId)
    var dirPath = new Path(
      extURI.getScheme,
      extURI.getAuthority,
      scratchPath.toUri.getPath + "-" + TaskRunner.getTaskRunnerID())

    try {
      val fs: FileSystem = dirPath.getFileSystem(hadoopConf)
      dirPath = new Path(fs.makeQualified(dirPath).toString())

      if (!FileUtils.mkdir(fs, dirPath, true, hadoopConf)) {
        throw new IllegalStateException("Cannot create staging directory: " + dirPath.toString)
      }
      createdTempDir = Some(dirPath)
      fs.deleteOnExit(dirPath)
    } catch {
      case e: IOException =>
        throw new RuntimeException("Cannot create staging directory: " + dirPath.toString, e)
    }
    dirPath
  }

  // Mostly copied from Context.java#getExternalTmpPath of Hive 1.2
  def newVersionExternalTempPath(
      path: Path,
      hadoopConf: Configuration,
      stagingDir: String): Path = {
    val extURI: URI = path.toUri
    if (extURI.getScheme == "viewfs") {
      getExtTmpPathRelTo(path.getParent, hadoopConf, stagingDir)
    } else {
      new Path(getExternalScratchDir(extURI, hadoopConf, stagingDir), "-ext-10000")
    }
  }

  def getExtTmpPathRelTo(
      path: Path,
      hadoopConf: Configuration,
      stagingDir: String): Path = {
    new Path(getStagingDir(path, hadoopConf, stagingDir), "-ext-10000") // Hive uses 10000
  }

  /**
   * Inserts all the rows in the table into Hive.  Row objects are properly serialized with the
   * `org.apache.hadoop.hive.serde2.SerDe` and the
   * `org.apache.hadoop.mapred.OutputFormat` provided by the table definition.
   */
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sessionState = sparkSession.sessionState
    val externalCatalog = sparkSession.sharedState.externalCatalog
    val hiveVersion = externalCatalog.asInstanceOf[HiveExternalCatalog].client.version
    val hadoopConf = sessionState.newHadoopConf()
    val stagingDir = hadoopConf.get("hive.exec.stagingdir", ".hive-staging")
    val scratchDir = hadoopConf.get("hive.exec.scratchdir", "/tmp/hive")

    val hiveQlTable = HiveClientImpl.toHiveTable(table)
    // Have to pass the TableDesc object to RDD.mapPartitions and then instantiate new serializer
    // instances within the closure, since Serializer is not serializable while TableDesc is.
    val tableDesc = new TableDesc(
      hiveQlTable.getInputFormatClass,
      // The class of table should be org.apache.hadoop.hive.ql.metadata.Table because
      // getOutputFormatClass will use HiveFileFormatUtils.getOutputFormatSubstitute to
      // substitute some output formats, e.g. substituting SequenceFileOutputFormat to
      // HiveSequenceFileOutputFormat.
      hiveQlTable.getOutputFormatClass,
      hiveQlTable.getMetadata
    )
    val tableLocation = hiveQlTable.getDataLocation
    val tmpLocation =
      getExternalTmpPath(tableLocation, hiveVersion, hadoopConf, stagingDir, scratchDir)
    val fileSinkConf = new FileSinkDesc(tmpLocation.toString, tableDesc, false)
    val isCompressed = hadoopConf.get("hive.exec.compress.output", "false").toBoolean

    if (isCompressed) {
      // Please note that isCompressed, "mapreduce.output.fileoutputformat.compress",
      // "mapreduce.output.fileoutputformat.compress.codec", and
      // "mapreduce.output.fileoutputformat.compress.type"
      // have no impact on ORC because it uses table properties to store compression information.
      hadoopConf.set("mapreduce.output.fileoutputformat.compress", "true")
      fileSinkConf.setCompressed(true)
      fileSinkConf.setCompressCodec(hadoopConf
        .get("mapreduce.output.fileoutputformat.compress.codec"))
      fileSinkConf.setCompressType(hadoopConf
        .get("mapreduce.output.fileoutputformat.compress.type"))
    }

    val numDynamicPartitions = partition.values.count(_.isEmpty)
    val numStaticPartitions = partition.values.count(_.nonEmpty)
    val partitionSpec = partition.map {
      case (key, Some(value)) => key -> value
      case (key, None) => key -> ""
    }

    // All partition column names in the format of "<column name 1>/<column name 2>/..."
    val partitionColumns = fileSinkConf.getTableInfo.getProperties.getProperty("partition_columns")
    val partitionColumnNames = Option(partitionColumns).map(_.split("/")).getOrElse(Array.empty)

    // By this time, the partition map must match the table's partition columns
    if (partitionColumnNames.toSet != partition.keySet) {
      throw new SparkException(
        s"""Requested partitioning does not match the ${table.identifier.table} table:
           |Requested partitions: ${partition.keys.mkString(",")}
           |Table partitions: ${table.partitionColumnNames.mkString(",")}""".stripMargin)
    }

    // Validate partition spec if there exist any dynamic partitions
    if (numDynamicPartitions > 0) {
      // Report error if dynamic partitioning is not enabled
      if (!hadoopConf.get("hive.exec.dynamic.partition", "true").toBoolean) {
        throw new SparkException(ErrorMsg.DYNAMIC_PARTITION_DISABLED.getMsg)
      }

      // Report error if dynamic partition strict mode is on but no static partition is found
      if (numStaticPartitions == 0 &&
        hadoopConf.get("hive.exec.dynamic.partition.mode", "strict").equalsIgnoreCase("strict")) {
        throw new SparkException(ErrorMsg.DYNAMIC_PARTITION_STRICT_MODE.getMsg)
      }

      // Report error if any static partition appears after a dynamic partition
      val isDynamic = partitionColumnNames.map(partitionSpec(_).isEmpty)
      if (isDynamic.init.zip(isDynamic.tail).contains((true, false))) {
        throw new AnalysisException(ErrorMsg.PARTITION_DYN_STA_ORDER.getMsg)
      }
    }

    val committer = FileCommitProtocol.instantiate(
      sparkSession.sessionState.conf.fileCommitProtocolClass,
      jobId = java.util.UUID.randomUUID().toString,
      outputPath = tmpLocation.toString,
      isAppend = false)

    val partitionAttributes = partitionColumnNames.takeRight(numDynamicPartitions).map { name =>
      query.resolve(name :: Nil, sparkSession.sessionState.analyzer.resolver).getOrElse {
        throw new AnalysisException(
          s"Unable to resolve $name given [${query.output.map(_.name).mkString(", ")}]")
      }.asInstanceOf[Attribute]
    }

    FileFormatWriter.write(
      sparkSession = sparkSession,
      queryExecution = Dataset.ofRows(sparkSession, query).queryExecution,
      fileFormat = new HiveFileFormat(fileSinkConf),
      committer = committer,
      outputSpec = FileFormatWriter.OutputSpec(tmpLocation.toString, Map.empty),
      hadoopConf = hadoopConf,
      partitionColumns = partitionAttributes,
      bucketSpec = None,
      refreshFunction = _ => (),
      options = Map.empty)

    if (partition.nonEmpty) {
      if (numDynamicPartitions > 0) {
        externalCatalog.loadDynamicPartitions(
          db = table.database,
          table = table.identifier.table,
          tmpLocation.toString,
          partitionSpec,
          overwrite,
          numDynamicPartitions)
      } else {
        // scalastyle:off
        // ifNotExists is only valid with static partition, refer to
        // https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML#LanguageManualDML-InsertingdataintoHiveTablesfromqueries
        // scalastyle:on
        val oldPart =
          externalCatalog.getPartitionOption(
            table.database,
            table.identifier.table,
            partitionSpec)

        var doHiveOverwrite = overwrite

        if (oldPart.isEmpty || !ifPartitionNotExists) {
          // SPARK-18107: Insert overwrite runs much slower than hive-client.
          // Newer Hive largely improves insert overwrite performance. As Spark uses older Hive
          // version and we may not want to catch up new Hive version every time. We delete the
          // Hive partition first and then load data file into the Hive partition.
          if (oldPart.nonEmpty && overwrite) {
            oldPart.get.storage.locationUri.foreach { uri =>
              val partitionPath = new Path(uri)
              val fs = partitionPath.getFileSystem(hadoopConf)
              if (fs.exists(partitionPath)) {
                if (!fs.delete(partitionPath, true)) {
                  throw new RuntimeException(
                    "Cannot remove partition directory '" + partitionPath.toString)
                }
                // Don't let Hive do overwrite operation since it is slower.
                doHiveOverwrite = false
              }
            }
          }

          // inheritTableSpecs is set to true. It should be set to false for an IMPORT query
          // which is currently considered as a Hive native command.
          val inheritTableSpecs = true
          externalCatalog.loadPartition(
            table.database,
            table.identifier.table,
            tmpLocation.toString,
            partitionSpec,
            isOverwrite = doHiveOverwrite,
            inheritTableSpecs = inheritTableSpecs,
            isSrcLocal = false)
        }
      }
    } else {
      externalCatalog.loadTable(
        table.database,
        table.identifier.table,
        tmpLocation.toString, // TODO: URI
        overwrite,
        isSrcLocal = false)
    }

    // Attempt to delete the staging directory and the inclusive files. If failed, the files are
    // expected to be dropped at the normal termination of VM since deleteOnExit is used.
    try {
      createdTempDir.foreach { path =>
        val fs = path.getFileSystem(hadoopConf)
        if (fs.delete(path, true)) {
          // If we successfully delete the staging directory, remove it from FileSystem's cache.
          fs.cancelDeleteOnExit(path)
        }
      }
    } catch {
      case NonFatal(e) =>
        logWarning(s"Unable to delete staging directory: $stagingDir.\n" + e)
    }

    // un-cache this table.
    sparkSession.catalog.uncacheTable(table.identifier.quotedString)
    sparkSession.sessionState.catalog.refreshTable(table.identifier)

    // It would be nice to just return the childRdd unchanged so insert operations could be chained,
    // however for now we return an empty list to simplify compatibility checks with hive, which
    // does not return anything for insert operations.
    // TODO: implement hive compatibility as rules.
    Seq.empty[Row]
  }
}
