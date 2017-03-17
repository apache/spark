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

import java.io.IOException
import java.net.URI
import java.text.SimpleDateFormat
import java.util.{Date, Locale, Random}

import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.common.FileUtils
import org.apache.hadoop.hive.ql.exec.TaskRunner
import org.apache.hadoop.hive.ql.ErrorMsg
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.HiveShim.{ShimFileSinkDesc => FileSinkDesc}
import org.apache.spark.SparkException
import org.apache.spark.util.SerializableJobConf


/**
 * Command for writing data out to a Hive table.
 *
 * This class is mostly a mess, for legacy reasons (since it evolved in organic ways and had to
 * follow Hive's internal implementations closely, which itself was a mess too). Please don't
 * blame Reynold for this! He was just moving code around!
 *
 * In the future we should converge the write path for Hive with the normal data source write path,
 * as defined in [[org.apache.spark.sql.execution.datasources.FileFormatWriter]].
 *
 * @param table the logical plan representing the table. In the future this should be a
 *              [[org.apache.spark.sql.catalyst.catalog.CatalogTable]] once we converge Hive tables
 *              and data source tables.
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
 * @param child the logical plan representing data to write to.
 * @param overwrite overwrite existing table or partitions.
 * @param ifNotExists If true, only write if the table or partition does not exist.
 */
case class InsertIntoHiveTable(
    table: MetastoreRelation,
    partition: Map[String, Option[String]],
    child: SparkPlan,
    overwrite: Boolean,
    ifNotExists: Boolean) extends UnaryExecNode {

  @transient private val sessionState = sqlContext.sessionState.asInstanceOf[HiveSessionState]
  @transient private val externalCatalog = sqlContext.sharedState.externalCatalog

  def output: Seq[Attribute] = Seq.empty

  val hadoopConf = sessionState.newHadoopConf()
  var createdTempDir: Option[Path] = None
  val stagingDir = hadoopConf.get("hive.exec.stagingdir", ".hive-staging")
  val scratchDir = hadoopConf.get("hive.exec.scratchdir", "/tmp/hive")

  private def executionId: String = {
    val rand: Random = new Random
    val format = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss_SSS", Locale.US)
    "hive_" + format.format(new Date) + "_" + Math.abs(rand.nextLong)
  }

  private def getStagingDir(inputPath: Path): Path = {
    val inputPathUri: URI = inputPath.toUri
    val inputPathName: String = inputPathUri.getPath
    val fs: FileSystem = inputPath.getFileSystem(hadoopConf)
    val stagingPathName: String =
      if (inputPathName.indexOf(stagingDir) == -1) {
        new Path(inputPathName, stagingDir).toString
      } else {
        inputPathName.substring(0, inputPathName.indexOf(stagingDir) + stagingDir.length)
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
    return dir
  }

  private def getExternalScratchDir(extURI: URI): Path = {
    getStagingDir(new Path(extURI.getScheme, extURI.getAuthority, extURI.getPath))
  }

  def getExternalTmpPath(path: Path): Path = {
    import org.apache.spark.sql.hive.client.hive._

    val hiveVersion = externalCatalog.asInstanceOf[HiveExternalCatalog].client.version
    // Before Hive 1.1, when inserting into a table, Hive will create the staging directory under
    // a common scratch directory. After the writing is finished, Hive will simply empty the table
    // directory and move the staging directory to it.
    // After Hive 1.1, Hive will create the staging directory under the table directory, and when
    // moving staging directory to table directory, Hive will still empty the table directory, but
    // will exclude the staging directory there.
    // We have to follow the Hive behavior here, to avoid troubles. For example, if we create
    // staging directory under the table director for Hive prior to 1.1, the staging directory will
    // be removed by Hive when Hive is trying to empty the table directory.
    if (hiveVersion == v12 || hiveVersion == v13 || hiveVersion == v14 || hiveVersion == v1_0) {
      oldVersionExternalTempPath(path)
    } else if (hiveVersion == v1_1 || hiveVersion == v1_2) {
      newVersionExternalTempPath(path)
    } else {
      throw new IllegalStateException("Unsupported hive version: " + hiveVersion.fullVersion)
    }
  }

  // Mostly copied from Context.java#getExternalTmpPath of Hive 0.13
  def oldVersionExternalTempPath(path: Path): Path = {
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
  def newVersionExternalTempPath(path: Path): Path = {
    val extURI: URI = path.toUri
    if (extURI.getScheme == "viewfs") {
      getExtTmpPathRelTo(path.getParent)
    } else {
      new Path(getExternalScratchDir(extURI), "-ext-10000")
    }
  }

  def getExtTmpPathRelTo(path: Path): Path = {
    new Path(getStagingDir(path), "-ext-10000") // Hive uses 10000
  }

  private def saveAsHiveFile(
      rdd: RDD[InternalRow],
      valueClass: Class[_],
      fileSinkConf: FileSinkDesc,
      conf: SerializableJobConf,
      writerContainer: SparkHiveWriterContainer): Unit = {
    assert(valueClass != null, "Output value class not set")
    conf.value.setOutputValueClass(valueClass)

    val outputFileFormatClassName = fileSinkConf.getTableInfo.getOutputFileFormatClassName
    assert(outputFileFormatClassName != null, "Output format class not set")
    conf.value.set("mapred.output.format.class", outputFileFormatClassName)

    FileOutputFormat.setOutputPath(
      conf.value,
      SparkHiveWriterContainer.createPathFromString(fileSinkConf.getDirName(), conf.value))
    log.debug("Saving as hadoop file of type " + valueClass.getSimpleName)
    writerContainer.driverSideSetup()
    sqlContext.sparkContext.runJob(rdd, writerContainer.writeToFile _)
    writerContainer.commitJob()
  }

  /**
   * Inserts all the rows in the table into Hive.  Row objects are properly serialized with the
   * `org.apache.hadoop.hive.serde2.SerDe` and the
   * `org.apache.hadoop.mapred.OutputFormat` provided by the table definition.
   *
   * Note: this is run once and then kept to avoid double insertions.
   */
  protected[sql] lazy val sideEffectResult: Seq[InternalRow] = {
    // Have to pass the TableDesc object to RDD.mapPartitions and then instantiate new serializer
    // instances within the closure, since Serializer is not serializable while TableDesc is.
    val tableDesc = table.tableDesc
    val tableLocation = table.hiveQlTable.getDataLocation
    val tmpLocation = getExternalTmpPath(tableLocation)
    val fileSinkConf = new FileSinkDesc(tmpLocation.toString, tableDesc, false)
    val isCompressed = hadoopConf.get("hive.exec.compress.output", "false").toBoolean

    if (isCompressed) {
      // Please note that isCompressed, "mapred.output.compress", "mapred.output.compression.codec",
      // and "mapred.output.compression.type" have no impact on ORC because it uses table properties
      // to store compression information.
      hadoopConf.set("mapred.output.compress", "true")
      fileSinkConf.setCompressed(true)
      fileSinkConf.setCompressCodec(hadoopConf.get("mapred.output.compression.codec"))
      fileSinkConf.setCompressType(hadoopConf.get("mapred.output.compression.type"))
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
        s"""Requested partitioning does not match the ${table.tableName} table:
           |Requested partitions: ${partition.keys.mkString(",")}
           |Table partitions: ${table.partitionKeys.map(_.name).mkString(",")}""".stripMargin)
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

    val jobConf = new JobConf(hadoopConf)
    val jobConfSer = new SerializableJobConf(jobConf)

    // When speculation is on and output committer class name contains "Direct", we should warn
    // users that they may loss data if they are using a direct output committer.
    val speculationEnabled = sqlContext.sparkContext.conf.getBoolean("spark.speculation", false)
    val outputCommitterClass = jobConf.get("mapred.output.committer.class", "")
    if (speculationEnabled && outputCommitterClass.contains("Direct")) {
      val warningMessage =
        s"$outputCommitterClass may be an output committer that writes data directly to " +
          "the final location. Because speculation is enabled, this output committer may " +
          "cause data loss (see the case in SPARK-10063). If possible, please use an output " +
          "committer that does not have this behavior (e.g. FileOutputCommitter)."
      logWarning(warningMessage)
    }

    val writerContainer = if (numDynamicPartitions > 0) {
      val dynamicPartColNames = partitionColumnNames.takeRight(numDynamicPartitions)
      new SparkHiveDynamicPartitionWriterContainer(
        jobConf,
        fileSinkConf,
        dynamicPartColNames,
        child.output)
    } else {
      new SparkHiveWriterContainer(
        jobConf,
        fileSinkConf,
        child.output)
    }

    @transient val outputClass = writerContainer.newSerializer(table.tableDesc).getSerializedClass
    saveAsHiveFile(child.execute(), outputClass, fileSinkConf, jobConfSer, writerContainer)

    val outputPath = FileOutputFormat.getOutputPath(jobConf)
    // TODO: Correctly set holdDDLTime.
    // In most of the time, we should have holdDDLTime = false.
    // holdDDLTime will be true when TOK_HOLD_DDLTIME presents in the query as a hint.
    val holdDDLTime = false
    if (partition.nonEmpty) {
      if (numDynamicPartitions > 0) {
        externalCatalog.loadDynamicPartitions(
          db = table.catalogTable.database,
          table = table.catalogTable.identifier.table,
          outputPath.toString,
          partitionSpec,
          overwrite,
          numDynamicPartitions,
          holdDDLTime = holdDDLTime)
      } else {
        // scalastyle:off
        // ifNotExists is only valid with static partition, refer to
        // https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML#LanguageManualDML-InsertingdataintoHiveTablesfromqueries
        // scalastyle:on
        val oldPart =
          externalCatalog.getPartitionOption(
            table.catalogTable.database,
            table.catalogTable.identifier.table,
            partitionSpec)

        var doHiveOverwrite = overwrite

        if (oldPart.isEmpty || !ifNotExists) {
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
            table.catalogTable.database,
            table.catalogTable.identifier.table,
            outputPath.toString,
            partitionSpec,
            isOverwrite = doHiveOverwrite,
            holdDDLTime = holdDDLTime,
            inheritTableSpecs = inheritTableSpecs)
        }
      }
    } else {
      externalCatalog.loadTable(
        table.catalogTable.database,
        table.catalogTable.identifier.table,
        outputPath.toString, // TODO: URI
        overwrite,
        holdDDLTime)
    }

    // Attempt to delete the staging directory and the inclusive files. If failed, the files are
    // expected to be dropped at the normal termination of VM since deleteOnExit is used.
    try {
      createdTempDir.foreach { path => path.getFileSystem(hadoopConf).delete(path, true) }
    } catch {
      case NonFatal(e) =>
        logWarning(s"Unable to delete staging directory: $stagingDir.\n" + e)
    }

    // un-cache this table.
    sqlContext.sparkSession.catalog.uncacheTable(table.catalogTable.identifier.quotedString)
    sqlContext.sessionState.catalog.refreshTable(table.catalogTable.identifier)

    // It would be nice to just return the childRdd unchanged so insert operations could be chained,
    // however for now we return an empty list to simplify compatibility checks with hive, which
    // does not return anything for insert operations.
    // TODO: implement hive compatibility as rules.
    Seq.empty[InternalRow]
  }

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def executeCollect(): Array[InternalRow] = sideEffectResult.toArray

  protected override def doExecute(): RDD[InternalRow] = {
    sqlContext.sparkContext.parallelize(sideEffectResult.asInstanceOf[Seq[InternalRow]], 1)
  }
}
