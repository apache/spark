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
import java.util
import java.util.{Date, Random}

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.common.FileUtils
import org.apache.hadoop.hive.ql.exec.TaskRunner
import org.apache.hadoop.hive.ql.ErrorMsg
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.HiveShim.{ShimFileSinkDesc => FileSinkDesc}
import org.apache.spark.SparkException
import org.apache.spark.util.SerializableJobConf


case class InsertIntoHiveTable(
    table: MetastoreRelation,
    partition: Map[String, Option[String]],
    child: SparkPlan,
    overwrite: Boolean,
    ifNotExists: Boolean) extends UnaryExecNode {

  @transient private val sessionState = sqlContext.sessionState.asInstanceOf[HiveSessionState]
  @transient private val client = sessionState.metadataHive

  def output: Seq[Attribute] = Seq.empty

  val stagingDir = sessionState.conf.getConfString("hive.exec.stagingdir", ".hive-staging")

  private def executionId: String = {
    val rand: Random = new Random
    val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss_SSS")
    val executionId: String = "hive_" + format.format(new Date) + "_" + Math.abs(rand.nextLong)
    return executionId
  }

  private def getStagingDir(inputPath: Path, hadoopConf: Configuration): Path = {
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
      fs.deleteOnExit(dir)
    }
    catch {
      case e: IOException =>
        throw new RuntimeException(
          "Cannot create staging directory '" + dir.toString + "': " + e.getMessage, e)

    }
    return dir
  }

  private def getExternalScratchDir(extURI: URI, hadoopConf: Configuration): Path = {
    getStagingDir(new Path(extURI.getScheme, extURI.getAuthority, extURI.getPath), hadoopConf)
  }

  def getExternalTmpPath(path: Path, hadoopConf: Configuration): Path = {
    val extURI: URI = path.toUri
    if (extURI.getScheme == "viewfs") {
      getExtTmpPathRelTo(path.getParent, hadoopConf)
    } else {
      new Path(getExternalScratchDir(extURI, hadoopConf), "-ext-10000")
    }
  }

  def getExtTmpPathRelTo(path: Path, hadoopConf: Configuration): Path = {
    new Path(getStagingDir(path, hadoopConf), "-ext-10000") // Hive uses 10000
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
      SparkHiveWriterContainer.createPathFromString(fileSinkConf.getDirName, conf.value))
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
    val hadoopConf = sessionState.newHadoopConf()
    val tmpLocation = getExternalTmpPath(tableLocation, hadoopConf)
    val fileSinkConf = new FileSinkDesc(tmpLocation.toString, tableDesc, false)
    val isCompressed =
      sessionState.conf.getConfString("hive.exec.compress.output", "false").toBoolean

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
      if (!sessionState.conf.getConfString("hive.exec.dynamic.partition", "true").toBoolean) {
        throw new SparkException(ErrorMsg.DYNAMIC_PARTITION_DISABLED.getMsg)
      }

      // Report error if dynamic partition strict mode is on but no static partition is found
      if (numStaticPartitions == 0 &&
          sessionState.conf.getConfString(
            "hive.exec.dynamic.partition.mode", "strict").equalsIgnoreCase("strict"))
      {
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
        child.output,
        table)
    } else {
      new SparkHiveWriterContainer(
        jobConf,
        fileSinkConf,
        child.output,
        table)
    }

    @transient val outputClass = writerContainer.newSerializer(table.tableDesc).getSerializedClass
    saveAsHiveFile(child.execute(), outputClass, fileSinkConf, jobConfSer, writerContainer)

    val outputPath = FileOutputFormat.getOutputPath(jobConf)
    // Have to construct the format of dbname.tablename.
    val qualifiedTableName = s"${table.databaseName}.${table.tableName}"
    // TODO: Correctly set holdDDLTime.
    // In most of the time, we should have holdDDLTime = false.
    // holdDDLTime will be true when TOK_HOLD_DDLTIME presents in the query as a hint.
    val holdDDLTime = false
    if (partition.nonEmpty) {

      // loadPartition call orders directories created on the iteration order of the this map
      val orderedPartitionSpec = new util.LinkedHashMap[String, String]()
      table.hiveQlTable.getPartCols.asScala.foreach { entry =>
        orderedPartitionSpec.put(entry.getName, partitionSpec.getOrElse(entry.getName, ""))
      }

      // inheritTableSpecs is set to true. It should be set to false for an IMPORT query
      // which is currently considered as a Hive native command.
      val inheritTableSpecs = true
      // TODO: Correctly set isSkewedStoreAsSubdir.
      val isSkewedStoreAsSubdir = false
      if (numDynamicPartitions > 0) {
        client.synchronized {
          client.loadDynamicPartitions(
            outputPath.toString,
            qualifiedTableName,
            orderedPartitionSpec,
            overwrite,
            numDynamicPartitions,
            holdDDLTime,
            isSkewedStoreAsSubdir)
        }
      } else {
        // scalastyle:off
        // ifNotExists is only valid with static partition, refer to
        // https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML#LanguageManualDML-InsertingdataintoHiveTablesfromqueries
        // scalastyle:on
        val oldPart =
          client.getPartitionOption(
            client.getTable(table.databaseName, table.tableName),
            partitionSpec)

        if (oldPart.isEmpty || !ifNotExists) {
            client.loadPartition(
              outputPath.toString,
              qualifiedTableName,
              orderedPartitionSpec,
              overwrite,
              holdDDLTime,
              inheritTableSpecs,
              isSkewedStoreAsSubdir)
        }
      }
    } else {
      client.loadTable(
        outputPath.toString, // TODO: URI
        qualifiedTableName,
        overwrite,
        holdDDLTime)
    }

    // Invalidate the cache.
    sqlContext.sharedState.cacheManager.invalidateCache(table)
    sqlContext.sessionState.catalog.refreshTable(table.catalogTable.identifier)

    // It would be nice to just return the childRdd unchanged so insert operations could be chained,
    // however for now we return an empty list to simplify compatibility checks with hive, which
    // does not return anything for insert operations.
    // TODO: implement hive compatibility as rules.
    Seq.empty[InternalRow]
  }

  override def executeCollect(): Array[InternalRow] = sideEffectResult.toArray

  protected override def doExecute(): RDD[InternalRow] = {
    sqlContext.sparkContext.parallelize(sideEffectResult.asInstanceOf[Seq[InternalRow]], 1)
  }
}
