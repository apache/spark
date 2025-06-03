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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.plan.FileSinkDesc
import org.apache.hadoop.hive.ql.plan.TableDesc

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.CommandUtils
import org.apache.spark.sql.execution.datasources.{FileFormat, V1WriteCommand, V1WritesUtils}
import org.apache.spark.sql.hive.client.HiveClientImpl


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
    ifPartitionNotExists: Boolean,
    outputColumnNames: Seq[String],
    partitionColumns: Seq[Attribute],
    bucketSpec: Option[BucketSpec],
    options: Map[String, String],
    fileFormat: FileFormat,
    @transient hiveTmpPath: HiveTempPath
  ) extends SaveAsHiveFile with V1WriteCommand with V1WritesHiveUtils {

  override def staticPartitions: TablePartitionSpec = {
    partition.filter(_._2.nonEmpty).map { case (k, v) => k -> v.get }
  }

  override def requiredOrdering: Seq[SortOrder] = {
    V1WritesUtils.getSortOrder(outputColumns, partitionColumns, bucketSpec, options)
  }

  /**
   * Inserts all the rows in the table into Hive.  Row objects are properly serialized with the
   * `org.apache.hadoop.hive.serde2.SerDe` and the
   * `org.apache.hadoop.mapred.OutputFormat` provided by the table definition.
   */
  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    val externalCatalog = sparkSession.sharedState.externalCatalog
    val hadoopConf = hiveTmpPath.hadoopConf
    val tmpLocation = hiveTmpPath.externalTempPath

    hiveTmpPath.createTmpPath()
    try {
      processInsert(sparkSession, externalCatalog, hadoopConf, tmpLocation, child)
    } finally {
      // Attempt to delete the staging directory and the inclusive files. If failed, the files are
      // expected to be dropped at the normal termination of VM since deleteOnExit is used.
      hiveTmpPath.deleteTmpPath()
    }

    // un-cache this table.
    CommandUtils.uncacheTableOrView(sparkSession, table.identifier)
    sparkSession.sessionState.catalog.refreshTable(table.identifier)

    CommandUtils.updateTableStats(sparkSession, table)

    // It would be nice to just return the childRdd unchanged so insert operations could be chained,
    // however for now we return an empty list to simplify compatibility checks with hive, which
    // does not return anything for insert operations.
    // TODO: implement hive compatibility as rules.
    Seq.empty[Row]
  }

  private def processInsert(
      sparkSession: SparkSession,
      externalCatalog: ExternalCatalog,
      hadoopConf: Configuration,
      tmpLocation: Path,
      child: SparkPlan): Unit = {

    val numDynamicPartitions = partition.values.count(_.isEmpty)
    val partitionSpec = getPartitionSpec(partition)

    val writtenParts = saveAsHiveFile(
      sparkSession = sparkSession,
      plan = child,
      hadoopConf = hadoopConf,
      fileFormat = fileFormat,
      outputLocation = tmpLocation.toString,
      partitionAttributes = partitionColumns,
      bucketSpec = bucketSpec,
      options = options)

    if (partition.nonEmpty) {
      if (numDynamicPartitions > 0) {
        if (overwrite && table.tableType == CatalogTableType.EXTERNAL) {
          val numWrittenParts = writtenParts.size
          val maxDynamicPartitionsKey = HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTS.varname
          val maxDynamicPartitions = hadoopConf.getInt(maxDynamicPartitionsKey,
            HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTS.defaultIntVal)
          if (numWrittenParts > maxDynamicPartitions) {
            throw QueryExecutionErrors.writePartitionExceedConfigSizeWhenDynamicPartitionError(
              numWrittenParts, maxDynamicPartitions, maxDynamicPartitionsKey)
          }
          // SPARK-29295: When insert overwrite to a Hive external table partition, if the
          // partition does not exist, Hive will not check if the external partition directory
          // exists or not before copying files. So if users drop the partition, and then do
          // insert overwrite to the same partition, the partition will have both old and new
          // data. We construct partition path. If the path exists, we delete it manually.
          writtenParts.foreach { partPath =>
            val dpMap = partPath.split("/").map { part =>
              val splitPart = part.split("=")
              assert(splitPart.size == 2, s"Invalid written partition path: $part")
              ExternalCatalogUtils.unescapePathName(splitPart(0)) ->
                ExternalCatalogUtils.unescapePathName(splitPart(1))
            }.toMap

            val caseInsensitiveDpMap = CaseInsensitiveMap(dpMap)

            val updatedPartitionSpec = partition.map {
              case (key, Some(null)) => key -> ExternalCatalogUtils.DEFAULT_PARTITION_NAME
              case (key, Some(value)) => key -> value
              case (key, None) if caseInsensitiveDpMap.contains(key) =>
                key -> caseInsensitiveDpMap(key)
              case (key, _) =>
                throw QueryExecutionErrors.dynamicPartitionKeyNotAmongWrittenPartitionPathsError(
                  key)
            }
            val partitionColumnNames = table.partitionColumnNames
            val tablePath = new Path(table.location)
            val partitionPath = ExternalCatalogUtils.generatePartitionPath(updatedPartitionSpec,
              partitionColumnNames, tablePath)

            val fs = partitionPath.getFileSystem(hadoopConf)
            if (fs.exists(partitionPath)) {
              if (!fs.delete(partitionPath, true)) {
                throw QueryExecutionErrors.cannotRemovePartitionDirError(partitionPath)
              }
            }
          }
        }

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
        if (oldPart.isEmpty || !ifPartitionNotExists) {
          // inheritTableSpecs is set to true. It should be set to false for an IMPORT query
          // which is currently considered as a Hive native command.
          val inheritTableSpecs = true
          externalCatalog.loadPartition(
            table.database,
            table.identifier.table,
            tmpLocation.toString,
            partitionSpec,
            isOverwrite = overwrite,
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
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): InsertIntoHiveTable =
    copy(query = newChild)
}

object InsertIntoHiveTable extends V1WritesHiveUtils {

  /**
   * A tag to identify if this command is created by a CTAS.
   */
  val BY_CTAS = TreeNodeTag[Unit]("by_ctas")

  def apply(
      table: CatalogTable,
      partition: Map[String, Option[String]],
      query: LogicalPlan,
      overwrite: Boolean,
      ifPartitionNotExists: Boolean,
      outputColumnNames: Seq[String]): InsertIntoHiveTable = {
    val sparkSession = SparkSession.getActiveSession.orNull
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
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    val tableLocation = hiveQlTable.getDataLocation
    val hiveTempPath = new HiveTempPath(sparkSession, hadoopConf, tableLocation)
    val fileSinkConf = new FileSinkDesc(hiveTempPath.externalTempPath, tableDesc, false)
    setupHadoopConfForCompression(fileSinkConf, hadoopConf, sparkSession)
    val fileFormat: FileFormat = new HiveFileFormat(fileSinkConf)

    val partitionColumns = getDynamicPartitionColumns(table, partition, query)
    val bucketSpec = table.bucketSpec
    val options = getOptionsWithHiveBucketWrite(bucketSpec)

    new InsertIntoHiveTable(table, partition, query, overwrite, ifPartitionNotExists,
      outputColumnNames, partitionColumns, bucketSpec, options, fileFormat, hiveTempPath)
  }
}
