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

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.common.{FileUtils, HiveStatsUtils}
import org.apache.hadoop.hive.ql.ErrorMsg
import org.apache.hadoop.hive.ql.plan.TableDesc

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, ExternalCatalog}
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Expression, HiveHash, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.CommandUtils
import org.apache.spark.sql.hive.HiveShim.{ShimFileSinkDesc => FileSinkDesc}
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
    outputColumns: Seq[Attribute]) extends SaveAsHiveFile {

  /**
   * Inserts all the rows in the table into Hive.  Row objects are properly serialized with the
   * `org.apache.hadoop.hive.serde2.SerDe` and the
   * `org.apache.hadoop.mapred.OutputFormat` provided by the table definition.
   */
  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    val externalCatalog = sparkSession.sharedState.externalCatalog
    val hadoopConf = sparkSession.sessionState.newHadoopConf()

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
    val tmpLocation = getExternalTmpPath(sparkSession, hadoopConf, tableLocation)

    try {
      processInsert(sparkSession, externalCatalog, hadoopConf, tableDesc, tmpLocation, child)
    } finally {
      // Attempt to delete the staging directory and the inclusive files. If failed, the files are
      // expected to be dropped at the normal termination of VM since deleteOnExit is used.
      deleteExternalTmpPath(hadoopConf)
    }

    // un-cache this table.
    sparkSession.catalog.uncacheTable(table.identifier.quotedString)
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
      tableDesc: TableDesc,
      tmpLocation: Path,
      child: SparkPlan): Unit = {
    val fileSinkConf = new FileSinkDesc(tmpLocation.toString, tableDesc, false)

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

    if (!overwrite && table.bucketSpec.isDefined) {
      throw new SparkException("Appending data to hive bucketed table is not allowed as it " +
        "will break the table's bucketing guarantee. Consider overwriting instead. Table = " +
        table.qualifiedName)
    }

    val partitionAttributes = partitionColumnNames.takeRight(numDynamicPartitions).map { name =>
      query.resolve(name :: Nil, sparkSession.sessionState.analyzer.resolver).getOrElse {
        throw new AnalysisException(
          s"Unable to resolve $name given [${query.output.map(_.name).mkString(", ")}]")
      }.asInstanceOf[Attribute]
    }

    saveAsHiveFile(
      sparkSession = sparkSession,
      plan = child,
      hadoopConf = hadoopConf,
      fileSinkConf = fileSinkConf,
      outputLocation = tmpLocation.toString,
      allColumns = outputColumns,
      partitionAttributes = partitionAttributes)

    // validate bucketing based on number of files before loading to metastore
    table.bucketSpec.foreach { spec =>
      if (partition.nonEmpty && numDynamicPartitions > 0) {
        val validPartitionPaths =
          getValidPartitionPaths(hadoopConf, tmpLocation, numDynamicPartitions)
        validateBuckets(hadoopConf, validPartitionPaths, table.bucketSpec.get.numBuckets)
      } else {
        validateBuckets(hadoopConf, Seq(tmpLocation), table.bucketSpec.get.numBuckets)
      }
    }

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
  }

  private def getValidPartitionPaths(
      conf: Configuration,
      outputPath: Path,
      numDynamicPartitions: Int): Seq[Path] = {
    val validPartitionPaths = mutable.HashSet[Path]()
    try {
      val fs = outputPath.getFileSystem(conf)
      HiveStatsUtils.getFileStatusRecurse(outputPath, numDynamicPartitions, fs)
        .filter(_.isDirectory)
        .foreach(d => validPartitionPaths.add(d.getPath))
    } catch {
      case e: IOException =>
        throw new SparkException("Unable to extract partition paths from temporary output " +
          s"location $outputPath due to : ${e.getMessage}", e)
    }
    validPartitionPaths.toSeq
  }

  private def validateBuckets(conf: Configuration, outputPaths: Seq[Path], numBuckets: Int) = {
    val bucketedFilePattern = """part-(\d+)(?:.*)?$""".r

    def getBucketIdFromFilename(fileName : String): Option[Int] =
      fileName match {
        case bucketedFilePattern(bucketId) => Some(bucketId.toInt)
        case _ => None
      }

    outputPaths.foreach(outputPath => {
      val fs = outputPath.getFileSystem(conf)
      val files =
        fs.listStatus(outputPath)
          .filterNot(_.getPath.getName == "_SUCCESS")
          .map(_.getPath.getName)
          .sortBy(_.toString)

      var expectedBucketId = 0
      files.foreach { case file =>
        getBucketIdFromFilename(file) match {
          case Some(id) if id == expectedBucketId =>
            expectedBucketId += 1
          case Some(_) =>
            throw new SparkException(
              s"Potentially missing bucketed output files in temporary bucketed output location. " +
                s"Aborting job. Output location : $outputPath, files found : " +
                files.mkString("[", ",", "]"))
          case None =>
            throw new SparkException(
              s"Invalid file found in temporary bucketed output location. Aborting job. " +
                s"Output location : $outputPath, bad file : $file")
        }
      }

      if (expectedBucketId != numBuckets) {
        throw new SparkException(
          s"Potentially missing bucketed output files in temporary bucketed output location. " +
            s"Aborting job. Output location : $outputPath, files found : " +
            files.mkString("[", ",", "]"))
      }
    })
  }

  private def getPartitionAndDataColumns: (Seq[Attribute], Seq[Attribute]) = {
    val allColumns = query.output
    val partitionColumnNames = partition.keySet
    allColumns.partition(c => partitionColumnNames.contains(c.name))
  }

  /**
   * Use `HashPartitioning.partitionIdExpression` as our bucket id expression, so that we can
   * guarantee the data distribution is same between shuffle and bucketed data source, which
   * enables us to only shuffle one side when join a bucketed table and a normal one.
   */
  private def getBucketIdExpression(dataColumns: Seq[Attribute]): Option[Expression] =
    table.bucketSpec.map { spec =>
      HashPartitioning(
        spec.bucketColumnNames.map(c => dataColumns.find(_.name == c).get),
        spec.numBuckets,
        classOf[HiveHash]
      ).partitionIdExpression
    }

  /**
   * If the table is bucketed, then requiredDistribution would be the bucket columns.
   * Else it would be empty
   */
  override def requiredDistribution: Seq[Distribution] = table.bucketSpec match {
    case Some(bucketSpec) =>
      val (_, dataColumns) = getPartitionAndDataColumns
      Seq(HashClusteredDistribution(
        bucketSpec.bucketColumnNames.map(b => dataColumns.find(_.name == b).get),
        Option(bucketSpec.numBuckets),
        classOf[HiveHash]))

    case _ => Seq(UnspecifiedDistribution)
  }

  /**
   * How is `requiredOrdering` determined ?
   *
   *     table type      |    normal table    |              bucketed table
   * --------------------+--------------------+-----------------------------------------------
   *   non-partitioned   |        Nil         |               sort columns
   *   static partition  |        Nil         |               sort columns
   *   dynamic partition |  partition columns | (partition columns + bucketId + sort columns)
   * --------------------+--------------------+-----------------------------------------------
   */
  override def requiredOrdering: Seq[Seq[SortOrder]] = {
    val (partitionColumns, dataColumns) = getPartitionAndDataColumns
    val isDynamicPartitioned =
      table.partitionColumnNames.nonEmpty && partition.values.exists(_.isEmpty)

    val sortExpressions = table.bucketSpec match {
      case Some(bucketSpec) =>
        val sortColumns = bucketSpec.sortColumnNames.map(c => dataColumns.find(_.name == c).get)
        if (isDynamicPartitioned) {
          partitionColumns ++ getBucketIdExpression(dataColumns) ++ sortColumns
        } else {
          sortColumns
        }
      case _ => if (isDynamicPartitioned) partitionColumns else Nil
    }

    Seq(sortExpressions.map(SortOrder(_, Ascending)))
  }
}
