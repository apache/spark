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

package org.apache.spark.sql.execution.datasources

import java.io.IOException
import java.nio.file.Paths

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat}
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.rdd.{RDD, UnionPartition, UnionRDD}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.util.SerializableConfiguration

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * A command for writing data to a [[HadoopFsRelation]].  Supports both overwriting and appending.
 * Writing to dynamic partitions is also supported.
 *
 * @param staticPartitions partial partitioning spec for write. This defines the scope of partition
 *                         overwrites: when the spec is empty, all partitions are overwritten.
 *                         When it covers a prefix of the partition keys, only partitions matching
 *                         the prefix are overwritten.
 * @param ifPartitionNotExists If true, only write if the partition does not exist.
 *                             Only valid for static partitions.
 */
case class InsertIntoHadoopFsRelationCommand(
    outputPath: Path,
    staticPartitions: TablePartitionSpec,
    ifPartitionNotExists: Boolean,
    partitionColumns: Seq[Attribute],
    bucketSpec: Option[BucketSpec],
    fileFormat: FileFormat,
    options: Map[String, String],
    query: LogicalPlan,
    mode: SaveMode,
    catalogTable: Option[CatalogTable],
    fileIndex: Option[FileIndex],
    outputColumnNames: Seq[String])
  extends DataWritingCommand {

  private lazy val parameters = CaseInsensitiveMap(options)

  private[sql] lazy val dynamicPartitionOverwrite: Boolean = {
    val partitionOverwriteMode = parameters.get("partitionOverwriteMode")
      // scalastyle:off caselocale
      .map(mode => PartitionOverwriteMode.withName(mode.toUpperCase))
      // scalastyle:on caselocale
      .getOrElse(SQLConf.get.partitionOverwriteMode)
    val enableDynamicOverwrite = partitionOverwriteMode == PartitionOverwriteMode.DYNAMIC
    // This config only makes sense when we are overwriting a partitioned dataset with dynamic
    // partition columns.
    enableDynamicOverwrite && mode == SaveMode.Overwrite &&
      staticPartitions.size < partitionColumns.length
  }

  var avgConditionSize = 0l
  var avgOutputSize = 0l
  var mergeEnabled = false

  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    mergeEnabled = SQLConf.get.getConf(SQLConf.MERGE_ENABLE_DATASOURCE)
    avgConditionSize = SQLConf.get.getConf(SQLConf.MERGE_AVG_CONDSIZE)
    avgOutputSize = SQLConf.get.getConf(SQLConf.MERGE_AVG_OUTSIZE)

    // Most formats don't do well with duplicate columns, so lets not allow that
    SchemaUtils.checkColumnNameDuplication(
      outputColumnNames,
      s"when inserting into $outputPath",
      sparkSession.sessionState.conf.caseSensitiveAnalysis)

    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(options)
    val fs = outputPath.getFileSystem(hadoopConf)
    val finalOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    var qualifiedOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)

    val partitionsTrackedByCatalog = sparkSession.sessionState.conf.manageFilesourcePartitions &&
      catalogTable.isDefined &&
      catalogTable.get.partitionColumnNames.nonEmpty &&
      catalogTable.get.tracksPartitionsInCatalog

    var initialMatchingPartitions: Seq[TablePartitionSpec] = Nil
    var customPartitionLocations: Map[TablePartitionSpec, String] = Map.empty
    var matchingPartitions: Seq[CatalogTablePartition] = Seq.empty

    // When partitions are tracked by the catalog, compute all custom partition locations that
    // may be relevant to the insertion job.
    if (partitionsTrackedByCatalog) {
      matchingPartitions = sparkSession.sessionState.catalog.listPartitions(
        catalogTable.get.identifier, Some(staticPartitions))
      initialMatchingPartitions = matchingPartitions.map(_.spec)
      customPartitionLocations = getCustomPartitionLocations(
        fs, catalogTable.get, qualifiedOutputPath, matchingPartitions)
    }

    val canMerge = mergeEnabled && bucketSpec.isEmpty && customPartitionLocations.isEmpty

    val mergedir = ".merge-temp-" + java.util.UUID.randomUUID().toString
    if(canMerge){
      qualifiedOutputPath = new Path(qualifiedOutputPath, mergedir)
    }
    logInfo("output path " + qualifiedOutputPath)

    val committer = FileCommitProtocol.instantiate(
      sparkSession.sessionState.conf.fileCommitProtocolClass,
      jobId = java.util.UUID.randomUUID().toString,
      outputPath = qualifiedOutputPath.toString,
      dynamicPartitionOverwrite = dynamicPartitionOverwrite)

    val doInsertion = if (mode == SaveMode.Append) {
      true
    } else {
      val pathExists = fs.exists(finalOutputPath)
      (mode, pathExists) match {
        case (SaveMode.ErrorIfExists, true) =>
          throw new AnalysisException(s"path $finalOutputPath already exists.")
        case (SaveMode.Overwrite, true) =>
          if (ifPartitionNotExists && matchingPartitions.nonEmpty) {
            false
          } else if (dynamicPartitionOverwrite) {
            // For dynamic partition overwrite, do not delete partition directories ahead.
            true
          } else {
            deleteMatchingPartitions(fs, finalOutputPath, customPartitionLocations, committer)
            true
          }
        case (SaveMode.Overwrite, _) | (SaveMode.ErrorIfExists, false) =>
          true
        case (SaveMode.Ignore, exists) =>
          !exists
        case (s, exists) =>
          throw new IllegalStateException(s"unsupported save mode $s ($exists)")
      }
    }

    if (doInsertion) {

      def refreshUpdatedPartitions(updatedPartitionPaths: Set[String]): Unit = {
        val updatedPartitions = updatedPartitionPaths.map(PartitioningUtils.parsePathFragment)
        if (partitionsTrackedByCatalog) {
          val newPartitions = updatedPartitions -- initialMatchingPartitions
          if (newPartitions.nonEmpty) {
            AlterTableAddPartitionCommand(
              catalogTable.get.identifier, newPartitions.toSeq.map(p => (p, None)),
              ifNotExists = true).run(sparkSession)
          }
          // For dynamic partition overwrite, we never remove partitions but only update existing
          // ones.
          if (mode == SaveMode.Overwrite && !dynamicPartitionOverwrite) {
            val deletedPartitions = initialMatchingPartitions.toSet -- updatedPartitions
            if (deletedPartitions.nonEmpty) {
              AlterTableDropPartitionCommand(
                catalogTable.get.identifier, deletedPartitions.toSeq,
                ifExists = true, purge = false,
                retainData = true /* already deleted */).run(sparkSession)
            }
          }
        }
      }

      var updatedPartitionPaths =
        FileFormatWriter.write(
          sparkSession = sparkSession,
          plan = child,
          fileFormat = fileFormat,
          committer = committer,
          outputSpec = FileFormatWriter.OutputSpec(
            qualifiedOutputPath.toString, customPartitionLocations, outputColumns),
          hadoopConf = hadoopConf,
          partitionColumns = partitionColumns,
          bucketSpec = bucketSpec,
          statsTrackers = Seq(basicWriteJobStatsTracker(hadoopConf)),
          options = options)

      logInfo("updated partition paths " + updatedPartitionPaths.mkString(","))
      logInfo("updated partition total " + updatedPartitionPaths.size)
      updatedPartitionPaths = updatedPartitionPaths.map(p=>p.replace("/"+ mergedir,""))

      if(canMerge) {
        try{
          logInfo("enable merge files for " + qualifiedOutputPath)
          val directRenamePathList = new ListBuffer[Path]
          val dataAttr = outputColumns.filterNot(AttributeSet(partitionColumns).contains)
          if (partitionColumns.isEmpty) {
            logInfo("non partition insert, merge single dir " + qualifiedOutputPath)
            val (reParitionNum, avgsize) = MergeUtils.getRePartitionNum(qualifiedOutputPath, hadoopConf, avgConditionSize, avgOutputSize)
            logInfo(s"[mergeFile] static $qualifiedOutputPath rePartionNum is: " + reParitionNum)
            if (reParitionNum > 0) {
                val committer2 = FileCommitProtocol.instantiate(
                  sparkSession.sessionState.conf.fileCommitProtocolClass,
                  jobId = java.util.UUID.randomUUID().toString,
                  outputPath = finalOutputPath.toString)
                val sourceRdd = getRdd(sparkSession, qualifiedOutputPath, dataAttr).coalesce(reParitionNum)
                FileFormatWriter.writeRdd(sparkSession, sourceRdd, fileFormat, committer2,
                  FileFormatWriter.OutputSpec(
                    finalOutputPath.toString, Map.empty, dataAttr), hadoopConf, Seq.empty, None,
                  statsTrackers = Seq(basicWriteJobStatsTracker(hadoopConf)), options)
              logInfo("[mergeFile] merge static dir finished")
            } else {
              logInfo(s"direct rename $qualifiedOutputPath's files")
              val files = fs.listStatus(qualifiedOutputPath)
              files.foreach(directRenamePathList += _.getPath)
            }
          } else {
            logInfo("has partition insert, merge multi dir " + qualifiedOutputPath)
            val tmpDynamicPartInfos = MergeUtils.getTmpDynamicPartPathInfo(qualifiedOutputPath, hadoopConf)
            if (dynamicPartitionOverwrite) {
              deleteExistingPartition(fs, tmpDynamicPartInfos.keys.toSeq, mergedir)
            }
            val mergeRule = generateDynamicMergeRule(tmpDynamicPartInfos, directRenamePathList)
            logInfo(s"[mergeFile] merge candidate " + mergeRule.plist.size)
            if (mergeRule.plist.nonEmpty) {
                val committerJobPair = new ArrayBuffer[(FileCommitProtocol, Job)]()
                val rdds = mergeRule.plist.map { case (path, repartnum) =>
                  val finalPartPath = path.toString.replace("/"+mergedir, "")
                  logInfo(s"[makeMergedRDDForPartitionedTable] create rdd for $path to $finalPartPath coalesce $repartnum")
                  val committer = FileCommitProtocol.instantiate(
                    sparkSession.sessionState.conf.fileCommitProtocolClass,
                    jobId = java.util.UUID.randomUUID().toString,
                    outputPath = finalPartPath)
                  val job = Job.getInstance(hadoopConf)
                  job.setOutputKeyClass(classOf[Void])
                  job.setOutputValueClass(classOf[InternalRow])
                  FileOutputFormat.setOutputPath(job, new Path(finalPartPath))
                  committerJobPair += ((committer, job))
                  getRdd(sparkSession, path, dataAttr).coalesce(repartnum)
                }

                for (i <- 0 until rdds.size) {
                  val outputdir = committerJobPair(i)._2.getConfiguration.get(FileOutputFormat.OUTDIR)
                  logInfo(s"rdd $i correspond with " + outputdir)
                }

                // index partition id of unionrdd with repsective committer
                val union = new UnionRDD(rdds(0).context, rdds)
                val rddIndex2Commiter = union.rdds.zipWithIndex.map(_._2).zip(committerJobPair.map(_._1)).toMap
                val map4Committer = union.getPartitions.map { p =>
                  val up = p.asInstanceOf[UnionPartition[RDD[InternalRow]]]
                  val withCommiter = rddIndex2Commiter(up.parentRddIndex)
                  logInfo(s"partition index ${up.index} correspond with rdd ${up.parentRddIndex}")
                  (up.index, withCommiter)
                }.toMap
                FileFormatWriter.writeRdd2(sparkSession, union, fileFormat, map4Committer, committerJobPair,
                  FileFormatWriter.OutputSpec(
                    "", Map.empty, dataAttr), hadoopConf, Seq.empty, Seq(basicWriteJobStatsTracker(hadoopConf)), options)
              logInfo("[mergeFile] merge dynamic partition finished")
            }
          }
          if (directRenamePathList.nonEmpty) {
            directRenamePathList.foreach { path =>
              if (fs.isDirectory(path)) {
                val (destPath, existed) = checkPartitionDirExists(fs, path, qualifiedOutputPath, finalOutputPath)
                if (existed) {
                  val files = fs.listStatus(path)
                  files.foreach { f =>
                    if(f.getPath.getName == FileOutputCommitter.SUCCEEDED_FILE_NAME){
                      if(!fs.rename(f.getPath, destPath))
                        logInfo("skip rename marked success file, maybe already exists")
                    } else {
                      if (!fs.rename(f.getPath, destPath)) throw new IOException(s"direct rename fail from ${f.getPath} to $destPath")
                      logInfo("direct rename file [" + f.getPath + " to " + destPath + "]")
                    }
                  }
                } else {
                  if (!fs.rename(path, destPath)) throw new IOException(s"direct rename fail from $path to $destPath")
                  logInfo("direct rename dir [" + path + " to " + destPath + "]")
                }
              } else {
                if(path.getName == FileOutputCommitter.SUCCEEDED_FILE_NAME){
                  if(!fs.rename(path, finalOutputPath))
                    logInfo("skip rename marked success file, maybe already exists")
                } else {
                  if (!fs.rename(path, finalOutputPath)) throw new IOException(s"direct rename fail from $path to $finalOutputPath")
                  logInfo("direct rename file [" + path + " to " + finalOutputPath + "]")
                }
              }
            }
          }
        }finally {
          fs.delete(qualifiedOutputPath, true)
          logInfo("delete temp path " + qualifiedOutputPath)
        }
      }

      // update metastore partition metadata
      if (updatedPartitionPaths.isEmpty && staticPartitions.nonEmpty
        && partitionColumns.length == staticPartitions.size) {
        // Avoid empty static partition can't loaded to datasource table.
        val staticPathFragment =
          PartitioningUtils.getPathFragment(staticPartitions, partitionColumns)
        refreshUpdatedPartitions(Set(staticPathFragment))
      } else {
        refreshUpdatedPartitions(updatedPartitionPaths)
      }

      // refresh cached files in FileIndex
      fileIndex.foreach(_.refresh())
      // refresh data cache if table is cached
      sparkSession.sharedState.cacheManager.recacheByPath(sparkSession, outputPath, fs)

      if (catalogTable.nonEmpty) {
        CommandUtils.updateTableStats(sparkSession, catalogTable.get)
      }

    } else {
      logInfo("Skipping insertion into a relation that already exists.")
    }

    Seq.empty[Row]
  }

  /**
   * Deletes all partition files that match the specified static prefix. Partitions with custom
   * locations are also cleared based on the custom locations map given to this class.
   */
  private def deleteMatchingPartitions(
      fs: FileSystem,
      qualifiedOutputPath: Path,
      customPartitionLocations: Map[TablePartitionSpec, String],
      committer: FileCommitProtocol): Unit = {
    val staticPartitionPrefix = if (staticPartitions.nonEmpty) {
      "/" + partitionColumns.flatMap { p =>
        staticPartitions.get(p.name).map(getPartitionPathString(p.name, _))
      }.mkString("/")
    } else {
      ""
    }
    // first clear the path determined by the static partition keys (e.g. /table/foo=1)
    val staticPrefixPath = qualifiedOutputPath.suffix(staticPartitionPrefix)
    if (fs.exists(staticPrefixPath) && !committer.deleteWithJob(fs, staticPrefixPath, true)) {
      throw new IOException(s"Unable to clear output " +
        s"directory $staticPrefixPath prior to writing to it")
    }
    // now clear all custom partition locations (e.g. /custom/dir/where/foo=2/bar=4)
    for ((spec, customLoc) <- customPartitionLocations) {
      assert(
        (staticPartitions.toSet -- spec).isEmpty,
        "Custom partition location did not match static partitioning keys")
      val path = new Path(customLoc)
      if (fs.exists(path) && !committer.deleteWithJob(fs, path, true)) {
        throw new IOException(s"Unable to clear partition " +
          s"directory $path prior to writing to it")
      }
    }
  }

  /**
   * Given a set of input partitions, returns those that have locations that differ from the
   * Hive default (e.g. /k1=v1/k2=v2). These partitions were manually assigned locations by
   * the user.
   *
   * @return a mapping from partition specs to their custom locations
   */
  private def getCustomPartitionLocations(
      fs: FileSystem,
      table: CatalogTable,
      qualifiedOutputPath: Path,
      partitions: Seq[CatalogTablePartition]): Map[TablePartitionSpec, String] = {
    partitions.flatMap { p =>
      val defaultLocation = qualifiedOutputPath.suffix(
        "/" + PartitioningUtils.getPathFragment(p.spec, table.partitionSchema)).toString
      val catalogLocation = new Path(p.location).makeQualified(
        fs.getUri, fs.getWorkingDirectory).toString
      if (catalogLocation != defaultLocation) {
        Some(p.spec -> catalogLocation)
      } else {
        None
      }
    }.toMap
  }

  // replay output data to filescan rdd, use prepared execution rule to columnar format
  private def getRdd(sparkSession:SparkSession, input:Path, dataAttr:Seq[Attribute]): RDD[InternalRow]={
    val fi = new InMemoryFileIndex(sparkSession, Seq(input), Map.empty, None)
    val relation = HadoopFsRelation(fi,new StructType(),dataAttr.toStructType,None,fileFormat,options)(sparkSession)
    val fsScanExec = FileSourceScanExec(relation,dataAttr,dataAttr.toStructType,Seq.empty,None,Seq.empty,None)
    QueryExecution.prepareExecutedPlan(sparkSession,fsScanExec).execute()
  }

  def generateDynamicMergeRule(tmpDynamicPartInfos:mutable.Map[Path,MergeUtils.AverageSize], directRenamePathList: ListBuffer[Path]): DynamicPartMergeRule = {
    logInfo("generate DynamicMergeRule tmp partInfo size: " + tmpDynamicPartInfos.size)
    val mergeRule = DynamicPartMergeRule(new ArrayBuffer[(Path, Int)]())
    for (part <- tmpDynamicPartInfos) {
      if (part._2.numFiles <= 1 || part._2.getAverageSize > avgConditionSize) {
        logInfo("direct rename " + part._1 + " number of files " + part._2.numFiles)
        directRenamePathList += part._1
      } else {
        val targetPart = MergeUtils.computeMergePartitionNum(part._2,avgConditionSize,avgOutputSize)
        mergeRule.plist += ((part._1, targetPart.toInt))
        logInfo(s"add merge path " + part._1 + s" and target partition $targetPart")
      }
    }
    mergeRule
  }

  //compatible with insert into/overwrite case that the dest dir handle
  def checkPartitionDirExists(fs: FileSystem, tempPath:Path, qualifiedPath:Path, finalOutputPath:Path):(Path,Boolean)={
    val tempJavaPath = Paths.get(tempPath.toUri.getPath)
    val qualifiedJavaPath = Paths.get(qualifiedPath.toUri.getPath)
    val relativeJavaPath = qualifiedJavaPath.relativize(tempJavaPath)
    val finalDestPath = new Path(finalOutputPath,relativeJavaPath.toString)
    val finalDestParent = finalDestPath.getParent
    if(!fs.exists(finalDestParent)){
      fs.mkdirs(finalDestParent)
      logInfo("mkdir direct parent " + finalDestParent)
    }
    if(fs.exists(finalDestPath)){
      logInfo(s"existing dest dir $finalDestPath")
      (finalDestPath, true)
    }else{
      logInfo(s"non existing dest dir $finalDestPath")
      (finalDestPath, false)
    }
  }

  def deleteExistingPartition(fs:FileSystem, paths:Seq[Path], mergedir:String): Unit ={
    paths.foreach{ p=>
      val finalPartPath = new Path(p.toString.replace("/"+ mergedir,""))
      if(fs.exists(finalPartPath)){
        fs.delete(finalPartPath,true)
        logInfo("delete overwrite dir " + finalPartPath)
      }
    }
  }

  case class DynamicPartMergeRule(plist: ArrayBuffer[(Path, Int)]) {
    override def toString: String = {
      plist.map { case (path, repartition) => s"$path: $repartition" }.mkString("\n")
    }
  }
}
