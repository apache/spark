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
import java.util.Date

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.hdfs.protocol.HdfsConstants

import org.apache.spark.{InsertFileSourceConflictException, SparkEnv}
import org.apache.spark.internal.io.{FileCommitProtocol, FileSourceWriteDesc, HadoopMapReduceCommitProtocol}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, CatalogTablePartition, ExternalCatalogUtils}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode
import org.apache.spark.sql.util.SchemaUtils

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
  import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.escapePathName
  import InsertIntoHadoopFsRelationCommand._
  import HadoopMapReduceCommitProtocol._

  // Staging dirs may be created for InsertHadoopFsRelation operation.
  var concurrentStagingDir: Path = null
  var concurrentStagingPartitionDir: Path = null

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

  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    // Most formats don't do well with duplicate columns, so lets not allow that
    SchemaUtils.checkColumnNameDuplication(
      outputColumnNames,
      s"when inserting into $outputPath",
      sparkSession.sessionState.conf.caseSensitiveAnalysis)

    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(options)
    val fs = outputPath.getFileSystem(hadoopConf)
    val qualifiedOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)

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

    val escapedStaticPartitionKVs = partitionColumns
      .filter(c => staticPartitions.contains(c.name))
      .map { attr =>
        val escapedKey = ExternalCatalogUtils.escapePathName(attr.name)
        val escapedValue = ExternalCatalogUtils.escapePathName(staticPartitions.get(attr.name).get)
        (escapedKey, escapedValue)
      }

    val fileSourceWriteDesc = Some(new FileSourceWriteDesc(
      isInsertIntoHadoopFsRelation = true,
      dynamicPartitionOverwrite = dynamicPartitionOverwrite,
      escapedStaticPartitionKVs = escapedStaticPartitionKVs))

    val committer = FileCommitProtocol.instantiate(
      sparkSession.sessionState.conf.fileCommitProtocolClass,
      jobId = java.util.UUID.randomUUID().toString,
      outputPath = outputPath.toString,
      fileSourceWriteDesc = fileSourceWriteDesc)

    try {
      var doDeleteMatchingPartitions: Boolean = false
      val doInsertion = if (mode == SaveMode.Append) {
        true
      } else {
        val pathExists = fs.exists(qualifiedOutputPath)
        (mode, pathExists) match {
          case (SaveMode.ErrorIfExists, true) =>
            throw new AnalysisException(s"path $qualifiedOutputPath already exists.")
          case (SaveMode.Overwrite, true) =>
            if (ifPartitionNotExists && matchingPartitions.nonEmpty) {
              false
            } else if (dynamicPartitionOverwrite) {
              // For dynamic partition overwrite, do not delete partition directories ahead.
              true
            } else {
              doDeleteMatchingPartitions = true
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
        // For insertion operation, detect whether there is a conflict.
        detectConflict(committer, fs, escapedStaticPartitionKVs)

        if (doDeleteMatchingPartitions) {
          deleteMatchingPartitions(fs, qualifiedOutputPath, customPartitionLocations, committer)
        }

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

        val updatedPartitionPaths =
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
        sparkSession.catalog.refreshByPath(outputPath.toString)

        if (catalogTable.nonEmpty) {
          CommandUtils.updateTableStats(sparkSession, catalogTable.get)
        }

      } else {
        logInfo("Skipping insertion into a relation that already exists.")
      }

      Seq.empty[Row]
    } catch {
      case e: Exception =>
        deleteStagingPath(fs, concurrentStagingDir,
          concurrentStagingPartitionDir)
        throw e
    }
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
        staticPartitions.get(p.name) match {
          case Some(value) =>
            Some(escapePathName(p.name) + "=" + escapePathName(value))
          case None =>
            None
        }
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

  /**
   * Check current committer whether supports several InsertIntoHadoopFsRelation operations write
   * to different partitions in a same table concurrently. If supports, then detect the conflict
   * whether there are several operations write to same partition in the same table or write to
   * a non-partitioned table.
   */
  private def detectConflict(
      commitProtocol: FileCommitProtocol,
      fs: FileSystem,
      staticPartitionKVs: Seq[(String, String)]): Unit = {

    val supportConcurrent = commitProtocol.isInstanceOf[HadoopMapReduceCommitProtocol] &&
      commitProtocol.asInstanceOf[HadoopMapReduceCommitProtocol].supportConcurrent
    if (supportConcurrent && fs.isDirectory(outputPath)) {
      val stagingDirName = ".spark-staging-" + staticPartitionKVs.size
      concurrentStagingDir = new Path(outputPath, stagingDirName)
      val stagingPartitionPathToCheck = new Path(outputPath, buildPath(stagingDirName,
        getEscapedStaticPartitionPath(staticPartitionKVs)))

      if (tryLock(stagingPartitionPathToCheck, fs)) {
        concurrentStagingPartitionDir = stagingPartitionPathToCheck
        fs.mkdirs(new Path(concurrentStagingPartitionDir, SparkEnv.get.conf.getAppId))
      } else {
        throwConflictedException(stagingPartitionPathToCheck, concurrentStagingDir, fs)
      }

      // Check whether there are some conflicted insert operations with different specified
      // partition key-values number.
      for (i <- 0 to partitionColumns.size) {
        if (i != staticPartitionKVs.size) {
          val stagingPath = new Path(outputPath, ".spark-staging-" + i)
          val subPartitions = staticPartitionKVs.slice(0, i)
          detectConflictPath(fs, stagingPath, subPartitions.size, i, staticPartitionKVs)
        }
      }
    }
  }

  private def detectConflictPath(
      fs: FileSystem,
      stagingPath: Path,
      kvSize: Int,
      depth: Int,
      staticPartitionKVs: Seq[(String, String)]): Unit = {
    val currentPath = if (kvSize == 0) {
      stagingPath
    } else {
      new Path(outputPath, buildPath(stagingPath.getName, getEscapedStaticPartitionPath(
        staticPartitionKVs.slice(0, kvSize))))
    }

    findConflictedStagingPartitionPaths(fs, currentPath, depth - kvSize)
      .foreach { stagingPartitionPath =>
        if (tryLock(stagingPartitionPath, fs)) {
          logInfo(
            s"""
               | Get the lock of conflicted staging partition:$stagingPartitionPath
               | successfully, it should be owned to a completed application, whose staging
               | partition path has not been cleaned up. Would clean up it.
               |""".stripMargin)
          unlock(stagingPartitionPath, stagingPath, fs)
        } else {
          throwConflictedException(stagingPartitionPath, stagingPath, fs)
        }
    }
  }

  private def throwConflictedException(
      stagingPartitionPath: Path,
      stagingPath: Path,
      fs: FileSystem): Unit = {
    val absolutePath = stagingPartitionPath.toUri.getPath
    val relativePath = absolutePath.substring(absolutePath.lastIndexOf(stagingPath.getName))
    var modificationTime: Date = null
    val appId = Try {
      modificationTime = new Date(fs.getFileStatus(stagingPartitionPath).getModificationTime)
      fs.listStatus(stagingPartitionPath).filter { status =>
        fs.isDirectory(status.getPath)
      }.apply(0).getPath.getName
    } match {
      case Success(appDirName) => appDirName
      case Failure(e) =>
        logWarning(
          s"""
             | Exception occurred when getting appId dir name under stagingPartitionDir:
             | $stagingPartitionPath""".stripMargin, e)
        "NOT FOUND"
    }

    // Unlock the concurrentStagingPartitionDir, which may has been created.
    unlock(concurrentStagingPartitionDir, concurrentStagingDir, fs)
    throw new InsertFileSourceConflictException(
      s"""
         | CONFLICT !!!. There is conflicted output path under tablePath:
         | ($outputPath).
         | Detailed information (conflicted path , appId, last modification time):
         | $relativePath,$appId,$modificationTime.
         |
         | There could be two possibilities:
         | 1. The path is being written by another InsertDataSource operation and you need wait
         |    for it to complete.
         | 2. This path belongs to a failed application who didn't have a chance to clean it up
         |    gracefully.
         |
         | Please check with the provided appId and last modification time
         | whether another application is still writing to the path .
         | If not, it's safe to delete it and retry your application.
         |""".stripMargin)
  }

  /**
   * Find relative staging partition paths, which is conflicted with current
   * InsertIntoHadoopFsRelation operation.
   */
  private def findConflictedStagingPartitionPaths(
      fs: FileSystem,
      path: Path,
      depth: Int): Seq[Path] = {
    val paths = ListBuffer[Path]()
    try {
      if (fs.exists(path)) {
        if (depth == 0) {
          paths += path
        } else {
          for (file <- fs.listStatus(path)) {
            paths ++= findConflictedStagingPartitionPaths(fs, file.getPath, depth - 1)
          }
        }
      }
    } catch {
      case e: Exception =>
        logError("Exception occurred when finding conflicted staging partition paths.")
        throw e
    }
    paths
  }

  /**
   * Try to get the lock of a partition. If the fileSystem is not [[DistributedFileSystem]], check
   * whether the lock file exists, if existed, we can not get the partition lock, otherwise, create
   * the lock file and get the partition lock.
   * Otherwise, this filesystem is [[DistributedFileSystem]], if the lock file exists, we can try to
   * append this lock file, if append successfully, we get the partition lock; else if the lock file
   * does not exist, we create this lock file and get the partition lock.
   */
  private def tryLock(
      stagingPartitionPath: Path,
      fs: FileSystem): Boolean = {
    try {
      if (!fs.exists(stagingPartitionPath)) {
        fs.mkdirs(stagingPartitionPath)
      }
      val partitionLock = new Path(stagingPartitionPath, getLockName)
      if (!fs.isInstanceOf[DistributedFileSystem]) {
        val existed = fs.exists(partitionLock)
        if (!existed) {
          fs.create(partitionLock).close()
        }
        !existed
      } else {

        def pickLock: Boolean = Try {
          if (fs.exists(partitionLock)) {
            fs.append(partitionLock)
          } else {
            fs.create(partitionLock)
          }
        } match {
          case Success(_) => true
          case _ => false
        }

        var picked: Boolean = false
        val startTime = System.currentTimeMillis()
        var lastTime = startTime
        val timeOut = HdfsConstants.LEASE_SOFTLIMIT_PERIOD

        while (!picked && lastTime - startTime < timeOut) {
          lastTime = System.currentTimeMillis()
          picked = pickLock
          if (!picked) {
            Thread.sleep(1000)
          }
        }
        if (!picked) {
          logWarning(
            s"""
               | Can not get the partition lock after waiting for a soft limit period:
               | ${HdfsConstants.LEASE_SOFTLIMIT_PERIOD} ms, it should be owned by another
               | running operation.
               |""".stripMargin)
        }
        picked
      }
    } catch {
      case e: Exception =>
        logWarning(
          s"""
             | Exception occurred when checking the lock of stagingPartition:$stagingPartitionPath.
             |""".stripMargin, e)
        false
    }
  }

  private def unlock(
      stagingPartitionPath: Path,
      stagingPath: Path,
      fs: FileSystem): Unit = {
    HadoopMapReduceCommitProtocol.deleteStagingPath(fs, stagingPath, stagingPartitionPath)
  }
}

object InsertIntoHadoopFsRelationCommand {
  def getLockName: String = "_LOCK"
}
