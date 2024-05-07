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

import java.io.FileNotFoundException

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{COUNT, PERCENT, TOTAL}
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{expressions, InternalRow}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ArrayImplicits._

/**
 * An abstract class that represents [[FileIndex]]s that are aware of partitioned tables.
 * It provides the necessary methods to parse partition data based on a set of files.
 *
 * @param parameters as set of options to control partition discovery
 * @param userSpecifiedSchema an optional user specified schema that will be use to provide
 *                            types for the discovered partitions
 */
abstract class PartitioningAwareFileIndex(
    sparkSession: SparkSession,
    parameters: Map[String, String],
    userSpecifiedSchema: Option[StructType],
    fileStatusCache: FileStatusCache = NoopCache) extends FileIndex with Logging {

  /** Returns the specification of the partitions inferred from the data. */
  def partitionSpec(): PartitionSpec

  override def partitionSchema: StructType = partitionSpec().partitionColumns

  protected val hadoopConf: Configuration =
    sparkSession.sessionState.newHadoopConfWithOptions(parameters)

  protected def leafFiles: mutable.LinkedHashMap[Path, FileStatus]

  protected def leafDirToChildrenFiles: Map[Path, Array[FileStatus]]

  private val caseInsensitiveMap = CaseInsensitiveMap(parameters)
  private val pathFilters = PathFilterFactory.create(caseInsensitiveMap)

  protected def matchPathPattern(file: FileStatus): Boolean =
    pathFilters.forall(_.accept(file))

  protected lazy val recursiveFileLookup: Boolean = {
    caseInsensitiveMap.getOrElse(FileIndexOptions.RECURSIVE_FILE_LOOKUP, "false").toBoolean
  }

  override def listFiles(
      partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    def isNonEmptyFile(f: FileStatus): Boolean = {
      isDataPath(f.getPath) && f.getLen > 0
    }

    val filePruningRunner = new FilePruningRunner(dataFilters)
    val selectedPartitions = if (partitionSpec().partitionColumns.isEmpty) {
      filePruningRunner.prune(
        PartitionDirectory(InternalRow.empty, allFiles().toArray.filter(isNonEmptyFile))) :: Nil
    } else {
      if (recursiveFileLookup) {
        throw new IllegalArgumentException(
          "Datasource with partition do not allow recursive file loading.")
      }
      prunePartitions(partitionFilters, partitionSpec()).map {
        case PartitionPath(values, path) =>
          val files: Seq[FileStatus] = leafDirToChildrenFiles.get(path) match {
            case Some(existingDir) =>
              // Directory has children files in it, return them
              existingDir.filter(f => matchPathPattern(f) && isNonEmptyFile(f)).toImmutableArraySeq

            case None =>
              // Directory does not exist, or has no children files
              Nil
          }
          filePruningRunner.prune(PartitionDirectory(values, files.toArray))
      }
    }
    logTrace("Selected files after partition pruning:\n\t" + selectedPartitions.mkString("\n\t"))
    selectedPartitions
  }

  /** Returns the list of files that will be read when scanning this relation. */
  override def inputFiles: Array[String] =
    allFiles().map(fs => SparkPath.fromFileStatus(fs).urlEncoded).toArray

  override def sizeInBytes: Long = allFiles().map(_.getLen).sum

  def allFiles(): Seq[FileStatus] = {
    val files = if (partitionSpec().partitionColumns.isEmpty && !recursiveFileLookup) {
      // For each of the root input paths, get the list of files inside them
      rootPaths.flatMap { path =>
        // Make the path qualified (consistent with listLeafFiles and bulkListLeafFiles).
        val fs = path.getFileSystem(hadoopConf)
        val qualifiedPathPre = fs.makeQualified(path)
        val qualifiedPath: Path = if (qualifiedPathPre.isRoot && !qualifiedPathPre.isAbsolute) {
          // SPARK-17613: Always append `Path.SEPARATOR` to the end of parent directories,
          // because the `leafFile.getParent` would have returned an absolute path with the
          // separator at the end.
          new Path(qualifiedPathPre, Path.SEPARATOR)
        } else {
          qualifiedPathPre
        }

        // There are three cases possible with each path
        // 1. The path is a directory and has children files in it. Then it must be present in
        //    leafDirToChildrenFiles as those children files will have been found as leaf files.
        //    Find its children files from leafDirToChildrenFiles and include them.
        // 2. The path is a file, then it will be present in leafFiles. Include this path.
        // 3. The path is a directory, but has no children files. Do not include this path.

        leafDirToChildrenFiles.get(qualifiedPath)
          .orElse { leafFiles.get(qualifiedPath).map(Array(_)) }
          .getOrElse(Array.empty)
      }
    } else {
      leafFiles.values.toSeq
    }
    files.filter(matchPathPattern)
  }

  protected def inferPartitioning(): PartitionSpec = {
    if (recursiveFileLookup) {
      PartitionSpec.emptySpec
    } else {
      // We use leaf dirs containing data files to discover the schema.
      val leafDirs = leafDirToChildrenFiles.filter { case (_, files) =>
        files.exists(f => isDataPath(f.getPath))
      }.keys.toSeq

      val caseInsensitiveOptions = CaseInsensitiveMap(parameters)
      val timeZoneId = caseInsensitiveOptions.get(FileIndexOptions.TIME_ZONE)
        .getOrElse(sparkSession.sessionState.conf.sessionLocalTimeZone)

      PartitioningUtils.parsePartitions(
        leafDirs,
        typeInference = sparkSession.sessionState.conf.partitionColumnTypeInferenceEnabled,
        basePaths = basePaths,
        userSpecifiedSchema = userSpecifiedSchema,
        caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis,
        validatePartitionColumns = sparkSession.sessionState.conf.validatePartitionColumns,
        timeZoneId = timeZoneId)
    }
  }

  private def prunePartitions(
      predicates: Seq[Expression],
      partitionSpec: PartitionSpec): Seq[PartitionPath] = {
    val PartitionSpec(partitionColumns, partitions) = partitionSpec
    val partitionColumnNames = partitionColumns.map(_.name).toSet
    val partitionPruningPredicates = predicates.filter {
      _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
    }

    if (partitionPruningPredicates.nonEmpty) {
      val predicate = partitionPruningPredicates.reduce(expressions.And)

      val boundPredicate = Predicate.createInterpreted(predicate.transform {
        case a: AttributeReference =>
          val index = partitionColumns.indexWhere(a.name == _.name)
          BoundReference(index, partitionColumns(index).dataType, nullable = true)
      })

      val selected = partitions.filter {
        case PartitionPath(values, _) => boundPredicate.eval(values)
      }
      logInfo {
        val total = partitions.length
        val selectedSize = selected.length
        val percentPruned = (1 - selectedSize.toDouble / total.toDouble) * 100
        log"Selected ${MDC(COUNT, selectedSize)} partitions out of ${MDC(TOTAL, total)}, " +
          log"pruned ${MDC(PERCENT, if (total == 0) "0" else percentPruned)} partitions."
      }

      selected
    } else {
      partitions
    }
  }

  /**
   * Contains a set of paths that are considered as the base dirs of the input datasets.
   * The partitioning discovery logic will make sure it will stop when it reaches any
   * base path.
   *
   * By default, the paths of the dataset provided by users will be base paths.
   * Below are three typical examples,
   * Case 1) `spark.read.parquet("/path/something=true/")`: the base path will be
   * `/path/something=true/`, and the returned DataFrame will not contain a column of `something`.
   * Case 2) `spark.read.parquet("/path/something=true/a.parquet")`: the base path will be
   * still `/path/something=true/`, and the returned DataFrame will also not contain a column of
   * `something`.
   * Case 3) `spark.read.parquet("/path/")`: the base path will be `/path/`, and the returned
   * DataFrame will have the column of `something`.
   *
   * Users also can override the basePath by setting `basePath` in the options to pass the new base
   * path to the data source.
   * For example, `spark.read.option("basePath", "/path/").parquet("/path/something=true/")`,
   * and the returned DataFrame will have the column of `something`.
   */
  private def basePaths: Set[Path] = {
    caseInsensitiveMap.get(FileIndexOptions.BASE_PATH_PARAM).map(new Path(_)) match {
      case Some(userDefinedBasePath) =>
        val fs = userDefinedBasePath.getFileSystem(hadoopConf)
        try {
          if (!fs.getFileStatus(userDefinedBasePath).isDirectory) {
            throw new IllegalArgumentException(s"Option '${FileIndexOptions.BASE_PATH_PARAM}' " +
              s"must be a directory")
          }
        } catch {
          case _: FileNotFoundException =>
            throw new IllegalArgumentException(s"Option '${FileIndexOptions.BASE_PATH_PARAM}' " +
             s"not found")
        }
        val qualifiedBasePath = fs.makeQualified(userDefinedBasePath)
        val qualifiedBasePathStr = qualifiedBasePath.toString
        rootPaths
          .find(!fs.makeQualified(_).toString.startsWith(qualifiedBasePathStr))
          .foreach { rp =>
            throw new IllegalArgumentException(
              s"Wrong basePath $userDefinedBasePath for the root path: $rp")
          }
        Set(qualifiedBasePath)

      case None =>
        rootPaths.map { path =>
          // Make the path qualified (consistent with listLeafFiles and bulkListLeafFiles).
          val qualifiedPath = path.getFileSystem(hadoopConf).makeQualified(path)
          if (leafFiles.contains(qualifiedPath)) qualifiedPath.getParent else qualifiedPath }.toSet
    }
  }

  // SPARK-15895: Metadata files (e.g. Parquet summary files) and temporary files should not be
  // counted as data files, so that they shouldn't participate partition discovery.
  private def isDataPath(path: Path): Boolean = {
    val name = path.getName
    !((name.startsWith("_") && !name.contains("=")) || name.startsWith("."))
  }
}
