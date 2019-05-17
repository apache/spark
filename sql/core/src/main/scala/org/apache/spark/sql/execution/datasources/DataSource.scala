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

import java.util.{Locale, ServiceConfigurationError, ServiceLoader}

import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, CatalogTable, CatalogUtils}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.execution.datasources.v2.orc.OrcDataSourceV2
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.sources.{RateStreamProvider, TextSocketSourceProvider}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{CalendarIntervalType, StructField, StructType}
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.util.Utils

/**
 * The main class responsible for representing a pluggable Data Source in Spark SQL. In addition to
 * acting as the canonical set of parameters that can describe a Data Source, this class is used to
 * resolve a description to a concrete implementation that can be used in a query plan
 * (either batch or streaming) or to write out data using an external library.
 *
 * From an end user's perspective a DataSource description can be created explicitly using
 * [[org.apache.spark.sql.DataFrameReader]] or CREATE TABLE USING DDL.  Additionally, this class is
 * used when resolving a description from a metastore to a concrete implementation.
 *
 * Many of the arguments to this class are optional, though depending on the specific API being used
 * these optional arguments might be filled in during resolution using either inference or external
 * metadata.  For example, when reading a partitioned table from a file system, partition columns
 * will be inferred from the directory layout even if they are not specified.
 *
 * @param paths A list of file system paths that hold data.  These will be globbed before and
 *              qualified. This option only works when reading from a [[FileFormat]].
 * @param userSpecifiedSchema An optional specification of the schema of the data. When present
 *                            we skip attempting to infer the schema.
 * @param partitionColumns A list of column names that the relation is partitioned by. This list is
 *                         generally empty during the read path, unless this DataSource is managed
 *                         by Hive. In these cases, during `resolveRelation`, we will call
 *                         `getOrInferFileFormatSchema` for file based DataSources to infer the
 *                         partitioning. In other cases, if this list is empty, then this table
 *                         is unpartitioned.
 * @param bucketSpec An optional specification for bucketing (hash-partitioning) of the data.
 * @param catalogTable Optional catalog table reference that can be used to push down operations
 *                     over the datasource to the catalog service.
 */
case class DataSource(
    sparkSession: SparkSession,
    className: String,
    paths: Seq[String] = Nil,
    userSpecifiedSchema: Option[StructType] = None,
    partitionColumns: Seq[String] = Seq.empty,
    bucketSpec: Option[BucketSpec] = None,
    options: Map[String, String] = Map.empty,
    catalogTable: Option[CatalogTable] = None) extends Logging {

  case class SourceInfo(name: String, schema: StructType, partitionColumns: Seq[String])

  lazy val providingClass: Class[_] = {
    val cls = DataSource.lookupDataSource(className, sparkSession.sessionState.conf)
    // `providingClass` is used for resolving data source relation for catalog tables.
    // As now catalog for data source V2 is under development, here we fall back all the
    // [[FileDataSourceV2]] to [[FileFormat]] to guarantee the current catalog works.
    // [[FileDataSourceV2]] will still be used if we call the load()/save() method in
    // [[DataFrameReader]]/[[DataFrameWriter]], since they use method `lookupDataSource`
    // instead of `providingClass`.
    cls.newInstance() match {
      case f: FileDataSourceV2 => f.fallbackFileFormat
      case _ => cls
    }
  }
  lazy val sourceInfo: SourceInfo = sourceSchema()
  private val caseInsensitiveOptions = CaseInsensitiveMap(options)
  private val equality = sparkSession.sessionState.conf.resolver

  bucketSpec.map { bucket =>
    SchemaUtils.checkColumnNameDuplication(
      bucket.bucketColumnNames, "in the bucket definition", equality)
    SchemaUtils.checkColumnNameDuplication(
      bucket.sortColumnNames, "in the sort definition", equality)
  }

  /**
   * Get the schema of the given FileFormat, if provided by `userSpecifiedSchema`, or try to infer
   * it. In the read path, only managed tables by Hive provide the partition columns properly when
   * initializing this class. All other file based data sources will try to infer the partitioning,
   * and then cast the inferred types to user specified dataTypes if the partition columns exist
   * inside `userSpecifiedSchema`, otherwise we can hit data corruption bugs like SPARK-18510.
   * This method will try to skip file scanning whether `userSpecifiedSchema` and
   * `partitionColumns` are provided. Here are some code paths that use this method:
   *   1. `spark.read` (no schema): Most amount of work. Infer both schema and partitioning columns
   *   2. `spark.read.schema(userSpecifiedSchema)`: Parse partitioning columns, cast them to the
   *     dataTypes provided in `userSpecifiedSchema` if they exist or fallback to inferred
   *     dataType if they don't.
   *   3. `spark.readStream.schema(userSpecifiedSchema)`: For streaming use cases, users have to
   *     provide the schema. Here, we also perform partition inference like 2, and try to use
   *     dataTypes in `userSpecifiedSchema`. All subsequent triggers for this stream will re-use
   *     this information, therefore calls to this method should be very cheap, i.e. there won't
   *     be any further inference in any triggers.
   *
   * @param format the file format object for this DataSource
   * @param getFileIndex [[InMemoryFileIndex]] for getting partition schema and file list
   * @return A pair of the data schema (excluding partition columns) and the schema of the partition
   *         columns.
   */
  private def getOrInferFileFormatSchema(
      format: FileFormat,
      getFileIndex: () => InMemoryFileIndex): (StructType, StructType) = {
    lazy val tempFileIndex = getFileIndex()

    val partitionSchema = if (partitionColumns.isEmpty) {
      // Try to infer partitioning, because no DataSource in the read path provides the partitioning
      // columns properly unless it is a Hive DataSource
      tempFileIndex.partitionSchema
    } else {
      // maintain old behavior before SPARK-18510. If userSpecifiedSchema is empty used inferred
      // partitioning
      if (userSpecifiedSchema.isEmpty) {
        val inferredPartitions = tempFileIndex.partitionSchema
        inferredPartitions
      } else {
        val partitionFields = partitionColumns.map { partitionColumn =>
          userSpecifiedSchema.flatMap(_.find(c => equality(c.name, partitionColumn))).orElse {
            val inferredPartitions = tempFileIndex.partitionSchema
            val inferredOpt = inferredPartitions.find(p => equality(p.name, partitionColumn))
            if (inferredOpt.isDefined) {
              logDebug(
                s"""Type of partition column: $partitionColumn not found in specified schema
                   |for $format.
                   |User Specified Schema
                   |=====================
                   |${userSpecifiedSchema.orNull}
                   |
                   |Falling back to inferred dataType if it exists.
                 """.stripMargin)
            }
            inferredOpt
          }.getOrElse {
            throw new AnalysisException(s"Failed to resolve the schema for $format for " +
              s"the partition column: $partitionColumn. It must be specified manually.")
          }
        }
        StructType(partitionFields)
      }
    }

    val dataSchema = userSpecifiedSchema.map { schema =>
      StructType(schema.filterNot(f => partitionSchema.exists(p => equality(p.name, f.name))))
    }.orElse {
      format.inferSchema(
        sparkSession,
        caseInsensitiveOptions,
        tempFileIndex.allFiles())
    }.getOrElse {
      throw new AnalysisException(
        s"Unable to infer schema for $format. It must be specified manually.")
    }

    // We just print a waring message if the data schema and partition schema have the duplicate
    // columns. This is because we allow users to do so in the previous Spark releases and
    // we have the existing tests for the cases (e.g., `ParquetHadoopFsRelationSuite`).
    // See SPARK-18108 and SPARK-21144 for related discussions.
    try {
      SchemaUtils.checkColumnNameDuplication(
        (dataSchema ++ partitionSchema).map(_.name),
        "in the data schema and the partition schema",
        equality)
    } catch {
      case e: AnalysisException => logWarning(e.getMessage)
    }

    (dataSchema, partitionSchema)
  }

  /** Returns the name and schema of the source that can be used to continually read data. */
  private def sourceSchema(): SourceInfo = {
    providingClass.getConstructor().newInstance() match {
      case s: StreamSourceProvider =>
        val (name, schema) = s.sourceSchema(
          sparkSession.sqlContext, userSpecifiedSchema, className, caseInsensitiveOptions)
        SourceInfo(name, schema, Nil)

      case format: FileFormat =>
        val path = caseInsensitiveOptions.getOrElse("path", {
          throw new IllegalArgumentException("'path' is not specified")
        })

        // Check whether the path exists if it is not a glob pattern.
        // For glob pattern, we do not check it because the glob pattern might only make sense
        // once the streaming job starts and some upstream source starts dropping data.
        val hdfsPath = new Path(path)
        if (!SparkHadoopUtil.get.isGlobPath(hdfsPath)) {
          val fs = hdfsPath.getFileSystem(sparkSession.sessionState.newHadoopConf())
          if (!fs.exists(hdfsPath)) {
            throw new AnalysisException(s"Path does not exist: $path")
          }
        }

        val isSchemaInferenceEnabled = sparkSession.sessionState.conf.streamingSchemaInference
        val isTextSource = providingClass == classOf[text.TextFileFormat]
        // If the schema inference is disabled, only text sources require schema to be specified
        if (!isSchemaInferenceEnabled && !isTextSource && userSpecifiedSchema.isEmpty) {
          throw new IllegalArgumentException(
            "Schema must be specified when creating a streaming source DataFrame. " +
              "If some files already exist in the directory, then depending on the file format " +
              "you may be able to create a static DataFrame on that directory with " +
              "'spark.read.load(directory)' and infer schema from it.")
        }

        val (dataSchema, partitionSchema) = getOrInferFileFormatSchema(format, () => {
          // The operations below are expensive therefore try not to do them if we don't need to,
          // e.g., in streaming mode, we have already inferred and registered partition columns,
          // we will never have to materialize the lazy val below
          val globbedPaths =
            checkAndGlobPathIfNecessary(checkEmptyGlobPath = false, checkFilesExist = false)
          createInMemoryFileIndex(globbedPaths)
        })
        SourceInfo(
          s"FileSource[$path]",
          StructType(dataSchema ++ partitionSchema),
          partitionSchema.fieldNames)

      case _ =>
        throw new UnsupportedOperationException(
          s"Data source $className does not support streamed reading")
    }
  }

  /** Returns a source that can be used to continually read data. */
  def createSource(metadataPath: String): Source = {
    providingClass.getConstructor().newInstance() match {
      case s: StreamSourceProvider =>
        s.createSource(
          sparkSession.sqlContext,
          metadataPath,
          userSpecifiedSchema,
          className,
          caseInsensitiveOptions)

      case format: FileFormat =>
        val path = caseInsensitiveOptions.getOrElse("path", {
          throw new IllegalArgumentException("'path' is not specified")
        })
        new FileStreamSource(
          sparkSession = sparkSession,
          path = path,
          fileFormatClassName = className,
          schema = sourceInfo.schema,
          partitionColumns = sourceInfo.partitionColumns,
          metadataPath = metadataPath,
          options = caseInsensitiveOptions)
      case _ =>
        throw new UnsupportedOperationException(
          s"Data source $className does not support streamed reading")
    }
  }

  /** Returns a sink that can be used to continually write data. */
  def createSink(outputMode: OutputMode): Sink = {
    providingClass.getConstructor().newInstance() match {
      case s: StreamSinkProvider =>
        s.createSink(sparkSession.sqlContext, caseInsensitiveOptions, partitionColumns, outputMode)

      case fileFormat: FileFormat =>
        val path = caseInsensitiveOptions.getOrElse("path", {
          throw new IllegalArgumentException("'path' is not specified")
        })
        if (outputMode != OutputMode.Append) {
          throw new AnalysisException(
            s"Data source $className does not support $outputMode output mode")
        }
        new FileStreamSink(sparkSession, path, fileFormat, partitionColumns, caseInsensitiveOptions)

      case _ =>
        throw new UnsupportedOperationException(
          s"Data source $className does not support streamed writing")
    }
  }

  /**
   * Create a resolved [[BaseRelation]] that can be used to read data from or write data into this
   * [[DataSource]]
   *
   * @param checkFilesExist Whether to confirm that the files exist when generating the
   *                        non-streaming file based datasource. StructuredStreaming jobs already
   *                        list file existence, and when generating incremental jobs, the batch
   *                        is considered as a non-streaming file based data source. Since we know
   *                        that files already exist, we don't need to check them again.
   */
  def resolveRelation(checkFilesExist: Boolean = true): BaseRelation = {
    val relation = (providingClass.getConstructor().newInstance(), userSpecifiedSchema) match {
      // TODO: Throw when too much is given.
      case (dataSource: SchemaRelationProvider, Some(schema)) =>
        dataSource.createRelation(sparkSession.sqlContext, caseInsensitiveOptions, schema)
      case (dataSource: RelationProvider, None) =>
        dataSource.createRelation(sparkSession.sqlContext, caseInsensitiveOptions)
      case (_: SchemaRelationProvider, None) =>
        throw new AnalysisException(s"A schema needs to be specified when using $className.")
      case (dataSource: RelationProvider, Some(schema)) =>
        val baseRelation =
          dataSource.createRelation(sparkSession.sqlContext, caseInsensitiveOptions)
        if (baseRelation.schema != schema) {
          throw new AnalysisException(s"$className does not allow user-specified schemas.")
        }
        baseRelation

      // We are reading from the results of a streaming query. Load files from the metadata log
      // instead of listing them using HDFS APIs.
      case (format: FileFormat, _)
          if FileStreamSink.hasMetadata(
            caseInsensitiveOptions.get("path").toSeq ++ paths,
            sparkSession.sessionState.newHadoopConf(),
            sparkSession.sessionState.conf) =>
        val basePath = new Path((caseInsensitiveOptions.get("path").toSeq ++ paths).head)
        val fileCatalog = new MetadataLogFileIndex(sparkSession, basePath,
          caseInsensitiveOptions, userSpecifiedSchema)
        val dataSchema = userSpecifiedSchema.orElse {
          format.inferSchema(
            sparkSession,
            caseInsensitiveOptions,
            fileCatalog.allFiles())
        }.getOrElse {
          throw new AnalysisException(
            s"Unable to infer schema for $format at ${fileCatalog.allFiles().mkString(",")}. " +
                "It must be specified manually")
        }

        HadoopFsRelation(
          fileCatalog,
          partitionSchema = fileCatalog.partitionSchema,
          dataSchema = dataSchema,
          bucketSpec = None,
          format,
          caseInsensitiveOptions)(sparkSession)

      // This is a non-streaming file based datasource.
      case (format: FileFormat, _) =>
        val globbedPaths =
          checkAndGlobPathIfNecessary(checkEmptyGlobPath = true, checkFilesExist = checkFilesExist)
        val useCatalogFileIndex = sparkSession.sqlContext.conf.manageFilesourcePartitions &&
          catalogTable.isDefined && catalogTable.get.tracksPartitionsInCatalog &&
          catalogTable.get.partitionColumnNames.nonEmpty
        val (fileCatalog, dataSchema, partitionSchema) = if (useCatalogFileIndex) {
          val defaultTableSize = sparkSession.sessionState.conf.defaultSizeInBytes
          val index = new CatalogFileIndex(
            sparkSession,
            catalogTable.get,
            catalogTable.get.stats.map(_.sizeInBytes.toLong).getOrElse(defaultTableSize))
          (index, catalogTable.get.dataSchema, catalogTable.get.partitionSchema)
        } else {
          val index = createInMemoryFileIndex(globbedPaths)
          val (resultDataSchema, resultPartitionSchema) =
            getOrInferFileFormatSchema(format, () => index)
          (index, resultDataSchema, resultPartitionSchema)
        }

        HadoopFsRelation(
          fileCatalog,
          partitionSchema = partitionSchema,
          dataSchema = dataSchema.asNullable,
          bucketSpec = bucketSpec,
          format,
          caseInsensitiveOptions)(sparkSession)

      case _ =>
        throw new AnalysisException(
          s"$className is not a valid Spark SQL Data Source.")
    }

    relation match {
      case hs: HadoopFsRelation =>
        SchemaUtils.checkColumnNameDuplication(
          hs.dataSchema.map(_.name),
          "in the data schema",
          equality)
        SchemaUtils.checkColumnNameDuplication(
          hs.partitionSchema.map(_.name),
          "in the partition schema",
          equality)
        DataSourceUtils.verifySchema(hs.fileFormat, hs.dataSchema)
      case _ =>
        SchemaUtils.checkColumnNameDuplication(
          relation.schema.map(_.name),
          "in the data schema",
          equality)
    }

    relation
  }

  /**
   * Creates a command node to write the given [[LogicalPlan]] out to the given [[FileFormat]].
   * The returned command is unresolved and need to be analyzed.
   */
  private def planForWritingFileFormat(
      format: FileFormat, mode: SaveMode, data: LogicalPlan): InsertIntoHadoopFsRelationCommand = {
    // Don't glob path for the write path.  The contracts here are:
    //  1. Only one output path can be specified on the write path;
    //  2. Output path must be a legal HDFS style file system path;
    //  3. It's OK that the output path doesn't exist yet;
    val allPaths = paths ++ caseInsensitiveOptions.get("path")
    val outputPath = if (allPaths.length == 1) {
      val path = new Path(allPaths.head)
      val fs = path.getFileSystem(sparkSession.sessionState.newHadoopConf())
      path.makeQualified(fs.getUri, fs.getWorkingDirectory)
    } else {
      throw new IllegalArgumentException("Expected exactly one path to be specified, but " +
        s"got: ${allPaths.mkString(", ")}")
    }

    val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
    PartitioningUtils.validatePartitionColumn(data.schema, partitionColumns, caseSensitive)

    val fileIndex = catalogTable.map(_.identifier).map { tableIdent =>
      sparkSession.table(tableIdent).queryExecution.analyzed.collect {
        case LogicalRelation(t: HadoopFsRelation, _, _, _) => t.location
      }.head
    }
    // For partitioned relation r, r.schema's column ordering can be different from the column
    // ordering of data.logicalPlan (partition columns are all moved after data column).  This
    // will be adjusted within InsertIntoHadoopFsRelation.
    InsertIntoHadoopFsRelationCommand(
      outputPath = outputPath,
      staticPartitions = Map.empty,
      ifPartitionNotExists = false,
      partitionColumns = partitionColumns.map(UnresolvedAttribute.quoted),
      bucketSpec = bucketSpec,
      fileFormat = format,
      options = options,
      query = data,
      mode = mode,
      catalogTable = catalogTable,
      fileIndex = fileIndex,
      outputColumnNames = data.output.map(_.name))
  }

  /**
   * Writes the given [[LogicalPlan]] out to this [[DataSource]] and returns a [[BaseRelation]] for
   * the following reading.
   *
   * @param mode The save mode for this writing.
   * @param data The input query plan that produces the data to be written. Note that this plan
   *             is analyzed and optimized.
   * @param outputColumnNames The original output column names of the input query plan. The
   *                          optimizer may not preserve the output column's names' case, so we need
   *                          this parameter instead of `data.output`.
   * @param physicalPlan The physical plan of the input query plan. We should run the writing
   *                     command with this physical plan instead of creating a new physical plan,
   *                     so that the metrics can be correctly linked to the given physical plan and
   *                     shown in the web UI.
   */
  def writeAndRead(
      mode: SaveMode,
      data: LogicalPlan,
      outputColumnNames: Seq[String],
      physicalPlan: SparkPlan): BaseRelation = {
    val outputColumns = DataWritingCommand.logicalPlanOutputWithNames(data, outputColumnNames)
    if (outputColumns.map(_.dataType).exists(_.isInstanceOf[CalendarIntervalType])) {
      throw new AnalysisException("Cannot save interval data type into external storage.")
    }

    providingClass.getConstructor().newInstance() match {
      case dataSource: CreatableRelationProvider =>
        dataSource.createRelation(
          sparkSession.sqlContext, mode, caseInsensitiveOptions, Dataset.ofRows(sparkSession, data))
      case format: FileFormat =>
        val cmd = planForWritingFileFormat(format, mode, data)
        val resolvedPartCols = cmd.partitionColumns.map { col =>
          // The partition columns created in `planForWritingFileFormat` should always be
          // `UnresolvedAttribute` with a single name part.
          assert(col.isInstanceOf[UnresolvedAttribute])
          val unresolved = col.asInstanceOf[UnresolvedAttribute]
          assert(unresolved.nameParts.length == 1)
          val name = unresolved.nameParts.head
          outputColumns.find(a => equality(a.name, name)).getOrElse {
            throw new AnalysisException(
              s"Unable to resolve $name given [${data.output.map(_.name).mkString(", ")}]")
          }
        }
        val resolved = cmd.copy(
          partitionColumns = resolvedPartCols,
          outputColumnNames = outputColumnNames)
        resolved.run(sparkSession, physicalPlan)
        // Replace the schema with that of the DataFrame we just wrote out to avoid re-inferring
        copy(userSpecifiedSchema = Some(outputColumns.toStructType.asNullable)).resolveRelation()
      case _ =>
        sys.error(s"${providingClass.getCanonicalName} does not allow create table as select.")
    }
  }

  /**
   * Returns a logical plan to write the given [[LogicalPlan]] out to this [[DataSource]].
   */
  def planForWriting(mode: SaveMode, data: LogicalPlan): LogicalPlan = {
    if (data.schema.map(_.dataType).exists(_.isInstanceOf[CalendarIntervalType])) {
      throw new AnalysisException("Cannot save interval data type into external storage.")
    }

    providingClass.getConstructor().newInstance() match {
      case dataSource: CreatableRelationProvider =>
        SaveIntoDataSourceCommand(data, dataSource, caseInsensitiveOptions, mode)
      case format: FileFormat =>
        DataSource.validateSchema(data.schema)
        planForWritingFileFormat(format, mode, data)
      case _ =>
        sys.error(s"${providingClass.getCanonicalName} does not allow create table as select.")
    }
  }

  /** Returns an [[InMemoryFileIndex]] that can be used to get partition schema and file list. */
  private def createInMemoryFileIndex(globbedPaths: Seq[Path]): InMemoryFileIndex = {
    val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)
    new InMemoryFileIndex(
      sparkSession, globbedPaths, options, userSpecifiedSchema, fileStatusCache)
  }

  /**
   * Checks and returns files in all the paths.
   */
  private def checkAndGlobPathIfNecessary(
      checkEmptyGlobPath: Boolean,
      checkFilesExist: Boolean): Seq[Path] = {
    val allPaths = caseInsensitiveOptions.get("path") ++ paths
    val hadoopConf = sparkSession.sessionState.newHadoopConf()

    DataSource.checkAndGlobPathIfNecessary(allPaths.toSeq, hadoopConf,
      checkEmptyGlobPath, checkFilesExist)
  }
}

object DataSource extends Logging {

  /** A map to maintain backward compatibility in case we move data sources around. */
  private val backwardCompatibilityMap: Map[String, String] = {
    val jdbc = classOf[JdbcRelationProvider].getCanonicalName
    val json = classOf[JsonFileFormat].getCanonicalName
    val parquet = classOf[ParquetFileFormat].getCanonicalName
    val csv = classOf[CSVFileFormat].getCanonicalName
    val libsvm = "org.apache.spark.ml.source.libsvm.LibSVMFileFormat"
    val orc = "org.apache.spark.sql.hive.orc.OrcFileFormat"
    val nativeOrc = classOf[OrcFileFormat].getCanonicalName
    val socket = classOf[TextSocketSourceProvider].getCanonicalName
    val rate = classOf[RateStreamProvider].getCanonicalName

    Map(
      "org.apache.spark.sql.jdbc" -> jdbc,
      "org.apache.spark.sql.jdbc.DefaultSource" -> jdbc,
      "org.apache.spark.sql.execution.datasources.jdbc.DefaultSource" -> jdbc,
      "org.apache.spark.sql.execution.datasources.jdbc" -> jdbc,
      "org.apache.spark.sql.json" -> json,
      "org.apache.spark.sql.json.DefaultSource" -> json,
      "org.apache.spark.sql.execution.datasources.json" -> json,
      "org.apache.spark.sql.execution.datasources.json.DefaultSource" -> json,
      "org.apache.spark.sql.parquet" -> parquet,
      "org.apache.spark.sql.parquet.DefaultSource" -> parquet,
      "org.apache.spark.sql.execution.datasources.parquet" -> parquet,
      "org.apache.spark.sql.execution.datasources.parquet.DefaultSource" -> parquet,
      "org.apache.spark.sql.hive.orc.DefaultSource" -> orc,
      "org.apache.spark.sql.hive.orc" -> orc,
      "org.apache.spark.sql.execution.datasources.orc.DefaultSource" -> nativeOrc,
      "org.apache.spark.sql.execution.datasources.orc" -> nativeOrc,
      "org.apache.spark.ml.source.libsvm.DefaultSource" -> libsvm,
      "org.apache.spark.ml.source.libsvm" -> libsvm,
      "com.databricks.spark.csv" -> csv,
      "org.apache.spark.sql.execution.streaming.TextSocketSourceProvider" -> socket,
      "org.apache.spark.sql.execution.streaming.RateSourceProvider" -> rate
    )
  }

  /**
   * Class that were removed in Spark 2.0. Used to detect incompatibility libraries for Spark 2.0.
   */
  private val spark2RemovedClasses = Set(
    "org.apache.spark.sql.DataFrame",
    "org.apache.spark.sql.sources.HadoopFsRelationProvider",
    "org.apache.spark.Logging")

  /** Given a provider name, look up the data source class definition. */
  def lookupDataSource(provider: String, conf: SQLConf): Class[_] = {
    val provider1 = backwardCompatibilityMap.getOrElse(provider, provider) match {
      case name if name.equalsIgnoreCase("orc") &&
          conf.getConf(SQLConf.ORC_IMPLEMENTATION) == "native" =>
        classOf[OrcDataSourceV2].getCanonicalName
      case name if name.equalsIgnoreCase("orc") &&
          conf.getConf(SQLConf.ORC_IMPLEMENTATION) == "hive" =>
        "org.apache.spark.sql.hive.orc.OrcFileFormat"
      case "com.databricks.spark.avro" if conf.replaceDatabricksSparkAvroEnabled =>
        "org.apache.spark.sql.avro.AvroFileFormat"
      case name => name
    }
    val provider2 = s"$provider1.DefaultSource"
    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoader = ServiceLoader.load(classOf[DataSourceRegister], loader)

    try {
      serviceLoader.asScala.filter(_.shortName().equalsIgnoreCase(provider1)).toList match {
        // the provider format did not match any given registered aliases
        case Nil =>
          try {
            Try(loader.loadClass(provider1)).orElse(Try(loader.loadClass(provider2))) match {
              case Success(dataSource) =>
                // Found the data source using fully qualified path
                dataSource
              case Failure(error) =>
                if (provider1.startsWith("org.apache.spark.sql.hive.orc")) {
                  throw new AnalysisException(
                    "Hive built-in ORC data source must be used with Hive support enabled. " +
                    "Please use the native ORC data source by setting 'spark.sql.orc.impl' to " +
                    "'native'")
                } else if (provider1.toLowerCase(Locale.ROOT) == "avro" ||
                  provider1 == "com.databricks.spark.avro" ||
                  provider1 == "org.apache.spark.sql.avro") {
                  throw new AnalysisException(
                    s"Failed to find data source: $provider1. Avro is built-in but external data " +
                    "source module since Spark 2.4. Please deploy the application as per " +
                    "the deployment section of \"Apache Avro Data Source Guide\".")
                } else if (provider1.toLowerCase(Locale.ROOT) == "kafka") {
                  throw new AnalysisException(
                    s"Failed to find data source: $provider1. Please deploy the application as " +
                    "per the deployment section of " +
                    "\"Structured Streaming + Kafka Integration Guide\".")
                } else {
                  throw new ClassNotFoundException(
                    s"Failed to find data source: $provider1. Please find packages at " +
                      "http://spark.apache.org/third-party-projects.html",
                    error)
                }
            }
          } catch {
            case e: NoClassDefFoundError => // This one won't be caught by Scala NonFatal
              // NoClassDefFoundError's class name uses "/" rather than "." for packages
              val className = e.getMessage.replaceAll("/", ".")
              if (spark2RemovedClasses.contains(className)) {
                throw new ClassNotFoundException(s"$className was removed in Spark 2.0. " +
                  "Please check if your library is compatible with Spark 2.0", e)
              } else {
                throw e
              }
          }
        case head :: Nil =>
          // there is exactly one registered alias
          head.getClass
        case sources =>
          // There are multiple registered aliases for the input. If there is single datasource
          // that has "org.apache.spark" package in the prefix, we use it considering it is an
          // internal datasource within Spark.
          val sourceNames = sources.map(_.getClass.getName)
          val internalSources = sources.filter(_.getClass.getName.startsWith("org.apache.spark"))
          if (internalSources.size == 1) {
            logWarning(s"Multiple sources found for $provider1 (${sourceNames.mkString(", ")}), " +
              s"defaulting to the internal datasource (${internalSources.head.getClass.getName}).")
            internalSources.head.getClass
          } else {
            throw new AnalysisException(s"Multiple sources found for $provider1 " +
              s"(${sourceNames.mkString(", ")}), please specify the fully qualified class name.")
          }
      }
    } catch {
      case e: ServiceConfigurationError if e.getCause.isInstanceOf[NoClassDefFoundError] =>
        // NoClassDefFoundError's class name uses "/" rather than "." for packages
        val className = e.getCause.getMessage.replaceAll("/", ".")
        if (spark2RemovedClasses.contains(className)) {
          throw new ClassNotFoundException(s"Detected an incompatible DataSourceRegister. " +
            "Please remove the incompatible library from classpath or upgrade it. " +
            s"Error: ${e.getMessage}", e)
        } else {
          throw e
        }
    }
  }

  /**
   * Checks and returns files in all the paths.
   */
  private[sql] def checkAndGlobPathIfNecessary(
      paths: Seq[String],
      hadoopConf: Configuration,
      checkEmptyGlobPath: Boolean,
      checkFilesExist: Boolean): Seq[Path] = {
    val allGlobPath = paths.flatMap { path =>
      val hdfsPath = new Path(path)
      val fs = hdfsPath.getFileSystem(hadoopConf)
      val qualified = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
      val globPath = SparkHadoopUtil.get.globPathIfNecessary(fs, qualified)

      if (checkEmptyGlobPath && globPath.isEmpty) {
        throw new AnalysisException(s"Path does not exist: $qualified")
      }

      // Sufficient to check head of the globPath seq for non-glob scenario
      // Don't need to check once again if files exist in streaming mode
      if (checkFilesExist && !fs.exists(globPath.head)) {
        throw new AnalysisException(s"Path does not exist: ${globPath.head}")
      }
      globPath
    }

    if (checkFilesExist) {
      val (filteredOut, filteredIn) = allGlobPath.partition { path =>
        InMemoryFileIndex.shouldFilterOut(path.getName)
      }
      if (filteredIn.isEmpty) {
        logWarning(
          s"All paths were ignored:\n  ${filteredOut.mkString("\n  ")}")
      } else {
        logDebug(
          s"Some paths were ignored:\n  ${filteredOut.mkString("\n  ")}")
      }
    }

    allGlobPath
  }

  /**
   * When creating a data source table, the `path` option has a special meaning: the table location.
   * This method extracts the `path` option and treat it as table location to build a
   * [[CatalogStorageFormat]]. Note that, the `path` option is removed from options after this.
   */
  def buildStorageFormatFromOptions(options: Map[String, String]): CatalogStorageFormat = {
    val path = CaseInsensitiveMap(options).get("path")
    val optionsWithoutPath = options.filterKeys(_.toLowerCase(Locale.ROOT) != "path")
    CatalogStorageFormat.empty.copy(
      locationUri = path.map(CatalogUtils.stringToURI), properties = optionsWithoutPath)
  }

  /**
   * Called before writing into a FileFormat based data source to make sure the
   * supplied schema is not empty.
   * @param schema
   */
  def validateSchema(schema: StructType): Unit = {
    def hasEmptySchema(schema: StructType): Boolean = {
      schema.size == 0 || schema.find {
        case StructField(_, b: StructType, _, _) => hasEmptySchema(b)
        case _ => false
      }.isDefined
    }


    if (hasEmptySchema(schema)) {
      throw new AnalysisException(
        s"""
           |Datasource does not support writing empty or nested empty schemas.
           |Please make sure the data schema has at least one or more column(s).
         """.stripMargin)
    }
  }
}
