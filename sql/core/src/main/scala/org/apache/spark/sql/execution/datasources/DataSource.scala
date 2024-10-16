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

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{CLASS_NAME, DATA_SOURCE, DATA_SOURCES, PATHS}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, CatalogTable, CatalogUtils}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, TypeUtils}
import org.apache.spark.sql.connector.catalog.TableProvider
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.execution.datasources.v2.orc.OrcDataSourceV2
import org.apache.spark.sql.execution.datasources.v2.python.PythonDataSourceV2
import org.apache.spark.sql.execution.datasources.xml.XmlFileFormat
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.sources.{RateStreamProvider, TextSocketSourceProvider}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.util.{HadoopFSUtils, ThreadUtils, Utils}
import org.apache.spark.util.ArrayImplicits._

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
 * @param paths A list of file system paths that hold data. These will be globbed before if
 *              the "__globPaths__" option is true, and will be qualified. This option only works
 *              when reading from a [[FileFormat]]. These paths are expected to be hadoop [[Path]]
 *              strings.
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
    cls.getDeclaredConstructor().newInstance() match {
      case f: FileDataSourceV2 => f.fallbackFileFormat
      case _ => cls
    }
  }

  private[sql] def providingInstance(): Any = providingClass.getConstructor().newInstance()

  private def newHadoopConfiguration(): Configuration =
    sparkSession.sessionState.newHadoopConfWithOptions(options)

  lazy val sourceInfo: SourceInfo = sourceSchema()
  private val caseInsensitiveOptions = CaseInsensitiveMap(options)
  private val equality = sparkSession.sessionState.conf.resolver

  /**
   * Whether or not paths should be globbed before being used to access files.
   */
  def globPaths: Boolean = {
    options.get(DataSource.GLOB_PATHS_KEY)
      .map(_ == "true")
      .getOrElse(true)
  }

  bucketSpec.foreach { bucket =>
    SchemaUtils.checkColumnNameDuplication(bucket.bucketColumnNames, equality)
    SchemaUtils.checkColumnNameDuplication(bucket.sortColumnNames, equality)
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
            throw QueryCompilationErrors.partitionColumnNotSpecifiedError(
              format.toString, partitionColumn)
          }
        }
        StructType(partitionFields)
      }
    }

    val dataSchema = userSpecifiedSchema.map { schema =>
      StructType(schema.filterNot(f => partitionSchema.exists(p => equality(p.name, f.name))))
    }.orElse {
      // Remove "path" option so that it is not added to the paths returned by
      // `tempFileIndex.allFiles()`.
      format.inferSchema(
        sparkSession,
        caseInsensitiveOptions - "path",
        tempFileIndex.allFiles())
    }.getOrElse {
      throw QueryCompilationErrors.dataSchemaNotSpecifiedError(format.toString)
    }

    // We just print a warning message if the data schema and partition schema have the duplicate
    // columns. This is because we allow users to do so in the previous Spark releases and
    // we have the existing tests for the cases (e.g., `ParquetHadoopFsRelationSuite`).
    // See SPARK-18108 and SPARK-21144 for related discussions.
    try {
      SchemaUtils.checkColumnNameDuplication(
        (dataSchema ++ partitionSchema).map(_.name),
        equality)
    } catch {
      case e: AnalysisException => logWarning(e.getMessage)
    }

    (dataSchema, partitionSchema)
  }

  /** Returns the name and schema of the source that can be used to continually read data. */
  private def sourceSchema(): SourceInfo = {
    providingInstance() match {
      case s: StreamSourceProvider =>
        val (name, schema) = s.sourceSchema(
          sparkSession.sqlContext, userSpecifiedSchema, className, caseInsensitiveOptions)
        SourceInfo(name, schema, Nil)

      case format: FileFormat =>
        val path = caseInsensitiveOptions.getOrElse("path", {
          throw QueryExecutionErrors.dataPathNotSpecifiedError()
        })

        // Check whether the path exists if it is not a glob pattern.
        // For glob pattern, we do not check it because the glob pattern might only make sense
        // once the streaming job starts and some upstream source starts dropping data.
        val hdfsPath = new Path(path)
        if (!globPaths || !SparkHadoopUtil.get.isGlobPath(hdfsPath)) {
          val fs = hdfsPath.getFileSystem(newHadoopConfiguration())
          if (!fs.exists(hdfsPath)) {
            throw QueryCompilationErrors.dataPathNotExistError(path)
          }
        }

        val isSchemaInferenceEnabled = sparkSession.sessionState.conf.streamingSchemaInference
        val isTextSource = providingClass == classOf[text.TextFileFormat]
        // If the schema inference is disabled, only text sources require schema to be specified
        if (!isSchemaInferenceEnabled && !isTextSource && userSpecifiedSchema.isEmpty) {
          throw QueryExecutionErrors.createStreamingSourceNotSpecifySchemaError()
        }

        val (dataSchema, partitionSchema) = getOrInferFileFormatSchema(format, () => {
          // The operations below are expensive therefore try not to do them if we don't need to,
          // e.g., in streaming mode, we have already inferred and registered partition columns,
          // we will never have to materialize the lazy val below
          val globbedPaths =
            checkAndGlobPathIfNecessary(checkEmptyGlobPath = false, checkFilesExist = false)
          createInMemoryFileIndex(globbedPaths)
        })
        val forceNullable = sparkSession.sessionState.conf
          .getConf(SQLConf.FILE_SOURCE_SCHEMA_FORCE_NULLABLE)
        val sourceDataSchema = if (forceNullable) dataSchema.asNullable else dataSchema
        SourceInfo(
          s"FileSource[$path]",
          StructType(sourceDataSchema ++ partitionSchema),
          partitionSchema.fieldNames.toImmutableArraySeq)

      case _ =>
        throw QueryExecutionErrors.streamedOperatorUnsupportedByDataSourceError(
          className, "reading")
    }
  }

  /** Returns a source that can be used to continually read data. */
  def createSource(metadataPath: String): Source = {
    providingInstance() match {
      case s: StreamSourceProvider =>
        s.createSource(
          sparkSession.sqlContext,
          metadataPath,
          userSpecifiedSchema,
          className,
          caseInsensitiveOptions)

      case format: FileFormat =>
        val path = caseInsensitiveOptions.getOrElse("path", {
          throw QueryExecutionErrors.dataPathNotSpecifiedError()
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
        throw QueryExecutionErrors.streamedOperatorUnsupportedByDataSourceError(
          className, "reading")
    }
  }

  /** Returns a sink that can be used to continually write data. */
  def createSink(outputMode: OutputMode): Sink = {
    providingInstance() match {
      case s: StreamSinkProvider =>
        s.createSink(sparkSession.sqlContext, caseInsensitiveOptions, partitionColumns, outputMode)

      case fileFormat: FileFormat =>
        val path = caseInsensitiveOptions.getOrElse("path", {
          throw QueryExecutionErrors.dataPathNotSpecifiedError()
        })
        if (outputMode != OutputMode.Append) {
          throw QueryCompilationErrors.dataSourceOutputModeUnsupportedError(className, outputMode)
        }
        new FileStreamSink(sparkSession, path, fileFormat, partitionColumns, caseInsensitiveOptions)

      case _ =>
        throw QueryExecutionErrors.streamedOperatorUnsupportedByDataSourceError(
          className, "writing")
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
    val relation = (providingInstance(), userSpecifiedSchema) match {
      // TODO: Throw when too much is given.
      case (dataSource: SchemaRelationProvider, Some(schema)) =>
        dataSource.createRelation(sparkSession.sqlContext, caseInsensitiveOptions, schema)
      case (dataSource: RelationProvider, None) =>
        dataSource.createRelation(sparkSession.sqlContext, caseInsensitiveOptions)
      case (_: SchemaRelationProvider, None) =>
        throw QueryCompilationErrors.schemaNotSpecifiedForSchemaRelationProviderError(className)
      case (dataSource: RelationProvider, Some(schema)) =>
        val baseRelation =
          dataSource.createRelation(sparkSession.sqlContext, caseInsensitiveOptions)
        if (!DataType.equalsIgnoreCompatibleNullability(baseRelation.schema, schema)) {
          throw QueryCompilationErrors.userSpecifiedSchemaMismatchActualSchemaError(
            schema, baseRelation.schema)
        }
        baseRelation

      // We are reading from the results of a streaming query. Load files from the metadata log
      // instead of listing them using HDFS APIs. Note that the config
      // `spark.sql.streaming.fileStreamSink.metadata.ignored` can be enabled to ignore the
      // metadata log.
      case (format: FileFormat, _)
          if FileStreamSink.hasMetadata(
            caseInsensitiveOptions.get("path").toSeq ++ paths,
            newHadoopConfiguration(),
            sparkSession.sessionState.conf) =>
        val basePath = new Path((caseInsensitiveOptions.get("path").toSeq ++ paths).head)
        val fileCatalog = new MetadataLogFileIndex(sparkSession, basePath,
          caseInsensitiveOptions, userSpecifiedSchema)
        val dataSchema = userSpecifiedSchema.orElse {
          // Remove "path" option so that it is not added to the paths returned by
          // `fileCatalog.allFiles()`.
          format.inferSchema(
            sparkSession,
            caseInsensitiveOptions - "path",
            fileCatalog.allFiles())
        }.getOrElse {
          throw QueryCompilationErrors.dataSchemaNotSpecifiedError(
            format.toString, fileCatalog.allFiles().mkString(","))
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
        val useCatalogFileIndex = sparkSession.sessionState.conf.manageFilesourcePartitions &&
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
          val globbedPaths = checkAndGlobPathIfNecessary(
            checkEmptyGlobPath = true, checkFilesExist = checkFilesExist)
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
        throw QueryCompilationErrors.invalidDataSourceError(className)
    }

    relation match {
      case hs: HadoopFsRelation =>
        SchemaUtils.checkSchemaColumnNameDuplication(
          hs.dataSchema,
          equality)
        SchemaUtils.checkSchemaColumnNameDuplication(
          hs.partitionSchema,
          equality)
        DataSourceUtils.verifySchema(hs.fileFormat, hs.dataSchema)
      case _ =>
        SchemaUtils.checkSchemaColumnNameDuplication(
          relation.schema,
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
      val fs = path.getFileSystem(newHadoopConfiguration())
      path.makeQualified(fs.getUri, fs.getWorkingDirectory)
    } else {
      throw QueryExecutionErrors.multiplePathsSpecifiedError(allPaths)
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
   */
  def writeAndRead(
      mode: SaveMode,
      data: LogicalPlan,
      outputColumnNames: Seq[String]): BaseRelation = {
    val outputColumns = DataWritingCommand.logicalPlanOutputWithNames(data, outputColumnNames)
    providingInstance() match {
      case dataSource: CreatableRelationProvider =>
        outputColumns.foreach { attr =>
          if (!dataSource.supportsDataType(attr.dataType)) {
            throw QueryCompilationErrors.dataTypeUnsupportedByDataSourceError(
              dataSource.toString, StructField(attr.toString, attr.dataType))
          }
        }
        dataSource.createRelation(
          sparkSession.sqlContext, mode, caseInsensitiveOptions, Dataset.ofRows(sparkSession, data))
      case format: FileFormat =>
        disallowWritingIntervals(
          outputColumns.toStructType.asNullable, format.toString, forbidAnsiIntervals = false)
        val cmd = planForWritingFileFormat(format, mode, data)
        val qe = sparkSession.sessionState.executePlan(cmd)
        qe.assertCommandExecuted()
        // Replace the schema with that of the DataFrame we just wrote out to avoid re-inferring
        copy(userSpecifiedSchema = Some(outputColumns.toStructType.asNullable)).resolveRelation()
      case _ => throw SparkException.internalError(
        s"${providingClass.getCanonicalName} does not allow create table as select.")
    }
  }

  /**
   * Returns a logical plan to write the given [[LogicalPlan]] out to this [[DataSource]].
   */
  def planForWriting(mode: SaveMode, data: LogicalPlan): LogicalPlan = {
    providingInstance() match {
      case dataSource: CreatableRelationProvider =>
        data.schema.foreach { field =>
          if (!dataSource.supportsDataType(field.dataType)) {
            throw QueryCompilationErrors.dataTypeUnsupportedByDataSourceError(
              dataSource.toString, field)
          }
        }
        SaveIntoDataSourceCommand(data, dataSource, caseInsensitiveOptions, mode)
      case format: FileFormat =>
        disallowWritingIntervals(data.schema, format.toString, forbidAnsiIntervals = false)
        DataSource.validateSchema(format.toString, data.schema, sparkSession.sessionState.conf)
        planForWritingFileFormat(format, mode, data)
      case _ => throw SparkException.internalError(
        s"${providingClass.getCanonicalName} does not allow create table as select.")
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
    DataSource.checkAndGlobPathIfNecessary(allPaths.toSeq, newHadoopConfiguration(),
      checkEmptyGlobPath, checkFilesExist, enableGlobbing = globPaths)
  }

  private def disallowWritingIntervals(
      outputColumns: Seq[StructField],
      format: String,
      forbidAnsiIntervals: Boolean): Unit = {
    outputColumns.foreach { field =>
      TypeUtils.invokeOnceForInterval(field.dataType, forbidAnsiIntervals) {
      throw QueryCompilationErrors.dataTypeUnsupportedByDataSourceError(
        format, field
      )}
    }
  }
}

object DataSource extends Logging {

  /** A map to maintain backward compatibility in case we move data sources around. */
  private val backwardCompatibilityMap: Map[String, String] = {
    val jdbc = classOf[JdbcRelationProvider].getCanonicalName
    val json = classOf[JsonFileFormat].getCanonicalName
    val xml = classOf[XmlFileFormat].getCanonicalName
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
      "com.databricks.spark.xml" -> xml,
      "org.apache.spark.sql.execution.datasources.xml" -> xml,
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
    lazy val isUserDefinedDataSource = SparkSession.getActiveSession.exists(
      _.sessionState.dataSourceManager.dataSourceExists(provider))

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
                  throw QueryCompilationErrors.orcNotUsedWithHiveEnabledError()
                } else if (provider1.toLowerCase(Locale.ROOT) == "avro" ||
                  provider1 == "com.databricks.spark.avro" ||
                  provider1 == "org.apache.spark.sql.avro") {
                  throw QueryCompilationErrors.failedToFindAvroDataSourceError(provider1)
                } else if (provider1.toLowerCase(Locale.ROOT) == "kafka") {
                  throw QueryCompilationErrors.failedToFindKafkaDataSourceError(provider1)
                } else if (isUserDefinedDataSource) {
                  classOf[PythonDataSourceV2]
                } else {
                  throw QueryExecutionErrors.dataSourceNotFoundError(provider1, error)
                }
            }
          } catch {
            case e: NoClassDefFoundError => // This one won't be caught by Scala NonFatal
              // NoClassDefFoundError's class name uses "/" rather than "." for packages
              val className = e.getMessage.replaceAll("/", ".")
              if (spark2RemovedClasses.contains(className)) {
                throw QueryExecutionErrors.removedClassInSpark2Error(className, e)
              } else {
                throw e
              }
          }
        case _ :: Nil if isUserDefinedDataSource =>
          // There was DSv1 or DSv2 loaded, but the same name source was found
          // in user defined data source.
          throw QueryCompilationErrors.foundMultipleDataSources(provider)
        case head :: Nil =>
          head.getClass
        case sources =>
          // There are multiple registered aliases for the input. If there is single datasource
          // that has "org.apache.spark" package in the prefix, we use it considering it is an
          // internal datasource within Spark.
          val sourceNames = sources.map(_.getClass.getName).sortBy(_.toString)
          val internalSources = sources.filter(_.getClass.getName.startsWith("org.apache.spark"))
          if (provider.equalsIgnoreCase("xml") && sources.size == 2) {
            val externalSource = sources.filterNot(_.getClass.getName
              .startsWith("org.apache.spark.sql.execution.datasources.xml.XmlFileFormat")
            ).head.getClass
            throw QueryCompilationErrors
              .foundMultipleXMLDataSourceError(provider1, sourceNames, externalSource.getName)
          } else if (internalSources.size == 1) {
            logWarning(log"Multiple sources found for ${MDC(DATA_SOURCE, provider1)} " +
              log"(${MDC(DATA_SOURCES, sourceNames.mkString(", "))}), defaulting to the " +
              log"internal datasource (${MDC(CLASS_NAME, internalSources.head.getClass.getName)}).")
            internalSources.head.getClass
          } else {
            throw QueryCompilationErrors.findMultipleDataSourceError(provider1, sourceNames)
          }
      }
    } catch {
      case e: ServiceConfigurationError if e.getCause.isInstanceOf[NoClassDefFoundError] =>
        // NoClassDefFoundError's class name uses "/" rather than "." for packages
        val className = e.getCause.getMessage.replaceAll("/", ".")
        if (spark2RemovedClasses.contains(className)) {
          throw QueryExecutionErrors.incompatibleDataSourceRegisterError(e)
        } else {
          throw e
        }
    }
  }

  /**
   * Returns an optional [[TableProvider]] instance for the given provider. It returns None if
   * there is no corresponding Data Source V2 implementation, or the provider is configured to
   * fallback to Data Source V1 code path.
   */
  def lookupDataSourceV2(provider: String, conf: SQLConf): Option[TableProvider] = {
    val useV1Sources = conf.getConf(SQLConf.USE_V1_SOURCE_LIST).toLowerCase(Locale.ROOT)
      .split(",").map(_.trim)
    val cls = lookupDataSource(provider, conf)
    val instance = try {
      cls.getDeclaredConstructor().newInstance()
    } catch {
      // Throw the original error from the data source implementation.
      case e: java.lang.reflect.InvocationTargetException => throw e.getCause
    }
    instance match {
      case d: DataSourceRegister if useV1Sources.contains(d.shortName()) => None
      case t: TableProvider
          if !useV1Sources.contains(cls.getCanonicalName.toLowerCase(Locale.ROOT)) =>
        t match {
          case p: PythonDataSourceV2 => p.setShortName(provider)
          case _ =>
        }
        Some(t)
      case _ => None
    }
  }

  /**
   * The key in the "options" map for deciding whether or not to glob paths before use.
   */
  val GLOB_PATHS_KEY = "__globPaths__"

  /**
   * Checks and returns files in all the paths.
   */
  private[sql] def checkAndGlobPathIfNecessary(
      pathStrings: Seq[String],
      hadoopConf: Configuration,
      checkEmptyGlobPath: Boolean,
      checkFilesExist: Boolean,
      numThreads: Integer = 40,
      enableGlobbing: Boolean): Seq[Path] = {
    val qualifiedPaths = pathStrings.map { pathString =>
      val path = new Path(pathString)
      val fs = path.getFileSystem(hadoopConf)
      fs.makeQualified(path)
    }

    // Split the paths into glob and non glob paths, because we don't need to do an existence check
    // for globbed paths.
    val (globPaths, nonGlobPaths) = qualifiedPaths.partition(SparkHadoopUtil.get.isGlobPath)

    val globbedPaths =
      try {
        ThreadUtils.parmap(globPaths, "globPath", numThreads) { globPath =>
          val fs = globPath.getFileSystem(hadoopConf)
          val globResult = if (enableGlobbing) {
            SparkHadoopUtil.get.globPath(fs, globPath)
          } else {
            qualifiedPaths
          }

          if (checkEmptyGlobPath && globResult.isEmpty) {
            throw QueryCompilationErrors.dataPathNotExistError(globPath.toString)
          }

          globResult
        }.flatten
      } catch {
        case e: SparkException => throw ThreadUtils.wrapCallerStacktrace(e.getCause)
      }

    if (checkFilesExist) {
      try {
        ThreadUtils.parmap(nonGlobPaths, "checkPathsExist", numThreads) { path =>
          val fs = path.getFileSystem(hadoopConf)
          if (!fs.exists(path)) {
            throw QueryCompilationErrors.dataPathNotExistError(path.toString)
          }
        }
      } catch {
        case e: SparkException => throw ThreadUtils.wrapCallerStacktrace(e.getCause)
      }
    }

    val allPaths = globbedPaths ++ nonGlobPaths
    if (checkFilesExist) {
      val (filteredOut, filteredIn) = allPaths.partition { path =>
        HadoopFSUtils.shouldFilterOutPathName(path.getName)
      }
      if (filteredIn.isEmpty) {
        logWarning(
          log"All paths were ignored:\n  ${MDC(PATHS, filteredOut.mkString("\n  "))}")
      } else {
        logDebug(
          s"Some paths were ignored:\n  ${filteredOut.mkString("\n  ")}")
      }
    }

    allPaths
  }

  /**
   * When creating a data source table, the `path` option has a special meaning: the table location.
   * This method extracts the `path` option and treat it as table location to build a
   * [[CatalogStorageFormat]]. Note that, the `path` option is removed from options after this.
   */
  def buildStorageFormatFromOptions(options: Map[String, String]): CatalogStorageFormat = {
    val path = CaseInsensitiveMap(options).get("path")
    val optionsWithoutPath = options.filter { case (k, _) => k.toLowerCase(Locale.ROOT) != "path" }
    CatalogStorageFormat.empty.copy(
      locationUri = path.map(CatalogUtils.stringToURI), properties = optionsWithoutPath)
  }

  /**
   * Called before writing into a FileFormat based data source to validate whether
   * the supplied schema is not empty.
   * @param schema
   * @param conf
   */
  def validateSchema(formatName: String, schema: StructType, conf: SQLConf): Unit = {
    val shouldAllowEmptySchema = conf.getConf(SQLConf.ALLOW_EMPTY_SCHEMAS_FOR_WRITES)
    def hasEmptySchema(schema: StructType): Boolean = {
      schema.size == 0 || schema.exists {
        case StructField(_, b: StructType, _, _) => hasEmptySchema(b)
        case _ => false
      }
    }


    if (!shouldAllowEmptySchema && hasEmptySchema(schema)) {
      throw QueryCompilationErrors.writeEmptySchemasUnsupportedByDataSourceError(formatName)
    }
  }
}
