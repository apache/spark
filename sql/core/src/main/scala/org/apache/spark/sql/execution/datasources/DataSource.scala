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

import java.util.{ServiceConfigurationError, ServiceLoader}

import scala.collection.JavaConverters._
import scala.language.{existentials, implicitConversions}
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{CalendarIntervalType, StructType}
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
 * @param partitionColumns A list of column names that the relation is partitioned by. When this
 *                         list is empty, the relation is unpartitioned.
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

  case class SourceInfo(name: String, schema: StructType)

  lazy val providingClass: Class[_] = lookupDataSource(className)
  lazy val sourceInfo = sourceSchema()

  /** A map to maintain backward compatibility in case we move data sources around. */
  private val backwardCompatibilityMap: Map[String, String] = {
    val jdbc = classOf[JdbcRelationProvider].getCanonicalName
    val json = classOf[JsonFileFormat].getCanonicalName
    val parquet = classOf[ParquetFileFormat].getCanonicalName
    val csv = classOf[CSVFileFormat].getCanonicalName
    val libsvm = "org.apache.spark.ml.source.libsvm.LibSVMFileFormat"
    val orc = "org.apache.spark.sql.hive.orc.OrcFileFormat"

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
      "org.apache.spark.ml.source.libsvm.DefaultSource" -> libsvm,
      "org.apache.spark.ml.source.libsvm" -> libsvm,
      "com.databricks.spark.csv" -> csv
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
  private def lookupDataSource(provider0: String): Class[_] = {
    val provider = backwardCompatibilityMap.getOrElse(provider0, provider0)
    val provider2 = s"$provider.DefaultSource"
    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoader = ServiceLoader.load(classOf[DataSourceRegister], loader)

    try {
      serviceLoader.asScala.filter(_.shortName().equalsIgnoreCase(provider)).toList match {
        // the provider format did not match any given registered aliases
        case Nil =>
          try {
            Try(loader.loadClass(provider)).orElse(Try(loader.loadClass(provider2))) match {
              case Success(dataSource) =>
                // Found the data source using fully qualified path
                dataSource
              case Failure(error) =>
                if (provider.toLowerCase == "orc" ||
                  provider.startsWith("org.apache.spark.sql.hive.orc")) {
                  throw new AnalysisException(
                    "The ORC data source must be used with Hive support enabled")
                } else if (provider.toLowerCase == "avro" ||
                  provider == "com.databricks.spark.avro") {
                  throw new AnalysisException(
                    s"Failed to find data source: ${provider.toLowerCase}. Please find an Avro " +
                      "package at " +
                      "https://cwiki.apache.org/confluence/display/SPARK/Third+Party+Projects")
                } else {
                  throw new ClassNotFoundException(
                    s"Failed to find data source: $provider. Please find packages at " +
                      "https://cwiki.apache.org/confluence/display/SPARK/Third+Party+Projects",
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
          // There are multiple registered aliases for the input
          sys.error(s"Multiple sources found for $provider " +
            s"(${sources.map(_.getClass.getName).mkString(", ")}), " +
            "please specify the fully qualified class name.")
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

  private def inferFileFormatSchema(format: FileFormat): StructType = {
    userSpecifiedSchema.orElse {
      val caseInsensitiveOptions = new CaseInsensitiveMap(options)
      val allPaths = caseInsensitiveOptions.get("path")
      val globbedPaths = allPaths.toSeq.flatMap { path =>
        val hdfsPath = new Path(path)
        val fs = hdfsPath.getFileSystem(sparkSession.sessionState.newHadoopConf())
        val qualified = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
        SparkHadoopUtil.get.globPathIfNecessary(qualified)
      }.toArray
      val fileCatalog = new ListingFileCatalog(sparkSession, globbedPaths, options, None)
      val partitionCols = fileCatalog.partitionSpec().partitionColumns.fields
      val inferred = format.inferSchema(
        sparkSession,
        caseInsensitiveOptions,
        fileCatalog.allFiles())

      inferred.map { inferredSchema =>
        StructType(inferredSchema ++ partitionCols)
      }
    }.getOrElse {
      throw new AnalysisException("Unable to infer schema. It must be specified manually.")
    }
  }

  /** Returns the name and schema of the source that can be used to continually read data. */
  private def sourceSchema(): SourceInfo = {
    providingClass.newInstance() match {
      case s: StreamSourceProvider =>
        val (name, schema) = s.sourceSchema(
          sparkSession.sqlContext, userSpecifiedSchema, className, options)
        SourceInfo(name, schema)

      case format: FileFormat =>
        val caseInsensitiveOptions = new CaseInsensitiveMap(options)
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
        SourceInfo(s"FileSource[$path]", inferFileFormatSchema(format))

      case _ =>
        throw new UnsupportedOperationException(
          s"Data source $className does not support streamed reading")
    }
  }

  /** Returns a source that can be used to continually read data. */
  def createSource(metadataPath: String): Source = {
    providingClass.newInstance() match {
      case s: StreamSourceProvider =>
        s.createSource(
          sparkSession.sqlContext, metadataPath, userSpecifiedSchema, className, options)

      case format: FileFormat =>
        val path = new CaseInsensitiveMap(options).getOrElse("path", {
          throw new IllegalArgumentException("'path' is not specified")
        })
        new FileStreamSource(
          sparkSession, path, className, sourceInfo.schema, metadataPath, options)
      case _ =>
        throw new UnsupportedOperationException(
          s"Data source $className does not support streamed reading")
    }
  }

  /** Returns a sink that can be used to continually write data. */
  def createSink(outputMode: OutputMode): Sink = {
    providingClass.newInstance() match {
      case s: StreamSinkProvider =>
        s.createSink(sparkSession.sqlContext, options, partitionColumns, outputMode)

      case parquet: parquet.ParquetFileFormat =>
        val caseInsensitiveOptions = new CaseInsensitiveMap(options)
        val path = caseInsensitiveOptions.getOrElse("path", {
          throw new IllegalArgumentException("'path' is not specified")
        })
        if (outputMode != OutputMode.Append) {
          throw new IllegalArgumentException(
            s"Data source $className does not support $outputMode output mode")
        }
        new FileStreamSink(sparkSession, path, parquet, partitionColumns, options)

      case _ =>
        throw new UnsupportedOperationException(
          s"Data source $className does not support streamed writing")
    }
  }

  /**
   * Returns true if there is a single path that has a metadata log indicating which files should
   * be read.
   */
  def hasMetadata(path: Seq[String]): Boolean = {
    path match {
      case Seq(singlePath) =>
        try {
          val hdfsPath = new Path(singlePath)
          val fs = hdfsPath.getFileSystem(sparkSession.sessionState.newHadoopConf())
          val metadataPath = new Path(hdfsPath, FileStreamSink.metadataDir)
          val res = fs.exists(metadataPath)
          res
        } catch {
          case NonFatal(e) =>
            logWarning(s"Error while looking for metadata directory.")
            false
        }
      case _ => false
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
    val caseInsensitiveOptions = new CaseInsensitiveMap(options)
    val relation = (providingClass.newInstance(), userSpecifiedSchema) match {
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
          if hasMetadata(caseInsensitiveOptions.get("path").toSeq ++ paths) =>
        val basePath = new Path((caseInsensitiveOptions.get("path").toSeq ++ paths).head)
        val fileCatalog = new MetadataLogFileCatalog(sparkSession, basePath)
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
          partitionSchema = fileCatalog.partitionSpec().partitionColumns,
          dataSchema = dataSchema,
          bucketSpec = None,
          format,
          options)(sparkSession)

      // This is a non-streaming file based datasource.
      case (format: FileFormat, _) =>
        val allPaths = caseInsensitiveOptions.get("path") ++ paths
        val globbedPaths = allPaths.flatMap { path =>
          val hdfsPath = new Path(path)
          val fs = hdfsPath.getFileSystem(sparkSession.sessionState.newHadoopConf())
          val qualified = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
          val globPath = SparkHadoopUtil.get.globPathIfNecessary(qualified)

          if (globPath.isEmpty) {
            throw new AnalysisException(s"Path does not exist: $qualified")
          }
          // Sufficient to check head of the globPath seq for non-glob scenario
          if (checkFilesExist && !fs.exists(globPath.head)) {
            throw new AnalysisException(s"Path does not exist: ${globPath.head}")
          }
          globPath
        }.toArray

        // If they gave a schema, then we try and figure out the types of the partition columns
        // from that schema.
        val partitionSchema = userSpecifiedSchema.map { schema =>
          StructType(
            partitionColumns.map { c =>
              // TODO: Case sensitivity.
              schema
                  .find(_.name.toLowerCase() == c.toLowerCase())
                  .getOrElse(throw new AnalysisException(s"Invalid partition column '$c'"))
            })
        }

        val enableDatasourceHivePartitionProvider = true  // TODO(ekl) conf
        val fileCatalog = if (enableDatasourceHivePartitionProvider &&
            catalogTable.isDefined && catalogTable.get.partitionProviderIsHive) {
          new TableFileCatalog(
            sparkSession,
            catalogTable.get.identifier.database.get,
            catalogTable.get.identifier.table,
            partitionSchema.getOrElse(StructType(Nil)),
            catalogTable.get.stats.map(_.sizeInBytes.toLong).getOrElse(0L) /* TODO(ekl) */)
        } else {
          new ListingFileCatalog(
            sparkSession, globbedPaths, options, partitionSchema)
        }

        val dataSchema = userSpecifiedSchema.map { schema =>
          val equality = sparkSession.sessionState.conf.resolver
          StructType(schema.filterNot(f => partitionColumns.exists(equality(_, f.name))))
        }.orElse {
          format.inferSchema(
            sparkSession,
            caseInsensitiveOptions,
            fileCatalog.asInstanceOf[ListingFileCatalog].allFiles())
        }.getOrElse {
          throw new AnalysisException(
            s"Unable to infer schema for $format at ${allPaths.take(2).mkString(",")}. " +
              "It must be specified manually")
        }

        HadoopFsRelation(
          fileCatalog,
          partitionSchema = fileCatalog.partitionSchema,
          dataSchema = dataSchema.asNullable,
          bucketSpec = bucketSpec,
          format,
          caseInsensitiveOptions)(sparkSession)

      case _ =>
        throw new AnalysisException(
          s"$className is not a valid Spark SQL Data Source.")
    }

    relation
  }

  /** Writes the given [[DataFrame]] out to this [[DataSource]]. */
  def write(
      mode: SaveMode,
      data: DataFrame): BaseRelation = {
    if (data.schema.map(_.dataType).exists(_.isInstanceOf[CalendarIntervalType])) {
      throw new AnalysisException("Cannot save interval data type into external storage.")
    }

    providingClass.newInstance() match {
      case dataSource: CreatableRelationProvider =>
        dataSource.createRelation(sparkSession.sqlContext, mode, options, data)
      case format: FileFormat =>
        // Don't glob path for the write path.  The contracts here are:
        //  1. Only one output path can be specified on the write path;
        //  2. Output path must be a legal HDFS style file system path;
        //  3. It's OK that the output path doesn't exist yet;
        val caseInsensitiveOptions = new CaseInsensitiveMap(options)
        val outputPath = {
          val path = new Path(caseInsensitiveOptions.getOrElse("path", {
            throw new IllegalArgumentException("'path' is not specified")
          }))
          val fs = path.getFileSystem(sparkSession.sessionState.newHadoopConf())
          path.makeQualified(fs.getUri, fs.getWorkingDirectory)
        }

        val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
        PartitioningUtils.validatePartitionColumn(
          data.schema, partitionColumns, caseSensitive)

        // If we are appending to a table that already exists, make sure the partitioning matches
        // up.  If we fail to load the table for whatever reason, ignore the check.
        if (mode == SaveMode.Append) {
          val existingPartitionColumns = Try {
            resolveRelation()
              .asInstanceOf[HadoopFsRelation]
              .partitionSchema
              .fieldNames
              .toSeq
          }.getOrElse(Seq.empty[String])
          // TODO: Case sensitivity.
          val sameColumns =
            existingPartitionColumns.map(_.toLowerCase()) == partitionColumns.map(_.toLowerCase())
          if (existingPartitionColumns.size > 0 && !sameColumns) {
            throw new AnalysisException(
              s"""Requested partitioning does not match existing partitioning.
                 |Existing partitioning columns:
                 |  ${existingPartitionColumns.mkString(", ")}
                 |Requested partitioning columns:
                 |  ${partitionColumns.mkString(", ")}
                 |""".stripMargin)
          }
        }

        // SPARK-17230: Resolve the partition columns so InsertIntoHadoopFsRelationCommand does
        // not need to have the query as child, to avoid to analyze an optimized query,
        // because InsertIntoHadoopFsRelationCommand will be optimized first.
        val columns = partitionColumns.map { name =>
          val plan = data.logicalPlan
          plan.resolve(name :: Nil, data.sparkSession.sessionState.analyzer.resolver).getOrElse {
            throw new AnalysisException(
              s"Unable to resolve ${name} given [${plan.output.map(_.name).mkString(", ")}]")
          }.asInstanceOf[Attribute]
        }
        // For partitioned relation r, r.schema's column ordering can be different from the column
        // ordering of data.logicalPlan (partition columns are all moved after data column).  This
        // will be adjusted within InsertIntoHadoopFsRelation.
        val plan =
          InsertIntoHadoopFsRelationCommand(
            outputPath,
            columns,
            bucketSpec,
            format,
            () => Unit, // No existing table needs to be refreshed.
            options,
            data.logicalPlan,
            mode)
        sparkSession.sessionState.executePlan(plan).toRdd
        // Replace the schema with that of the DataFrame we just wrote out to avoid re-inferring it.
        copy(userSpecifiedSchema = Some(data.schema.asNullable)).resolveRelation()

      case _ =>
        sys.error(s"${providingClass.getCanonicalName} does not allow create table as select.")
    }
  }
}
