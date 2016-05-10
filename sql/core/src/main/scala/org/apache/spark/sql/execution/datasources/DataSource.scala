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

import java.util.ServiceLoader

import scala.collection.JavaConverters._
import scala.language.{existentials, implicitConversions}
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.sources._
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
 * @param partitionColumns A list of column names that the relation is partitioned by.  When this
 *                         list is empty, the relation is unpartitioned.
 * @param bucketSpec An optional specification for bucketing (hash-partitioning) of the data.
 */
case class DataSource(
    sparkSession: SparkSession,
    className: String,
    paths: Seq[String] = Nil,
    userSpecifiedSchema: Option[StructType] = None,
    partitionColumns: Seq[String] = Seq.empty,
    bucketSpec: Option[BucketSpec] = None,
    options: Map[String, String] = Map.empty) extends Logging {

  case class SourceInfo(name: String, schema: StructType)

  lazy val providingClass: Class[_] = lookupDataSource(className)
  lazy val sourceInfo = sourceSchema()

  /** A map to maintain backward compatibility in case we move data sources around. */
  private val backwardCompatibilityMap = Map(
    "org.apache.spark.sql.jdbc" -> classOf[jdbc.DefaultSource].getCanonicalName,
    "org.apache.spark.sql.jdbc.DefaultSource" -> classOf[jdbc.DefaultSource].getCanonicalName,
    "org.apache.spark.sql.json" -> classOf[json.DefaultSource].getCanonicalName,
    "org.apache.spark.sql.json.DefaultSource" -> classOf[json.DefaultSource].getCanonicalName,
    "org.apache.spark.sql.parquet" -> classOf[parquet.DefaultSource].getCanonicalName,
    "org.apache.spark.sql.parquet.DefaultSource" -> classOf[parquet.DefaultSource].getCanonicalName,
    "com.databricks.spark.csv" -> classOf[csv.DefaultSource].getCanonicalName
  )

  /** Given a provider name, look up the data source class definition. */
  private def lookupDataSource(provider0: String): Class[_] = {
    val provider = backwardCompatibilityMap.getOrElse(provider0, provider0)
    val provider2 = s"$provider.DefaultSource"
    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoader = ServiceLoader.load(classOf[DataSourceRegister], loader)

    serviceLoader.asScala.filter(_.shortName().equalsIgnoreCase(provider)).toList match {
      // the provider format did not match any given registered aliases
      case Nil =>
        Try(loader.loadClass(provider)).orElse(Try(loader.loadClass(provider2))) match {
          case Success(dataSource) =>
            // Found the data source using fully qualified path
            dataSource
          case Failure(error) =>
            if (provider.startsWith("org.apache.spark.sql.hive.orc")) {
              throw new ClassNotFoundException(
                "The ORC data source must be used with Hive support enabled.", error)
            } else {
              if (provider == "avro" || provider == "com.databricks.spark.avro") {
                throw new ClassNotFoundException(
                  s"Failed to find data source: $provider. Please use Spark package " +
                  "http://spark-packages.org/package/databricks/spark-avro",
                  error)
              } else {
                throw new ClassNotFoundException(
                  s"Failed to find data source: $provider. Please find packages at " +
                  "http://spark-packages.org",
                  error)
              }
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
      format.inferSchema(
        sparkSession,
        caseInsensitiveOptions,
        fileCatalog.allFiles())
    }.getOrElse {
      throw new AnalysisException("Unable to infer schema. It must be specified manually.")
    }
  }

  /** Returns the name and schema of the source that can be used to continually read data. */
  private def sourceSchema(): SourceInfo = {
    providingClass.newInstance() match {
      case s: StreamSourceProvider =>
        val (name, schema) = s.sourceSchema(
          sparkSession.wrapped, userSpecifiedSchema, className, options)
        SourceInfo(name, schema)

      case format: FileFormat =>
        val caseInsensitiveOptions = new CaseInsensitiveMap(options)
        val path = caseInsensitiveOptions.getOrElse("path", {
          throw new IllegalArgumentException("'path' is not specified")
        })
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
        s.createSource(sparkSession.wrapped, metadataPath, userSpecifiedSchema, className, options)

      case format: FileFormat =>
        val caseInsensitiveOptions = new CaseInsensitiveMap(options)
        val path = caseInsensitiveOptions.getOrElse("path", {
          throw new IllegalArgumentException("'path' is not specified")
        })

        def dataFrameBuilder(files: Array[String]): DataFrame = {
          val newOptions = options.filterKeys(_ != "path") + ("basePath" -> path)
          val newDataSource =
            DataSource(
              sparkSession,
              paths = files,
              userSpecifiedSchema = Some(sourceInfo.schema),
              className = className,
              options = new CaseInsensitiveMap(newOptions))
          Dataset.ofRows(sparkSession, LogicalRelation(newDataSource.resolveRelation()))
        }

        new FileStreamSource(
          sparkSession, metadataPath, path, sourceInfo.schema, dataFrameBuilder)
      case _ =>
        throw new UnsupportedOperationException(
          s"Data source $className does not support streamed reading")
    }
  }

  /** Returns a sink that can be used to continually write data. */
  def createSink(): Sink = {
    providingClass.newInstance() match {
      case s: StreamSinkProvider => s.createSink(sparkSession.wrapped, options, partitionColumns)

      case parquet: parquet.DefaultSource =>
        val caseInsensitiveOptions = new CaseInsensitiveMap(options)
        val path = caseInsensitiveOptions.getOrElse("path", {
          throw new IllegalArgumentException("'path' is not specified")
        })
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
   * @param checkPathExist A flag to indicate whether to check the existence of path or not.
   *                       This flag will be set to false when we create an empty table (the
   *                       path of the table does not exist).
   */
  def resolveRelation(checkPathExist: Boolean = true): BaseRelation = {
    val caseInsensitiveOptions = new CaseInsensitiveMap(options)
    val relation = (providingClass.newInstance(), userSpecifiedSchema) match {
      // TODO: Throw when too much is given.
      case (dataSource: SchemaRelationProvider, Some(schema)) =>
        dataSource.createRelation(sparkSession.wrapped, caseInsensitiveOptions, schema)
      case (dataSource: RelationProvider, None) =>
        dataSource.createRelation(sparkSession.wrapped, caseInsensitiveOptions)
      case (_: SchemaRelationProvider, None) =>
        throw new AnalysisException(s"A schema needs to be specified when using $className.")
      case (_: RelationProvider, Some(_)) =>
        throw new AnalysisException(s"$className does not allow user-specified schemas.")

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
          sparkSession,
          fileCatalog,
          partitionSchema = fileCatalog.partitionSpec().partitionColumns,
          dataSchema = dataSchema,
          bucketSpec = None,
          format,
          options)

      // This is a non-streaming file based datasource.
      case (format: FileFormat, _) =>
        val allPaths = caseInsensitiveOptions.get("path") ++ paths
        val globbedPaths = allPaths.flatMap { path =>
          val hdfsPath = new Path(path)
          val fs = hdfsPath.getFileSystem(sparkSession.sessionState.newHadoopConf())
          val qualified = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
          val globPath = SparkHadoopUtil.get.globPathIfNecessary(qualified)

          if (checkPathExist && globPath.isEmpty) {
            throw new AnalysisException(s"Path does not exist: $qualified")
          }
          // Sufficient to check head of the globPath seq for non-glob scenario
          if (checkPathExist && !fs.exists(globPath.head)) {
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

        val fileCatalog =
          new ListingFileCatalog(sparkSession, globbedPaths, options, partitionSchema)

        val dataSchema = userSpecifiedSchema.map { schema =>
          val equality =
            if (sparkSession.sessionState.conf.caseSensitiveAnalysis) {
              org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
            } else {
              org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
            }

          StructType(schema.filterNot(f => partitionColumns.exists(equality(_, f.name))))
        }.orElse {
          format.inferSchema(
            sparkSession,
            caseInsensitiveOptions,
            fileCatalog.allFiles())
        }.getOrElse {
          throw new AnalysisException(
            s"Unable to infer schema for $format at ${allPaths.take(2).mkString(",")}. " +
              "It must be specified manually")
        }

        val enrichedOptions =
          format.prepareRead(sparkSession, caseInsensitiveOptions, fileCatalog.allFiles())

        HadoopFsRelation(
          sparkSession,
          fileCatalog,
          partitionSchema = fileCatalog.partitionSpec().partitionColumns,
          dataSchema = dataSchema.asNullable,
          bucketSpec = bucketSpec,
          format,
          enrichedOptions)

      case _ =>
        throw new AnalysisException(
          s"$className is not a valid Spark SQL Data Source.")
    }

    relation
  }

  /** Writes the give [[DataFrame]] out to this [[DataSource]]. */
  def write(
      mode: SaveMode,
      data: DataFrame): BaseRelation = {
    if (data.schema.map(_.dataType).exists(_.isInstanceOf[CalendarIntervalType])) {
      throw new AnalysisException("Cannot save interval data type into external storage.")
    }

    providingClass.newInstance() match {
      case dataSource: CreatableRelationProvider =>
        dataSource.createRelation(sparkSession.wrapped, mode, options, data)
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
        PartitioningUtils.validatePartitionColumnDataTypes(
          data.schema, partitionColumns, caseSensitive)

        // If we are appending to a table that already exists, make sure the partitioning matches
        // up.  If we fail to load the table for whatever reason, ignore the check.
        if (mode == SaveMode.Append) {
          val existingPartitionColumnSet = try {
            Some(
              resolveRelation()
                .asInstanceOf[HadoopFsRelation]
                .location
                .partitionSpec()
                .partitionColumns
                .fieldNames
                .toSet)
          } catch {
            case e: Exception =>
              None
          }

          existingPartitionColumnSet.foreach { ex =>
            if (ex.map(_.toLowerCase) != partitionColumns.map(_.toLowerCase()).toSet) {
              throw new AnalysisException(
                s"Requested partitioning does not equal existing partitioning: " +
                s"$ex != ${partitionColumns.toSet}.")
            }
          }
        }

        // For partitioned relation r, r.schema's column ordering can be different from the column
        // ordering of data.logicalPlan (partition columns are all moved after data column).  This
        // will be adjusted within InsertIntoHadoopFsRelation.
        val plan =
          InsertIntoHadoopFsRelation(
            outputPath,
            partitionColumns.map(UnresolvedAttribute.quoted),
            bucketSpec,
            format,
            () => Unit, // No existing table needs to be refreshed.
            options,
            data.logicalPlan,
            mode)
        sparkSession.executePlan(plan).toRdd

      case _ =>
        sys.error(s"${providingClass.getCanonicalName} does not allow create table as select.")
    }

    // We replace the schema with that of the DataFrame we just wrote out to avoid re-inferring it.
    copy(userSpecifiedSchema = Some(data.schema.asNullable)).resolveRelation()
  }
}
