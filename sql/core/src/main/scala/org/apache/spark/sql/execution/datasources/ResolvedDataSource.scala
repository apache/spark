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

import org.apache.hadoop.fs.Path

import org.apache.spark.Logging
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.execution.streaming.{FileStreamSource, Sink, Source}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{CalendarIntervalType, StructType}
import org.apache.spark.util.Utils

case class ResolvedDataSource(provider: Class[_], relation: BaseRelation)

/**
 * Responsible for taking a description of a datasource (either from
 * [[org.apache.spark.sql.DataFrameReader]], or a metastore) and converting it into a logical
 * relation that can be used in a query plan.
 */
object ResolvedDataSource extends Logging {

  /** A map to maintain backward compatibility in case we move data sources around. */
  private val backwardCompatibilityMap = Map(
    "org.apache.spark.sql.jdbc" -> classOf[jdbc.DefaultSource].getCanonicalName,
    "org.apache.spark.sql.jdbc.DefaultSource" -> classOf[jdbc.DefaultSource].getCanonicalName,
    "org.apache.spark.sql.json" -> classOf[json.DefaultSource].getCanonicalName,
    "org.apache.spark.sql.json.DefaultSource" -> classOf[json.DefaultSource].getCanonicalName,
    "org.apache.spark.sql.parquet" -> classOf[parquet.DefaultSource].getCanonicalName,
    "org.apache.spark.sql.parquet.DefaultSource" -> classOf[parquet.DefaultSource].getCanonicalName
  )

  /** Given a provider name, look up the data source class definition. */
  def lookupDataSource(provider0: String): Class[_] = {
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

  // TODO: Combine with apply?
  def createSource(
      sqlContext: SQLContext,
      userSpecifiedSchema: Option[StructType],
      providerName: String,
      options: Map[String, String]): Source = {
    val provider = lookupDataSource(providerName).newInstance() match {
      case s: StreamSourceProvider =>
        s.createSource(sqlContext, userSpecifiedSchema, providerName, options)

      case format: FileFormat =>
        val caseInsensitiveOptions = new CaseInsensitiveMap(options)
        val path = caseInsensitiveOptions.getOrElse("path", {
          throw new IllegalArgumentException("'path' is not specified")
        })
        val metadataPath = caseInsensitiveOptions.getOrElse("metadataPath", s"$path/_metadata")

        val allPaths = caseInsensitiveOptions.get("path")
        val globbedPaths = allPaths.toSeq.flatMap { path =>
          val hdfsPath = new Path(path)
          val fs = hdfsPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
          val qualified = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
          SparkHadoopUtil.get.globPathIfNecessary(qualified)
        }.toArray

        val fileCatalog: FileCatalog = new HDFSFileCatalog(sqlContext, options, globbedPaths)
        val dataSchema = userSpecifiedSchema.orElse {
          format.inferSchema(
            sqlContext,
            caseInsensitiveOptions,
            fileCatalog.allFiles())
        }.getOrElse {
          throw new AnalysisException("Unable to infer schema.  It must be specified manually.")
        }

        def dataFrameBuilder(files: Array[String]): DataFrame = {
          new DataFrame(
            sqlContext,
            LogicalRelation(
              apply(
                sqlContext,
                paths = files,
                userSpecifiedSchema = Some(dataSchema),
                provider = providerName,
                options = options.filterKeys(_ != "path")).relation))
        }

        new FileStreamSource(
          sqlContext, metadataPath, path, Some(dataSchema), providerName, dataFrameBuilder)
      case _ =>
        throw new UnsupportedOperationException(
          s"Data source $providerName does not support streamed reading")
    }

    provider
  }

  def createSink(
      sqlContext: SQLContext,
      providerName: String,
      options: Map[String, String],
      partitionColumns: Seq[String]): Sink = {
    val provider = lookupDataSource(providerName).newInstance() match {
      case s: StreamSinkProvider => s
      case _ =>
        throw new UnsupportedOperationException(
          s"Data source $providerName does not support streamed writing")
    }

    provider.createSink(sqlContext, options, partitionColumns)
  }

  /** Create a [[ResolvedDataSource]] for reading data in. */
  def apply(
      sqlContext: SQLContext,
      paths: Seq[String] = Nil,
      userSpecifiedSchema: Option[StructType] = None,
      partitionColumns: Array[String] = Array.empty,
      bucketSpec: Option[BucketSpec] = None,
      provider: String,
      options: Map[String, String]): ResolvedDataSource = {
    val clazz: Class[_] = lookupDataSource(provider)
    def className: String = clazz.getCanonicalName

    val caseInsensitiveOptions = new CaseInsensitiveMap(options)
    val relation = (clazz.newInstance(), userSpecifiedSchema) match {
      // TODO: Throw when too much is given.
      case (dataSource: SchemaRelationProvider, Some(schema)) =>
        dataSource.createRelation(sqlContext, caseInsensitiveOptions, schema)
      case (dataSource: RelationProvider, None) =>
        dataSource.createRelation(sqlContext, caseInsensitiveOptions)
      case (_: SchemaRelationProvider, None) =>
        throw new AnalysisException(s"A schema needs to be specified when using $className.")
      case (_: RelationProvider, Some(_)) =>
        throw new AnalysisException(s"$className does not allow user-specified schemas.")

      case (format: FileFormat, _) =>
        val allPaths = caseInsensitiveOptions.get("path") ++ paths
        val globbedPaths = allPaths.flatMap { path =>
          val hdfsPath = new Path(path)
          val fs = hdfsPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
          val qualified = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
          SparkHadoopUtil.get.globPathIfNecessary(qualified)
        }.toArray

        val fileCatalog: FileCatalog = new HDFSFileCatalog(sqlContext, options, globbedPaths)
        val dataSchema = userSpecifiedSchema.orElse {
          format.inferSchema(
            sqlContext,
            caseInsensitiveOptions,
            fileCatalog.allFiles())
        }.getOrElse {
          throw new AnalysisException(
            s"Unable to infer schema for $format at ${allPaths.take(2).mkString(",")}. " +
            "It must be specified manually")
        }

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
        }.getOrElse(fileCatalog.partitionSpec(None).partitionColumns)

        HadoopFsRelation(
          sqlContext,
          fileCatalog,
          partitionSchema = partitionSchema,
          dataSchema = dataSchema.asNullable,
          bucketSpec = bucketSpec,
          format,
          options)

      case _ =>
        throw new AnalysisException(
          s"$className is not a valid Spark SQL Data Source.")
    }
    new ResolvedDataSource(clazz, relation)
  }

  def partitionColumnsSchema(
      schema: StructType,
      partitionColumns: Array[String],
      caseSensitive: Boolean): StructType = {
    val equality = columnNameEquality(caseSensitive)
    StructType(partitionColumns.map { col =>
      schema.find(f => equality(f.name, col)).getOrElse {
        throw new RuntimeException(s"Partition column $col not found in schema $schema")
      }
    }).asNullable
  }

  private def columnNameEquality(caseSensitive: Boolean): (String, String) => Boolean = {
    if (caseSensitive) {
      org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
    } else {
      org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
    }
  }

  /** Create a [[ResolvedDataSource]] for saving the content of the given DataFrame. */
  def apply(
      sqlContext: SQLContext,
      provider: String,
      partitionColumns: Array[String],
      bucketSpec: Option[BucketSpec],
      mode: SaveMode,
      options: Map[String, String],
      data: DataFrame): ResolvedDataSource = {
    if (data.schema.map(_.dataType).exists(_.isInstanceOf[CalendarIntervalType])) {
      throw new AnalysisException("Cannot save interval data type into external storage.")
    }
    val clazz: Class[_] = lookupDataSource(provider)
    clazz.newInstance() match {
      case dataSource: CreatableRelationProvider =>
        dataSource.createRelation(sqlContext, mode, options, data)
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
          val fs = path.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
          path.makeQualified(fs.getUri, fs.getWorkingDirectory)
        }

        val caseSensitive = sqlContext.conf.caseSensitiveAnalysis
        PartitioningUtils.validatePartitionColumnDataTypes(
          data.schema, partitionColumns, caseSensitive)

        val equality = columnNameEquality(caseSensitive)
        val dataSchema = StructType(
          data.schema.filterNot(f => partitionColumns.exists(equality(_, f.name))))

        // If we are appending to a table that already exists, make sure the partitioning matches
        // up.  If we fail to load the table for whatever reason, ignore the check.
        if (mode == SaveMode.Append) {
          val existingPartitionColumnSet = try {
            val resolved = apply(
              sqlContext,
              userSpecifiedSchema = Some(data.schema.asNullable),
              provider = provider,
              options = options)

            Some(resolved.relation
              .asInstanceOf[HadoopFsRelation]
              .location
              .partitionSpec(None)
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
        sqlContext.executePlan(plan).toRdd

      case _ =>
        sys.error(s"${clazz.getCanonicalName} does not allow create table as select.")
    }

    apply(
      sqlContext,
      userSpecifiedSchema = Some(data.schema.asNullable),
      partitionColumns = partitionColumns,
      bucketSpec = bucketSpec,
      provider = provider,
      options = options)
  }
}
