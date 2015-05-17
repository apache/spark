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

package org.apache.spark.sql.parquet

import java.util.{List => JList}

import scala.collection.JavaConversions._
import scala.util.Try

import com.google.common.base.Objects
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import parquet.filter2.predicate.FilterApi
import parquet.format.converter.ParquetMetadataConverter
import parquet.hadoop._
import parquet.hadoop.metadata.CompressionCodecName
import parquet.hadoop.util.ContextUtil

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD._
import org.apache.spark.rdd.{NewHadoopPartition, NewHadoopRDD, RDD}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Row, SQLConf, SQLContext}
import org.apache.spark.{Logging, Partition => SparkPartition, SparkException}

private[sql] class DefaultSource extends HadoopFsRelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      paths: Array[String],
      schema: Option[StructType],
      partitionColumns: Option[StructType],
      parameters: Map[String, String]): HadoopFsRelation = {
    val partitionSpec = partitionColumns.map(PartitionSpec(_, Seq.empty))
    new ParquetRelation2(paths, schema, partitionSpec, parameters)(sqlContext)
  }
}

// NOTE: This class is instantiated and used on executor side only, no need to be serializable.
private[sql] class ParquetOutputWriter(path: String, context: TaskAttemptContext)
  extends OutputWriter {

  private val recordWriter: RecordWriter[Void, Row] = {
    val conf = context.getConfiguration
    val outputFormat = {
      // When appending new Parquet files to an existing Parquet file directory, to avoid
      // overwriting existing data files, we need to find out the max task ID encoded in these data
      // file names.
      // TODO Make this snippet a utility function for other data source developers
      val maxExistingTaskId = {
        // Note that `path` may point to a temporary location.  Here we retrieve the real
        // destination path from the configuration
        val outputPath = new Path(conf.get("spark.sql.sources.output.path"))
        val fs = outputPath.getFileSystem(conf)

        if (fs.exists(outputPath)) {
          // Pattern used to match task ID in part file names, e.g.:
          //
          //   part-r-00001.gz.parquet
          //          ^~~~~
          val partFilePattern = """part-.-(\d{1,}).*""".r

          fs.listStatus(outputPath).map(_.getPath.getName).map {
            case partFilePattern(id) => id.toInt
            case name if name.startsWith("_") => 0
            case name if name.startsWith(".") => 0
            case name => sys.error(
              s"Trying to write Parquet files to directory $outputPath, " +
                s"but found items with illegal name '$name'.")
          }.reduceOption(_ max _).getOrElse(0)
        } else {
          0
        }
      }

      new ParquetOutputFormat[Row]() {
        // Here we override `getDefaultWorkFile` for two reasons:
        //
        //  1. To allow appending.  We need to generate output file name based on the max available
        //     task ID computed above.
        //
        //  2. To allow dynamic partitioning.  Default `getDefaultWorkFile` uses
        //     `FileOutputCommitter.getWorkPath()`, which points to the base directory of all
        //     partitions in the case of dynamic partitioning.
        override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
          val split = context.getTaskAttemptID.getTaskID.getId + maxExistingTaskId + 1
          new Path(path, f"part-r-$split%05d$extension")
        }
      }
    }

    outputFormat.getRecordWriter(context)
  }

  override def write(row: Row): Unit = recordWriter.write(null, row)

  override def close(): Unit = recordWriter.close(context)
}

private[sql] class ParquetRelation2(
    override val paths: Array[String],
    private val maybeDataSchema: Option[StructType],
    private val maybePartitionSpec: Option[PartitionSpec],
    parameters: Map[String, String])(
    val sqlContext: SQLContext)
  extends HadoopFsRelation(maybePartitionSpec)
  with Logging {

  // Should we merge schemas from all Parquet part-files?
  private val shouldMergeSchemas =
    parameters.getOrElse(ParquetRelation2.MERGE_SCHEMA, "true").toBoolean

  private val maybeMetastoreSchema = parameters
    .get(ParquetRelation2.METASTORE_SCHEMA)
    .map(DataType.fromJson(_).asInstanceOf[StructType])

  private lazy val metadataCache: MetadataCache = {
    val meta = new MetadataCache
    meta.refresh()
    meta
  }

  override def equals(other: scala.Any): Boolean = other match {
    case that: ParquetRelation2 =>
      val schemaEquality = if (shouldMergeSchemas) {
        this.shouldMergeSchemas == that.shouldMergeSchemas
      } else {
        this.dataSchema == that.dataSchema &&
          this.schema == that.schema
      }

      this.paths.toSet == that.paths.toSet &&
        schemaEquality &&
        this.maybeDataSchema == that.maybeDataSchema &&
        this.partitionColumns == that.partitionColumns

    case _ => false
  }

  override def hashCode(): Int = {
    if (shouldMergeSchemas) {
      Objects.hashCode(
        Boolean.box(shouldMergeSchemas),
        paths.toSet,
        maybeDataSchema,
        maybePartitionSpec)
    } else {
      Objects.hashCode(
        Boolean.box(shouldMergeSchemas),
        paths.toSet,
        dataSchema,
        schema,
        maybeDataSchema,
        maybePartitionSpec)
    }
  }

  override def dataSchema: StructType = metadataCache.dataSchema

  override private[sql] def refresh(): Unit = {
    metadataCache.refresh()
    super.refresh()
  }

  // Parquet data source always uses Catalyst internal representations.
  override val needConversion: Boolean = false

  override def sizeInBytes: Long = metadataCache.dataStatuses.map(_.getLen).sum

  override def userDefinedPartitionColumns: Option[StructType] =
    maybePartitionSpec.map(_.partitionColumns)

  override def prepareJobForWrite(job: Job): OutputWriterFactory = {
    val conf = ContextUtil.getConfiguration(job)

    val committerClass =
      conf.getClass(
        "spark.sql.parquet.output.committer.class",
        classOf[ParquetOutputCommitter],
        classOf[ParquetOutputCommitter])

    conf.setClass(
      "mapred.output.committer.class",
      committerClass,
      classOf[ParquetOutputCommitter])

    // TODO There's no need to use two kinds of WriteSupport
    // We should unify them. `SpecificMutableRow` can process both atomic (primitive) types and
    // complex types.
    val writeSupportClass =
      if (dataSchema.map(_.dataType).forall(ParquetTypesConverter.isPrimitiveType)) {
        classOf[MutableRowWriteSupport]
      } else {
        classOf[RowWriteSupport]
      }

    ParquetOutputFormat.setWriteSupportClass(job, writeSupportClass)
    RowWriteSupport.setSchema(dataSchema.toAttributes, conf)

    // Sets compression scheme
    conf.set(
      ParquetOutputFormat.COMPRESSION,
      ParquetRelation
        .shortParquetCompressionCodecNames
        .getOrElse(
          sqlContext.conf.parquetCompressionCodec.toUpperCase,
          CompressionCodecName.UNCOMPRESSED).name())

    new OutputWriterFactory {
      override def newInstance(
          path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter = {
        new ParquetOutputWriter(path, context)
      }
    }
  }

  override def buildScan(
      requiredColumns: Array[String],
      filters: Array[Filter],
      inputPaths: Array[String]): RDD[Row] = {

    val job = new Job(SparkHadoopUtil.get.conf)
    val conf = ContextUtil.getConfiguration(job)

    ParquetInputFormat.setReadSupportClass(job, classOf[RowReadSupport])

    if (inputPaths.nonEmpty) {
      FileInputFormat.setInputPaths(job, inputPaths.map(new Path(_)): _*)
    }

    // Try to push down filters when filter push-down is enabled.
    if (sqlContext.conf.parquetFilterPushDown) {
      filters
        // Collects all converted Parquet filter predicates. Notice that not all predicates can be
        // converted (`ParquetFilters.createFilter` returns an `Option`). That's why a `flatMap`
        // is used here.
        .flatMap(ParquetFilters.createFilter(dataSchema, _))
        .reduceOption(FilterApi.and)
        .foreach(ParquetInputFormat.setFilterPredicate(conf, _))
    }

    conf.set(RowReadSupport.SPARK_ROW_REQUESTED_SCHEMA, {
      val requestedSchema = StructType(requiredColumns.map(dataSchema(_)))
      ParquetTypesConverter.convertToString(requestedSchema.toAttributes)
    })

    conf.set(
      RowWriteSupport.SPARK_ROW_SCHEMA,
      ParquetTypesConverter.convertToString(dataSchema.toAttributes))

    // Tell FilteringParquetRowInputFormat whether it's okay to cache Parquet and FS metadata
    val useMetadataCache = sqlContext.getConf(SQLConf.PARQUET_CACHE_METADATA, "true").toBoolean
    conf.set(SQLConf.PARQUET_CACHE_METADATA, useMetadataCache.toString)

    val inputFileStatuses =
      metadataCache.dataStatuses.filter(f => inputPaths.contains(f.getPath.toString))

    val footers = inputFileStatuses.map(metadataCache.footers)

    // TODO Stop using `FilteringParquetRowInputFormat` and overriding `getPartition`.
    // After upgrading to Parquet 1.6.0, we should be able to stop caching `FileStatus` objects and
    // footers.  Especially when a global arbitrative schema (either from metastore or data source
    // DDL) is available.
    new NewHadoopRDD(
      sqlContext.sparkContext,
      classOf[FilteringParquetRowInputFormat],
      classOf[Void],
      classOf[Row],
      conf) {

      val cacheMetadata = useMetadataCache

      @transient val cachedStatuses = inputFileStatuses.map { f =>
        // In order to encode the authority of a Path containing special characters such as /,
        // we need to use the string returned by the URI of the path to create a new Path.
        val pathWithAuthority = new Path(f.getPath.toUri.toString)

        new FileStatus(
          f.getLen, f.isDir, f.getReplication, f.getBlockSize, f.getModificationTime,
          f.getAccessTime, f.getPermission, f.getOwner, f.getGroup, pathWithAuthority)
      }.toSeq

      @transient val cachedFooters = footers.map { f =>
        // In order to encode the authority of a Path containing special characters such as /,
        // we need to use the string returned by the URI of the path to create a new Path.
        new Footer(new Path(f.getFile.toUri.toString), f.getParquetMetadata)
      }.toSeq

      // Overridden so we can inject our own cached files statuses.
      override def getPartitions: Array[SparkPartition] = {
        val inputFormat = if (cacheMetadata) {
          new FilteringParquetRowInputFormat {
            override def listStatus(jobContext: JobContext): JList[FileStatus] = cachedStatuses

            override def getFooters(jobContext: JobContext): JList[Footer] = cachedFooters
          }
        } else {
          new FilteringParquetRowInputFormat
        }

        val jobContext = newJobContext(getConf, jobId)
        val rawSplits = inputFormat.getSplits(jobContext)

        Array.tabulate[SparkPartition](rawSplits.size) { i =>
          new NewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
        }
      }
    }.values
  }

  private class MetadataCache {
    // `FileStatus` objects of all "_metadata" files.
    private var metadataStatuses: Array[FileStatus] = _

    // `FileStatus` objects of all "_common_metadata" files.
    private var commonMetadataStatuses: Array[FileStatus] = _

    // Parquet footer cache.
    var footers: Map[FileStatus, Footer] = _

    // `FileStatus` objects of all data files (Parquet part-files).
    var dataStatuses: Array[FileStatus] = _

    // Schema of the actual Parquet files, without partition columns discovered from partition
    // directory paths.
    var dataSchema: StructType = _

    // Schema of the whole table, including partition columns.
    var schema: StructType = _

    /**
     * Refreshes `FileStatus`es, footers, partition spec, and table schema.
     */
    def refresh(): Unit = {
      // Support either reading a collection of raw Parquet part-files, or a collection of folders
      // containing Parquet files (e.g. partitioned Parquet table).
      val baseStatuses = paths.distinct.flatMap { p =>
        val path = new Path(p)
        val fs = path.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
        val qualified = path.makeQualified(fs.getUri, fs.getWorkingDirectory)
        Try(fs.getFileStatus(qualified)).toOption
      }
      assert(baseStatuses.forall(!_.isDir) || baseStatuses.forall(_.isDir))

      // Lists `FileStatus`es of all leaf nodes (files) under all base directories.
      val leaves = baseStatuses.flatMap { f =>
        val fs = FileSystem.get(f.getPath.toUri, SparkHadoopUtil.get.conf)
        SparkHadoopUtil.get.listLeafStatuses(fs, f.getPath).filter { f =>
          isSummaryFile(f.getPath) ||
            !(f.getPath.getName.startsWith("_") || f.getPath.getName.startsWith("."))
        }
      }

      dataStatuses = leaves.filterNot(f => isSummaryFile(f.getPath))
      metadataStatuses = leaves.filter(_.getPath.getName == ParquetFileWriter.PARQUET_METADATA_FILE)
      commonMetadataStatuses =
        leaves.filter(_.getPath.getName == ParquetFileWriter.PARQUET_COMMON_METADATA_FILE)

      footers = (dataStatuses ++ metadataStatuses ++ commonMetadataStatuses).par.map { f =>
        val parquetMetadata = ParquetFileReader.readFooter(
          SparkHadoopUtil.get.conf, f, ParquetMetadataConverter.NO_FILTER)
        f -> new Footer(f.getPath, parquetMetadata)
      }.seq.toMap

      dataSchema = {
        val dataSchema0 =
          maybeDataSchema
            .orElse(readSchema())
            .orElse(maybeMetastoreSchema)
            .getOrElse(sys.error("Failed to get the schema."))

        // If this Parquet relation is converted from a Hive Metastore table, must reconcile case
        // case insensitivity issue and possible schema mismatch (probably caused by schema
        // evolution).
        maybeMetastoreSchema
          .map(ParquetRelation2.mergeMetastoreParquetSchema(_, dataSchema0))
          .getOrElse(dataSchema0)
      }
    }

    private def isSummaryFile(file: Path): Boolean = {
      file.getName == ParquetFileWriter.PARQUET_COMMON_METADATA_FILE ||
        file.getName == ParquetFileWriter.PARQUET_METADATA_FILE
    }

    private def readSchema(): Option[StructType] = {
      // Sees which file(s) we need to touch in order to figure out the schema.
      //
      // Always tries the summary files first if users don't require a merged schema.  In this case,
      // "_common_metadata" is more preferable than "_metadata" because it doesn't contain row
      // groups information, and could be much smaller for large Parquet files with lots of row
      // groups.
      //
      // NOTE: Metadata stored in the summary files are merged from all part-files.  However, for
      // user defined key-value metadata (in which we store Spark SQL schema), Parquet doesn't know
      // how to merge them correctly if some key is associated with different values in different
      // part-files.  When this happens, Parquet simply gives up generating the summary file.  This
      // implies that if a summary file presents, then:
      //
      //   1. Either all part-files have exactly the same Spark SQL schema, or
      //   2. Some part-files don't contain Spark SQL schema in the key-value metadata at all (thus
      //      their schemas may differ from each other).
      //
      // Here we tend to be pessimistic and take the second case into account.  Basically this means
      // we can't trust the summary files if users require a merged schema, and must touch all part-
      // files to do the merge.
      val filesToTouch =
        if (shouldMergeSchemas) {
          // Also includes summary files, 'cause there might be empty partition directories.
          (metadataStatuses ++ commonMetadataStatuses ++ dataStatuses).toSeq
        } else {
          // Tries any "_common_metadata" first. Parquet files written by old versions or Parquet
          // don't have this.
          commonMetadataStatuses.headOption
            // Falls back to "_metadata"
            .orElse(metadataStatuses.headOption)
            // Summary file(s) not found, the Parquet file is either corrupted, or different part-
            // files contain conflicting user defined metadata (two or more values are associated
            // with a same key in different files).  In either case, we fall back to any of the
            // first part-file, and just assume all schemas are consistent.
            .orElse(dataStatuses.headOption)
            .toSeq
        }

      assert(
        filesToTouch.nonEmpty || maybeDataSchema.isDefined || maybeMetastoreSchema.isDefined,
        "No schema defined, " +
          s"and no Parquet data file or summary file found under ${paths.mkString(", ")}.")

      ParquetRelation2.readSchema(filesToTouch.map(footers.apply), sqlContext)
    }
  }
}

private[sql] object ParquetRelation2 extends Logging {
  // Whether we should merge schemas collected from all Parquet part-files.
  private[sql] val MERGE_SCHEMA = "mergeSchema"

  // Hive Metastore schema, used when converting Metastore Parquet tables.  This option is only used
  // internally.
  private[sql] val METASTORE_SCHEMA = "metastoreSchema"

  private[parquet] def readSchema(
      footers: Seq[Footer], sqlContext: SQLContext): Option[StructType] = {
    footers.map { footer =>
      val metadata = footer.getParquetMetadata.getFileMetaData
      val parquetSchema = metadata.getSchema
      val maybeSparkSchema = metadata
        .getKeyValueMetaData
        .toMap
        .get(RowReadSupport.SPARK_METADATA_KEY)
        .flatMap { serializedSchema =>
          // Don't throw even if we failed to parse the serialized Spark schema. Just fallback to
          // whatever is available.
          Try(DataType.fromJson(serializedSchema))
            .recover { case _: Throwable =>
              logInfo(
                s"Serialized Spark schema in Parquet key-value metadata is not in JSON format, " +
                  "falling back to the deprecated DataType.fromCaseClassString parser.")
              DataType.fromCaseClassString(serializedSchema)
            }
            .recover { case cause: Throwable =>
              logWarning(
                s"""Failed to parse serialized Spark schema in Parquet key-value metadata:
                   |\t$serializedSchema
                 """.stripMargin,
                cause)
            }
            .map(_.asInstanceOf[StructType])
            .toOption
        }

      maybeSparkSchema.getOrElse {
        // Falls back to Parquet schema if Spark SQL schema is absent.
        StructType.fromAttributes(
          // TODO Really no need to use `Attribute` here, we only need to know the data type.
          ParquetTypesConverter.convertToAttributes(
            parquetSchema,
            sqlContext.conf.isParquetBinaryAsString,
            sqlContext.conf.isParquetINT96AsTimestamp))
      }
    }.reduceOption { (left, right) =>
      try left.merge(right) catch { case e: Throwable =>
        throw new SparkException(s"Failed to merge incompatible schemas $left and $right", e)
      }
    }
  }

  /**
   * Reconciles Hive Metastore case insensitivity issue and data type conflicts between Metastore
   * schema and Parquet schema.
   *
   * Hive doesn't retain case information, while Parquet is case sensitive. On the other hand, the
   * schema read from Parquet files may be incomplete (e.g. older versions of Parquet doesn't
   * distinguish binary and string).  This method generates a correct schema by merging Metastore
   * schema data types and Parquet schema field names.
   */
  private[parquet] def mergeMetastoreParquetSchema(
      metastoreSchema: StructType,
      parquetSchema: StructType): StructType = {
    def schemaConflictMessage: String =
      s"""Converting Hive Metastore Parquet, but detected conflicting schemas. Metastore schema:
         |${metastoreSchema.prettyJson}
         |
         |Parquet schema:
         |${parquetSchema.prettyJson}
       """.stripMargin

    val mergedParquetSchema = mergeMissingNullableFields(metastoreSchema, parquetSchema)

    assert(metastoreSchema.size <= mergedParquetSchema.size, schemaConflictMessage)

    val ordinalMap = metastoreSchema.zipWithIndex.map {
      case (field, index) => field.name.toLowerCase -> index
    }.toMap

    val reorderedParquetSchema = mergedParquetSchema.sortBy(f =>
      ordinalMap.getOrElse(f.name.toLowerCase, metastoreSchema.size + 1))

    StructType(metastoreSchema.zip(reorderedParquetSchema).map {
      // Uses Parquet field names but retains Metastore data types.
      case (mSchema, pSchema) if mSchema.name.toLowerCase == pSchema.name.toLowerCase =>
        mSchema.copy(name = pSchema.name)
      case _ =>
        throw new SparkException(schemaConflictMessage)
    })
  }

  /**
   * Returns the original schema from the Parquet file with any missing nullable fields from the
   * Hive Metastore schema merged in.
   *
   * When constructing a DataFrame from a collection of structured data, the resulting object has
   * a schema corresponding to the union of the fields present in each element of the collection.
   * Spark SQL simply assigns a null value to any field that isn't present for a particular row.
   * In some cases, it is possible that a given table partition stored as a Parquet file doesn't
   * contain a particular nullable field in its schema despite that field being present in the
   * table schema obtained from the Hive Metastore. This method returns a schema representing the
   * Parquet file schema along with any additional nullable fields from the Metastore schema
   * merged in.
   */
  private[parquet] def mergeMissingNullableFields(
      metastoreSchema: StructType,
      parquetSchema: StructType): StructType = {
    val fieldMap = metastoreSchema.map(f => f.name.toLowerCase -> f).toMap
    val missingFields = metastoreSchema
      .map(_.name.toLowerCase)
      .diff(parquetSchema.map(_.name.toLowerCase))
      .map(fieldMap(_))
      .filter(_.nullable)
    StructType(parquetSchema ++ missingFields)
  }
}
