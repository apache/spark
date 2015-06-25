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

import java.net.URI
import java.util.{List => JList}

import scala.collection.JavaConversions._
import scala.util.Try

import com.google.common.base.Objects
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import parquet.filter2.predicate.FilterApi
import parquet.hadoop._
import parquet.hadoop.metadata.CompressionCodecName
import parquet.hadoop.util.ContextUtil

import org.apache.spark.{Partition => SparkPartition, SerializableWritable, Logging, SparkException}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Row, SQLConf, SQLContext}
import org.apache.spark.util.Utils

private[sql] class DefaultSource extends HadoopFsRelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      paths: Array[String],
      schema: Option[StructType],
      partitionColumns: Option[StructType],
      parameters: Map[String, String]): HadoopFsRelation = {
    new ParquetRelation2(paths, schema, None, partitionColumns, parameters)(sqlContext)
  }
}

// NOTE: This class is instantiated and used on executor side only, no need to be serializable.
private[sql] class ParquetOutputWriter(path: String, context: TaskAttemptContext)
  extends OutputWriter {

  private val recordWriter: RecordWriter[Void, Row] = {
    val outputFormat = {
      new ParquetOutputFormat[Row]() {
        // Here we override `getDefaultWorkFile` for two reasons:
        //
        //  1. To allow appending.  We need to generate unique output file names to avoid
        //     overwriting existing files (either exist before the write job, or are just written
        //     by other tasks within the same write job).
        //
        //  2. To allow dynamic partitioning.  Default `getDefaultWorkFile` uses
        //     `FileOutputCommitter.getWorkPath()`, which points to the base directory of all
        //     partitions in the case of dynamic partitioning.
        override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
          val uniqueWriteJobId = context.getConfiguration.get("spark.sql.sources.writeJobUUID")
          val split = context.getTaskAttemptID.getTaskID.getId
          new Path(path, f"part-r-$split%05d-$uniqueWriteJobId$extension")
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
    // This is for metastore conversion.
    private val maybePartitionSpec: Option[PartitionSpec],
    override val userDefinedPartitionColumns: Option[StructType],
    parameters: Map[String, String])(
    val sqlContext: SQLContext)
  extends HadoopFsRelation(maybePartitionSpec)
  with Logging {

  private[sql] def this(
      paths: Array[String],
      maybeDataSchema: Option[StructType],
      maybePartitionSpec: Option[PartitionSpec],
      parameters: Map[String, String])(
      sqlContext: SQLContext) = {
    this(
      paths,
      maybeDataSchema,
      maybePartitionSpec,
      maybePartitionSpec.map(_.partitionColumns),
      parameters)(sqlContext)
  }

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

  override def equals(other: Any): Boolean = other match {
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
        partitionColumns)
    } else {
      Objects.hashCode(
        Boolean.box(shouldMergeSchemas),
        paths.toSet,
        dataSchema,
        schema,
        maybeDataSchema,
        partitionColumns)
    }
  }

  override def dataSchema: StructType = maybeDataSchema.getOrElse(metadataCache.dataSchema)

  override private[sql] def refresh(): Unit = {
    super.refresh()
    metadataCache.refresh()
  }

  // Parquet data source always uses Catalyst internal representations.
  override val needConversion: Boolean = false

  override def sizeInBytes: Long = metadataCache.dataStatuses.map(_.getLen).sum

  override def prepareJobForWrite(job: Job): OutputWriterFactory = {
    val conf = ContextUtil.getConfiguration(job)

    val committerClass =
      conf.getClass(
        "spark.sql.parquet.output.committer.class",
        classOf[ParquetOutputCommitter],
        classOf[ParquetOutputCommitter])

    if (conf.get("spark.sql.parquet.output.committer.class") == null) {
      logInfo("Using default output committer for Parquet: " +
        classOf[ParquetOutputCommitter].getCanonicalName)
    } else {
      logInfo("Using user defined output committer for Parquet: " + committerClass.getCanonicalName)
    }

    conf.setClass(
      SQLConf.OUTPUT_COMMITTER_CLASS,
      committerClass,
      classOf[ParquetOutputCommitter])

    // We're not really using `ParquetOutputFormat[Row]` for writing data here, because we override
    // it in `ParquetOutputWriter` to support appending and dynamic partitioning.  The reason why
    // we set it here is to setup the output committer class to `ParquetOutputCommitter`, which is
    // bundled with `ParquetOutputFormat[Row]`.
    job.setOutputFormatClass(classOf[ParquetOutputFormat[Row]])

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
      inputFiles: Array[FileStatus],
      broadcastedConf: Broadcast[SerializableWritable[Configuration]]): RDD[Row] = {
    val useMetadataCache = sqlContext.getConf(SQLConf.PARQUET_CACHE_METADATA, "true").toBoolean
    val parquetFilterPushDown = sqlContext.conf.parquetFilterPushDown
    // Create the function to set variable Parquet confs at both driver and executor side.
    val initLocalJobFuncOpt =
      ParquetRelation2.initializeLocalJobFunc(
        requiredColumns,
        filters,
        dataSchema,
        useMetadataCache,
        parquetFilterPushDown) _
    // Create the function to set input paths at the driver side.
    val setInputPaths = ParquetRelation2.initializeDriverSideJobFunc(inputFiles) _

    val footers = inputFiles.map(f => metadataCache.footers(f.getPath))

    Utils.withDummyCallSite(sqlContext.sparkContext) {
      // TODO Stop using `FilteringParquetRowInputFormat` and overriding `getPartition`.
      // After upgrading to Parquet 1.6.0, we should be able to stop caching `FileStatus` objects
      // and footers. Especially when a global arbitrative schema (either from metastore or data
      // source DDL) is available.
      new SqlNewHadoopRDD(
        sc = sqlContext.sparkContext,
        broadcastedConf = broadcastedConf,
        initDriverSideJobFuncOpt = Some(setInputPaths),
        initLocalJobFuncOpt = Some(initLocalJobFuncOpt),
        inputFormatClass = classOf[FilteringParquetRowInputFormat],
        keyClass = classOf[Void],
        valueClass = classOf[Row]) {

        val cacheMetadata = useMetadataCache

        @transient val cachedStatuses = inputFiles.map { f =>
          // In order to encode the authority of a Path containing special characters such as '/'
          // (which does happen in some S3N credentials), we need to use the string returned by the
          // URI of the path to create a new Path.
          val pathWithEscapedAuthority = escapePathUserInfo(f.getPath)
          new FileStatus(
            f.getLen, f.isDir, f.getReplication, f.getBlockSize, f.getModificationTime,
            f.getAccessTime, f.getPermission, f.getOwner, f.getGroup, pathWithEscapedAuthority)
        }.toSeq

        @transient val cachedFooters = footers.map { f =>
          // In order to encode the authority of a Path containing special characters such as /,
          // we need to use the string returned by the URI of the path to create a new Path.
          new Footer(escapePathUserInfo(f.getFile), f.getParquetMetadata)
        }.toSeq

        private def escapePathUserInfo(path: Path): Path = {
          val uri = path.toUri
          new Path(new URI(
            uri.getScheme, uri.getRawUserInfo, uri.getHost, uri.getPort, uri.getPath,
            uri.getQuery, uri.getFragment))
        }

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

          val jobContext = newJobContext(getConf(isDriverSide = true), jobId)
          val rawSplits = inputFormat.getSplits(jobContext)

          Array.tabulate[SparkPartition](rawSplits.size) { i =>
            new SqlNewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
          }
        }
      }.values
    }
  }

  private class MetadataCache {
    // `FileStatus` objects of all "_metadata" files.
    private var metadataStatuses: Array[FileStatus] = _

    // `FileStatus` objects of all "_common_metadata" files.
    private var commonMetadataStatuses: Array[FileStatus] = _

    // Parquet footer cache.
    var footers: Map[Path, Footer] = _

    // `FileStatus` objects of all data files (Parquet part-files).
    var dataStatuses: Array[FileStatus] = _

    // Schema of the actual Parquet files, without partition columns discovered from partition
    // directory paths.
    var dataSchema: StructType = null

    // Schema of the whole table, including partition columns.
    var schema: StructType = _

    /**
     * Refreshes `FileStatus`es, footers, partition spec, and table schema.
     */
    def refresh(): Unit = {
      // Lists `FileStatus`es of all leaf nodes (files) under all base directories.
      val leaves = cachedLeafStatuses().filter { f =>
        isSummaryFile(f.getPath) ||
          !(f.getPath.getName.startsWith("_") || f.getPath.getName.startsWith("."))
      }.toArray

      dataStatuses = leaves.filterNot(f => isSummaryFile(f.getPath))
      metadataStatuses = leaves.filter(_.getPath.getName == ParquetFileWriter.PARQUET_METADATA_FILE)
      commonMetadataStatuses =
        leaves.filter(_.getPath.getName == ParquetFileWriter.PARQUET_COMMON_METADATA_FILE)

      footers = {
        val conf = SparkHadoopUtil.get.conf
        val taskSideMetaData = conf.getBoolean(ParquetInputFormat.TASK_SIDE_METADATA, true)
        val rawFooters = if (shouldMergeSchemas) {
          ParquetFileReader.readAllFootersInParallel(
            conf, seqAsJavaList(leaves), taskSideMetaData)
        } else {
          ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(
            conf, seqAsJavaList(leaves), taskSideMetaData)
        }

        rawFooters.map(footer => footer.getFile -> footer).toMap
      }

      // If we already get the schema, don't need to re-compute it since the schema merging is
      // time-consuming.
      if (dataSchema == null) {
        dataSchema = {
          val dataSchema0 = maybeDataSchema
            .orElse(readSchema())
            .orElse(maybeMetastoreSchema)
            .getOrElse(throw new AnalysisException(
              s"Failed to discover schema of Parquet file(s) in the following location(s):\n" +
                paths.mkString("\n\t")))

          // If this Parquet relation is converted from a Hive Metastore table, must reconcile case
          // case insensitivity issue and possible schema mismatch (probably caused by schema
          // evolution).
          maybeMetastoreSchema
            .map(ParquetRelation2.mergeMetastoreParquetSchema(_, dataSchema0))
            .getOrElse(dataSchema0)
        }
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

      ParquetRelation2.readSchema(filesToTouch.map(f => footers.apply(f.getPath)), sqlContext)
    }
  }
}

private[sql] object ParquetRelation2 extends Logging {
  // Whether we should merge schemas collected from all Parquet part-files.
  private[sql] val MERGE_SCHEMA = "mergeSchema"

  // Hive Metastore schema, used when converting Metastore Parquet tables.  This option is only used
  // internally.
  private[sql] val METASTORE_SCHEMA = "metastoreSchema"

  /** This closure sets various Parquet configurations at both driver side and executor side. */
  private[parquet] def initializeLocalJobFunc(
      requiredColumns: Array[String],
      filters: Array[Filter],
      dataSchema: StructType,
      useMetadataCache: Boolean,
      parquetFilterPushDown: Boolean)(job: Job): Unit = {
    val conf = job.getConfiguration
    conf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[RowReadSupport].getName())

    // Try to push down filters when filter push-down is enabled.
    if (parquetFilterPushDown) {
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
    conf.set(SQLConf.PARQUET_CACHE_METADATA, useMetadataCache.toString)
  }

  /** This closure sets input paths at the driver side. */
  private[parquet] def initializeDriverSideJobFunc(
      inputFiles: Array[FileStatus])(job: Job): Unit = {
    // We side the input paths at the driver side.
    if (inputFiles.nonEmpty) {
      FileInputFormat.setInputPaths(job, inputFiles.map(_.getPath): _*)
    }
  }

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
