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
package org.apache.spark.sql.execution.datasources.v2

import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{Expressions, SortDirection}
import org.apache.spark.sql.connector.expressions.{SortOrder => V2SortOrder}
import org.apache.spark.sql.connector.write.{BatchWrite, LogicalWriteInfo, RequiresDistributionAndOrdering, Write}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, DataSource, OutputWriterFactory, WriteJobDescription}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.SerializableConfiguration

trait FileWrite extends Write
    with RequiresDistributionAndOrdering {
  def paths: Seq[String]
  def formatName: String
  def supportsDataType: DataType => Boolean
  def allowDuplicatedColumnNames: Boolean = false
  def info: LogicalWriteInfo
  def partitionSchema: StructType
  def dynamicPartitionOverwrite: Boolean = false
  def isTruncate: Boolean = false

  private val schema = info.schema()
  private val queryId = info.queryId()
  val options = info.options()

  override def description(): String = formatName

  // RequiresDistributionAndOrdering: sort by partition columns
  // to ensure DynamicPartitionDataSingleWriter sees each
  // partition value contiguously (preventing file name
  // collisions from fileCounter resets).
  override def requiredDistribution(): Distribution =
    Distributions.unspecified()

  override def requiredOrdering(): Array[V2SortOrder] = {
    if (partitionSchema.isEmpty) {
      Array.empty
    } else {
      partitionSchema.fieldNames.map { col =>
        Expressions.sort(
          Expressions.column(col),
          SortDirection.ASCENDING)
      }
    }
  }

  override def toBatch: BatchWrite = {
    val sparkSession = SparkSession.active
    validateInputs(sparkSession.sessionState.conf)
    val path = new Path(paths.head)
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)

    // Ensure the output path exists. For new writes (Append to a new path, Overwrite on a new
    // path), the path may not exist yet.
    val fs = path.getFileSystem(hadoopConf)
    val qualifiedPath = path.makeQualified(fs.getUri, fs.getWorkingDirectory)
    if (!fs.exists(qualifiedPath)) {
      fs.mkdirs(qualifiedPath)
    }

    // For truncate (full overwrite), delete existing data before writing.
    // TODO: This is not atomic - if the write fails after deletion, old data is lost.
    // Consider moving into FileBatchWrite.commit() for atomic overwrite semantics.
    if (isTruncate && fs.exists(qualifiedPath)) {
      fs.listStatus(qualifiedPath).foreach { status =>
        // Preserve hidden files/dirs (e.g., _SUCCESS, .spark-staging-*)
        if (!status.getPath.getName.startsWith("_") &&
            !status.getPath.getName.startsWith(".")) {
          fs.delete(status.getPath, true)
        }
      }
    }

    val job = getJobInstance(hadoopConf, path)
    val jobId = java.util.UUID.randomUUID().toString
    val committer = FileCommitProtocol.instantiate(
      sparkSession.sessionState.conf.fileCommitProtocolClass,
      jobId = jobId,
      outputPath = paths.head,
      dynamicPartitionOverwrite = dynamicPartitionOverwrite)
    // Evaluate description (which calls prepareWrite) BEFORE
    // setupJob, so that format-specific Job configuration
    // (e.g., Parquet JOB_SUMMARY_LEVEL) is set before the
    // OutputCommitter is created.
    val description =
      createWriteJobDescription(sparkSession, hadoopConf, job, paths.head, options.asScala.toMap,
        jobId)

    committer.setupJob(job)
    new FileBatchWrite(job, description, committer)
  }

  /**
   * Prepares a write job and returns an [[OutputWriterFactory]].  Client side job preparation can
   * be put here.  For example, user defined output committer can be configured here
   * by setting the output committer class in the conf of spark.sql.sources.outputCommitterClass.
   */
  def prepareWrite(
      sqlConf: SQLConf,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory

  private def validateInputs(sqlConf: SQLConf): Unit = {
    val caseSensitiveAnalysis = sqlConf.caseSensitiveAnalysis
    assert(schema != null, "Missing input data schema")
    assert(queryId != null, "Missing query ID")

    if (paths.length != 1) {
      throw new IllegalArgumentException("Expected exactly one path to be specified, but " +
        s"got: ${paths.mkString(", ")}")
    }
    if (!allowDuplicatedColumnNames) {
      SchemaUtils.checkColumnNameDuplication(
        schema.fields.map(_.name).toImmutableArraySeq, caseSensitiveAnalysis)
    }
    if (!sqlConf.allowCollationsInMapKeys) {
      SchemaUtils.checkNoCollationsInMapKeys(schema)
    }
    DataSource.validateSchema(formatName, schema, sqlConf)

    // Only validate data column types, not partition columns.
    // Partition columns may use types unsupported by the format
    // (e.g., INT in text) since they are written as directory
    // names, not as data values.
    val resolver = sqlConf.resolver
    val partColNames = partitionSchema.fieldNames
    schema.foreach { field =>
      if (!partColNames.exists(resolver(_, field.name)) &&
          !supportsDataType(field.dataType)) {
        throw QueryCompilationErrors.dataTypeUnsupportedByDataSourceError(formatName, field)
      }
    }
  }

  private def getJobInstance(hadoopConf: Configuration, path: Path): Job = {
    val job = Job.getInstance(hadoopConf)
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[InternalRow])
    FileOutputFormat.setOutputPath(job, path)
    job
  }

  private def createWriteJobDescription(
      sparkSession: SparkSession,
      hadoopConf: Configuration,
      job: Job,
      pathName: String,
      options: Map[String, String],
      jobId: String): WriteJobDescription = {
    val caseInsensitiveOptions = CaseInsensitiveMap(options)
    val allColumns = toAttributes(schema)
    val partitionColumnNames = partitionSchema.fields.map(_.name).toSet
    val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
    val partitionColumns = if (partitionColumnNames.nonEmpty) {
      allColumns.filter { col =>
        if (caseSensitive) {
          partitionColumnNames.contains(col.name)
        } else {
          partitionColumnNames.exists(_.equalsIgnoreCase(col.name))
        }
      }
    } else {
      Seq.empty
    }
    val dataColumns = allColumns.filterNot(partitionColumns.contains)
    // Note: prepareWrite has side effect. It sets "job".
    val dataSchema = StructType(dataColumns.map(col => schema(col.name)))
    val outputWriterFactory =
      prepareWrite(sparkSession.sessionState.conf, job, caseInsensitiveOptions, dataSchema)
    val metrics: Map[String, SQLMetric] = BasicWriteJobStatsTracker.metrics
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    val statsTracker = new BasicWriteJobStatsTracker(serializableHadoopConf, metrics)
    new WriteJobDescription(
      uuid = jobId,
      serializableHadoopConf = new SerializableConfiguration(job.getConfiguration),
      outputWriterFactory = outputWriterFactory,
      allColumns = allColumns,
      dataColumns = dataColumns,
      partitionColumns = partitionColumns,
      bucketSpec = None,
      path = pathName,
      customPartitionLocations = Map.empty,
      maxRecordsPerFile = caseInsensitiveOptions.get("maxRecordsPerFile").map(_.toLong)
        .getOrElse(sparkSession.sessionState.conf.maxRecordsPerFile),
      timeZoneId = caseInsensitiveOptions.get(DateTimeUtils.TIMEZONE_OPTION)
        .getOrElse(sparkSession.sessionState.conf.sessionLocalTimeZone),
      statsTrackers = Seq(statsTracker)
    )
  }
}

