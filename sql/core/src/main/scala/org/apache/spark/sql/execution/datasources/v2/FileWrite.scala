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

import java.util.UUID

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
import org.apache.spark.sql.connector.write.{BatchWrite, LogicalWriteInfo, Write}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, DataSource, OutputWriterFactory, WriteJobDescription}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.SerializableConfiguration

trait FileWrite extends Write {
  def paths: Seq[String]
  def formatName: String
  def supportsDataType: DataType => Boolean
  def allowDuplicatedColumnNames: Boolean = false
  def info: LogicalWriteInfo

  private val schema = info.schema()
  private val queryId = info.queryId()
  val options = info.options()

  override def description(): String = formatName

  override def toBatch: BatchWrite = {
    val sparkSession = SparkSession.active
    validateInputs(sparkSession.sessionState.conf)
    val path = new Path(paths.head)
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val job = getJobInstance(hadoopConf, path)
    val committer = FileCommitProtocol.instantiate(
      sparkSession.sessionState.conf.fileCommitProtocolClass,
      jobId = java.util.UUID.randomUUID().toString,
      outputPath = paths.head)
    lazy val description =
      createWriteJobDescription(sparkSession, hadoopConf, job, paths.head, options.asScala.toMap)

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
    DataSource.validateSchema(formatName, schema, sqlConf)

    // TODO: [SPARK-36340] Unify check schema filed of DataSource V2 Insert.
    schema.foreach { field =>
      if (!supportsDataType(field.dataType)) {
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
      options: Map[String, String]): WriteJobDescription = {
    val caseInsensitiveOptions = CaseInsensitiveMap(options)
    // Note: prepareWrite has side effect. It sets "job".
    val outputWriterFactory =
      prepareWrite(sparkSession.sessionState.conf, job, caseInsensitiveOptions, schema)
    val allColumns = toAttributes(schema)
    val metrics: Map[String, SQLMetric] = BasicWriteJobStatsTracker.metrics
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    val statsTracker = new BasicWriteJobStatsTracker(serializableHadoopConf, metrics)
    // TODO: after partitioning is supported in V2:
    //       1. filter out partition columns in `dataColumns`.
    //       2. Don't use Seq.empty for `partitionColumns`.
    new WriteJobDescription(
      uuid = UUID.randomUUID().toString,
      serializableHadoopConf = new SerializableConfiguration(job.getConfiguration),
      outputWriterFactory = outputWriterFactory,
      allColumns = allColumns,
      dataColumns = allColumns,
      partitionColumns = Seq.empty,
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

