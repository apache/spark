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

package org.apache.spark.sql.execution.datasources.json

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.ExprUtils
import org.apache.spark.sql.catalyst.json._
import org.apache.spark.sql.catalyst.util.CompressionCodecs
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.types.variant.VariantMetrics
import org.apache.spark.util.SerializableConfiguration

class JsonFileFormat extends TextBasedFileFormat with DataSourceRegister {
  override val shortName: String = "json"

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    val parsedOptions = new JSONOptionsInRead(
      options,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)
    val jsonDataSource = JsonDataSource(parsedOptions)
    jsonDataSource.isSplitable && super.isSplitable(sparkSession, options, path)
  }

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    val parsedOptions = new JSONOptionsInRead(
      options,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)
    parsedOptions.singleVariantColumn match {
      case Some(columnName) => Some(StructType(Array(StructField(columnName, VariantType))))
      case None => JsonDataSource(parsedOptions).inferSchema(sparkSession, files, parsedOptions)
    }
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    val conf = job.getConfiguration
    val parsedOptions = new JSONOptions(
      options,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)
    parsedOptions.compressionCodec.foreach { codec =>
      CompressionCodecs.setCodecConfiguration(conf, codec)
    }

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new JsonOutputWriter(path, parsedOptions, dataSchema, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        ".json" + CodecStreams.getCompressionExtension(context)
      }
    }
  }

  /**
   * Exactly the same as [[buildReaderWithPartitionValues]] except that the reader function returned
   * also updates Variant Metrics
   */
  def buildReaderWithPartitionValuesAndVariantMetrics(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration,
      topLevelVariantMetrics: VariantMetrics,
      nestedVariantMetrics: VariantMetrics): PartitionedFile => Iterator[InternalRow] = {
    val dataReader = buildReaderWithVariantMetrics(sparkSession, dataSchema, partitionSchema,
      requiredSchema, filters, options, hadoopConf, topLevelVariantMetrics, nestedVariantMetrics)
    buildReaderWithPartitionValuesHelper(dataReader, partitionSchema, requiredSchema)
  }

  /**
   * Helper function for [[buildReader]] and [[buildReaderWithVariantMetrics]]
   */
  private def buildReaderHelper(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration,
      topLevelVariantMetrics: Option[VariantMetrics] = None,
      nestedVariantMetrics: Option[VariantMetrics] = None)
      : PartitionedFile => Iterator[InternalRow] = {
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    val parsedOptions = new JSONOptionsInRead(
      options,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)

    val actualSchema =
      StructType(requiredSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))
    // Check a field requirement for corrupt records here to throw an exception in a driver side
    ExprUtils.verifyColumnNameOfCorruptRecord(dataSchema, parsedOptions.columnNameOfCorruptRecord)

    if (requiredSchema.length == 1 &&
      requiredSchema.head.name == parsedOptions.columnNameOfCorruptRecord) {
      throw QueryCompilationErrors.queryFromRawFilesIncludeCorruptRecordColumnError()
    }

    (file: PartitionedFile) => {
      val parser = new JacksonParser(
        actualSchema,
        parsedOptions,
        allowArrayAsStructs = true,
        filters, topLevelVariantMetrics, nestedVariantMetrics)
      JsonDataSource(parsedOptions).readFile(
        broadcastedHadoopConf.value.value,
        file,
        parser,
        requiredSchema)
    }
  }

  private def buildReaderWithVariantMetrics(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration,
      topLevelVariantMetrics: VariantMetrics,
      nestedVariantMetrics: VariantMetrics): PartitionedFile => Iterator[InternalRow] = {
    buildReaderHelper(sparkSession, dataSchema, partitionSchema, requiredSchema, filters, options,
      hadoopConf, Some(topLevelVariantMetrics), Some(nestedVariantMetrics))
  }

  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration)
      : PartitionedFile => Iterator[InternalRow] = {
    buildReaderHelper(sparkSession, dataSchema, partitionSchema, requiredSchema, filters, options,
      hadoopConf)
  }

  override def toString: String = "JSON"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[JsonFileFormat]

  override def supportDataType(dataType: DataType): Boolean = dataType match {
    case _: VariantType => true

    case _: AtomicType => true

    case st: StructType => st.forall { f => supportDataType(f.dataType) }

    case ArrayType(elementType, _) => supportDataType(elementType)

    case MapType(keyType, valueType, _) =>
      supportDataType(keyType) && supportDataType(valueType)

    case udt: UserDefinedType[_] => supportDataType(udt.sqlType)

    case _: NullType => true

    case _ => false
  }
}
