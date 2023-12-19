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

package org.apache.spark.sql.execution.datasources.xml

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.ExprUtils
import org.apache.spark.sql.catalyst.util.CompressionCodecs
import org.apache.spark.sql.catalyst.xml.{StaxXmlParser, XmlOptions}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

/**
 * Provides access to XML data from pure SQL statements.
 */
class XmlFileFormat extends TextBasedFileFormat with DataSourceRegister {

  override def shortName(): String = "xml"

  def getXmlOptions(
      sparkSession: SparkSession,
      parameters: Map[String, String]): XmlOptions = {
    new XmlOptions(parameters,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord,
      true)
  }

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    val xmlOptions = getXmlOptions(sparkSession, options)
    val xmlDataSource = XmlDataSource(xmlOptions)
    xmlDataSource.isSplitable && super.isSplitable(sparkSession, options, path)
  }

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    val xmlOptions = getXmlOptions(sparkSession, options)

    XmlDataSource(xmlOptions).inferSchema(
      sparkSession, files, xmlOptions)
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    val conf = job.getConfiguration
    val xmlOptions = getXmlOptions(sparkSession, options)

    xmlOptions.compressionCodec.foreach { codec =>
      CompressionCodecs.setCodecConfiguration(conf, codec)
    }

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new XmlOutputWriter(path, dataSchema, context, xmlOptions)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        ".xml" + CodecStreams.getCompressionExtension(context)
      }
    }
  }

  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    val xmlOptions = getXmlOptions(sparkSession, options)

    val columnNameOfCorruptRecord = xmlOptions.columnNameOfCorruptRecord
    // Check a field requirement for corrupt records here to throw an exception in a driver side
    ExprUtils.verifyColumnNameOfCorruptRecord(dataSchema, columnNameOfCorruptRecord)
    // Don't push any filter which refers to the "virtual" column which cannot present in the input.
    // Such filters will be applied later on the upper layer.
    val actualRequiredSchema = StructType(
      requiredSchema.filterNot(_.name == columnNameOfCorruptRecord))
    if (requiredSchema.length == 1 &&
      requiredSchema.head.name == columnNameOfCorruptRecord) {
      throw QueryCompilationErrors.queryFromRawFilesIncludeCorruptRecordColumnError()
    }

    (file: PartitionedFile) => {
      val parser = new StaxXmlParser(
        actualRequiredSchema,
        xmlOptions)
      XmlDataSource(xmlOptions).readFile(
        broadcastedHadoopConf.value.value,
        file,
        parser,
        requiredSchema)
    }
  }

  override def toString: String = "XML"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[XmlFileFormat]

  override def supportDataType(dataType: DataType): Boolean = dataType match {
    case _: VariantType => false

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
