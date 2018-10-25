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

import java.nio.charset.{Charset, StandardCharsets}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.{JacksonGenerator, JacksonParser, JSONOptions, JSONOptionsInRead}
import org.apache.spark.sql.catalyst.util.CompressionCodecs
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
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
    JsonDataSource(parsedOptions).inferSchema(
      sparkSession, files, parsedOptions)
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

  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    val parsedOptions = new JSONOptionsInRead(
      options,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)

    val actualSchema =
      StructType(requiredSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))
    // Check a field requirement for corrupt records here to throw an exception in a driver side
    dataSchema.getFieldIndex(parsedOptions.columnNameOfCorruptRecord).foreach { corruptFieldIndex =>
      val f = dataSchema(corruptFieldIndex)
      if (f.dataType != StringType || !f.nullable) {
        throw new AnalysisException(
          "The field for corrupt records must be string type and nullable")
      }
    }

    if (requiredSchema.length == 1 &&
      requiredSchema.head.name == parsedOptions.columnNameOfCorruptRecord) {
      throw new AnalysisException(
        "Since Spark 2.3, the queries from raw JSON/CSV files are disallowed when the\n" +
        "referenced columns only include the internal corrupt record column\n" +
        s"(named _corrupt_record by default). For example:\n" +
        "spark.read.schema(schema).json(file).filter($\"_corrupt_record\".isNotNull).count()\n" +
        "and spark.read.schema(schema).json(file).select(\"_corrupt_record\").show().\n" +
        "Instead, you can cache or save the parsed results and then send the same query.\n" +
        "For example, val df = spark.read.schema(schema).json(file).cache() and then\n" +
        "df.filter($\"_corrupt_record\".isNotNull).count()."
      )
    }

    (file: PartitionedFile) => {
      val parser = new JacksonParser(actualSchema, parsedOptions)
      JsonDataSource(parsedOptions).readFile(
        broadcastedHadoopConf.value.value,
        file,
        parser,
        requiredSchema)
    }
  }

  override def toString: String = "JSON"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[JsonFileFormat]

  override def supportDataType(dataType: DataType, isReadPath: Boolean): Boolean = dataType match {
    case _: AtomicType => true

    case st: StructType => st.forall { f => supportDataType(f.dataType, isReadPath) }

    case ArrayType(elementType, _) => supportDataType(elementType, isReadPath)

    case MapType(keyType, valueType, _) =>
      supportDataType(keyType, isReadPath) && supportDataType(valueType, isReadPath)

    case udt: UserDefinedType[_] => supportDataType(udt.sqlType, isReadPath)

    case _: NullType => true

    case _ => false
  }
}

private[json] class JsonOutputWriter(
    path: String,
    options: JSONOptions,
    dataSchema: StructType,
    context: TaskAttemptContext)
  extends OutputWriter with Logging {

  private val encoding = options.encoding match {
    case Some(charsetName) => Charset.forName(charsetName)
    case None => StandardCharsets.UTF_8
  }

  if (JSONOptionsInRead.blacklist.contains(encoding)) {
    logWarning(s"The JSON file ($path) was written in the encoding ${encoding.displayName()}" +
         " which can be read back by Spark only if multiLine is enabled.")
  }

  private val writer = CodecStreams.createOutputStreamWriter(
    context, new Path(path), encoding)

  // create the Generator without separator inserted between 2 records
  private[this] val gen = new JacksonGenerator(dataSchema, writer, options)

  override def write(row: InternalRow): Unit = {
    gen.write(row)
    gen.writeLineEnding()
  }

  override def close(): Unit = {
    gen.close()
    writer.close()
  }
}
