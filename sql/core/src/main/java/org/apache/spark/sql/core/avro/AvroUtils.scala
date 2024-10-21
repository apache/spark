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
package org.apache.spark.sql.core.avro

import java.io.{FileNotFoundException, IOException}
import java.util.Locale

import scala.jdk.CollectionConverters._

import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, FileReader}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.mapred.{AvroOutputFormat, FsInput}
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.{SparkException, SparkIllegalArgumentException}
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{CODEC_LEVEL, CODEC_NAME, CONFIG, PATH}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{FileSourceOptions, InternalRow}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.core.avro.AvroCompressionCodec._
import org.apache.spark.sql.core.avro.AvroOptions.IGNORE_EXTENSION
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

private[sql] object AvroUtils extends Logging {
  def inferSchema(
                   spark: SparkSession,
                   options: Map[String, String],
                   files: Seq[FileStatus]): Option[StructType] = {
    val conf = spark.sessionState.newHadoopConfWithOptions(options)
    val parsedOptions = new AvroOptions(options, conf)

    if (parsedOptions.parameters.contains(IGNORE_EXTENSION)) {
      logWarning(log"Option ${MDC(CONFIG, IGNORE_EXTENSION)} is deprecated. Please use the " +
        log"general data source option pathGlobFilter for filtering file names.")
    }
    // User can specify an optional avro json schema.
    val avroSchema = parsedOptions.schema
      .getOrElse {
        inferAvroSchemaFromFiles(files, conf, parsedOptions.ignoreExtension,
          new FileSourceOptions(CaseInsensitiveMap(options)).ignoreCorruptFiles)
      }

    SchemaConverters.toSqlType(
      avroSchema,
      parsedOptions.useStableIdForUnionType,
      parsedOptions.stableIdPrefixForUnionType,
      parsedOptions.recursiveFieldMaxDepth).dataType match {
      case t: StructType => Some(t)
      case _ => throw new RuntimeException(
        s"""Avro schema cannot be converted to a Spark SQL StructType:
           |
           |${avroSchema.toString(true)}
           |""".stripMargin)
    }
  }

  def supportsDataType(dataType: DataType): Boolean = dataType match {
    case _: VariantType => false

    case _: AtomicType => true

    case st: StructType => st.forall { f => supportsDataType(f.dataType) }

    case ArrayType(elementType, _) => supportsDataType(elementType)

    case MapType(keyType, valueType, _) =>
      supportsDataType(keyType) && supportsDataType(valueType)

    case udt: UserDefinedType[_] => supportsDataType(udt.sqlType)

    case _: NullType => true

    case _ => false
  }

  def prepareWrite(
                    sqlConf: SQLConf,
                    job: Job,
                    options: Map[String, String],
                    dataSchema: StructType): OutputWriterFactory = {
    val parsedOptions = new AvroOptions(options, job.getConfiguration)
    val outputAvroSchema: Schema = parsedOptions.schema
      .getOrElse(SchemaConverters.toAvroType(dataSchema, nullable = false,
        parsedOptions.recordName, parsedOptions.recordNamespace))

    AvroJob.setOutputKeySchema(job, outputAvroSchema)

    parsedOptions.compression.toLowerCase(Locale.ROOT) match {
      case codecName if AvroCompressionCodec.values().exists(c => c.lowerCaseName() == codecName) =>
        val jobConf = job.getConfiguration
        AvroCompressionCodec.fromString(codecName) match {
          case UNCOMPRESSED =>
            jobConf.setBoolean("mapreduce.output.fileoutputformat.compress", false)
          case compressed =>
            jobConf.setBoolean("mapreduce.output.fileoutputformat.compress", true)
            jobConf.set(AvroJob.CONF_OUTPUT_CODEC, compressed.getCodecName)
            if (compressed.getSupportCompressionLevel) {
              val level = sqlConf.getConfString(s"spark.sql.avro.$codecName.level",
                compressed.getDefaultCompressionLevel.toString)
              logInfo(log"Compressing Avro output using the ${MDC(CODEC_NAME, codecName)} codec " +
                log"at level ${MDC(CODEC_LEVEL, level)}")
              val s = if (compressed == ZSTANDARD) {
                val bufferPoolEnabled = sqlConf.getConf(SQLConf.AVRO_ZSTANDARD_BUFFER_POOL_ENABLED)
                jobConf.setBoolean(AvroOutputFormat.ZSTD_BUFFERPOOL_KEY, bufferPoolEnabled)
                "zstd"
              } else {
                codecName
              }
              jobConf.setInt(s"avro.mapred.$s.level", level.toInt)
            } else {
              logInfo(log"Compressing Avro output using the ${MDC(CODEC_NAME, codecName)} codec")
            }
        }
      case unknown =>
        throw new SparkIllegalArgumentException(
          "CODEC_SHORT_NAME_NOT_FOUND", Map("codecName" -> unknown))
    }

    new AvroOutputWriterFactory(dataSchema,
      outputAvroSchema.toString,
      parsedOptions.positionalFieldMatching)
  }

  private def inferAvroSchemaFromFiles(
                                        files: Seq[FileStatus],
                                        conf: Configuration,
                                        ignoreExtension: Boolean,
                                        ignoreCorruptFiles: Boolean): Schema = {
    // Schema evolution is not supported yet. Here we only pick first random readable sample file to
    // figure out the schema of the whole dataset.
    val avroReader = files.iterator.map { f =>
      val path = f.getPath
      if (!ignoreExtension && !path.getName.endsWith(".avro")) {
        None
      } else {
        Utils.tryWithResource {
          new FsInput(path, conf)
        } { in =>
          try {
            Some(DataFileReader.openReader(in, new GenericDatumReader[GenericRecord]()))
          } catch {
            case e: IOException =>
              if (ignoreCorruptFiles) {
                logWarning(log"Skipped the footer in the corrupted file: ${MDC(PATH, path)}", e)
                None
              } else {
                throw new SparkException(s"Could not read file: $path", e)
              }
          }
        }
      }
    }.collectFirst {
      case Some(reader) => reader
    }

    avroReader match {
      case Some(reader) =>
        try {
          reader.getSchema
        } finally {
          reader.close()
        }
      case None =>
        throw new FileNotFoundException(
          "No Avro files found. If files don't have .avro extension, set ignoreExtension to true")
    }
  }

  // The trait provides iterator-like interface for reading records from an Avro file,
  // deserializing and returning them as internal rows.
  trait RowReader {
    protected val fileReader: FileReader[GenericRecord]
    protected val deserializer: AvroDeserializer
    protected val stopPosition: Long

    private[this] var completed = false
    private[this] var currentRow: Option[InternalRow] = None

    def hasNextRow: Boolean = {
      while (!completed && currentRow.isEmpty) {
        // In the case of empty blocks in an Avro file, `blockRemaining` could still read as 0 so
        // `fileReader.hasNext()` returns false but advances the cursor to the next block, so we
        // need to call `fileReader.hasNext()` again to correctly report if the next record
        // exists.
        val moreData =
          (fileReader.hasNext || fileReader.hasNext) && !fileReader.pastSync(stopPosition)
        if (!moreData) {
          fileReader.close()
          completed = true
          currentRow = None
        } else {
          val record = fileReader.next()
          // the row must be deserialized in hasNextRow, because AvroDeserializer#deserialize
          // potentially filters rows
          currentRow = deserializer.deserialize(record).asInstanceOf[Option[InternalRow]]
        }
      }
      currentRow.isDefined
    }

    def nextRow: InternalRow = {
      if (currentRow.isEmpty) {
        hasNextRow
      }
      val returnRow = currentRow
      currentRow = None // free up hasNextRow to consume more Avro records, if not exhausted
      returnRow.getOrElse {
        throw new NoSuchElementException("next on empty iterator")
      }
    }
  }

  /** Wrapper for a pair of matched fields, one Catalyst and one corresponding Avro field. */
  private[sql] case class AvroMatchedField(
                                            catalystField: StructField,
                                            catalystPosition: Int,
                                            avroField: Schema.Field)

  /**
   * Helper class to perform field lookup/matching on Avro schemas.
   *
   * This will match `avroSchema` against `catalystSchema`, attempting to find a matching field in
   * the Avro schema for each field in the Catalyst schema and vice-versa, respecting settings for
   * case sensitivity. The match results can be accessed using the getter methods.
   *
   * @param avroSchema The schema in which to search for fields. Must be of type RECORD.
   * @param catalystSchema The Catalyst schema to use for matching.
   * @param avroPath The seq of parent field names leading to `avroSchema`.
   * @param catalystPath The seq of parent field names leading to `catalystSchema`.
   * @param positionalFieldMatch If true, perform field matching in a positional fashion
   *                             (structural comparison between schemas, ignoring names);
   *                             otherwise, perform field matching using field names.
   */
  class AvroSchemaHelper(
                          avroSchema: Schema,
                          catalystSchema: StructType,
                          avroPath: Seq[String],
                          catalystPath: Seq[String],
                          positionalFieldMatch: Boolean) {
    if (avroSchema.getType != Schema.Type.RECORD) {
      throw new IncompatibleSchemaException(
        s"Attempting to treat ${avroSchema.getName} as a RECORD, but it was: ${avroSchema.getType}")
    }

    private[this] val avroFieldArray = avroSchema.getFields.asScala.toArray
    private[this] val fieldMap = avroSchema.getFields.asScala
      .groupBy(_.name.toLowerCase(Locale.ROOT))
      .transform((_, v) => v.toSeq) // toSeq needed for scala 2.13

    /** The fields which have matching equivalents in both Avro and Catalyst schemas. */
    val matchedFields: Seq[AvroMatchedField] = catalystSchema.zipWithIndex.flatMap {
      case (sqlField, sqlPos) =>
        getAvroField(sqlField.name, sqlPos).map(AvroMatchedField(sqlField, sqlPos, _))
    }

    /**
     * Validate that there are no Catalyst fields which don't have a matching Avro field, throwing
     * [[IncompatibleSchemaException]] if such extra fields are found. If `ignoreNullable` is false,
     * consider nullable Catalyst fields to be eligible to be an extra field; otherwise,
     * ignore nullable Catalyst fields when checking for extras.
     */
    def validateNoExtraCatalystFields(ignoreNullable: Boolean): Unit =
      catalystSchema.zipWithIndex.foreach { case (sqlField, sqlPos) =>
        if (getAvroField(sqlField.name, sqlPos).isEmpty &&
          (!ignoreNullable || !sqlField.nullable)) {
          if (positionalFieldMatch) {
            throw new IncompatibleSchemaException("Cannot find field at position " +
              s"$sqlPos of ${toFieldStr(avroPath)} from Avro schema (using positional matching)")
          } else {
            throw new IncompatibleSchemaException(
              s"Cannot find ${toFieldStr(catalystPath :+ sqlField.name)} in Avro schema")
          }
        }
      }

    /**
     * Validate that there are no Avro fields which don't have a matching Catalyst field, throwing
     * [[IncompatibleSchemaException]] if such extra fields are found. Only required (non-nullable)
     * fields are checked; nullable fields are ignored.
     */
    def validateNoExtraRequiredAvroFields(): Unit = {
      val extraFields = avroFieldArray.toSet -- matchedFields.map(_.avroField)
      extraFields.filterNot(isNullable).foreach { extraField =>
        if (positionalFieldMatch) {
          throw new IncompatibleSchemaException(s"Found field '${extraField.name()}' at position " +
            s"${extraField.pos()} of ${toFieldStr(avroPath)} from Avro schema but there is no " +
            s"match in the SQL schema at ${toFieldStr(catalystPath)} (using positional matching)")
        } else {
          throw new IncompatibleSchemaException(
            s"Found ${toFieldStr(avroPath :+ extraField.name())} in Avro schema but there is no " +
              "match in the SQL schema")
        }
      }
    }

    /**
     * Extract a single field from the contained avro schema which has the desired field name,
     * performing the matching with proper case sensitivity according to SQLConf.resolver.
     *
     * @param name The name of the field to search for.
     * @return `Some(match)` if a matching Avro field is found, otherwise `None`.
     */
    def getFieldByName(name: String): Option[Schema.Field] = {

      // get candidates, ignoring case of field name
      val candidates = fieldMap.getOrElse(name.toLowerCase(Locale.ROOT), Seq.empty)

      // search candidates, taking into account case sensitivity settings
      candidates.filter(f => SQLConf.get.resolver(f.name(), name)) match {
        case Seq(avroField) => Some(avroField)
        case Seq() => None
        case matches => throw new IncompatibleSchemaException(s"Searching for '$name' in Avro " +
          s"schema at ${toFieldStr(avroPath)} gave ${matches.size} matches. Candidates: " +
          matches.map(_.name()).mkString("[", ", ", "]")
        )
      }
    }

    /** Get the Avro field corresponding to the provided Catalyst field name/position, if any. */
    def getAvroField(fieldName: String, catalystPos: Int): Option[Schema.Field] = {
      if (positionalFieldMatch) {
        avroFieldArray.lift(catalystPos)
      } else {
        getFieldByName(fieldName)
      }
    }
  }

  /**
   * Convert a sequence of hierarchical field names (like `Seq(foo, bar)`) into a human-readable
   * string representing the field, like "field 'foo.bar'". If `names` is empty, the string
   * "top-level record" is returned.
   */
  private[avro] def toFieldStr(names: Seq[String]): String = names match {
    case Seq() => "top-level record"
    case n => s"field '${n.mkString(".")}'"
  }

  /** Return true iff `avroField` is nullable, i.e. `UNION` type and has `NULL` as an option. */
  private[avro] def isNullable(avroField: Schema.Field): Boolean =
    avroField.schema().getType == Schema.Type.UNION &&
      avroField.schema().getTypes.asScala.exists(_.getType == Schema.Type.NULL)

  /** Collect all non null branches of a union in order. */
  private[avro] def nonNullUnionBranches(avroType: Schema): Seq[Schema] = {
    avroType.getTypes.asScala.filter(_.getType != Schema.Type.NULL).toSeq
  }
}
