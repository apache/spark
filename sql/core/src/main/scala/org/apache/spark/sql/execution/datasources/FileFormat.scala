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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.compress.{CompressionCodecFactory, SplittableCompressionCodec}
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DataType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String


/**
 * Used to read and write data stored in files to/from the [[InternalRow]] format.
 */
trait FileFormat {
  /**
   * When possible, this method should return the schema of the given `files`.  When the format
   * does not support inference, or no valid files are given should return None.  In these cases
   * Spark will require that user specify the schema manually.
   */
  def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType]

  /**
   * Prepares a write job and returns an [[OutputWriterFactory]].  Client side job preparation can
   * be put here.  For example, user defined output committer can be configured here
   * by setting the output committer class in the conf of spark.sql.sources.outputCommitterClass.
   */
  def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory

  /**
   * Returns whether this format supports returning columnar batch or not.
   * If columnar batch output is requested, users shall supply
   * FileFormat.OPTION_RETURNING_BATCH -> true
   * in relation options when calling buildReaderWithPartitionValues.
   * This should only be passed as true if it can actually be supported.
   * For ParquetFileFormat and OrcFileFormat, passing this option is required.
   *
   * TODO: we should just have different traits for the different formats.
   */
  def supportBatch(sparkSession: SparkSession, dataSchema: StructType): Boolean = {
    false
  }

  /**
   * Returns concrete column vector class names for each column to be used in a columnar batch
   * if this format supports returning columnar batch.
   */
  def vectorTypes(
      requiredSchema: StructType,
      partitionSchema: StructType,
      sqlConf: SQLConf): Option[Seq[String]] = {
    None
  }

  /**
   * Returns whether a file with `path` could be split or not.
   */
  def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    false
  }

  /**
   * Returns a function that can be used to read a single file in as an Iterator of InternalRow.
   *
   * @param dataSchema The global data schema. It can be either specified by the user, or
   *                   reconciled/merged from all underlying data files. If any partition columns
   *                   are contained in the files, they are preserved in this schema.
   * @param partitionSchema The schema of the partition column row that will be present in each
   *                        PartitionedFile. These columns should be appended to the rows that
   *                        are produced by the iterator.
   * @param requiredSchema The schema of the data that should be output for each row.  This may be a
   *                       subset of the columns that are present in the file if column pruning has
   *                       occurred.
   * @param filters A set of filters than can optionally be used to reduce the number of rows output
   * @param options A set of string -> string configuration options.
   * @return
   */
  protected def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    throw QueryExecutionErrors.buildReaderUnsupportedForFileFormatError(this.toString)
  }

  /**
   * Exactly the same as [[buildReader]] except that the reader function returned by this method
   * appends partition values to [[InternalRow]]s produced by the reader function [[buildReader]]
   * returns.
   */
  def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val dataReader = buildReader(
      sparkSession, dataSchema, partitionSchema, requiredSchema, filters, options, hadoopConf)

    new (PartitionedFile => Iterator[InternalRow]) with Serializable {
      private val fullSchema = requiredSchema.toAttributes ++ partitionSchema.toAttributes

      // Using lazy val to avoid serialization
      private lazy val appendPartitionColumns =
        GenerateUnsafeProjection.generate(fullSchema, fullSchema)

      override def apply(file: PartitionedFile): Iterator[InternalRow] = {
        // Using local val to avoid per-row lazy val check (pre-mature optimization?...)
        val converter = appendPartitionColumns

        // Note that we have to apply the converter even though `file.partitionValues` is empty.
        // This is because the converter is also responsible for converting safe `InternalRow`s into
        // `UnsafeRow`s.
        if (partitionSchema.isEmpty) {
          dataReader(file).map { dataRow =>
            converter(dataRow)
          }
        } else {
          val joinedRow = new JoinedRow()
          dataReader(file).map { dataRow =>
            converter(joinedRow(dataRow, file.partitionValues))
          }
        }
      }
    }
  }

  /**
   * Returns whether this format supports the given [[DataType]] in read/write path.
   * By default all data types are supported.
   */
  def supportDataType(dataType: DataType): Boolean = true

  /**
   * Returns whether this format supports the given filed name in read/write path.
   * By default all field name is supported.
   */
  def supportFieldName(name: String): Boolean = true
}

object FileFormat {

  val FILE_PATH = "file_path"

  val FILE_NAME = "file_name"

  val FILE_SIZE = "file_size"

  val FILE_MODIFICATION_TIME = "file_modification_time"

  val ROW_INDEX = "row_index"

  // A name for a temporary column that holds row indexes computed by the file format reader
  // until they can be placed in the _metadata struct.
  val ROW_INDEX_TEMPORARY_COLUMN_NAME = s"_tmp_metadata_$ROW_INDEX"

  val METADATA_NAME = "_metadata"

  /**
   * Option to pass to buildReaderWithPartitionValues to return columnar batch output or not.
   * For ParquetFileFormat and OrcFileFormat, passing this option is required.
   * This should only be passed as true if it can actually be supported, which can be checked
   * by calling supportBatch.
   */
  val OPTION_RETURNING_BATCH = "returning_batch"

  /** Schema of metadata struct that can be produced by every file format. */
  val BASE_METADATA_STRUCT: StructType = new StructType()
    .add(StructField(FileFormat.FILE_PATH, StringType))
    .add(StructField(FileFormat.FILE_NAME, StringType))
    .add(StructField(FileFormat.FILE_SIZE, LongType))
    .add(StructField(FileFormat.FILE_MODIFICATION_TIME, TimestampType))

  /**
   * Create a file metadata struct column containing fields supported by the given file format.
   */
  def createFileMetadataCol(fileFormat: FileFormat): AttributeReference = {
    val struct = if (fileFormat.isInstanceOf[ParquetFileFormat]) {
      BASE_METADATA_STRUCT.add(StructField(FileFormat.ROW_INDEX, LongType))
    } else {
      BASE_METADATA_STRUCT
    }
    FileSourceMetadataAttribute(FileFormat.METADATA_NAME, struct)
  }

  // create an internal row given required metadata fields and file information
  def createMetadataInternalRow(
      fieldNames: Seq[String],
      filePath: Path,
      fileSize: Long,
      fileModificationTime: Long): InternalRow =
    updateMetadataInternalRow(new GenericInternalRow(fieldNames.length), fieldNames,
      filePath, fileSize, fileModificationTime)

  // update an internal row given required metadata fields and file information
  def updateMetadataInternalRow(
      row: InternalRow,
      fieldNames: Seq[String],
      filePath: Path,
      fileSize: Long,
      fileModificationTime: Long): InternalRow = {
    fieldNames.zipWithIndex.foreach { case (name, i) =>
      name match {
        case FILE_PATH => row.update(i, UTF8String.fromString(filePath.toString))
        case FILE_NAME => row.update(i, UTF8String.fromString(filePath.getName))
        case FILE_SIZE => row.update(i, fileSize)
        case FILE_MODIFICATION_TIME =>
          // the modificationTime from the file is in millisecond,
          // while internally, the TimestampType `file_modification_time` is stored in microsecond
          row.update(i, fileModificationTime * 1000L)
        case ROW_INDEX =>
          // Do nothing. Only the metadata fields that have identical values for each row of the
          // file are set by this function, while fields that have different values (such as row
          // index) are set separately.
      }
    }
    row
  }

  /**
   * Returns true if the given metadata column always contains identical values for all rows
   * originating from the same data file.
   */
  def isConstantMetadataAttr(name: String): Boolean = name match {
    case FILE_PATH | FILE_NAME | FILE_SIZE | FILE_MODIFICATION_TIME => true
    case ROW_INDEX => false
  }
}

/**
 * The base class file format that is based on text file.
 */
abstract class TextBasedFileFormat extends FileFormat {
  private var codecFactory: CompressionCodecFactory = _

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    if (codecFactory == null) {
      codecFactory = new CompressionCodecFactory(
        sparkSession.sessionState.newHadoopConfWithOptions(options))
    }
    val codec = codecFactory.getCodec(path)
    codec == null || codec.isInstanceOf[SplittableCompressionCodec]
  }
}
