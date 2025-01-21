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

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._


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
      private val fullSchema = toAttributes(requiredSchema) ++ toAttributes(partitionSchema)

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
   * Create a file metadata struct column containing fields supported by the given file format.
   */
  def createFileMetadataCol(): AttributeReference = {
    // Strip out the fields' metadata to avoid exposing it to the user. [[FileSourceStrategy]]
    // avoids confusion by mapping back to [[metadataSchemaFields]].
    val fields = metadataSchemaFields
      .map(FileSourceMetadataAttribute.cleanupFileSourceMetadataInformation)
    FileSourceMetadataAttribute(FileFormat.METADATA_NAME, StructType(fields), nullable = false)
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

  /**
   * Returns whether this format allows duplicated column names in the input query during writing.
   */
  def allowDuplicatedColumnNames: Boolean = false

  /**
   * All fields the file format's _metadata struct defines.
   *
   * Each metadata struct field is either "constant" or "generated" (respectively defined/matched by
   * [[FileSourceConstantMetadataStructField]] or [[FileSourceGeneratedMetadataAttribute]]).
   *
   * Constant metadata columns are derived from the [[PartitionedFile]] instances a scan's
   * [[FileIndex]] provides. Thus, a custom [[FileFormat]] that defines constant metadata columns
   * will generally pair with a a custom [[FileIndex]] that populates [[PartitionedFile]] with
   * appropriate metadata values. By default, constant attribute values are obtained by a simple
   * name-based lookup in [[PartitionedFile.extraConstantMetadataColumnValues]], but implementations
   * can override [[fileConstantMetadataExtractors]] to define custom extractors that have access to
   * the entire [[PartitionedFile]] when deriving the column's value.
   *
   * Generated metadata columns map to a hidden/internal column the underlying reader provides, and
   * so will often pair with a custom reader that can populate those columns. For example,
   * [[ParquetFileFormat]] defines a "_metadata.row_index" column that relies on
   * [[VectorizedParquetRecordReader]] to extract the actual row index values from the parquet scan.
   */
  def metadataSchemaFields: Seq[StructField] = FileFormat.BASE_METADATA_FIELDS

  /**
   * The extractors to use when deriving file-constant metadata columns for this file format.
   *
   * Implementations that define custom constant metadata columns can override this method to
   * associate a custom extractor with a given metadata column name, when a simple name-based lookup
   * in [[PartitionedFile.extraConstantMetadataColumnValues]] is not expressive enough; extractors
   * have access to the entire [[PartitionedFile]] and can perform arbitrary computations.
   *
   * NOTE: Extractors are lazy, invoked only if the query actually selects their column at runtime.
   *
   * See also [[FileFormat.getFileConstantMetadataColumnValue]].
   */
  def fileConstantMetadataExtractors: Map[String, PartitionedFile => Any] =
    FileFormat.BASE_METADATA_EXTRACTORS
}

object FileFormat {

  val FILE_PATH = "file_path"

  val FILE_NAME = "file_name"

  val FILE_BLOCK_START = "file_block_start"

  val FILE_BLOCK_LENGTH = "file_block_length"

  val FILE_SIZE = "file_size"

  val FILE_MODIFICATION_TIME = "file_modification_time"

  val METADATA_NAME = "_metadata"

  /**
   * Option to pass to buildReaderWithPartitionValues to return columnar batch output or not.
   * For ParquetFileFormat and OrcFileFormat, passing this option is required.
   * This should only be passed as true if it can actually be supported, which can be checked
   * by calling supportBatch.
   */
  val OPTION_RETURNING_BATCH = "returning_batch"

  /**
   * Schema of metadata struct that can be produced by every file format,
   * metadata fields for every file format must be *not* nullable.
   */
  val BASE_METADATA_FIELDS: Seq[StructField] = Seq(
    FileSourceConstantMetadataStructField(FILE_PATH, StringType, nullable = false),
    FileSourceConstantMetadataStructField(FILE_NAME, StringType, nullable = false),
    FileSourceConstantMetadataStructField(FILE_SIZE, LongType, nullable = false),
    FileSourceConstantMetadataStructField(FILE_BLOCK_START, LongType, nullable = false),
    FileSourceConstantMetadataStructField(FILE_BLOCK_LENGTH, LongType, nullable = false),
    FileSourceConstantMetadataStructField(FILE_MODIFICATION_TIME, TimestampType, nullable = false))

  /**
   * All [[BASE_METADATA_FIELDS]] require custom extractors because they are derived directly from
   * fields of the [[PartitionedFile]], and do have entries in the file's metadata map.
   */
  val BASE_METADATA_EXTRACTORS: Map[String, PartitionedFile => Any] = Map(
    FILE_PATH -> { pf: PartitionedFile =>
      // Use `new Path(Path.toString)` as a form of canonicalization
      new Path(pf.filePath.toPath.toString).toUri.toString
    },
    FILE_NAME -> { pf: PartitionedFile =>
      pf.filePath.toUri.getRawPath.split("/").lastOption.getOrElse("")
    },
    FILE_SIZE -> { pf: PartitionedFile => pf.fileSize },
    FILE_BLOCK_START -> { pf: PartitionedFile => pf.start },
    FILE_BLOCK_LENGTH -> { pf: PartitionedFile => pf.length },
    // The modificationTime from the file has millisecond granularity, but the TimestampType for
    // `file_modification_time` has microsecond granularity.
    FILE_MODIFICATION_TIME -> { pf: PartitionedFile => pf.modificationTime * 1000 }
  )

  /**
   * Extracts the [[Literal]] value of a file-constant metadata column from a [[PartitionedFile]].
   *
   * If an extractor is available, apply it. Otherwise, look up the column's name in the file's
   * column value map and return the result (or null, if not found).
   *
   * Raw values (including null) are automatically converted to literals as a courtesy.
   */
  def getFileConstantMetadataColumnValue(
      name: String,
      file: PartitionedFile,
      metadataExtractors: Map[String, PartitionedFile => Any]): Literal = {
    val extractor = metadataExtractors.getOrElse(name,
      { pf: PartitionedFile => pf.otherConstantMetadataColumnValues.get(name).orNull }
    )
    Literal(extractor.apply(file))
  }

  // create an internal row given required metadata fields and file information
  def createMetadataInternalRow(
      partitionValues: InternalRow,
      fieldNames: Seq[String],
      filePath: SparkPath,
      fileSize: Long,
      fileModificationTime: Long): InternalRow = {
    // When scanning files directly from the filesystem, we only support file-constant metadata
    // fields whose values can be derived from a file status. In particular, we don't have accurate
    // file split information yet, nor do we have a way to provide custom metadata column values.
    val validFieldNames = Set(FILE_PATH, FILE_NAME, FILE_SIZE, FILE_MODIFICATION_TIME)
    val extractors =
      FileFormat.BASE_METADATA_EXTRACTORS.filter { case (k, _) => validFieldNames.contains(k) }
    assert(fieldNames.forall(validFieldNames.contains))
    val pf = PartitionedFile(
      partitionValues = partitionValues,
      filePath = filePath,
      start = 0L,
      length = fileSize,
      locations = Array.empty,
      modificationTime = fileModificationTime,
      fileSize = fileSize,
      otherConstantMetadataColumnValues = Map.empty)
    updateMetadataInternalRow(new GenericInternalRow(fieldNames.length), fieldNames, pf, extractors)
  }

  // update an internal row given required metadata fields and file information
  def updateMetadataInternalRow(
      row: InternalRow,
      fieldNames: Seq[String],
      file: PartitionedFile,
      metadataExtractors: Map[String, PartitionedFile => Any]): InternalRow = {
    fieldNames.zipWithIndex.foreach { case (name, i) =>
      getFileConstantMetadataColumnValue(name, file, metadataExtractors) match {
        case Literal(null, _) => row.setNullAt(i)
        case literal => row.update(i, literal.value)
      }
    }
    row
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
