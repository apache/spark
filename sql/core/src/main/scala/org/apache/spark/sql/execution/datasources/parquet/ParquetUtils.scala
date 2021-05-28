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
package org.apache.spark.sql.execution.datasources.parquet

import java.math.{BigDecimal, BigInteger}
import java.time.{ZoneId, ZoneOffset}
import java.util

import scala.collection.mutable.ArrayBuilder
import scala.language.existentials

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.{ColumnChunkMetaData, ParquetMetadata}
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.PrimitiveType

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.expressions.{Aggregation, Count, Max, Min}
import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.types.{BinaryType, ByteType, DateType, Decimal, DecimalType, IntegerType, LongType, ShortType, StringType, StructType, TimestampType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.types.UTF8String

object ParquetUtils {
  def inferSchema(
      sparkSession: SparkSession,
      parameters: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    val parquetOptions = new ParquetOptions(parameters, sparkSession.sessionState.conf)

    // Should we merge schemas from all Parquet part-files?
    val shouldMergeSchemas = parquetOptions.mergeSchema

    val mergeRespectSummaries = sparkSession.sessionState.conf.isParquetSchemaRespectSummaries

    val filesByType = splitFiles(files)

    // Sees which file(s) we need to touch in order to figure out the schema.
    //
    // Always tries the summary files first if users don't require a merged schema.  In this case,
    // "_common_metadata" is more preferable than "_metadata" because it doesn't contain row
    // groups information, and could be much smaller for large Parquet files with lots of row
    // groups.  If no summary file is available, falls back to some random part-file.
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

        // If mergeRespectSummaries config is true, we assume that all part-files are the same for
        // their schema with summary files, so we ignore them when merging schema.
        // If the config is disabled, which is the default setting, we merge all part-files.
        // In this mode, we only need to merge schemas contained in all those summary files.
        // You should enable this configuration only if you are very sure that for the parquet
        // part-files to read there are corresponding summary files containing correct schema.

        // As filed in SPARK-11500, the order of files to touch is a matter, which might affect
        // the ordering of the output columns. There are several things to mention here.
        //
        //  1. If mergeRespectSummaries config is false, then it merges schemas by reducing from
        //     the first part-file so that the columns of the lexicographically first file show
        //     first.
        //
        //  2. If mergeRespectSummaries config is true, then there should be, at least,
        //     "_metadata"s for all given files, so that we can ensure the columns of
        //     the lexicographically first file show first.
        //
        //  3. If shouldMergeSchemas is false, but when multiple files are given, there is
        //     no guarantee of the output order, since there might not be a summary file for the
        //     lexicographically first file, which ends up putting ahead the columns of
        //     the other files. However, this should be okay since not enabling
        //     shouldMergeSchemas means (assumes) all the files have the same schemas.

        val needMerged: Seq[FileStatus] =
          if (mergeRespectSummaries) {
            Seq.empty
          } else {
            filesByType.data
          }
        needMerged ++ filesByType.metadata ++ filesByType.commonMetadata
      } else {
        // Tries any "_common_metadata" first. Parquet files written by old versions or Parquet
        // don't have this.
        filesByType.commonMetadata.headOption
          // Falls back to "_metadata"
          .orElse(filesByType.metadata.headOption)
          // Summary file(s) not found, the Parquet file is either corrupted, or different part-
          // files contain conflicting user defined metadata (two or more values are associated
          // with a same key in different files).  In either case, we fall back to any of the
          // first part-file, and just assume all schemas are consistent.
          .orElse(filesByType.data.headOption)
          .toSeq
      }
    ParquetFileFormat.mergeSchemasInParallel(parameters, filesToTouch, sparkSession)
  }

  case class FileTypes(
      data: Seq[FileStatus],
      metadata: Seq[FileStatus],
      commonMetadata: Seq[FileStatus])

  private def splitFiles(allFiles: Seq[FileStatus]): FileTypes = {
    val leaves = allFiles.toArray.sortBy(_.getPath.toString)

    FileTypes(
      data = leaves.filterNot(f => isSummaryFile(f.getPath)),
      metadata =
        leaves.filter(_.getPath.getName == ParquetFileWriter.PARQUET_METADATA_FILE),
      commonMetadata =
        leaves.filter(_.getPath.getName == ParquetFileWriter.PARQUET_COMMON_METADATA_FILE))
  }

  private def isSummaryFile(file: Path): Boolean = {
    file.getName == ParquetFileWriter.PARQUET_COMMON_METADATA_FILE ||
      file.getName == ParquetFileWriter.PARQUET_METADATA_FILE
  }

  /**
   * When the partial Aggregates (Max/Min/Count) are pushed down to parquet, we don't need to
   * createRowBaseReader to read data from parquet and aggregate at spark layer. Instead we want
   * to calculate the partial Aggregates (Max/Min/Count) result using the statistics information
   * from parquet footer file, and then construct an InternalRow from these Aggregate results.
   *
   * @return Aggregate results in the format of InternalRow
   */
  private[sql] def createInternalRowFromAggResult(
      footer: ParquetMetadata,
      dataSchema: StructType,
      aggregation: Aggregation,
      aggSchema: StructType,
      datetimeRebaseModeInRead: String,
      int96RebaseModeInRead: String,
      convertTz: Option[ZoneId]): InternalRow = {
    val (parquetTypes, values) =
      ParquetUtils.getPushedDownAggResult(footer, dataSchema, aggregation)
    val mutableRow = new SpecificInternalRow(aggSchema.fields.map(x => x.dataType))
    val footerFileMetaData = footer.getFileMetaData
    val datetimeRebaseMode = DataSourceUtils.datetimeRebaseMode(
      footerFileMetaData.getKeyValueMetaData.get,
      datetimeRebaseModeInRead)
    val int96RebaseMode = DataSourceUtils.int96RebaseMode(
      footerFileMetaData.getKeyValueMetaData.get,
      int96RebaseModeInRead)
    parquetTypes.zipWithIndex.foreach {
      case (PrimitiveType.PrimitiveTypeName.INT32, i) =>
        aggSchema.fields(i).dataType match {
          case ByteType =>
            mutableRow.setByte(i, values(i).asInstanceOf[Integer].toByte)
          case ShortType =>
            mutableRow.setShort(i, values(i).asInstanceOf[Integer].toShort)
          case IntegerType =>
            mutableRow.setInt(i, values(i).asInstanceOf[Integer])
          case DateType =>
            val dateRebaseFunc = DataSourceUtils.creteDateRebaseFuncInRead(
              datetimeRebaseMode, "Parquet")
            mutableRow.update(i, dateRebaseFunc(values(i).asInstanceOf[Integer]))
          case d: DecimalType =>
            val decimal = Decimal(values(i).asInstanceOf[Integer].toLong, d.precision, d.scale)
            mutableRow.setDecimal(i, decimal, d.precision)
          case _ => throw new SparkException("Unexpected type for INT32")
        }
      case (PrimitiveType.PrimitiveTypeName.INT64, i) =>
        aggSchema.fields(i).dataType match {
          case LongType =>
            mutableRow.setLong(i, values(i).asInstanceOf[Long])
          case d: DecimalType =>
            val decimal = Decimal(values(i).asInstanceOf[Long], d.precision, d.scale)
            mutableRow.setDecimal(i, decimal, d.precision)
          case _ => throw new SparkException("Unexpected type for INT64")
        }
      case (PrimitiveType.PrimitiveTypeName.INT96, i) =>
        aggSchema.fields(i).dataType match {
          case LongType =>
            mutableRow.setLong(i, values(i).asInstanceOf[Long])
          case TimestampType =>
            val int96RebaseFunc = DataSourceUtils.creteTimestampRebaseFuncInRead(
              int96RebaseMode, "Parquet INT96")
            val julianMicros =
              ParquetRowConverter.binaryToSQLTimestamp(values(i).asInstanceOf[Binary])
            val gregorianMicros = int96RebaseFunc(julianMicros)
            val adjTime =
              convertTz.map(DateTimeUtils.convertTz(gregorianMicros, _, ZoneOffset.UTC))
                .getOrElse(gregorianMicros)
            mutableRow.setLong(i, adjTime)
          case _ => throw new SparkException("Unexpected type for INT96")
        }
      case (PrimitiveType.PrimitiveTypeName.FLOAT, i) =>
        mutableRow.setFloat(i, values(i).asInstanceOf[Float])
      case (PrimitiveType.PrimitiveTypeName.DOUBLE, i) =>
        mutableRow.setDouble(i, values(i).asInstanceOf[Double])
      case (PrimitiveType.PrimitiveTypeName.BOOLEAN, i) =>
        mutableRow.setBoolean(i, values(i).asInstanceOf[Boolean])
      case (PrimitiveType.PrimitiveTypeName.BINARY, i) =>
        val bytes = values(i).asInstanceOf[Binary].getBytes
        aggSchema.fields(i).dataType match {
          case StringType =>
            mutableRow.update(i, UTF8String.fromBytes(bytes))
          case BinaryType =>
            mutableRow.update(i, bytes)
          case d: DecimalType =>
            val decimal =
              Decimal(new BigDecimal(new BigInteger(bytes), d.scale), d.precision, d.scale)
            mutableRow.setDecimal(i, decimal, d.precision)
          case _ => throw new SparkException("Unexpected type for Binary")
        }
      case (PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, i) =>
        val bytes = values(i).asInstanceOf[Binary].getBytes
        aggSchema.fields(i).dataType match {
          case d: DecimalType =>
            val decimal =
              Decimal(new BigDecimal(new BigInteger(bytes), d.scale), d.precision, d.scale)
            mutableRow.setDecimal(i, decimal, d.precision)
          case _ => throw new SparkException("Unexpected type for FIXED_LEN_BYTE_ARRAY")
        }
      case _ =>
        throw new SparkException("Unexpected parquet type name")
    }
    mutableRow
  }

  /**
   * When the Aggregates (Max/Min/Count) are pushed down to parquet, in the case of
   * PARQUET_VECTORIZED_READER_ENABLED sets to true, we don't need buildColumnarReader
   * to read data from parquet and aggregate at spark layer. Instead we want
   * to calculate the Aggregates (Max/Min/Count) result using the statistics information
   * from parquet footer file, and then construct a ColumnarBatch from these Aggregate results.
   *
   * @return Aggregate results in the format of ColumnarBatch
   */
  private[sql] def createColumnarBatchFromAggResult(
      footer: ParquetMetadata,
      dataSchema: StructType,
      aggregation: Aggregation,
      aggSchema: StructType,
      offHeap: Boolean,
      datetimeRebaseModeInRead: String,
      int96RebaseModeInRead: String,
      convertTz: Option[ZoneId]): ColumnarBatch = {
    val (parquetTypes, values) =
      ParquetUtils.getPushedDownAggResult(footer, dataSchema, aggregation)
    val capacity = 4 * 1024
    val footerFileMetaData = footer.getFileMetaData
    val datetimeRebaseMode = DataSourceUtils.datetimeRebaseMode(
      footerFileMetaData.getKeyValueMetaData.get,
      datetimeRebaseModeInRead)
    val int96RebaseMode = DataSourceUtils.int96RebaseMode(
      footerFileMetaData.getKeyValueMetaData.get,
      int96RebaseModeInRead)
    val columnVectors = if (offHeap) {
      OffHeapColumnVector.allocateColumns(capacity, aggSchema)
    } else {
      OnHeapColumnVector.allocateColumns(capacity, aggSchema)
    }

    parquetTypes.zipWithIndex.foreach {
      case (PrimitiveType.PrimitiveTypeName.INT32, i) =>
        aggSchema.fields(i).dataType match {
          case ByteType =>
            columnVectors(i).appendByte(values(i).asInstanceOf[Integer].toByte)
          case ShortType =>
            columnVectors(i).appendShort(values(i).asInstanceOf[Integer].toShort)
          case IntegerType =>
            columnVectors(i).appendInt(values(i).asInstanceOf[Integer])
          case DateType =>
            val dateRebaseFunc = DataSourceUtils.creteDateRebaseFuncInRead(
              datetimeRebaseMode, "Parquet")
            columnVectors(i).appendInt(dateRebaseFunc(values(i).asInstanceOf[Integer]))
          case _ => throw new SparkException("Unexpected type for INT32")
        }
      case (PrimitiveType.PrimitiveTypeName.INT64, i) =>
        columnVectors(i).appendLong(values(i).asInstanceOf[Long])
      case (PrimitiveType.PrimitiveTypeName.INT96, i) =>
        aggSchema.fields(i).dataType match {
          case LongType =>
            columnVectors(i).appendLong(values(i).asInstanceOf[Long])
          case TimestampType =>
            val int96RebaseFunc = DataSourceUtils.creteTimestampRebaseFuncInRead(
              int96RebaseMode, "Parquet INT96")
            val julianMicros =
              ParquetRowConverter.binaryToSQLTimestamp(values(i).asInstanceOf[Binary])
            val gregorianMicros = int96RebaseFunc(julianMicros)
            val adjTime =
              convertTz.map(DateTimeUtils.convertTz(gregorianMicros, _, ZoneOffset.UTC))
                .getOrElse(gregorianMicros)
            columnVectors(i).appendLong(adjTime)
          case _ => throw new SparkException("Unexpected type for INT96")
        }
      case (PrimitiveType.PrimitiveTypeName.FLOAT, i) =>
        columnVectors(i).appendFloat(values(i).asInstanceOf[Float])
      case (PrimitiveType.PrimitiveTypeName.DOUBLE, i) =>
        columnVectors(i).appendDouble(values(i).asInstanceOf[Double])
      case (PrimitiveType.PrimitiveTypeName.BINARY, i) =>
        val bytes = values(i).asInstanceOf[Binary].getBytes
        columnVectors(i).putByteArray(0, bytes, 0, bytes.length)
      case (PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, i) =>
        val bytes = values(i).asInstanceOf[Binary].getBytes
        columnVectors(i).putByteArray(0, bytes, 0, bytes.length)
      case (PrimitiveType.PrimitiveTypeName.BOOLEAN, i) =>
        columnVectors(i).appendBoolean(values(i).asInstanceOf[Boolean])
      case _ =>
        throw new SparkException("Unexpected parquet type name")
    }
    new ColumnarBatch(columnVectors.asInstanceOf[Array[ColumnVector]], 1)
  }

  /**
   * Calculate the pushed down Aggregates (Max/Min/Count) result using the statistics
   * information from parquet footer file.
   *
   * @return A tuple of `Array[PrimitiveType.PrimitiveTypeName]` and Array[Any].
   *         The first element is the PrimitiveTypeName of the Aggregate column,
   *         and the second element is the aggregated value.
   */
  private[sql] def getPushedDownAggResult(
      footer: ParquetMetadata,
      dataSchema: StructType,
      aggregation: Aggregation)
  : (Array[PrimitiveType.PrimitiveTypeName], Array[Any]) = {
    val footerFileMetaData = footer.getFileMetaData
    val fields = footerFileMetaData.getSchema.getFields
    val blocks = footer.getBlocks()
    val typesBuilder = ArrayBuilder.make[PrimitiveType.PrimitiveTypeName]
    val valuesBuilder = ArrayBuilder.make[Any]

    aggregation.aggregateExpressions.indices.foreach { i =>
      var value: Any = None
      var rowCount = 0L
      var isCount = false
      var index = 0
      blocks.forEach { block =>
        val blockMetaData = block.getColumns()
        aggregation.aggregateExpressions(i) match {
          case Max(col, _) =>
            index = dataSchema.fieldNames.toList.indexOf(col.fieldNames.head)
            val currentMax = getCurrentBlockMaxOrMin(blockMetaData, index, true)
            if (currentMax != None &&
              (value == None || currentMax.asInstanceOf[Comparable[Any]].compareTo(value) > 0)) {
              value = currentMax
            }

          case Min(col, _) =>
            index = dataSchema.fieldNames.toList.indexOf(col.fieldNames.head)
            val currentMin = getCurrentBlockMaxOrMin(blockMetaData, index, false)
            if (currentMin != None &&
              (value == None || currentMin.asInstanceOf[Comparable[Any]].compareTo(value) < 0)) {
              value = currentMin
            }

          case Count(col, _, _) =>
            index = dataSchema.fieldNames.toList.indexOf(col.fieldNames.head)
            rowCount += block.getRowCount
            if (!col.fieldNames.head.equals("1")) {  // "1" is for count(*)
              rowCount -= getNumNulls(blockMetaData, index)
            }
            isCount = true

          case _ =>
        }
      }
      if (isCount) {
        valuesBuilder += rowCount
        typesBuilder += PrimitiveType.PrimitiveTypeName.INT96
      } else {
        valuesBuilder += value
        typesBuilder += fields.get(index).asPrimitiveType.getPrimitiveTypeName
      }
    }
    (typesBuilder.result(), valuesBuilder.result())
  }

  /**
   * get the Max or Min value for ith column in the current block
   *
   * @return the Max or Min value
   */
  private def getCurrentBlockMaxOrMin(
      columnChunkMetaData: util.List[ColumnChunkMetaData],
      i: Int,
      isMax: Boolean): Any = {
    val statistics = columnChunkMetaData.get(i).getStatistics()
    if (!statistics.hasNonNullValue) {
      throw new UnsupportedOperationException("No min/max found for parquet file, Set SQLConf" +
        " PARQUET_AGGREGATE_PUSHDOWN_ENABLED to false and execute again")
    } else {
      if (isMax) statistics.genericGetMax() else statistics.genericGetMin()
    }
  }

  private def getNumNulls(
      columnChunkMetaData: util.List[ColumnChunkMetaData],
      i: Int): Long = {
    val statistics = columnChunkMetaData.get(i).getStatistics()
    if (!statistics.isNumNullsSet()) {
      throw new UnsupportedOperationException("Number of nulls not set for parquet file." +
      "  Set SQLConf PARQUET_AGGREGATE_PUSHDOWN_ENABLED to false and execute again")
    }
    statistics.getNumNulls();
  }
}
