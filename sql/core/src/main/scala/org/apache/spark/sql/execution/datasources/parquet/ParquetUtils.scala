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

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuilder
import scala.language.existentials

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.{ColumnChunkMetaData, ParquetMetadata}
import org.apache.parquet.schema.PrimitiveType

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.sources.{Aggregation, Count, Max, Min}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

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

  private[sql] def aggResultToSparkInternalRows(
      parquetTypes: Seq[PrimitiveType.PrimitiveTypeName],
      values: Seq[Any],
      dataSchema: StructType): InternalRow = {
    val mutableRow = new SpecificInternalRow(dataSchema.fields.map(x => x.dataType))

    parquetTypes.zipWithIndex.map {
      case (PrimitiveType.PrimitiveTypeName.INT32, i) =>
        mutableRow.setInt(i, values(i).asInstanceOf[Int])
      case (PrimitiveType.PrimitiveTypeName.INT64, i) =>
        mutableRow.setLong(i, values(i).asInstanceOf[Long])
      case (PrimitiveType.PrimitiveTypeName.INT96, i) =>
        mutableRow.setLong(i, values(i).asInstanceOf[Long])
      case (PrimitiveType.PrimitiveTypeName.FLOAT, i) =>
        mutableRow.setFloat(i, values(i).asInstanceOf[Float])
      case (PrimitiveType.PrimitiveTypeName.DOUBLE, i) =>
        mutableRow.setDouble(i, values(i).asInstanceOf[Double])
      case (PrimitiveType.PrimitiveTypeName.BINARY, i) =>
        mutableRow.update(i, values(i).asInstanceOf[Array[Byte]])
      case (PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, i) =>
        mutableRow.update(i, values(i).asInstanceOf[Array[Byte]])
      case _ =>
        throw new IllegalArgumentException("Unexpected parquet type name")
    }
    mutableRow
  }

  private[sql] def aggResultToSparkColumnarBatch(
      parquetTypes: Seq[PrimitiveType.PrimitiveTypeName],
      values: Seq[Any],
      readDataSchema: StructType,
      offHeap: Boolean): ColumnarBatch = {
    val capacity = 4 * 1024
    val columnVectors = if (offHeap) {
      OffHeapColumnVector.allocateColumns(capacity, readDataSchema)
    } else {
      OnHeapColumnVector.allocateColumns(capacity, readDataSchema)
    }

    parquetTypes.zipWithIndex.map {
      case (PrimitiveType.PrimitiveTypeName.INT32, i) =>
        columnVectors(i).appendInt(values(i).asInstanceOf[Int])
      case (PrimitiveType.PrimitiveTypeName.INT64, i) =>
        columnVectors(i).appendLong(values(i).asInstanceOf[Long])
      case (PrimitiveType.PrimitiveTypeName.INT96, i) =>
        columnVectors(i).appendLong(values(i).asInstanceOf[Long])
      case (PrimitiveType.PrimitiveTypeName.FLOAT, i) =>
        columnVectors(i).appendFloat(values(i).asInstanceOf[Float])
      case (PrimitiveType.PrimitiveTypeName.DOUBLE, i) =>
        columnVectors(i).appendDouble(values(i).asInstanceOf[Double])
      case (PrimitiveType.PrimitiveTypeName.BINARY, i) =>
        val byteArray = values(i).asInstanceOf[Array[Byte]]
        columnVectors(i).appendBytes(byteArray.length, byteArray, 0)
      case (PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, i) =>
        val byteArray = values(i).asInstanceOf[Array[Byte]]
        columnVectors(i).appendBytes(byteArray.length, byteArray, 0)
      case _ =>
        throw new IllegalArgumentException("Unexpected parquet type name")
    }
    new ColumnarBatch(columnVectors.asInstanceOf[Array[ColumnVector]], 1)
  }

  private[sql] def getPushedDownAggResult(
      conf: Configuration,
      file: Path,
      dataSchema: StructType,
      aggregation: Aggregation)
  : (Array[PrimitiveType.PrimitiveTypeName], Array[Any]) = {

    val footer = ParquetFooterReader.readFooter(conf, file, NO_FILTER)
    val fields = footer.getFileMetaData.getSchema.getFields
    val typesBuilder = ArrayBuilder.make[PrimitiveType.PrimitiveTypeName]
    val valuesBuilder = ArrayBuilder.make[Any]
    val blocks = footer.getBlocks()

    blocks.forEach { block =>
      val columns = block.getColumns()
      for (i <- 0 until aggregation.aggregateExpressions.size) {
        var index = 0
        aggregation.aggregateExpressions(i) match {
          case Max(col, _) =>
            index = dataSchema.fieldNames.toList.indexOf(col)
            valuesBuilder += getPushedDownMaxMin(footer, columns, index, true)
            typesBuilder += fields.get(index).asPrimitiveType.getPrimitiveTypeName
          case Min(col, _) =>
            index = dataSchema.fieldNames.toList.indexOf(col)
            valuesBuilder += getPushedDownMaxMin(footer, columns, index, false)
            typesBuilder += fields.get(index).asPrimitiveType.getPrimitiveTypeName
          case Count(col, _, _) =>
            index = dataSchema.fieldNames.toList.indexOf(col)
            var rowCount = getRowCountFromParquetMetadata(footer)
            if (!col.equals("1")) {  // count(*)
              rowCount -= getNumNulls(footer, columns, index)
            }
            valuesBuilder += rowCount
            typesBuilder += PrimitiveType.PrimitiveTypeName.INT96
          case _ =>
        }
      }
    }
    (typesBuilder.result(), valuesBuilder.result())
  }

  private def getPushedDownMaxMin(
      footer: ParquetMetadata,
      columnChunkMetaData: util.List[ColumnChunkMetaData],
      i: Int,
      isMax: Boolean) = {
    val parquetType = footer.getFileMetaData.getSchema.getType(i)
    if (!parquetType.isPrimitive) {
      throw new IllegalArgumentException("Unsupported type : " + parquetType.toString)
    }
    var value: Any = None
    val statistics = columnChunkMetaData.get(i).getStatistics()
    if (isMax) {
      val currentMax = statistics.genericGetMax()
      if (currentMax != None &&
        (value == None || currentMax.asInstanceOf[Comparable[Any]].compareTo(value) > 0)) {
        value = currentMax
      }
    } else {
      val currentMin = statistics.genericGetMin()
      if (currentMin != None &&
        (value == None || currentMin.asInstanceOf[Comparable[Any]].compareTo(value) < 0)) {
        value = currentMin
      }
    }
    value
  }

  private def getRowCountFromParquetMetadata(footer: ParquetMetadata): Long = {
    var rowCount: Long = 0
    for (blockMetaData <- footer.getBlocks.asScala) {
      rowCount += blockMetaData.getRowCount
    }
    rowCount
  }

  private def getNumNulls(
      footer: ParquetMetadata,
      columnChunkMetaData: util.List[ColumnChunkMetaData],
      i: Int): Long = {
    val parquetType = footer.getFileMetaData.getSchema.getType(i)
    if (!parquetType.isPrimitive) {
      throw new IllegalArgumentException("Unsupported type : " + parquetType.toString)
    }
    var numNulls: Long = 0;
    val statistics = columnChunkMetaData.get(i).getStatistics()
    if (!statistics.isNumNullsSet()) {
      throw new UnsupportedOperationException("Number of nulls not set for parquet file." +
        " Set session property hive.pushdown_partial_aggregations_into_scan=false and execute" +
        " query again");
    }
    numNulls += statistics.getNumNulls();
    numNulls
  }
}
