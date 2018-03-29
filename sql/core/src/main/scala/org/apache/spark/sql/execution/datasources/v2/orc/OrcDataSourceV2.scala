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
package org.apache.spark.sql.execution.datasources.v2.orc

import java.util.{List => JList, Locale}

import scala.collection.JavaConverters._

import org.apache.orc.mapreduce.OrcInputFormat

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeRow}
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.orc.{OrcFilters, OrcUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, ReadSupportWithSchema}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types.{AtomicType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

class OrcDataSourceV2 extends DataSourceV2 with ReadSupport with ReadSupportWithSchema {
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    new OrcDataSourceReader(options, None)
  }

  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader = {
    new OrcDataSourceReader(options, Some(schema))
  }
}

class OrcDataSourceReader(options: DataSourceOptions, userSpecifiedSchema: Option[StructType])
  extends DataSourceReader
  with SupportsScanColumnarBatch
  with SupportsScanUnsafeRow
  with SupportsPushDownCatalystFilters
  with SupportsPushDownRequiredColumns {

  private val sparkSession = SparkSession.getActiveSession
    .getOrElse(SparkSession.getDefaultSession.get)
  private val hadoopConf =
    sparkSession.sessionState.newHadoopConfWithOptions(options.asMap().asScala.toMap)
  private val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
  private val fileIndex = {
    val filePath = options.get("path")
    if (!filePath.isPresent) {
      throw new AnalysisException("ORC data source requires a" +
        " path (e.g. data backed by a local or distributed file system).")
    }
    val rootPathsSpecified =
      DataSource.checkAndGlobPathIfNecessary(hadoopConf, filePath.get, checkFilesExist = true)
    new InMemoryFileIndex(sparkSession, rootPathsSpecified, options.asMap().asScala.toMap, None)
  }

  private val partitionSchema = PartitioningUtils.combineInferredAndUserSpecifiedPartitionSchema(
    fileIndex, userSpecifiedSchema, isCaseSensitive)

  private val dataSchema = userSpecifiedSchema.getOrElse {
    val files = fileIndex.allFiles()
    OrcUtils.readSchema(sparkSession, files).getOrElse {
      throw new AnalysisException(
        s"Unable to infer schema for Orc. It must be specified manually.")
    }
  }
  private val (fullSchema, _) =
    PartitioningUtils.mergeDataAndPartitionSchema(dataSchema, partitionSchema, isCaseSensitive)
  private val broadcastedConf =
    sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
  private var pushedFiltersArray: Array[Expression] = Array.empty
  private var partitionKeyFilters: Array[Expression] = Array.empty
  private var requiredSchema = fullSchema

  private def partitions: Seq[FilePartition] = {
    val selectedPartitions = fileIndex.listFiles(partitionKeyFilters, Seq.empty)
    val maxSplitBytes = PartitionedFileUtil.maxSplitBytes(sparkSession, selectedPartitions)
    val splitFiles = selectedPartitions.flatMap { partition =>
      partition.files.flatMap { file =>
        PartitionedFileUtil.splitFiles(
          sparkSession = sparkSession,
          file = file,
          filePath = file.getPath,
          isSplitable = true,
          maxSplitBytes = maxSplitBytes,
          partitionValues = partition.values
        )
      }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    }
    FilePartitionUtil.getFilePartition(sparkSession, splitFiles, maxSplitBytes)
  }


  override def readSchema(): StructType = {
    requiredSchema
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  override def createBatchDataReaderFactories(): JList[DataReaderFactory[ColumnarBatch]] = {
    val sqlConf = sparkSession.sessionState.conf
    val enableOffHeapColumnVector = sqlConf.offHeapColumnVectorEnabled
    val capacity = sqlConf.orcVectorizedReaderBatchSize
    val copyToSpark = sparkSession.sessionState.conf.getConf(SQLConf.ORC_COPY_BATCH_TO_SPARK)

    partitions.map { partitionedFile =>
      new OrcBatchDataReaderFactory(partitionedFile, dataSchema, partitionSchema,
        readSchema(), enableOffHeapColumnVector,
        copyToSpark, capacity, broadcastedConf, isCaseSensitive)
        .asInstanceOf[DataReaderFactory[ColumnarBatch]]
    }.asJava
  }

  override def createUnsafeRowReaderFactories: JList[DataReaderFactory[UnsafeRow]] = {
    partitions.map { partitionedFile =>
      new OrcUnsafeRowReaderFactory(partitionedFile, dataSchema, partitionSchema,
        readSchema(), broadcastedConf, isCaseSensitive)
        .asInstanceOf[DataReaderFactory[UnsafeRow]]
    }.asJava
  }

  override def enableBatchRead(): Boolean = {
    val conf = sparkSession.sessionState.conf
    val schema = readSchema()
    conf.orcVectorizedReaderEnabled && conf.wholeStageEnabled &&
      schema.length <= conf.wholeStageMaxNumFields &&
      schema.forall(_.dataType.isInstanceOf[AtomicType])
  }

  override def pushCatalystFilters(filters: Array[Expression]): Array[Expression] = {
    val partitionColumnNames = partitionSchema.toAttributes.map(_.name).toSet
    val (partitionKeyFilters, otherFilters) = filters.partition {
      _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
    }
    this.partitionKeyFilters = partitionKeyFilters
    pushedFiltersArray = partitionKeyFilters
    if (sparkSession.sessionState.conf.orcFilterPushDown) {
      val dataFilters = otherFilters.map { f =>
        (DataSourceStrategy.translateFilter(f), f)
      }.collect { case (optionalFilter, catalystFilter) if optionalFilter.isDefined =>
        (optionalFilter.get, catalystFilter)
      }.toMap
      val pushedDataFilters =
        OrcFilters.convertibleFilters(fullSchema, dataFilters.keys.toSeq).map(dataFilters).toArray
      pushedFiltersArray ++= pushedDataFilters
      OrcFilters.createFilter(fullSchema, dataFilters.keys.toSeq).foreach { f =>
        OrcInputFormat.setSearchArgument(hadoopConf, f, fullSchema.fieldNames)
      }
    }
    otherFilters
  }

  override def pushedCatalystFilters(): Array[Expression] = {
    pushedFiltersArray
  }
}

object OrcDataSourceV2 {
  def satisfy(sparkSession: SparkSession, source: String, paths: Seq[String]): Option[String] = {
    val enabledV2Readers = sparkSession.sqlContext.conf.enabledV2DataSourceReader.split(",")
    val isNative = sparkSession.sqlContext.conf.getConf(SQLConf.ORC_IMPLEMENTATION) == "native"
    if (source.toLowerCase(Locale.ROOT) == "orc" && isNative &&
      enabledV2Readers.contains(source) && paths.length == 1) {
      Some("org.apache.spark.sql.execution.datasources.v2.orc.OrcDataSourceV2")
    } else {
      None
    }
  }
}
