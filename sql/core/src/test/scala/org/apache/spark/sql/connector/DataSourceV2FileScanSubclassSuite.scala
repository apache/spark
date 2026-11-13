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

package org.apache.spark.sql.connector

import java.io.File

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.FileStatus

import org.apache.spark.sql.{DataFrame, QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, DynamicPruningSubquery, EqualTo, Expression, In, Literal}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.{Expressions, NamedReference}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory, Scan, SupportsRuntimeFiltering}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.{FileFormat, FilePartition, PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetUtils}
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanRelation, FileDataSourceV2, FileScan, FileScanBuilder, FileTable}
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.SupportsRuntimeCatalystFiltering
import org.apache.spark.sql.sources
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * The built-in formats opt in to runtime partition pruning through
 * `RuntimePartitionPruningFileScan`, so a scan outside Spark that extends `FileScan` directly is
 * neither handed Catalyst runtime filters nor rejected for implementing another runtime-filtering
 * interface.
 */
class DataSourceV2FileScanSubclassSuite extends QueryTest with SharedSparkSession {

  private val plain = "SELECT id, part FROM fact"
  private val dpp = "SELECT f.id FROM fact f JOIN dim d ON f.part = d.dim_id WHERE d.dim_val = 7"
  private val subquery =
    "SELECT f.id FROM fact f WHERE f.part = (SELECT max(dim_id) FROM dim WHERE dim_val = 7)"

  // `fact` has 100 rows over `part` in [0, 10); `dim` has `dim_id` = `dim_val` in [0, 10).
  private def withFactAndDim(source: Class[_])(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "2") {
      withTempDir { dir =>
        val factPath = new File(dir, "fact").getCanonicalPath
        val dimPath = new File(dir, "dim").getCanonicalPath
        spark.range(100).selectExpr("id", "id % 10 AS part")
          .write.partitionBy("part").parquet(factPath)
        spark.range(10).selectExpr("id AS dim_id", "id AS dim_val").write.parquet(dimPath)
        withTempView("fact", "dim") {
          spark.read.format(source.getName).schema("id long, part int").load(factPath)
            .createOrReplaceTempView("fact")
          spark.read.schema("dim_id long, dim_val long").parquet(dimPath)
            .createOrReplaceTempView("dim")
          f
        }
      }
    }
  }

  private def factScanOf(df: DataFrame): BatchScanExec = {
    df.queryExecution.executedPlan.collectFirst {
      case b: BatchScanExec if b.scan.isInstanceOf[FileScan] &&
        b.scan.asInstanceOf[FileScan].readPartitionSchema.nonEmpty => b
    }.getOrElse(fail(s"no fact scan in:\n${df.queryExecution.executedPlan}"))
  }

  private def factIdsIn(part: Int): Seq[Row] = (0 until 10).map(i => Row(i * 10L + part))

  Seq(classOf[RowBasedReListingSource], classOf[CachingReListingSource]).foreach { source =>
    test(s"a FileScan with SupportsRuntimeFiltering plans and prunes: ${source.getSimpleName}") {
      withFactAndDim(source) {
        val all = sql(plain)
        assert(all.collect().length == 100)
        assert(!factScanOf(all).scan.isInstanceOf[SupportsRuntimeCatalystFiltering])

        // The scan's own `SupportsRuntimeFiltering.filter` prunes the DPP join. For the caching
        // scan this also pins that `planInputPartitions()` does not memoize what `partitions`
        // returned: planning fills the scan's cache before the runtime filter arrives.
        val joined = sql(dpp)
        checkAnswer(joined, factIdsIn(7))
        assert(factScanOf(joined).metrics("numOutputRows").value == 10)

        checkAnswer(sql(subquery), factIdsIn(7))
      }
    }
  }

  test("a FileScan with SupportsRuntimeFiltering keeps the DPP filter a pushed equality fixes") {
    // CleanupDynamicPruningFilters reads the fully pushed filters only of a built-in file scan.
    // The cast is on the dim side, so the pruning key is `f.part` itself, as in `f.part = 7`.
    withFactAndDim(classOf[RowBasedReListingSource]) {
      val df = sql("""SELECT f.id FROM fact f JOIN dim d ON f.part = CAST(d.dim_id AS INT)
                     |WHERE d.dim_val = 7 AND f.part = 7""".stripMargin)
      checkAnswer(df, factIdsIn(7))
      val optimized = df.queryExecution.optimizedPlan
      val pushed = optimized.collectFirst {
        case r: DataSourceV2ScanRelation if r.scan.isInstanceOf[ReListingFileScan] =>
          r.pushedFilters
      }.getOrElse(fail(s"no fact scan relation in:\n$optimized"))
      assert(pushed.exists {
        case EqualTo(a: Attribute, Literal(7, _)) => a.name == "part"
        case _ => false
      }, s"expected `f.part = 7` fully pushed, got $pushed")
      assert(optimized.exists {
        case f: Filter => f.condition.exists(_.isInstanceOf[DynamicPruningSubquery])
        case _ => false
      }, s"expected the DPP filter to stay:\n$optimized")
    }
  }

  test("a FileScan with its own partition type reads a DPP-shaped join and a scalar subquery") {
    withFactAndDim(classOf[OwnPartitionTypeSource]) {
      Seq(dpp, subquery).foreach { query =>
        val df = sql(query)
        checkAnswer(df, factIdsIn(7))
        assert(factScanOf(df).runtimeFilters.isEmpty, s"$query got runtime filters")
      }
    }
  }
}

/**
 * Shaped like Kyuubi's `HiveScan`: implements `SupportsRuntimeFiltering`, takes only `In`, and
 * lists the files again with the runtime filters added to the partition filters.
 */
abstract class ReListingFileScan extends FileScan with SupportsRuntimeFiltering {
  def options: CaseInsensitiveStringMap
  def staticPartitionFilters: Seq[Expression]

  @transient private var runtimeFilters: Seq[Expression] = Seq.empty

  override def partitionFilters: Seq[Expression] = staticPartitionFilters ++ runtimeFilters

  override def filterAttributes(): Array[NamedReference] =
    readPartitionSchema.fieldNames.map(Expressions.column(_): NamedReference)

  override def filter(filters: Array[sources.Filter]): Unit = {
    val attrs = toAttributes(fileIndex.partitionSchema).map(a => a.name -> a).toMap
    runtimeFilters = filters.toSeq.flatMap {
      case sources.In(name, values) =>
        attrs.get(name).map(a => In(a, values.toSeq.map(v => Literal.create(v, a.dataType))))
      case _ => None
    }
  }

  override def createReaderFactory(): PartitionReaderFactory =
    SubclassFileScans.parquetReaderFactory(this, options)
}

/** Row-based, so planning never asks for its partitions. */
case class RowBasedReListingScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    options: CaseInsensitiveStringMap,
    staticPartitionFilters: Seq[Expression],
    dataFilters: Seq[Expression]) extends ReListingFileScan {
  override def columnarSupportMode(): Scan.ColumnarSupportMode =
    Scan.ColumnarSupportMode.UNSUPPORTED
}

/** Shaped like YTsaurus SPYT's `YtScan`: caches `partitions` and clears the cache in `filter`. */
case class CachingReListingScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    options: CaseInsensitiveStringMap,
    staticPartitionFilters: Seq[Expression],
    dataFilters: Seq[Expression]) extends ReListingFileScan {
  @transient private var cached: Option[Seq[FilePartition]] = None

  override protected def partitions: Seq[FilePartition] = cached.getOrElse {
    val computed = super.partitions
    cached = Some(computed)
    computed
  }

  override def filter(filters: Array[sources.Filter]): Unit = {
    super.filter(filters)
    cached = None
  }
}

case class OwnTypePartition(index: Int, files: Array[PartitionedFile]) extends InputPartition

/** Reads only [[OwnTypePartition]]s, like Sedona's `ShapefilePartitionReaderFactory`. */
case class OwnTypeReaderFactory(parquet: PartitionReaderFactory) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    partition match {
      case p: OwnTypePartition => parquet.createReader(FilePartition(p.index, p.files))
      case other => throw new IllegalArgumentException(
        s"Unexpected partition type: ${other.getClass.getCanonicalName}")
    }
}

/** Shaped like Sedona's `ShapefileScan`: regroups the planned files into its own partitions. */
case class OwnPartitionTypeScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    options: CaseInsensitiveStringMap,
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression]) extends FileScan {
  override def planInputPartitions(): Array[InputPartition] = {
    super.planInputPartitions().flatMap(_.asInstanceOf[FilePartition].files).zipWithIndex.map {
      case (file, i) => OwnTypePartition(i, Array(file)): InputPartition
    }
  }

  override def createReaderFactory(): PartitionReaderFactory =
    OwnTypeReaderFactory(SubclassFileScans.parquetReaderFactory(this, options))
}

object SubclassFileScans {
  def parquetReaderFactory(
      scan: FileScan,
      options: CaseInsensitiveStringMap): PartitionReaderFactory = {
    val hadoopConf = scan.sparkSession.sessionState.newHadoopConfWithOptions(
      options.asCaseSensitiveMap.asScala.toMap)
    ParquetScan(scan.sparkSession, hadoopConf, scan.fileIndex, scan.dataSchema,
      scan.readDataSchema, scan.readPartitionSchema, Array.empty, options).createReaderFactory()
  }
}

class SubclassFileScanBuilder(
    kind: String,
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    options: CaseInsensitiveStringMap)
  extends FileScanBuilder(sparkSession, fileIndex, dataSchema) {
  override def build(): Scan = kind match {
    case "rowBased" => RowBasedReListingScan(sparkSession, fileIndex, dataSchema,
      readDataSchema(), readPartitionSchema(), options, partitionFilters, dataFilters)
    case "caching" => CachingReListingScan(sparkSession, fileIndex, dataSchema,
      readDataSchema(), readPartitionSchema(), options, partitionFilters, dataFilters)
    case "ownType" => OwnPartitionTypeScan(sparkSession, fileIndex, dataSchema,
      readDataSchema(), readPartitionSchema(), options, partitionFilters, dataFilters)
  }
}

case class SubclassFileTable(
    kind: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType])
  extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {
  override def name(): String = s"subclass_$kind"
  override def newScanBuilder(opts: CaseInsensitiveStringMap): SubclassFileScanBuilder =
    new SubclassFileScanBuilder(kind, sparkSession, fileIndex, dataSchema, mergedOptions(opts))
  override def inferSchema(files: Seq[FileStatus]): Option[StructType] =
    ParquetUtils.inferSchema(sparkSession, options.asScala.toMap, files)
  override def formatName: String = "Parquet"
  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[ParquetFileFormat]
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    throw new UnsupportedOperationException("read-only")
}

abstract class SubclassFileSource(kind: String) extends FileDataSourceV2 {
  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[ParquetFileFormat]
  override def shortName(): String = s"subclass_$kind"
  override protected def getTable(options: CaseInsensitiveStringMap): Table =
    SubclassFileTable(kind, sparkSession, getOptionsWithoutPaths(options), getPaths(options), None)
  override protected def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table =
    SubclassFileTable(kind, sparkSession, getOptionsWithoutPaths(options), getPaths(options),
      Some(schema))
}

class RowBasedReListingSource extends SubclassFileSource("rowBased")
class CachingReListingSource extends SubclassFileSource("caching")
class OwnPartitionTypeSource extends SubclassFileSource("ownType")
