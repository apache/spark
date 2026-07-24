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

package org.apache.spark.sql.execution.datasources.v2

import java.util
import java.util.OptionalLong

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.catalog.{CatalogColumnStat, CatalogStatistics}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.plans.logical.{Histogram, HistogramBin}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.catalyst.util.FieldMetadataUtils.FIELD_ID_METADATA_KEY
import org.apache.spark.sql.catalyst.util.INTERNAL_METADATA_KEYS
import org.apache.spark.sql.connector.catalog.{Column, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference}
import org.apache.spark.sql.connector.read.{Scan, Statistics => V2Statistics, SupportsReportStatistics}
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DataSourceV2RelationSuite extends SparkFunSuite with SQLHelper {

  test("DataSourceV2Relation.v1StatsToV2Stats") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType)))

    val idColStat = CatalogColumnStat(
      distinctCount = Some(10),
      min = Some("1"),
      max = Some("100"),
      nullCount = Some(0),
      avgLen = Some(4),
      maxLen = Some(4))

    val catalogStats = CatalogStatistics(
      sizeInBytes = 1024,
      rowCount = Some(10),
      colStats = Map(
        "id" -> idColStat,
        // "extra" is not in schema, should be silently skipped
        "extra" -> CatalogColumnStat(distinctCount = Some(5))))

    val v2Stats = DataSourceV2Relation.v1StatsToV2Stats(catalogStats, schema)

    // sizeInBytes is always populated
    assert(v2Stats.sizeInBytes().getAsLong === 1024L)

    // numRows is present when rowCount is defined
    assert(v2Stats.numRows().isPresent)
    assert(v2Stats.numRows().getAsLong === 10L)

    // only columns present in schema are returned; "extra" is skipped
    val colStats = v2Stats.columnStats()
    assert(colStats.size() === 1, "only 'id' is in schema; 'extra' should be skipped")
    val idV2 = colStats.get(FieldReference.column("id"))
    assert(idV2 != null)
    assert(idV2.distinctCount().getAsLong === 10L)
    assert(idV2.nullCount().getAsLong === 0L)
    assert(idV2.avgLen().getAsLong === 4L)
    assert(idV2.maxLen().getAsLong === 4L)
    assert(idV2.min().isPresent)
    assert(idV2.min().get() === 1)   // deserialized from "1" as IntegerType
    assert(idV2.max().get() === 100)

    // No rowCount: numRows should be empty
    val statsNoRows = CatalogStatistics(sizeInBytes = 512, colStats = Map.empty)
    val v2NoRows = DataSourceV2Relation.v1StatsToV2Stats(statsNoRows, schema)
    assert(v2NoRows.sizeInBytes().getAsLong === 512L)
    assert(!v2NoRows.numRows().isPresent)
    assert(v2NoRows.columnStats().isEmpty)

    // Histogram is round-tripped through v1ColStatToV2ColStat
    val hist = Histogram(10.0, Array(HistogramBin(1.0, 50.0, 5L), HistogramBin(50.0, 100.0, 5L)))
    val colStatWithHist = CatalogColumnStat(
      distinctCount = Some(10),
      min = Some("1"),
      max = Some("100"),
      nullCount = Some(0),
      avgLen = Some(4),
      maxLen = Some(4),
      histogram = Some(hist))
    val statsWithHist = CatalogStatistics(
      sizeInBytes = 1024,
      rowCount = Some(10),
      colStats = Map("id" -> colStatWithHist))
    val v2StatsWithHist = DataSourceV2Relation.v1StatsToV2Stats(statsWithHist, schema)
    val idV2WithHist = v2StatsWithHist.columnStats().get(FieldReference.column("id"))
    assert(idV2WithHist != null)
    assert(idV2WithHist.histogram().isPresent)
    val v2Hist = idV2WithHist.histogram().get()
    assert(v2Hist.height() === 10.0)
    assert(v2Hist.bins().length === 2)
    assert(v2Hist.bins()(0).lo() === 1.0)
    assert(v2Hist.bins()(0).hi() === 50.0)
    assert(v2Hist.bins()(0).ndv() === 5L)
    assert(v2Hist.bins()(1).lo() === 50.0)
    assert(v2Hist.bins()(1).hi() === 100.0)
    assert(v2Hist.bins()(1).ndv() === 5L)

    // When no histogram: histogram() should be empty
    val idV2NoHist = colStats.get(FieldReference.column("id"))
    assert(!idV2NoHist.histogram().isPresent)
  }

  private def scanRel(
      output: Seq[AttributeReference],
      scan: Scan,
      table: Table = new FakeTableWithSchema()): DataSourceV2ScanRelation = {
    DataSourceV2ScanRelation(
      DataSourceV2Relation(table, output, None, None, CaseInsensitiveStringMap.empty()),
      scan,
      output)
  }

  test("DataSourceV2ScanRelation.computeStats uses non-empty scan stats with CBO") {
    val idAttr = AttributeReference("id", IntegerType)()
    val output = Seq(idAttr)
    val scan = new Scan with SupportsReportStatistics {
      override def readSchema(): StructType = StructType(Seq(StructField("id", IntegerType)))
      override def estimateStatistics(): V2Statistics = new V2Statistics {
        override def sizeInBytes(): OptionalLong = OptionalLong.empty()
        override def numRows(): OptionalLong = OptionalLong.of(42L)
        override def columnStats(): java.util.Map[NamedReference, ColumnStatistics] = {
          val stats = new java.util.HashMap[NamedReference, ColumnStatistics]()
          stats.put(FieldReference.column("id"), new ColumnStatistics {
            override def distinctCount(): OptionalLong = OptionalLong.of(40L)
            override def avgLen(): OptionalLong = OptionalLong.of(4L)
          })
          stats
        }
      }
    }

    withSQLConf(SQLConf.CBO_ENABLED.key -> "true") {
      val stats = scanRel(output, scan).computeStats()

      assert(stats.rowCount.contains(BigInt(42)))
      assert(stats.attributeStats.size === 1)
      assert(stats.attributeStats(idAttr).distinctCount.contains(BigInt(40)))
      assert(stats.attributeStats(idAttr).avgLen.contains(4L))
      assert(stats.sizeInBytes ===
        EstimationUtils.getOutputSize(output, BigInt(42), stats.attributeStats))
    }
  }

  test("DataSourceV2ScanRelation.computeStats derives size 1 for a zero-row scan with CBO") {
    val idAttr = AttributeReference("id", IntegerType)()
    val output = Seq(idAttr)
    val scan = new Scan with SupportsReportStatistics {
      override def readSchema(): StructType = StructType(Seq(StructField("id", IntegerType)))
      override def estimateStatistics(): V2Statistics = new V2Statistics {
        override def sizeInBytes(): OptionalLong = OptionalLong.empty()
        override def numRows(): OptionalLong = OptionalLong.of(0L)
      }
    }

    withSQLConf(
        SQLConf.CBO_ENABLED.key -> "true",
        SQLConf.DEFAULT_SIZE_IN_BYTES.key -> "12345") {
      val stats = scanRel(output, scan).computeStats()

      assert(stats.rowCount.contains(BigInt(0)))
      assert(stats.sizeInBytes === BigInt(1))
    }
  }

  test("DataSourceV2ScanRelation.computeStats uses full stats with plan stats enabled") {
    val idAttr = AttributeReference("id", IntegerType)()
    val output = Seq(idAttr)
    val scan = new Scan with SupportsReportStatistics {
      override def readSchema(): StructType = StructType(Seq(StructField("id", IntegerType)))
      override def estimateSizeInBytes(): OptionalLong = OptionalLong.of(50L)
      override def estimateStatistics(): V2Statistics = new V2Statistics {
        override def sizeInBytes(): OptionalLong = OptionalLong.of(1000L)
        override def numRows(): OptionalLong = OptionalLong.of(7L)
      }
    }

    withSQLConf(
        SQLConf.CBO_ENABLED.key -> "false",
        SQLConf.PLAN_STATS_ENABLED.key -> "true") {
      val stats = scanRel(output, scan).computeStats()

      assert(stats.sizeInBytes === BigInt(1000))
      assert(stats.rowCount.contains(BigInt(7)))
    }
  }

  test("DataSourceV2ScanRelation.computeStats treats column-only scan stats as non-empty") {
    val idAttr = AttributeReference("id", IntegerType)()
    val output = Seq(idAttr)
    val scan = new Scan with SupportsReportStatistics {
      override def readSchema(): StructType = StructType(Seq(StructField("id", IntegerType)))
      override def estimateStatistics(): V2Statistics = new V2Statistics {
        override def sizeInBytes(): OptionalLong = OptionalLong.empty()
        override def numRows(): OptionalLong = OptionalLong.empty()
        override def columnStats(): java.util.Map[NamedReference, ColumnStatistics] = {
          val stats = new java.util.HashMap[NamedReference, ColumnStatistics]()
          stats.put(FieldReference.column("id"), new ColumnStatistics {
            override def distinctCount(): OptionalLong = OptionalLong.of(7L)
          })
          stats
        }
      }
    }

    withSQLConf(
        SQLConf.CBO_ENABLED.key -> "true",
        SQLConf.DEFAULT_SIZE_IN_BYTES.key -> "12345") {
      val stats = scanRel(output, scan).computeStats()

      assert(stats.sizeInBytes === BigInt(12345))
      assert(stats.rowCount.isEmpty)
      assert(stats.attributeStats.size === 1)
      assert(stats.attributeStats(idAttr).distinctCount.contains(BigInt(7)))
    }
  }

  test("DataSourceV2ScanRelation.computeStats uses size-only estimates without CBO") {
    val idAttr = AttributeReference("id", IntegerType)()
    val output = Seq(idAttr)
    val scan = new Scan with SupportsReportStatistics {
      override def readSchema(): StructType = StructType(Seq(StructField("id", IntegerType)))
      override def estimateSizeInBytes(): OptionalLong = OptionalLong.of(50L)
      override def estimateStatistics(): V2Statistics = new V2Statistics {
        override def sizeInBytes(): OptionalLong = OptionalLong.of(1000L)
        override def numRows(): OptionalLong = OptionalLong.of(5L)
      }
    }

    withSQLConf(
        SQLConf.CBO_ENABLED.key -> "false",
        SQLConf.PLAN_STATS_ENABLED.key -> "false") {
      val stats = scanRel(output, scan).computeStats()

      assert(stats.sizeInBytes === BigInt(50))
      assert(stats.rowCount.isEmpty)
      assert(stats.attributeStats.isEmpty)
    }
  }

  test("DataSourceV2ScanRelation.computeStats uses default estimateSizeInBytes without CBO") {
    val idAttr = AttributeReference("id", IntegerType)()
    val output = Seq(idAttr)
    val scan = new Scan with SupportsReportStatistics {
      override def readSchema(): StructType = StructType(Seq(StructField("id", IntegerType)))
      override def estimateStatistics(): V2Statistics = new V2Statistics {
        override def sizeInBytes(): OptionalLong = OptionalLong.of(64L)
        override def numRows(): OptionalLong = OptionalLong.of(5L)
      }
    }

    withSQLConf(
        SQLConf.CBO_ENABLED.key -> "false",
        SQLConf.PLAN_STATS_ENABLED.key -> "false") {
      val stats = scanRel(output, scan).computeStats()

      assert(stats.sizeInBytes === BigInt(64))
      assert(stats.rowCount.isEmpty)
      assert(stats.attributeStats.isEmpty)
    }
  }

  test("DataSourceV2ScanRelation.computeStats infers size-only estimates from row count") {
    val idAttr = AttributeReference("id", IntegerType)()
    val output = Seq(idAttr)
    val scan = new Scan with SupportsReportStatistics {
      override def readSchema(): StructType = StructType(Seq(StructField("id", IntegerType)))
      override def estimateStatistics(): V2Statistics = new V2Statistics {
        override def sizeInBytes(): OptionalLong = OptionalLong.empty()
        override def numRows(): OptionalLong = OptionalLong.of(5L)
      }
    }

    withSQLConf(
        SQLConf.CBO_ENABLED.key -> "false",
        SQLConf.PLAN_STATS_ENABLED.key -> "false") {
      val stats = scanRel(output, scan).computeStats()

      assert(stats.sizeInBytes === EstimationUtils.getSizePerRow(output) * BigInt(5))
      assert(stats.rowCount.isEmpty)
      assert(stats.attributeStats.isEmpty)
    }
  }

  test("DataSourceV2ScanRelation.computeStats uses default size without CBO for empty stats") {
    val output = Seq(AttributeReference("id", IntegerType)())
    val scan = new Scan with SupportsReportStatistics {
      override def readSchema(): StructType = StructType(Seq(StructField("id", IntegerType)))
      override def estimateStatistics(): V2Statistics = new V2Statistics {
        override def sizeInBytes(): OptionalLong = OptionalLong.empty()
        override def numRows(): OptionalLong = OptionalLong.empty()
      }
    }

    withSQLConf(
        SQLConf.CBO_ENABLED.key -> "false",
        SQLConf.PLAN_STATS_ENABLED.key -> "false",
        SQLConf.DEFAULT_SIZE_IN_BYTES.key -> "12345") {
      val stats = scanRel(output, scan).computeStats()

      assert(stats.sizeInBytes === BigInt(12345))
      assert(stats.rowCount.isEmpty)
    }
  }

  test("DataSourceV2ScanRelation.computeStats uses default size without reported stats") {
    val output = Seq(AttributeReference("id", IntegerType)())
    val scan = new Scan {
      override def readSchema(): StructType = StructType(Seq(StructField("id", IntegerType)))
    }

    withSQLConf(
        SQLConf.CBO_ENABLED.key -> "false",
        SQLConf.PLAN_STATS_ENABLED.key -> "false",
        SQLConf.DEFAULT_SIZE_IN_BYTES.key -> "12345") {
      val stats = scanRel(output, scan).computeStats()

      assert(stats.sizeInBytes === BigInt(12345))
      assert(stats.rowCount.isEmpty)
    }

    withSQLConf(
        SQLConf.CBO_ENABLED.key -> "true",
        SQLConf.DEFAULT_SIZE_IN_BYTES.key -> "12345") {
      val stats = scanRel(output, scan).computeStats()

      assert(stats.sizeInBytes === BigInt(12345))
      assert(stats.rowCount.isEmpty)
    }
  }

  test("DataSourceV2ScanRelation.computeStats uses default size for empty scan stats") {
    val output = Seq(AttributeReference("id", IntegerType)())
    val scan = new Scan with SupportsReportStatistics {
      override def readSchema(): StructType = StructType(Seq(StructField("id", IntegerType)))
      override def estimateStatistics(): V2Statistics = new V2Statistics {
        override def sizeInBytes(): OptionalLong = OptionalLong.empty()
        override def numRows(): OptionalLong = OptionalLong.empty()
      }
    }

    withSQLConf(
        SQLConf.CBO_ENABLED.key -> "true",
        SQLConf.DEFAULT_SIZE_IN_BYTES.key -> "12345") {
      val stats = scanRel(output, scan).computeStats()

      assert(stats.sizeInBytes === BigInt(12345))
      assert(stats.rowCount.isEmpty)
    }
  }

  test("DataSourceV2ScanRelation.computeStats uses default size for null scan stats") {
    val output = Seq(AttributeReference("id", IntegerType)())
    val nullStatsScan = new Scan with SupportsReportStatistics {
      override def readSchema(): StructType = StructType(Seq(StructField("id", IntegerType)))
      override def estimateStatistics(): V2Statistics = null
    }
    val nullColumnStatsScan = new Scan with SupportsReportStatistics {
      override def readSchema(): StructType = StructType(Seq(StructField("id", IntegerType)))
      override def estimateStatistics(): V2Statistics = new V2Statistics {
        override def sizeInBytes(): OptionalLong = OptionalLong.empty()
        override def numRows(): OptionalLong = OptionalLong.empty()
        override def columnStats(): java.util.Map[NamedReference, ColumnStatistics] = null
      }
    }

    withSQLConf(
        SQLConf.CBO_ENABLED.key -> "true",
        SQLConf.DEFAULT_SIZE_IN_BYTES.key -> "12345") {
      Seq(nullStatsScan, nullColumnStatsScan).foreach { scan =>
        val stats = scanRel(output, scan).computeStats()

        assert(stats.sizeInBytes === BigInt(12345))
        assert(stats.rowCount.isEmpty)
      }
    }

    withSQLConf(
        SQLConf.CBO_ENABLED.key -> "false",
        SQLConf.PLAN_STATS_ENABLED.key -> "false",
        SQLConf.DEFAULT_SIZE_IN_BYTES.key -> "12345") {
      Seq(nullStatsScan, nullColumnStatsScan).foreach { scan =>
        val stats = scanRel(output, scan).computeStats()

        assert(stats.sizeInBytes === BigInt(12345))
        assert(stats.rowCount.isEmpty)
      }
    }
  }

  test("create strips leaked internal metadata but preserves column IDs") {
    // A column carrying both a column ID (surfaced on purpose) and every internal metadata key
    // (listed in INTERNAL_METADATA_KEYS), simulating a v2 source that leaks internal metadata.
    // The column ID key is excluded here since it is set via `.id(...)` below.
    val leakedBuilder = new MetadataBuilder()
    INTERNAL_METADATA_KEYS.filter(_ != FIELD_ID_METADATA_KEY).foreach { key =>
      leakedBuilder.putString(key, "leaked")
    }
    val leakedMetadata = leakedBuilder.build()
    val table = new Table {
      override def name(): String = "t"
      override def columns(): Array[Column] = Array(
        Column.builderFor("id", IntegerType)
          .id("1")
          .metadata(leakedMetadata.json)
          .build())
      override def capabilities(): util.Set[TableCapability] =
        util.Set.of[TableCapability]()
    }

    val relation =
      DataSourceV2Relation.create(table, None, None, CaseInsensitiveStringMap.empty())
    val field = relation.schema.head

    // All leaked internal metadata keys are stripped from the relation output ...
    INTERNAL_METADATA_KEYS.filter(_ != FIELD_ID_METADATA_KEY).foreach { key =>
      assert(!field.metadata.contains(key), s"internal metadata key $key should be stripped")
    }
    // ... but the column ID is preserved.
    assert(field.id.contains("1"))
    assert(field.metadata.contains(FIELD_ID_METADATA_KEY))
  }
}

private class FakeTableWithSchema(
    tableSchema: StructType = StructType(Seq(StructField("id", IntegerType))))
    extends Table {

  override def name(): String = "fake"
  override def schema(): StructType = tableSchema
  override def capabilities(): java.util.Set[TableCapability] = java.util.Collections.emptySet()
}
