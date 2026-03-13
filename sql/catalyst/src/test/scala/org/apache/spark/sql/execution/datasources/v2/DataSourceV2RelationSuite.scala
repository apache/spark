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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.catalog.{CatalogColumnStat, CatalogStatistics}
import org.apache.spark.sql.catalyst.plans.logical.{Histogram, HistogramBin}
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class DataSourceV2RelationSuite extends SparkFunSuite {

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
        // "extra" is not in schema — should be silently skipped
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
}
