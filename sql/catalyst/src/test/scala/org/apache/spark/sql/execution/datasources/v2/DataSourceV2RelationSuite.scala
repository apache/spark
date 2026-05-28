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
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{Histogram, HistogramBin}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

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

  test("canonicalize matches across Table wrappers when catalog and identifier agree") {
    val schema = StructType(Seq(StructField("a", IntegerType)))
    val output = Seq(AttributeReference("a", IntegerType)())
    val catalog = Some(new StubCatalog("cat"))
    val ident = Some(Identifier.of(Array("ns"), "t"))
    val opts = CaseInsensitiveStringMap.empty()

    // Same id on both sides — strict path. Different Table instances, same id => equal.
    val a1 = DataSourceV2Relation(new StubTable("a", schema, id = "id-1"),
      output, catalog, ident, opts)
    val a2 = DataSourceV2Relation(new StubTable("a-wrapped", schema, id = "id-1"),
      output, catalog, ident, opts)
    assert(a1.sameResult(a2),
      "two relations to the same logical table should canonicalize equal")

    // Null id on both sides — permissive path. Same catalog+identifier => still equal.
    val b1 = DataSourceV2Relation(new StubTable("b", schema, id = null),
      output, catalog, ident, opts)
    val b2 = DataSourceV2Relation(new StubTable("b-wrapped", schema, id = null),
      output, catalog, ident, opts)
    assert(b1.sameResult(b2),
      "relations with null id should canonicalize equal when catalog+identifier match")

    // Mixed: one side has an id, the other null. Treated as different (couldn't both be the same
    // logical table for any connector that exposes id() consistently).
    val c1 = DataSourceV2Relation(new StubTable("c", schema, id = "id-1"),
      output, catalog, ident, opts)
    val c2 = DataSourceV2Relation(new StubTable("c", schema, id = null),
      output, catalog, ident, opts)
    assert(!c1.sameResult(c2),
      "id present vs null should not compare equal")

    // Different non-null ids — drop+recreate scenario. Must compare unequal.
    val d1 = DataSourceV2Relation(new StubTable("d", schema, id = "id-1"),
      output, catalog, ident, opts)
    val d2 = DataSourceV2Relation(new StubTable("d", schema, id = "id-2"),
      output, catalog, ident, opts)
    assert(!d1.sameResult(d2),
      "different ids (drop+recreate) should not compare equal")

    // Different identifier — must compare unequal even if ids would otherwise match.
    val otherIdent = Some(Identifier.of(Array("ns"), "other"))
    val e1 = DataSourceV2Relation(new StubTable("e", schema, id = "id-1"),
      output, catalog, ident, opts)
    val e2 = DataSourceV2Relation(new StubTable("e", schema, id = "id-1"),
      output, catalog, otherIdent, opts)
    assert(!e1.sameResult(e2),
      "different identifiers should not compare equal")

    // Different catalog — must compare unequal even with same identifier and id.
    val otherCatalog = Some(new StubCatalog("other"))
    val f1 = DataSourceV2Relation(new StubTable("f", schema, id = "id-1"),
      output, catalog, ident, opts)
    val f2 = DataSourceV2Relation(new StubTable("f", schema, id = "id-1"),
      output, otherCatalog, ident, opts)
    assert(!f1.sameResult(f2),
      "different catalog names should not compare equal")
  }

  test("canonicalize falls back when catalog or identifier is missing") {
    val schema = StructType(Seq(StructField("a", IntegerType)))
    val output = Seq(AttributeReference("a", IntegerType)())
    val ident = Some(Identifier.of(Array("ns"), "t"))
    val opts = CaseInsensitiveStringMap.empty()

    // No catalog: falls through to the default canonical form. Equality reduces to reference
    // equality on the `table` field — same instance is equal, different instances are not.
    val sharedTable = new StubTable("shared", schema, id = "id-1")
    val sameInstance1 = DataSourceV2Relation(sharedTable, output, None, ident, opts)
    val sameInstance2 = DataSourceV2Relation(sharedTable, output, None, ident, opts)
    assert(sameInstance1.sameResult(sameInstance2),
      "fallback path: same Table instance compares equal")

    val diff1 = DataSourceV2Relation(new StubTable("x", schema, id = "id-1"),
      output, None, ident, opts)
    val diff2 = DataSourceV2Relation(new StubTable("x", schema, id = "id-1"),
      output, None, ident, opts)
    assert(!diff1.sameResult(diff2),
      "fallback path: different Table instances are not equal even with matching id")
  }
}

private class StubCatalog(catalogName: String) extends CatalogPlugin {
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = ()
  override def name(): String = catalogName
}

private class StubTable(
    tableName: String,
    schema: StructType,
    id: String) extends Table {
  override def name(): String = tableName
  override def id(): String = id
  override def columns(): Array[org.apache.spark.sql.connector.catalog.Column] =
    org.apache.spark.sql.connector.catalog.CatalogV2Util.structTypeToV2Columns(schema)
  override def capabilities(): java.util.Set[TableCapability] = java.util.Collections.emptySet()
}
