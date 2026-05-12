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

package org.apache.spark.sql.connector.catalog

import org.mockito.Mockito.{mock, when}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{ArrayType, IntegerType, Metadata, StringType, StructField, StructType}

class CatalogV2UtilSuite extends SparkFunSuite {
  test("Load relation should encode the identifiers for V2Relations") {
    val testCatalog = mock(classOf[TableCatalog])
    val ident = mock(classOf[Identifier])
    val table = mock(classOf[Table])
    when(table.columns()).thenReturn(Array(Column.create("i", IntegerType)))
    when(testCatalog.loadTable(ident)).thenReturn(table)
    val r = CatalogV2Util.loadRelation(testCatalog, ident)
    assert(r.isDefined)
    assert(r.get.isInstanceOf[DataSourceV2Relation])
    val v2Relation = r.get.asInstanceOf[DataSourceV2Relation]
    assert(v2Relation.catalog.exists(_ == testCatalog))
    assert(v2Relation.identifier.exists(_ == ident))
  }

  // -- Column.toJson / Column.fromJson round-trip --
  //
  // The wire shape is the Spark `StructField` JSON: `{name, type, nullable, metadata}`
  // with the `Column.comment()` (if present) folded into `metadata` under the `"comment"`
  // key. The conversion goes through `CatalogV2Util.{v2ColumnToStructField,
  // structFieldToV2Column}` so the comment-vs-metadata rule, the empty-metadata-as-null
  // rule, and any future StructField-shape evolution are all defined in one place.

  test("Column.toJson / fromJson round-trips analyzer-attached metadata") {
    val metadataJson = "{\"metric_view.type\":\"dimension\",\"metric_view.expr\":\"region\"}"
    val input = Column.create("region", StringType, true, null, metadataJson)

    val wire = input.toJson()
    val output = Column.fromJson(wire)

    assert(output.name() === "region")
    assert(output.dataType() === StringType)
    assert(output.nullable())
    assert(output.comment() === null)
    val md = Metadata.fromJson(output.metadataInJSON())
    assert(md.getString("metric_view.type") === "dimension")
    assert(md.getString("metric_view.expr") === "region")
  }

  test("Column.toJson / fromJson round-trips comment + metadata without duplication") {
    val metadataJson = "{\"metric_view.type\":\"dimension\"}"
    val input = Column.create("region", StringType, true, "the region", metadataJson)

    val wire = input.toJson()
    // Wire format: comment is folded into metadata under the "comment" key.
    assert(wire.contains("\"comment\":\"the region\""))

    val output = Column.fromJson(wire)
    assert(output.comment() === "the region")
    val md = Metadata.fromJson(output.metadataInJSON())
    // Comment is exposed via Column.comment(), NOT in metadataInJSON, so the read side
    // must strip it on the way back out -- matches `structFieldToV2Column`'s convention.
    assert(!md.contains("comment"))
    assert(md.getString("metric_view.type") === "dimension")
  }

  test("Column.toJson / fromJson round-trips empty metadata as null metadataInJSON") {
    val input = Column.create("c", IntegerType, false, null, null)

    val wire = input.toJson()
    val output = Column.fromJson(wire)

    assert(output.name() === "c")
    assert(output.dataType() === IntegerType)
    assert(!output.nullable())
    assert(output.comment() === null)
    // Empty metadata must round-trip as `null` (not `"{}"`), matching
    // `structFieldToV2Column`'s `metadataAsJson` convention so consumers checking
    // `metadataInJSON() == null` work correctly.
    assert(output.metadataInJSON() === null)
  }

  test("Column.toJson / fromJson round-trips comment-only metadata as null metadataInJSON") {
    val input = Column.create("c", IntegerType, true, "only comment", null)

    val wire = input.toJson()
    val output = Column.fromJson(wire)

    assert(output.comment() === "only comment")
    // After the read side strips "comment", the metadata is empty -- so `metadataInJSON`
    // is null (not `"{}"`), consistent with the empty-metadata case above.
    assert(output.metadataInJSON() === null)
  }

  test("Column.toJson / fromJson round-trips a complex nested type") {
    val nested = ArrayType(
      StructType(Seq(
        StructField("a", IntegerType, nullable = false),
        StructField("b", StringType, nullable = true))),
      containsNull = true)
    val input = Column.create("nested", nested, true, null, null)

    val wire = input.toJson()
    val output = Column.fromJson(wire)

    assert(output.dataType() === nested)
    assert(output.nullable())
  }
}
