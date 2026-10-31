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

import org.apache.spark.{SparkFunSuite, SparkNumberFormatException}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Cast, EqualTo, EvalMode, Expression, GetStructField, GreaterThan, IsNotNull, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Column, Table, TableCapability}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.PartitionPredicateField
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, MapType, MetadataBuilder, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

class PushDownUtilsSuite extends SparkFunSuite with SQLHelper {

  test("SPARK-59572: only a runtime partition predicate keeps a key it cannot evaluate") {
    val ref = DataTypeUtils.toAttribute(StructField("p", StringType, nullable = true))
    val fields = Seq(PartitionPredicateField(Seq("p"), Some(ref)))
    // An ANSI cast of a non-numeric string throws when evaluated.
    val filter = EqualTo(Cast(ref, IntegerType, None, EvalMode.ANSI), Literal(1))
    val failing = InternalRow(UTF8String.fromString("hr"))

    // A runtime filter only prunes, so a partition it cannot evaluate is kept.
    val runtime = PushDownUtils.createRuntimePartitionPredicates(Seq(filter), fields)
    assert(runtime.size === 1)
    assert(runtime.head.eval(failing) === true)

    // Everywhere else Spark drops the filter the connector accepts, so the failure must surface.
    val (pushed, _) = PushDownUtils.createPartitionPredicates(Seq(filter), fields)
    assert(pushed.size === 1)
    intercept[SparkNumberFormatException](pushed.head.eval(failing))
  }

  private val innerStruct =
    StructType.fromDDL("x int, y struct<p: string, q: array<int>>, z string")
  private val commented = new MetadataBuilder().putString("comment", "kept").build()
  private val tableSchema = new StructType()
    .add("id", LongType, nullable = false)
    .add("s", innerStruct)
    .add("arr", ArrayType(innerStruct))
    .add("m", MapType(StringType, innerStruct))
    .add(StructField("Mixed", StringType, nullable = true, commented))

  /**
   * Runs [[PushDownUtils.pruneColumns]] with nested schema pruning enabled on a relation over
   * `tableSchema`, and returns the required schema pushed to the scan builder.
   */
  private def pushedSchema(
      query: Seq[AttributeReference] => (Seq[NamedExpression], Seq[Expression])): StructType = {
    val relation = DataSourceV2Relation.create(
      new TestTable(CatalogV2Util.structTypeToV2Columns(tableSchema)), None, None)
    val (projects, filters) = query(relation.output)
    val builder = new RecordingScanBuilder
    withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
      PushDownUtils.pruneColumns(builder, relation, projects, filters)
    }
    builder.requiredSchema
  }

  test("SPARK-60768: whole-column reads push the needed columns in table order") {
    val pushed = pushedSchema { output =>
      val Seq(id, s, _, m, mixed) = output
      (Seq(mixed, s, Alias(id, "id2")(), id), Seq(GreaterThan(id, Literal(1L)), IsNotNull(m)))
    }
    assert(pushed === StructType(Seq("id", "s", "m", "Mixed").map(tableSchema(_))))
    assert(!pushed("id").nullable)
    assert(pushed("Mixed").metadata === commented)

    assert(pushedSchema(output => (output, Nil)) === tableSchema)
  }

  test("SPARK-60768: reads that reference no column push an empty schema") {
    val pushed = pushedSchema { _ =>
      (Seq(Alias(Literal(1), "one")()), Seq(GreaterThan(Literal(2), Literal(1))))
    }
    assert(pushed.isEmpty)
  }

  test("SPARK-60768: nested reads still prune nested fields") {
    val nested = pushedSchema { output =>
      val Seq(id, s, _, _, _) = output
      (Seq(id, Alias(GetStructField(s, 0, Some("x")), "sx")()), Nil)
    }
    assert(nested === StructType(Seq(tableSchema("id"),
      StructField("s", StructType.fromDDL("x int")))))

    // A nested read and a whole-column read of the same column keep the whole column.
    val nestedAndWhole = pushedSchema { output =>
      val Seq(_, s, _, _, mixed) = output
      (Seq(Alias(GetStructField(s, 0, Some("x")), "sx")(), s), Seq(IsNotNull(mixed)))
    }
    assert(nestedAndWhole === StructType(Seq("s", "Mixed").map(tableSchema(_))))
  }

  /** A minimal table with the given columns. */
  private class TestTable(cols: Array[Column]) extends Table {
    override def name(): String = "test_table"

    override def columns(): Array[Column] = cols

    override def capabilities(): util.Set[TableCapability] = util.Set.of(TableCapability.BATCH_READ)
  }

  /** A scan builder that records the required schema of the last column pruning. */
  private class RecordingScanBuilder extends ScanBuilder with SupportsPushDownRequiredColumns {
    var requiredSchema: StructType = _

    override def pruneColumns(requiredSchema: StructType): Unit = {
      this.requiredSchema = requiredSchema
    }

    override def build(): Scan = new Scan {
      override def readSchema(): StructType = requiredSchema
    }
  }
}
