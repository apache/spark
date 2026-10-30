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
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, EqualTo, EvalMode, GetStructField, IsNotNull, Literal}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.catalog.{Column, Table, TableCapability}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.internal.connector.PartitionPredicateField
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

class PushDownUtilsSuite extends SparkFunSuite {

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

  test("SPARK-60768: prune nested columns only when a nested field is requested") {
    val structType = new StructType().add("a", IntegerType).add("b", IntegerType)
    val relation = DataSourceV2Relation.create(
      new TestTable(Array(Column.create("id", IntegerType), Column.create("s", structType))),
      None,
      None)
    val Seq(id, s) = relation.output

    // Only top-level columns are requested, so they are pushed as a whole.
    val topLevelBuilder = new RecordingScanBuilder
    PushDownUtils.pruneColumns(topLevelBuilder, relation, Seq(id, s), Seq(IsNotNull(s)))
    assert(topLevelBuilder.requiredSchema === relation.schema)

    // A nested field is requested, so the struct column is pruned to that field.
    val nestedBuilder = new RecordingScanBuilder
    val selectA = Alias(GetStructField(s, 0, Some("a")), "a")()
    PushDownUtils.pruneColumns(nestedBuilder, relation, Seq(selectA), Seq(IsNotNull(id)))
    assert(nestedBuilder.requiredSchema ===
      new StructType().add("id", IntegerType).add("s", new StructType().add("a", IntegerType)))
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
