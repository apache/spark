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

package org.apache.spark.sql.catalyst.analysis

import java.util

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{CreateTableAsSelect, LeafNode, OptionList, UnresolvedTableSpec}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.catalog.{InMemoryTableCatalog, Table, TableCapability, TableCatalog}
import org.apache.spark.sql.connector.expressions.Expressions
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._

class CreateTablePartitioningValidationSuite extends AnalysisTest {
  val tableSpec =
    UnresolvedTableSpec(Map.empty, None, OptionList(Seq.empty), None, None, None, None, false,
      Seq.empty)
  test("CreateTableAsSelect: fail missing top-level column") {
    val plan = CreateTableAsSelect(
      UnresolvedIdentifier(Array("table_name").toImmutableArraySeq),
      Expressions.bucket(4, "does_not_exist") :: Nil,
      TestRelation2,
      tableSpec,
      Map.empty,
      ignoreIfExists = false)

    assert(!plan.resolved)
    assertAnalysisErrorCondition(plan,
      expectedErrorCondition = "UNSUPPORTED_FEATURE.PARTITION_WITH_NESTED_COLUMN_IS_UNSUPPORTED",
      expectedMessageParameters = Map("cols" -> "`does_not_exist`"))
  }

  test("CreateTableAsSelect: fail missing top-level column nested reference") {
    val plan = CreateTableAsSelect(
      UnresolvedIdentifier(Array("table_name").toImmutableArraySeq),
      Expressions.bucket(4, "does_not_exist.z") :: Nil,
      TestRelation2,
      tableSpec,
      Map.empty,
      ignoreIfExists = false)

    assert(!plan.resolved)
    assertAnalysisErrorCondition(plan,
      expectedErrorCondition = "UNSUPPORTED_FEATURE.PARTITION_WITH_NESTED_COLUMN_IS_UNSUPPORTED",
      expectedMessageParameters = Map("cols" -> "`does_not_exist`.`z`"))
  }

  test("CreateTableAsSelect: fail missing nested column") {
    val plan = CreateTableAsSelect(
      UnresolvedIdentifier(Array("table_name").toImmutableArraySeq),
      Expressions.bucket(4, "point.z") :: Nil,
      TestRelation2,
      tableSpec,
      Map.empty,
      ignoreIfExists = false)

    assert(!plan.resolved)
    assertAnalysisErrorCondition(plan,
      expectedErrorCondition = "UNSUPPORTED_FEATURE.PARTITION_WITH_NESTED_COLUMN_IS_UNSUPPORTED",
      expectedMessageParameters = Map("cols" -> "`point`.`z`"))
  }

  test("CreateTableAsSelect: fail with multiple errors") {
    val plan = CreateTableAsSelect(
      UnresolvedIdentifier(Array("table_name").toImmutableArraySeq),
      Expressions.bucket(4, "does_not_exist", "point.z") :: Nil,
      TestRelation2,
      tableSpec,
      Map.empty,
      ignoreIfExists = false)

    assert(!plan.resolved)
    assertAnalysisErrorCondition(plan,
      expectedErrorCondition = "UNSUPPORTED_FEATURE.PARTITION_WITH_NESTED_COLUMN_IS_UNSUPPORTED",
      expectedMessageParameters = Map("cols" -> "`does_not_exist`, `point`.`z`"))
  }

  test("CreateTableAsSelect: success with top-level column") {
    val plan = CreateTableAsSelect(
      UnresolvedIdentifier(Array("table_name").toImmutableArraySeq),
      Expressions.bucket(4, "id") :: Nil,
      TestRelation2,
      tableSpec,
      Map.empty,
      ignoreIfExists = false)

    assertAnalysisSuccess(plan)
  }

  test("CreateTableAsSelect: success using nested column") {
    val plan = CreateTableAsSelect(
      UnresolvedIdentifier(Array("table_name").toImmutableArraySeq),
      Expressions.bucket(4, "point.x") :: Nil,
      TestRelation2,
      tableSpec,
      Map.empty,
      ignoreIfExists = false)

    assertAnalysisSuccess(plan)
  }

  test("CreateTableAsSelect: success using complex column") {
    val plan = CreateTableAsSelect(
      UnresolvedIdentifier(Array("table_name").toImmutableArraySeq),
      Expressions.bucket(4, "point") :: Nil,
      TestRelation2,
      tableSpec,
      Map.empty,
      ignoreIfExists = false)

    assertAnalysisSuccess(plan)
  }
}

private[sql] object CreateTablePartitioningValidationSuite {
  val catalog: TableCatalog = {
    val cat = new InMemoryTableCatalog()
    cat.initialize("test", CaseInsensitiveStringMap.empty())
    cat
  }

  val schema: StructType = new StructType()
      .add("id", LongType)
      .add("data", StringType)
      .add("point", new StructType().add("x", DoubleType).add("y", DoubleType))
}

private[sql] case object TestRelation2 extends LeafNode with NamedRelation {
  override def name: String = "source_relation"
  override def output: Seq[AttributeReference] =
    DataTypeUtils.toAttributes(CreateTablePartitioningValidationSuite.schema)
}

private[sql] case object TestTable2 extends Table {
  override def name: String = "table_name"
  override def schema: StructType = CreateTablePartitioningValidationSuite.schema
  override def capabilities: util.Set[TableCapability] =
    util.EnumSet.noneOf(classOf[TableCapability])
}
