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

import org.apache.spark.{SparkException, SparkFunSuite, SparkRuntimeException}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, ExpressionEvalHelper, Literal, MetadataAttribute}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.util.{GenericArrayData, MetadataColumnHelper}
import org.apache.spark.sql.connector.catalog.{Column, MetadataColumn, SupportsMetadataColumns, Table, TableCapability}
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, MapType, MetadataBuilder, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class CapturedSchemaProjectionSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("project structs by name while preserving null structs") {
    val currentType = StructType(
      Seq(StructField("age", IntegerType), StructField("name", StringType, nullable = false)))
    val capturedType = StructType(Seq(StructField("name", StringType, nullable = false)))

    val input = Literal(create_row(25, "Alice"), currentType)
    val projected = project(input, currentType, capturedType)
    assert(projected.dataType == capturedType)
    checkEvaluation(projected, create_row("Alice"))
    val projectedNull = project(Literal.create(null, currentType), currentType, capturedType)
    assert(projectedNull.dataType == capturedType)
    checkEvaluation(projectedNull, null)
  }

  test("project structs in arrays while preserving null arrays and elements") {
    val currentElement = StructType(
      Seq(StructField("extra", IntegerType), StructField("value", StringType, nullable = false)))
    val capturedElement = StructType(Seq(StructField("value", StringType, nullable = false)))
    val currentType = ArrayType(currentElement, containsNull = true)
    val capturedType = ArrayType(capturedElement, containsNull = true)
    val input = new GenericArrayData(Array(create_row(1, "a"), null))
    val expected = new GenericArrayData(Array(create_row("a"), null))

    val projected = project(Literal(input, currentType), currentType, capturedType)
    assert(projected.dataType == capturedType)
    checkEvaluation(projected, expected)
    checkEvaluation(project(Literal.create(null, currentType), currentType, capturedType), null)
  }

  test("project structs nested in an array of arrays") {
    val currentElement = StructType(
      Seq(StructField("extra", IntegerType), StructField("value", StringType, nullable = false)))
    val capturedElement = StructType(Seq(StructField("value", StringType, nullable = false)))
    val currentType =
      ArrayType(ArrayType(currentElement, containsNull = true), containsNull = true)
    val capturedType =
      ArrayType(ArrayType(capturedElement, containsNull = true), containsNull = true)
    val input = new GenericArrayData(
      Array[Any](new GenericArrayData(Array[Any](create_row(1, "a"), null)), null))
    val expected = new GenericArrayData(
      Array[Any](new GenericArrayData(Array[Any](create_row("a"), null)), null))

    // The inner array is projected by a lambda nested inside the outer array's lambda.
    val projected = project(Literal(input, currentType), currentType, capturedType)
    assert(projected.dataType == capturedType)
    checkEvaluation(projected, expected)
  }

  test("project structs in map keys and values") {
    val currentKey = StructType(
      Seq(StructField("extra", IntegerType), StructField("key", StringType, nullable = false)))
    val capturedKey = StructType(Seq(StructField("key", StringType, nullable = false)))
    val currentValue = StructType(
      Seq(StructField("extra", IntegerType), StructField("value", IntegerType, nullable = false)))
    val capturedValue = StructType(Seq(StructField("value", IntegerType, nullable = false)))
    val currentType = MapType(currentKey, currentValue, valueContainsNull = true)
    val capturedType = MapType(capturedKey, capturedValue, valueContainsNull = true)
    val input =
      create_map(Seq(create_row(1, "a"), create_row(2, "b")), Seq(create_row(3, 10), null))
    val expected = create_map(Seq(create_row("a"), create_row("b")), Seq(create_row(10), null))

    val projected = project(Literal(input, currentType), currentType, capturedType)
    assert(projected.dataType == capturedType)
    checkEvaluation(projected, expected)
    checkEvaluation(project(Literal.create(null, currentType), currentType, capturedType), null)
  }

  test("project nested structs and preserve captured field metadata") {
    val comment = new MetadataBuilder().putString("comment", "the name").build()
    val currentInner = StructType(Seq(
      StructField("added", IntegerType),
      StructField("name", StringType, nullable = false, comment)))
    val capturedInner =
      StructType(Seq(StructField("name", StringType, nullable = false, comment)))
    val currentType = StructType(Seq(
      StructField("inner", currentInner),
      StructField("added_outer", IntegerType)))
    val capturedType = StructType(Seq(StructField("inner", capturedInner)))

    val input = Literal(create_row(create_row(1, "Alice"), 2), currentType)
    val projected = project(input, currentType, capturedType)
    // StructField equality covers metadata, so this also asserts the comment survived.
    assert(projected.dataType == capturedType)
    checkEvaluation(projected, create_row(create_row("Alice")))
    checkEvaluation(
      project(Literal(create_row(null, 2), currentType), currentType, capturedType),
      create_row(null))
  }

  test("project a captured type through three levels of nested structs") {
    // Pure struct nesting, three levels deep, with a field added at every level and a different
    // ordinal shift at each one (l1 0 -> 1, l2 0 -> 2, leaf 0 -> 3), so reusing one level's
    // ordinals at another level would pick the wrong field. `l1` is nullable while `l2` is not,
    // which puts the KnownNotNull/If pair and the plain rebuild in one expression. The captured
    // comment sits on `l2`, a middle level rather than a leaf, and differs from the current one:
    // without `explicitMetadata` the alias would inherit the current comment from its
    // GetStructField child and the type check below would fail.
    val capturedComment = new MetadataBuilder().putString("comment", "captured").build()
    val currentComment = new MetadataBuilder().putString("comment", "current").build()

    val capturedL2 = StructType(Seq(StructField("leaf", StringType, nullable = false)))
    val currentL2 = StructType(Seq(
      StructField("added_c1", IntegerType),
      StructField("added_c2", IntegerType),
      StructField("added_c3", IntegerType),
      StructField("leaf", StringType, nullable = false)))

    val capturedL1 =
      StructType(Seq(StructField("l2", capturedL2, nullable = false, capturedComment)))
    val currentL1 = StructType(Seq(
      StructField("added_b1", IntegerType),
      StructField("added_b2", IntegerType),
      StructField("l2", currentL2, nullable = false, currentComment)))

    val capturedType = StructType(Seq(StructField("l1", capturedL1)))
    val currentType = StructType(Seq(
      StructField("added_a", IntegerType),
      StructField("l1", currentL1)))

    val input =
      Literal(create_row(1, create_row(2, 3, create_row(4, 5, 6, "deep"))), currentType)
    val projected = project(input, currentType, capturedType)
    // StructField equality covers metadata, so this also asserts `l2` kept the captured comment.
    assert(projected.dataType == capturedType)
    checkEvaluation(projected, create_row(create_row(create_row("deep"))))

    // A null at the middle level, then a null at the head so two If(IsNull(...)) guards nest.
    checkEvaluation(
      project(Literal(create_row(1, null), currentType), currentType, capturedType),
      create_row(null))
    val projectedNull = project(Literal.create(null, currentType), currentType, capturedType)
    assert(projectedNull.dataType == capturedType)
    checkEvaluation(projectedNull, null)
  }

  test("project a captured type nested through a struct, a map, and an array") {
    // A heterogeneous chain -- struct -> map value -> array -> struct -- with a field added at
    // every level, each shifting the captured field to a different ordinal (m 0 -> 1, v 0 -> 1,
    // w 0 -> 2). Reusing one level's ordinals at another level would pick the wrong field.
    val capturedInMap = StructType(Seq(StructField("v", StringType, nullable = false)))
    val currentInMap = StructType(Seq(
      StructField("before_v", IntegerType),
      StructField("v", StringType, nullable = false),
      StructField("after_v", IntegerType)))
    val capturedInArray = StructType(Seq(StructField("w", IntegerType, nullable = false)))
    val currentInArray = StructType(Seq(
      StructField("first_w", IntegerType),
      StructField("second_w", IntegerType),
      StructField("w", IntegerType, nullable = false)))

    val capturedType = StructType(Seq(
      StructField("m", MapType(
        StringType, ArrayType(capturedInMap, containsNull = true), valueContainsNull = true)),
      StructField("arr", ArrayType(capturedInArray, containsNull = true))))
    val currentType = StructType(Seq(
      StructField("added_first", IntegerType),
      StructField("m", MapType(
        StringType, ArrayType(currentInMap, containsNull = true), valueContainsNull = true)),
      StructField("arr", ArrayType(currentInArray, containsNull = true)),
      StructField("added_last", IntegerType)))

    val currentMap = create_map(
      Seq("k1", "k2"),
      Seq(new GenericArrayData(Array[Any](create_row(1, "a", 2), null)), null))
    val currentArray = new GenericArrayData(Array[Any](create_row(7, 8, 9), null))
    val expectedMap = create_map(
      Seq("k1", "k2"),
      Seq(new GenericArrayData(Array[Any](create_row("a"), null)), null))
    val expectedArray = new GenericArrayData(Array[Any](create_row(9), null))

    val projected =
      project(Literal(create_row(100, currentMap, currentArray, 200), currentType),
        currentType, capturedType)
    assert(projected.dataType == capturedType)
    checkEvaluation(projected, create_row(expectedMap, expectedArray))

    // A null map inside the struct: the map is only reached through GetStructField, so the whole
    // TransformValues chain must stay null-safe.
    checkEvaluation(
      project(Literal(create_row(100, null, currentArray, 200), currentType),
        currentType, capturedType),
      create_row(null, expectedArray))

    // A null struct at the head of the chain exercises the nullable branch, whose null literal
    // must carry the fully rebuilt nested type.
    val projectedNull = project(Literal.create(null, currentType), currentType, capturedType)
    assert(projectedNull.dataType == capturedType)
    checkEvaluation(projectedNull, null)
  }

  test("project only map values when the key type is unchanged") {
    val currentValue = StructType(
      Seq(StructField("extra", IntegerType), StructField("value", IntegerType, nullable = false)))
    val capturedValue = StructType(Seq(StructField("value", IntegerType, nullable = false)))
    val currentType = MapType(StringType, currentValue, valueContainsNull = true)
    val capturedType = MapType(StringType, capturedValue, valueContainsNull = true)
    val input = create_map(Seq("a"), Seq(create_row(1, 10)))
    val expected = create_map(Seq("a"), Seq(create_row(10)))

    val projected = project(Literal(input, currentType), currentType, capturedType)
    assert(projected.dataType == capturedType)
    checkEvaluation(projected, expected)
  }

  test("project only map keys when the value type is unchanged") {
    val currentKey = StructType(
      Seq(StructField("extra", IntegerType), StructField("key", StringType, nullable = false)))
    val capturedKey = StructType(Seq(StructField("key", StringType, nullable = false)))
    val currentType = MapType(currentKey, IntegerType, valueContainsNull = true)
    val capturedType = MapType(capturedKey, IntegerType, valueContainsNull = true)
    val input = create_map(Seq(create_row(1, "a")), Seq(10))
    val expected = create_map(Seq(create_row("a")), Seq(10))

    val projected = project(Literal(input, currentType), currentType, capturedType)
    assert(projected.dataType == capturedType)
    checkEvaluation(projected, expected)
  }

  test("dropping a map key field that made keys distinct fails at runtime") {
    val currentKey = StructType(
      Seq(StructField("extra", IntegerType), StructField("key", StringType, nullable = false)))
    val capturedKey = StructType(Seq(StructField("key", StringType, nullable = false)))
    val currentType = MapType(currentKey, IntegerType, valueContainsNull = true)
    val capturedType = MapType(capturedKey, IntegerType, valueContainsNull = true)
    // Both keys collapse to {key: "a"} once `extra` is dropped. Note this only fails under the
    // default mapKeyDedupPolicy; LAST_WIN silently keeps the last entry, matching the behavior
    // that predates the rebinding.
    val input = create_map(Seq(create_row(1, "a"), create_row(2, "a")), Seq(10, 20))

    checkExceptionInExpression[SparkRuntimeException](
      project(Literal(input, currentType), currentType, capturedType),
      "Duplicate map key")
  }

  test("match nested field names case-insensitively but not under case-sensitive analysis") {
    val currentType = StructType(
      Seq(StructField("added", IntegerType), StructField("NAME", StringType, nullable = false)))
    val capturedType = StructType(Seq(StructField("name", StringType, nullable = false)))
    val input = Literal(create_row(1, "Alice"), currentType)

    // Case-insensitive analysis resolves the captured `name` to the current `NAME` and restores the
    // captured field name.
    val projected = project(input, currentType, capturedType)
    assert(projected.dataType == capturedType)
    checkEvaluation(projected, create_row("Alice"))

    // Case-sensitive analysis treats them as different fields.
    checkRejected(
      input,
      currentType,
      capturedType,
      "captured struct field name is missing from",
      caseSensitive = true)
  }

  test("keep the first ordinal when a struct has duplicate field names") {
    // Upstream validation rejects duplicate names, but `projectToType` resolves ordinals and must
    // stay deterministic if it is ever reached without that validation: the first match wins.
    val capturedType = StructType(Seq(StructField("dup", IntegerType)))

    val exactDuplicates =
      StructType(Seq(StructField("dup", IntegerType), StructField("dup", IntegerType)))
    val projected =
      project(Literal(create_row(1, 2), exactDuplicates), exactDuplicates, capturedType)
    assert(projected.dataType == capturedType)
    checkEvaluation(projected, create_row(1))

    // Names that collide only after case folding resolve the same way.
    val caseDuplicates =
      StructType(Seq(StructField("dup", IntegerType), StructField("DUP", IntegerType)))
    checkEvaluation(
      project(Literal(create_row(1, 2), caseDuplicates), caseDuplicates, capturedType),
      create_row(1))
  }

  test("reject a captured field that is missing from the current type") {
    val currentType = StructType(Seq(StructField("name", StringType)))
    val capturedType = StructType(Seq(StructField("missing", StringType)))

    checkRejected(
      Literal.create(null, currentType),
      currentType,
      capturedType,
      "captured struct field missing is missing from")
  }

  test("reject an incompatible captured type") {
    checkRejected(Literal(1), IntegerType, StringType, "cannot project incompatible data type")
  }

  test("reject a changed array element nullability") {
    val current = ArrayType(IntegerType, containsNull = true)
    val captured = ArrayType(IntegerType, containsNull = false)

    checkRejected(
      Literal.create(null, current), current, captured, "array element nullability changed")
  }

  test("reject a changed map value nullability") {
    val current = MapType(StringType, IntegerType, valueContainsNull = true)
    val captured = MapType(StringType, IntegerType, valueContainsNull = false)

    checkRejected(
      Literal.create(null, current), current, captured, "map value nullability changed")
  }

  test("reject a captured column whose nullability changed") {
    // Validation is expected to reject a nullability change before rebinding runs, so getting here
    // is an internal error. The differing nullability is also what stops the captured attribute
    // from being reused, which is how the column reaches this check.
    val relation = DataSourceV2Relation(
      table = new TestTable(Array(Column.create("id", IntegerType, false))),
      output = Seq(AttributeReference("id", IntegerType, nullable = true)()),
      catalog = None,
      identifier = None,
      options = CaseInsensitiveStringMap.empty())

    val e = intercept[SparkException] {
      CapturedSchemaProjection.rebindToCapturedSchema(relation)
    }
    assert(e.getCondition == "INTERNAL_ERROR")
    assert(e.getMessage.contains("Unexpected incompatible table schema after refresh validation"))
    assert(e.getMessage.contains("nullability changed for captured column id"))
  }

  test("reject a captured metadata column that a data column hides") {
    // The connector keeps reporting `index` as a metadata column, but a data column has taken that
    // name, and this connector suppresses rather than renames the conflict. Validation is expected
    // to reject this before rebinding runs, so it fails as an internal error here.
    val relation = DataSourceV2Relation(
      table = new TestMetadataTable(canRename = false),
      output = Seq(
        AttributeReference("id", IntegerType)(),
        MetadataAttribute("index", IntegerType, nullable = false)),
      catalog = None,
      identifier = None,
      options = CaseInsensitiveStringMap.empty())

    val e = intercept[SparkException] {
      CapturedSchemaProjection.rebindToCapturedSchema(relation)
    }
    assert(e.getCondition == "INTERNAL_ERROR")
    assert(e.getMessage.contains("Unexpected incompatible table schema after refresh validation"))
    assert(e.getMessage.contains("captured metadata column index is missing from"))
  }

  test("rebind a captured metadata column that the connector renames on conflict") {
    // Same conflict, but this connector renames the metadata column, so it stays readable and
    // rebinding succeeds instead of failing.
    val relation = DataSourceV2Relation(
      table = new TestMetadataTable(canRename = true),
      output = Seq(
        AttributeReference("id", IntegerType)(),
        MetadataAttribute("index", IntegerType, nullable = false)),
      catalog = None,
      identifier = None,
      options = CaseInsensitiveStringMap.empty())

    val rebound = CapturedSchemaProjection.rebindToCapturedSchema(relation)
    val project = rebound.asInstanceOf[Project]
    assert(project.output.map(_.name) == Seq("id", "index"))
    // Rebinding exists to keep the captured expression IDs valid for the parent plan.
    assert(project.output.map(_.exprId) == relation.output.map(_.exprId))
    // The relation exposes the new data column too, but only so the scan stays aligned with it --
    // the projection must drop it rather than let it shadow the captured metadata column.
    assert(project.child.output.map(_.name) == Seq("id", "index", "_index"))
    // Checking names alone cannot tell the renamed metadata column apart from the new data column,
    // since both are called `index` at some point. Assert what the captured `index` actually reads.
    val indexChild =
      project.projectList(1).asInstanceOf[Alias].child.asInstanceOf[AttributeReference]
    assert(indexChild.isMetadataCol, "the captured index must read the metadata column")
    assert(indexChild.name == "_index", "which the connector exposes under a renamed name")
    // The alias carries the captured metadata explicitly; without that the projection's own output
    // attribute would stop being a metadata column even though its child still is one.
    assert(project.output(1).isMetadataCol, "the projected index must stay a metadata column")
    assert(project.output(1).metadata == relation.output(1).metadata)
  }

  test("preserve captured field metadata on a rebuilt nested column") {
    // The alias over a rebuilt struct cannot inherit metadata from its child, because that child is
    // a CreateNamedStruct rather than a NamedExpression. Only the explicit captured metadata keeps
    // a column comment on the rebound column, and neither the type check nor attribute reuse
    // compares metadata, so nothing else would catch its loss.
    val comment = new MetadataBuilder().putString("comment", "captured").build()
    val capturedStruct = StructType(Seq(StructField("name", StringType, nullable = false)))
    val currentStruct = StructType(Seq(
      StructField("added", IntegerType),
      StructField("name", StringType, nullable = false)))
    val relation = DataSourceV2Relation(
      table = new TestTable(Array(Column.create("person", currentStruct, true))),
      output = Seq(AttributeReference("person", capturedStruct, nullable = true, comment)()),
      catalog = None,
      identifier = None,
      options = CaseInsensitiveStringMap.empty())

    val project = CapturedSchemaProjection.rebindToCapturedSchema(relation).asInstanceOf[Project]
    assert(project.child.output.map(_.dataType) == Seq(currentStruct))
    assert(project.output.map(_.dataType) == Seq(capturedStruct))
    assert(project.output.head.metadata == comment, "the captured comment must survive the rebuild")
  }

  private def project(
      input: Expression,
      from: DataType,
      to: DataType,
      caseSensitive: Boolean = false): Expression = {
    CapturedSchemaProjection.projectToType(input, from, to, caseSensitive)
  }

  private def checkRejected(
      input: Expression,
      from: DataType,
      to: DataType,
      expectedMessage: String,
      caseSensitive: Boolean = false): Unit = {
    val e = intercept[SparkException](project(input, from, to, caseSensitive))
    assert(e.getCondition == "INTERNAL_ERROR")
    assert(e.getMessage.contains("Unexpected incompatible table schema after refresh validation"))
    assert(e.getMessage.contains(expectedMessage))
  }

  /** A minimal table with no metadata columns. */
  private class TestTable(cols: Array[Column]) extends Table {
    override def name(): String = "test_table"

    override def columns(): Array[Column] = cols

    override def capabilities(): util.Set[TableCapability] = util.Set.of(TableCapability.BATCH_READ)
  }

  /** A table with an `index` metadata column whose name a data column has taken over. */
  private class TestMetadataTable(canRename: Boolean) extends Table with SupportsMetadataColumns {
    override def name(): String = "test_table"

    override def columns(): Array[Column] = Array(
      Column.create("id", IntegerType),
      Column.create("index", IntegerType))

    override def capabilities(): util.Set[TableCapability] = util.Set.of(TableCapability.BATCH_READ)

    override def metadataColumns(): Array[MetadataColumn] = Array(new MetadataColumn {
      override def name(): String = "index"
      override def dataType(): DataType = IntegerType
      override def isNullable: Boolean = false
    })

    override val canRenameConflictingMetadataColumns: Boolean = canRename
  }
}
