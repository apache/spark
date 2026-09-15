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

import org.apache.spark.{SparkException, SparkFunSuite, SparkRuntimeException, SparkThrowable}
import org.apache.spark.sql.catalyst.analysis.{caseInsensitiveResolution, caseSensitiveResolution}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, AttributeSeq, CreateNamedStruct, Expression, ExpressionEvalHelper, ExtractValue, GetStructField, Literal, MetadataAttribute}
import org.apache.spark.sql.catalyst.optimizer.FoldablePropagation
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.catalyst.util.{GenericArrayData, MetadataColumnHelper}
import org.apache.spark.sql.connector.catalog.{Column, MetadataColumn, SupportsMetadataColumns, Table, TableCapability}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, MapType, Metadata, MetadataBuilder, StringType, StructField, StructType, VariantType}
import org.apache.spark.sql.util.{CaseInsensitiveStringMap, SchemaUtils, SchemaValidationMode}
import org.apache.spark.types.variant.VariantBuilder
import org.apache.spark.unsafe.types.VariantVal

class AnalyzedSchemaProjectionSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("rebind a root-level addition without inspecting unchanged column types") {
    val capturedTable = new TestTable(Array(
      Column.create("id", IntegerType),
      Column.create("payload", VariantType)))
    val currentTable = new TestTable(Array(
      Column.create("added", StringType),
      Column.create("id", IntegerType),
      Column.create("payload", VariantType)))
    val captured = DataSourceV2Relation.create(capturedTable, None, None)

    val project = AnalyzedSchemaProjection
      .rebindToAnalyzedSchema(captured.copy(table = currentTable))
      .asInstanceOf[Project]

    assert(project.child.output.map(_.name) == Seq("added", "id", "payload"))
    val childByName = project.child.output.map(attr => attr.name -> attr).toMap
    captured.output.foreach { capturedAttr =>
      assert(childByName(capturedAttr.name).exprId == capturedAttr.exprId)
    }
    assert(project.output.map(_.name) == Seq("id", "payload"))
    assert(project.output.map(_.dataType) == Seq(IntegerType, VariantType))
    assert(project.output.map(_.exprId) == captured.output.map(_.exprId))
  }

  test("return the relation unchanged when the captured output still matches the table") {
    val table = new TestTable(Array(
      Column.create("id", IntegerType),
      Column.create("payload", VariantType)))
    val captured = DataSourceV2Relation.create(table, None, None)

    // No projection is added, so an unchanged table keeps the plan it was analyzed with.
    assert(AnalyzedSchemaProjection.rebindToAnalyzedSchema(captured) eq captured)
  }

  test("rebind an already rebound relation without stacking a second projection") {
    val capturedTable = new TestTable(Array(Column.create("id", IntegerType)))
    val currentTable = new TestTable(Array(
      Column.create("added", StringType),
      Column.create("id", IntegerType)))
    val captured = DataSourceV2Relation.create(capturedTable, None, None)

    val project = AnalyzedSchemaProjection
      .rebindToAnalyzedSchema(captured.copy(table = currentTable))
      .asInstanceOf[Project]

    // `V2TableRefreshUtil` refreshes through `transformDown`, so the rule visits the relation it
    // just rebound. Rebinding must be idempotent there: the second pass has to leave the relation
    // and its attributes alone, or the projection above it would reference stale expression IDs.
    val rebound = project.child.asInstanceOf[DataSourceV2Relation]
    assert(AnalyzedSchemaProjection.rebindToAnalyzedSchema(rebound) eq rebound)
  }

  test("project a nested field addition around an unchanged variant leaf") {
    val currentType = StructType(Seq(
      StructField("added", IntegerType),
      StructField("payload", VariantType, nullable = false)))
    val capturedType =
      StructType(Seq(StructField("payload", VariantType, nullable = false)))
    val parsed = VariantBuilder.parseJson("""{"key":1}""", false)
    val variant = new VariantVal(parsed.getValue, parsed.getMetadata)

    val projected = project(
      Literal(create_row(10, variant), currentType), currentType, capturedType)
    assert(projected.dataType == capturedType)
    checkEvaluation(projected, create_row(variant))
  }

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

  test("project structs in maps nested in arrays") {
    val currentLeaf = StructType(
      Seq(StructField("extra", IntegerType), StructField("value", StringType, nullable = false)))
    val capturedLeaf = StructType(Seq(StructField("value", StringType, nullable = false)))
    val currentMap = MapType(StringType, currentLeaf, valueContainsNull = true)
    val capturedMap = MapType(StringType, capturedLeaf, valueContainsNull = true)
    val currentType = ArrayType(currentMap, containsNull = true)
    val capturedType = ArrayType(capturedMap, containsNull = true)
    val input = new GenericArrayData(Array[Any](
      create_map(Seq("k1", "k2"), Seq(create_row(1, "a"), null)),
      null))
    val expected = new GenericArrayData(Array[Any](
      create_map(Seq("k1", "k2"), Seq(create_row("a"), null)),
      null))

    val projected = project(Literal(input, currentType), currentType, capturedType)
    assert(projected.dataType == capturedType)
    checkEvaluation(projected, expected)
  }

  test("project structs in maps nested in map values") {
    val currentLeaf = StructType(
      Seq(StructField("extra", IntegerType), StructField("value", StringType, nullable = false)))
    val capturedLeaf = StructType(Seq(StructField("value", StringType, nullable = false)))
    val currentInnerMap = MapType(StringType, currentLeaf, valueContainsNull = true)
    val capturedInnerMap = MapType(StringType, capturedLeaf, valueContainsNull = true)
    val currentType = MapType(StringType, currentInnerMap, valueContainsNull = true)
    val capturedType = MapType(StringType, capturedInnerMap, valueContainsNull = true)
    val input = create_map(
      Seq("outer", "null"),
      Seq(create_map(Seq("inner"), Seq(create_row(1, "a"))), null))
    val expected = create_map(
      Seq("outer", "null"),
      Seq(create_map(Seq("inner"), Seq(create_row("a"))), null))

    val projected = project(Literal(input, currentType), currentType, capturedType)
    assert(projected.dataType == capturedType)
    checkEvaluation(projected, expected)
  }

  test("project structs in arrays nested in map keys") {
    val currentElement = StructType(
      Seq(StructField("extra", IntegerType), StructField("key", StringType, nullable = false)))
    val capturedElement = StructType(Seq(StructField("key", StringType, nullable = false)))
    val currentKey = ArrayType(currentElement, containsNull = true)
    val capturedKey = ArrayType(capturedElement, containsNull = true)
    val currentType = MapType(currentKey, IntegerType, valueContainsNull = false)
    val capturedType = MapType(capturedKey, IntegerType, valueContainsNull = false)
    val inputKey = new GenericArrayData(Array[Any](create_row(1, "a"), null))
    val expectedKey = new GenericArrayData(Array[Any](create_row("a"), null))

    val projected = project(
      Literal(create_map(Seq(inputKey), Seq(10)), currentType), currentType, capturedType)
    assert(projected.dataType == capturedType)
    checkEvaluation(projected, create_map(Seq(expectedKey), Seq(10)))
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
    // Both keys collapse to {key: "a"} once `extra` is dropped. Narrowing a key changes how many
    // entries the map holds, so it is a fold rather than a projection, and the captured type has no
    // correct answer to return. Rebuilding through `TransformKeys` keeps map-key uniqueness, so the
    // outcome is whatever `mapKeyDedupPolicy` prescribes rather than a map with duplicate keys.
    val input = create_map(Seq(create_row(1, "a"), create_row(2, "a")), Seq(10, 20))

    withSQLConf(
        SQLConf.MAP_KEY_DEDUP_POLICY.key -> SQLConf.MapKeyDedupPolicy.EXCEPTION.toString) {
      checkErrorInExpression[SparkRuntimeException](
        project(Literal(input, currentType), currentType, capturedType),
        condition = "DUPLICATED_MAP_KEY",
        parameters = Map(
          "key" -> "[a]",
          "mapKeyDedupPolicy" -> "\"spark.sql.mapKeyDedupPolicy\""))
    }

    withSQLConf(
        SQLConf.MAP_KEY_DEDUP_POLICY.key -> SQLConf.MapKeyDedupPolicy.LAST_WIN.toString) {
      checkEvaluation(
        project(Literal(input, currentType), currentType, capturedType),
        create_map(Seq(create_row("a")), Seq(20)))
    }
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

  test("reject a captured column the fold separates from every current name") {
    // U+0130 CAPITAL I WITH DOT separates the two halves of the identity rule: `equalsIgnoreCase`
    // equates it with `i`, while folding with `toLowerCase` maps it to `i` plus a combining dot.
    // Top-level resolution collects candidates by the fold before applying the resolver, so the
    // fold alone already rules this pair out: it names two different columns, and rebinding must
    // report the captured one missing rather than bind it to the other.
    val capitalIWithDot = new String(Character.toChars(0x130))
    val capturedNested = StructType(Seq(StructField("i", StringType, nullable = false)))
    val currentNested =
      StructType(Seq(StructField(capitalIWithDot, StringType, nullable = false)))
    val currentTable = new TestTable(
      Array(Column.create(capitalIWithDot, currentNested, false)))
    val relation = DataSourceV2Relation(
      table = currentTable,
      output = Seq(AttributeReference("i", capturedNested, nullable = false)()),
      catalog = None,
      identifier = None,
      options = CaseInsensitiveStringMap.empty())

    // Refresh rejects this evolution before rebinding runs, so the internal error below reports a
    // validation gap and is not reachable through a query.
    val capturedSchema = StructType(Seq(StructField("i", capturedNested, nullable = false)))
    val currentSchema =
      StructType(Seq(StructField(capitalIWithDot, currentNested, nullable = false)))
    Seq(false, true).foreach { caseSensitive =>
      val resolver = if (caseSensitive) caseSensitiveResolution else caseInsensitiveResolution
      assert(
        SchemaUtils.validateSchemaCompatibility(
          capturedSchema,
          currentSchema,
          resolver,
          SchemaValidationMode.ALLOW_NEW_FIELDS,
          checkFieldIds = false).nonEmpty,
        s"validation must reject this pair (caseSensitive = $caseSensitive)")

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        val e = intercept[SparkException] {
          AnalyzedSchemaProjection.rebindToAnalyzedSchema(relation)
        }
        assert(e.getCondition == "INTERNAL_ERROR")
        assert(e.getMessage.contains("captured column i is missing from current table"))
      }
    }
  }

  test("bind a captured column to the one that folds alike, not a resolver-equal sibling") {
    // A table can legally hold both `s` and U+017F LONG S: the duplicate-name check folds names
    // with `toLowerCase`, which keeps them apart, while the resolver compares with
    // `equalsIgnoreCase`, which does not. The captured name must read the column it folds to.
    val longS = new String(Character.toChars(0x17f))
    val currentTable = new TestTable(Array(
      Column.create(longS, IntegerType),
      Column.create("s", IntegerType)))
    val relation = DataSourceV2Relation(
      table = currentTable,
      output = Seq(AttributeReference("s", IntegerType)()),
      catalog = None,
      identifier = None,
      options = CaseInsensitiveStringMap.empty())

    val rebound = AnalyzedSchemaProjection.rebindToAnalyzedSchema(relation).asInstanceOf[Project]
    assert(rebound.child.output.map(_.name) == Seq(longS, "s"))
    assert(rebound.output.map(_.name) == Seq("s"))
    assert(rebound.output.map(_.exprId) == relation.output.map(_.exprId))
    val read = rebound.projectList.head.references.toSeq
    assert(read.map(_.name) == Seq("s"), "the captured column must not read the folding sibling")
    assert(read.map(_.exprId) == Seq(rebound.child.output.last.exprId))
  }

  test("a resolver-equal sibling makes a captured struct field ambiguous") {
    // Struct-field resolution compares with the resolver alone, so `S` and U+017F are both equal to
    // `s` and a fresh `SELECT st.s` fails. Rebinding reports the same ambiguity rather than picking
    // one, in either order. The same pair at the top level is NOT ambiguous, because top-level
    // resolution folds first - see "bind a captured column to the one that folds alike ..." above.
    val capturedType = StructType(Seq(StructField("s", IntegerType)))
    Seq(
      Seq(StructField(longS, IntegerType), StructField("s", IntegerType)),
      Seq(StructField("S", IntegerType), StructField(longS, IntegerType))).foreach { fields =>
      val currentType = StructType(fields)
      val e = intercept[Throwable] {
        project(Literal(create_row(1, 2), currentType), currentType, capturedType)
      }
      assert(condition(e) == "AMBIGUOUS_REFERENCE_TO_FIELDS", s"for ${fields.map(_.name)}")
    }
  }

  test("duplicate struct field names report the ambiguity resolution reports") {
    // The duplicate-name check rejects both of these schemas, so this path is unreachable through
    // refresh. It still must not invent an answer: struct-field resolution calls both ambiguous, so
    // reaching it without that check has to surface the same error, not silently take an ordinal.
    val capturedType = StructType(Seq(StructField("dup", IntegerType)))
    Seq(
      Seq(StructField("dup", IntegerType), StructField("dup", IntegerType)),
      Seq(StructField("dup", IntegerType), StructField("DUP", IntegerType))).foreach { fields =>
      val currentType = StructType(fields)
      val e = intercept[Throwable] {
        project(Literal(create_row(1, 2), currentType), currentType, capturedType)
      }
      assert(condition(e) == "AMBIGUOUS_REFERENCE_TO_FIELDS", s"for ${fields.map(_.name)}")
    }
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
      AnalyzedSchemaProjection.rebindToAnalyzedSchema(relation)
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
      AnalyzedSchemaProjection.rebindToAnalyzedSchema(relation)
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

    val rebound = AnalyzedSchemaProjection.rebindToAnalyzedSchema(relation)
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
    // Metadata-column-ness survives here either way: the alias child is a NamedExpression, so an
    // alias without explicit metadata would inherit its `__metadata_col` key. What the captured
    // metadata pins is the exact value, not whatever the current relation happens to expose.
    assert(project.output(1).isMetadataCol, "the projected index must stay a metadata column")
    assert(project.output(1).metadata == relation.output(1).metadata)
  }

  test("optimizer cleanup distinguishes explicit empty from unspecified empty nested metadata") {
    val currentComment = new MetadataBuilder().putString("comment", "current").build()
    val currentType = StructType(Seq(
      StructField("added", IntegerType),
      StructField("name", StringType, nullable = false, currentComment)))
    val capturedType =
      StructType(Seq(StructField("name", StringType, nullable = false, Metadata.empty)))
    val input = AttributeReference("person", currentType, nullable = false)()

    val rebuilt = project(input, currentType, capturedType)
    val cleanedRebuild = FoldablePropagation(
      Project(Seq(Alias(rebuilt, "person")()), LocalRelation(input))).asInstanceOf[Project]
    // The captured empty metadata is observable in the optimized output even though shared alias
    // cleanup remains unchanged and may remove empty-metadata aliases.
    assert(cleanedRebuild.output.head.dataType == capturedType)

    val unspecifiedEmpty = CreateNamedStruct(Seq(
      Literal("value"),
      Alias(Literal(1), "value")()))
    val cleanedUnspecified = FoldablePropagation(
      Project(Seq(Alias(unspecifiedEmpty, "record")()), LocalRelation(input)))
      .asInstanceOf[Project]
      .projectList
      .head
      .asInstanceOf[Alias]
      .child
      .asInstanceOf[CreateNamedStruct]
    assert(!cleanedUnspecified.children(1).isInstanceOf[Alias])
  }

  test("preserve captured field metadata on a rebuilt nested column") {
    // The alias over a rebuilt struct cannot inherit metadata from its child, because that child is
    // a CreateNamedStruct rather than a NamedExpression. Attribute reuse is inapplicable because
    // this path rebuilds the expression, so only explicit captured metadata keeps the column
    // comment on the rebound column.
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

    val project = AnalyzedSchemaProjection.rebindToAnalyzedSchema(relation).asInstanceOf[Project]
    assert(project.child.output.map(_.dataType) == Seq(currentStruct))
    assert(project.output.map(_.dataType) == Seq(capturedStruct))
    assert(project.output.head.metadata == comment, "the captured comment must survive the rebuild")
  }

  test("name matching matches Spark's own resolution (recorded outcomes)") {
    nameCases.foreach { c =>
      assert(
        rebindTopLevel(c.captured, c.current) == c.topExpected,
        s"top-level '${c.label}': captured ${escapeName(c.captured)} " +
          s"against ${c.current.map(escapeName)}")
      assert(
        projectNested(c.captured, c.current) == c.nestedExpected,
        s"nested '${c.label}': captured ${escapeName(c.captured)} " +
          s"against ${c.current.map(escapeName)}")
    }
  }

  test("name matching delegates to Spark's own resolution rather than reimplementing it") {
    // Guards against a future rule of our own drifting from resolution: it compares against the
    // live APIs, so it also covers name pairs the recorded table above does not list.
    nameCases.foreach { c =>
      assert(
        rebindTopLevel(c.captured, c.current) == topAuthority(c.captured, c.current),
        s"top-level '${c.label}' must answer what AttributeSeq.resolve answers")
      assert(
        projectNested(c.captured, c.current) == nestedAuthority(c.captured, c.current),
        s"nested '${c.label}' must answer what ExtractValue.extractValue answers")
    }
  }

  test("exact duplicate names resolve the way Spark resolves them") {
    // Kept apart from `nameCases` because two columns with the identical name make `canReuse`
    // give both the captured attribute, so the rebound output holds one attribute twice rather
    // than two ambiguous ones. The duplicate-name check rejects this schema under every locale.
    assert(topAuthority("dup", Seq("dup", "dup")) == Left("AMBIGUOUS_REFERENCE"))
    assert(nestedAuthority("dup", Seq("dup", "dup")) == Left("AMBIGUOUS_REFERENCE_TO_FIELDS"))
    assert(projectNested("dup", Seq("dup", "dup")) == Left("AMBIGUOUS_REFERENCE_TO_FIELDS"))
  }

  test("which locales admit a schema whose names collide under the fold") {
    // Pins the upstream facts this rebinding must not assume away: the duplicate-name check folds
    // with the JVM default locale (`SchemaUtils.checkColumnNameDuplication`) while every other fold
    // here uses `Locale.ROOT`, so a Turkish or Lithuanian default locale admits a pair that an
    // English one rejects. Rebinding must therefore not rely on the fold being single-valued.
    def admits(names: Seq[String], locale: String): Boolean = {
      val previous = util.Locale.getDefault
      try {
        util.Locale.setDefault(util.Locale.forLanguageTag(locale))
        SchemaUtils.checkColumnNameDuplication(names, caseSensitiveAnalysis = false)
        true
      } catch {
        case _: Throwable => false
      } finally {
        util.Locale.setDefault(previous)
      }
    }

    assert(admits(Seq("S", longS), "en"), "the fold keeps S and U+017F apart under any locale")
    assert(!admits(Seq(capitalIDot, iCombiningDot), "en"))
    assert(admits(Seq(capitalIDot, iCombiningDot), "tr"))
    assert(!admits(Seq(capitalIGrave, smallIGrave), "en"))
    assert(admits(Seq(capitalIGrave, smallIGrave), "lt"))
    assert(!admits(Seq("dup", "dup"), "en") && !admits(Seq("dup", "dup"), "tr"))
  }

  test("refresh validation does not blame field IDs for a fold collision") {
    // With the addition after the captured column, `SchemaUtils.index` folds both current names to
    // one key and keeps the later field, so the captured field is compared against the addition and
    // its ID mismatch is reported - a legal change rejected for the wrong reason.
    //
    // Runs under a Turkish default locale because that is what makes the schema admissible at all:
    // the duplicate-name check folds with the default locale and would otherwise reject the pair.
    val captured = StructType(Seq(StructField(iCombiningDot, IntegerType).withId("1")))
    val previousLocale = util.Locale.getDefault
    try {
      util.Locale.setDefault(util.Locale.forLanguageTag("tr"))
      Seq(
        "addition first" -> Seq(capitalIDot -> "2", iCombiningDot -> "1"),
        "addition last" -> Seq(iCombiningDot -> "1", capitalIDot -> "2")).foreach {
        case (label, fields) =>
          val current = StructType(fields.map { case (n, id) =>
            StructField(n, IntegerType).withId(id)
          })
          val errors = SchemaUtils.validateSchemaCompatibility(
            captured,
            current,
            caseInsensitiveResolution,
            SchemaValidationMode.ALLOW_NEW_FIELDS,
            checkFieldIds = true)
          assert(
            !errors.exists(_.contains("field ID has changed")),
            s"$label must not report a field ID change: $errors")
      }
    } finally {
      util.Locale.setDefault(previousLocale)
    }
  }

  test("array elements and map entries match names the same way as a bare struct") {
    // `projectToType` recurses into containers through the same name matching, so a pair that is
    // ambiguous in a bare struct must stay ambiguous inside an array, a map key and a map value.
    val currentField = StructType(
      Seq(StructField("S", IntegerType), StructField(longS, IntegerType)))
    val capturedField = StructType(Seq(StructField("s", IntegerType)))

    Seq[(String, DataType, DataType)](
      ("array element",
        ArrayType(currentField, containsNull = false),
        ArrayType(capturedField, containsNull = false)),
      ("map value",
        MapType(IntegerType, currentField, valueContainsNull = false),
        MapType(IntegerType, capturedField, valueContainsNull = false)),
      ("map key",
        MapType(currentField, IntegerType, valueContainsNull = false),
        MapType(capturedField, IntegerType, valueContainsNull = false))).foreach {
      case (label, from, to) =>
        val e = intercept[Throwable](project(AttributeReference("c", from)(), from, to))
        assert(
          condition(e) == "AMBIGUOUS_REFERENCE_TO_FIELDS",
          s"$label must report the ambiguity struct-field resolution reports, got ${condition(e)}")
    }
  }

  test("refresh validation and rebinding agree on which current field a captured name refers to") {
    // Refresh validation runs first and rebinding assumes it passed. Two things must therefore
    // never happen: validation accepting a schema that rebinding then cannot map (which would
    // surface as INTERNAL_ERROR), and rebinding mapping a name that validation would have
    // rejected. The pairs below make the two halves of the identity rule disagree in each
    // direction, so a rule that applied only one half fails here.
    //
    // The one divergence that is allowed is a struct field the resolver finds ambiguous: struct
    // fields are resolved with the resolver alone, so rebinding reports the ambiguity a fresh
    // query reports even though validation, which folds first, paired the field successfully.
    val rows = Seq(
      ("fold-equal and resolver-equal", "c", Seq("C")),
      ("fold-equal but resolver-unequal", iCombiningDot, Seq(capitalIDot)),
      ("fold-unequal but resolver-equal", "s", Seq(longS)),
      ("fold-equal beside an ambiguous sibling", "s", Seq("S", longS)))

    rows.foreach { case (label, captured, current) =>
      val capturedType = StructType(Seq(StructField(captured, IntegerType)))
      val currentType = StructType(current.map(n => StructField(n, IntegerType)))
      val validationPasses = SchemaUtils.validateSchemaCompatibility(
        capturedType,
        currentType,
        caseInsensitiveResolution,
        SchemaValidationMode.ALLOW_NEW_FIELDS,
        checkFieldIds = false).isEmpty

      Seq(
        ("top-level", rebindTopLevel(captured, current), topAuthority(captured, current)),
        ("nested", projectNested(captured, current), nestedAuthority(captured, current))
      ).foreach { case (level, ours, authority) =>
        if (validationPasses) {
          assert(
            ours != Left("INTERNAL_ERROR"),
            s"$level '$label': validation accepted this schema, so rebinding must map the name " +
              s"or report what a fresh query reports, not an internal error")
          assert(ours == authority, s"$level '$label': must answer what resolution answers")
        }
      }

      // The other direction is guarded by order rather than by the rules agreeing: validation folds
      // names, so it calls a captured field removed when only a name the fold separates but the
      // resolver equates is left, while struct-field resolution would still read that field. What
      // keeps the two from disagreeing in practice is that `V2TableRefreshUtil.refresh` throws
      // `columnsChangedAfterAnalysis` before it rebinds, so this pair never reaches the projection.
      if (!validationPasses) {
        assert(
          SchemaUtils.validateSchemaCompatibility(
            capturedType,
            currentType,
            caseInsensitiveResolution,
            SchemaValidationMode.ALLOW_NEW_FIELDS,
            checkFieldIds = false).nonEmpty,
          s"'$label' must be rejected by validation, which is what stops it reaching rebinding")
      }
    }
  }

  // U+017F LONG S folds to itself but `equalsIgnoreCase` equates it with `s`.
  private val longS = new String(Character.toChars(0x17f))
  // U+0130 CAPITAL I WITH DOT folds to `i` plus a combining dot, which `equalsIgnoreCase` does
  // not equate with either. A Turkish default locale keeps the two apart for the duplicate check.
  private val capitalIDot = new String(Character.toChars(0x130))
  private val iCombiningDot = "i" + new String(Character.toChars(0x307))
  // U+00CC/U+00EC differ only by case to both rules, so they are ambiguous rather than distinct.
  private val capitalIGrave = new String(Character.toChars(0xcc))
  private val smallIGrave = new String(Character.toChars(0xec))

  /**
   * One row of the name-matching contract.
   *
   * `topExpected` and `nestedExpected` are the recorded behaviour of Spark's own resolution:
   * `AttributeSeq.resolve` for a top-level column and `ExtractValue.extractValue` for a struct
   * field. They are written as literals rather than derived from those APIs at run time, so that a
   * change on either side - ours or Catalyst's - turns this test red instead of silently following.
   * `Left` is an expected error condition, `Right` an expected ordinal in `current`.
   *
   * The two levels differ because Spark's two rules differ: top-level resolution folds names with
   * `toLowerCase(ROOT)` to collect candidates and only then filters them with the resolver, while
   * struct-field resolution compares with the resolver alone. A pair the fold separates but the
   * resolver equates is therefore a distinct column at the top level and ambiguous inside a struct.
   */
  private case class NameCase(
      label: String,
      captured: String,
      current: Seq[String],
      topExpected: Either[String, Int],
      nestedExpected: Either[String, Int])

  private val nameCases = Seq(
    NameCase("ascii case rename", "c", Seq("C"), Right(0), Right(0)),
    NameCase("longS after the captured name", "s", Seq("S", longS),
      Right(0), Left("AMBIGUOUS_REFERENCE_TO_FIELDS")),
    NameCase("longS before the captured name", "s", Seq(longS, "S"),
      Right(1), Left("AMBIGUOUS_REFERENCE_TO_FIELDS")),
    NameCase("fold-colliding addition first", iCombiningDot, Seq(capitalIDot, iCombiningDot),
      Right(1), Right(1)),
    NameCase("fold-colliding addition last", iCombiningDot, Seq(iCombiningDot, capitalIDot),
      Right(0), Right(0)),
    NameCase("resolver-equal pair first", smallIGrave, Seq(capitalIGrave, smallIGrave),
      Left("AMBIGUOUS_REFERENCE"), Left("AMBIGUOUS_REFERENCE_TO_FIELDS")),
    NameCase("resolver-equal pair last", smallIGrave, Seq(smallIGrave, capitalIGrave),
      Left("AMBIGUOUS_REFERENCE"), Left("AMBIGUOUS_REFERENCE_TO_FIELDS")),
    NameCase("case-folding duplicates", "dup", Seq("dup", "DUP"),
      Left("AMBIGUOUS_REFERENCE"), Left("AMBIGUOUS_REFERENCE_TO_FIELDS")))

  private def condition(t: Throwable): String = t match {
    case s: SparkThrowable => s.getCondition
    case other => other.getClass.getSimpleName
  }

  /** Rebinds a captured column against `current` and reports the ordinal it reads. */
  private def rebindTopLevel(captured: String, current: Seq[String]): Either[String, Int] = {
    val relation = DataSourceV2Relation(
      table = new TestTable(current.map(n => Column.create(n, IntegerType)).toArray),
      output = Seq(AttributeReference(captured, IntegerType)()),
      catalog = None,
      identifier = None,
      options = CaseInsensitiveStringMap.empty())
    try {
      val rebound = AnalyzedSchemaProjection.rebindToAnalyzedSchema(relation).asInstanceOf[Project]
      val read = rebound.projectList.head.references.head
      Right(rebound.child.output.indexWhere(_.exprId == read.exprId))
    } catch {
      case e: Throwable => Left(condition(e))
    }
  }

  /** Projects a captured struct field out of `current` and reports the ordinal it extracts. */
  private def projectNested(captured: String, current: Seq[String]): Either[String, Int] = {
    val currentType = StructType(current.map(n => StructField(n, IntegerType)))
    val capturedType = StructType(Seq(StructField(captured, IntegerType)))
    try {
      val projected = project(AttributeReference("st", currentType)(), currentType, capturedType)
      projected.collect { case g: GetStructField => g.ordinal } match {
        case Seq(ordinal) => Right(ordinal)
        case ordinals => Left(s"expected one extraction, got $ordinals")
      }
    } catch {
      case e: Throwable => Left(condition(e))
    }
  }

  private def topAuthority(captured: String, current: Seq[String]): Either[String, Int] = {
    val attrs = current.map(n => AttributeReference(n, IntegerType)())
    try {
      AttributeSeq(attrs).resolve(Seq(captured), caseInsensitiveResolution) match {
        case Some(resolved) =>
          Right(attrs.indexWhere(_.exprId == resolved.references.head.exprId))
        case None => Left("NO MATCH")
      }
    } catch {
      case e: Throwable => Left(condition(e))
    }
  }

  private def nestedAuthority(captured: String, current: Seq[String]): Either[String, Int] = {
    val currentType = StructType(current.map(n => StructField(n, IntegerType)))
    ExtractValue
      .extractValue(
        AttributeReference("st", currentType)(), Literal(captured), caseInsensitiveResolution)
      .fold(
        extracted => Right(extracted.asInstanceOf[GetStructField].ordinal),
        throwable => Left(condition(throwable)))
  }

  private def escapeName(name: String): String =
    if (name.forall(c => c > 0x20 && c < 0x7f)) s"'$name'"
    else name.map(c => f"U+$c%04X").mkString("+")

  private def project(
      input: Expression,
      from: DataType,
      to: DataType,
      caseSensitive: Boolean = false): Expression = {
    val resolver = if (caseSensitive) caseSensitiveResolution else caseInsensitiveResolution
    AnalyzedSchemaProjection.projectToType(input, from, to, resolver)
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
