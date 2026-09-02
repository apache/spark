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

package org.apache.spark.sql.pipelines.util

import scala.util.Success

import org.apache.spark.SparkException
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.{
  CatalogV2Util,
  Identifier,
  InMemoryTableCatalog,
  TableChange,
  TableInfo
}
import org.apache.spark.sql.pipelines.graph.{
  FlowFunction,
  FlowFunctionResult,
  Input,
  QueryContext,
  QueryOrigin,
  ResolvedFlow,
  StreamingFlow,
  UntypedFlow
}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class SchemaInferenceUtilsSuite extends QueryTest with SharedSparkSession {
  import TableChangeExtractors._

  /** A [[FlowFunction]] that throws if invoked; the inferSchemaFromFlows test builds resolved
   * flows directly. */
  private val noOpFlowFunction: FlowFunction = new FlowFunction {
    override def call(
        allInputs: Set[TableIdentifier],
        availableInputs: Seq[Input],
        configuration: Map[String, String],
        queryContext: QueryContext,
        queryOrigin: QueryOrigin): FlowFunctionResult =
      throw new UnsupportedOperationException(
        "noOpFlowFunction.call should not be invoked from SchemaInferenceUtilsSuite tests")
  }

  private val queryContext = QueryContext(currentCatalog = Some("c"), currentDatabase = Some("d"))

  /** A resolved flow with the given identifier and output schema, writing to `destination`. */
  private def resolvedFlow(
      identifier: TableIdentifier,
      destination: TableIdentifier,
      schema: StructType): ResolvedFlow = {
    val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    val flow = UntypedFlow(
      identifier = identifier,
      destinationIdentifier = destination,
      func = noOpFlowFunction,
      queryContext = queryContext,
      sqlConf = Map.empty,
      once = false,
      origin = QueryOrigin.empty)
    new StreamingFlow(
      flow,
      FlowFunctionResult(
        requestedInputs = Set.empty,
        batchInputs = Set.empty,
        streamingInputs = Set.empty,
        usedExternalInputs = Set.empty,
        dataFrame = Success(df),
        sqlConf = Map.empty))
  }

  test("determineColumnChanges - adding new columns") {
    val currentSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType)

    val targetSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType)
      .add("age", IntegerType)
      .add("email", StringType, nullable = true, "Email address")

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)

    // Should have 2 changes - adding 'age' and 'email' columns
    assert(changes.length === 2)

    // Verify the changes are of the correct type and have the right properties
    val ageChange = changes
      .find {
        case addCol: TableChange.AddColumn => addCol.fieldNames().sameElements(Array("age"))
        case _ => false
      }
      .get
      .asInstanceOf[TableChange.AddColumn]

    val emailChange = changes
      .find {
        case addCol: TableChange.AddColumn => addCol.fieldNames().sameElements(Array("email"))
        case _ => false
      }
      .get
      .asInstanceOf[TableChange.AddColumn]

    // Verify age column properties
    assert(ageChange.dataType() === IntegerType)
    assert(ageChange.isNullable() === true) // Default nullable is true
    assert(ageChange.comment() === null)

    // Verify email column properties
    assert(emailChange.dataType() === StringType)
    assert(emailChange.isNullable() === true)
    assert(emailChange.comment() === "Email address")
  }

  test("determineColumnChanges - updating column types") {
    val currentSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("amount", DoubleType)
      .add("timestamp", TimestampType)

    val targetSchema = new StructType()
      .add("id", LongType, nullable = false) // Changed type from Int to Long
      .add("amount", DecimalType(10, 2)) // Changed type from Double to Decimal
      .add("timestamp", TimestampType) // No change

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)

    // Should have 2 changes - updating 'id' and 'amount' column types
    assert(changes.length === 2)

    // Verify the changes are of the correct type
    val idChange = changes
      .find {
        case update: TableChange.UpdateColumnType => update.fieldNames().sameElements(Array("id"))
        case _ => false
      }
      .get
      .asInstanceOf[TableChange.UpdateColumnType]

    val amountChange = changes
      .find {
        case update: TableChange.UpdateColumnType =>
          update.fieldNames().sameElements(Array("amount"))
        case _ => false
      }
      .get
      .asInstanceOf[TableChange.UpdateColumnType]

    // Verify the new data types
    assert(idChange.newDataType() === LongType)
    assert(amountChange.newDataType() === DecimalType(10, 2))
  }

  test("determineColumnChanges - updating nullability and comments") {
    val currentSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType, nullable = true)
      .add("description", StringType, nullable = true, "Item description")

    val targetSchema = new StructType()
      .add("id", IntegerType, nullable = true) // Changed nullability
      .add("name", StringType, nullable = false) // Changed nullability
      .add("description", StringType, nullable = true, "Product description") // Changed comment

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)

    // Should have 3 changes - updating nullability for 'id' and 'name', and comment for
    // 'description'
    assert(changes.length === 3)

    // Verify the nullability changes
    val idNullabilityChange = changes
      .find {
        case update: TableChange.UpdateColumnNullability =>
          update.fieldNames().sameElements(Array("id"))
        case _ => false
      }
      .get
      .asInstanceOf[TableChange.UpdateColumnNullability]

    val nameNullabilityChange = changes
      .find {
        case update: TableChange.UpdateColumnNullability =>
          update.fieldNames().sameElements(Array("name"))
        case _ => false
      }
      .get
      .asInstanceOf[TableChange.UpdateColumnNullability]

    // Verify the comment change
    val descriptionCommentChange = changes
      .find {
        case update: TableChange.UpdateColumnComment =>
          update.fieldNames().sameElements(Array("description"))
        case _ => false
      }
      .get
      .asInstanceOf[TableChange.UpdateColumnComment]

    // Verify the new nullability values
    assert(idNullabilityChange.nullable() === true)
    assert(nameNullabilityChange.nullable() === false)

    // Verify the new comment
    assert(descriptionCommentChange.newComment() === "Product description")
  }

  test("determineColumnChanges - complex changes") {
    val currentSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType)
      .add("old_field", BooleanType)

    val targetSchema = new StructType()
      .add("id", LongType, nullable = true) // Changed type and nullability
      // Added comment and changed nullability
      .add("name", StringType, nullable = false, "Full name")
      .add("new_field", StringType) // New field

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)

    // Should have these changes:
    // 1. Update id type
    // 2. Update id nullability
    // 3. Update name nullability
    // 4. Update name comment
    // 5. Add new_field
    // 6. Remove old_field
    assert(changes.length === 6)

    // Count the types of changes
    val typeChanges = changes.collect { case _: TableChange.UpdateColumnType => 1 }.size
    val nullabilityChanges = changes.collect {
      case _: TableChange.UpdateColumnNullability => 1
    }.size
    val commentChanges = changes.collect { case _: TableChange.UpdateColumnComment => 1 }.size
    val addColumnChanges = changes.collect { case _: TableChange.AddColumn => 1 }.size

    assert(typeChanges === 1)
    assert(nullabilityChanges === 2)
    assert(commentChanges === 1)
    assert(addColumnChanges === 1)
  }

  test("determineColumnChanges - no changes") {
    val schema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType)
      .add("timestamp", TimestampType)

    // Same schema, no changes expected
    val changes = SchemaInferenceUtils.diffSchemas(schema, schema)
    assert(changes.isEmpty)
  }

  test("determineColumnChanges - deleting columns") {
    val currentSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType)
      .add("age", IntegerType)
      .add("email", StringType)
      .add("phone", StringType)

    val targetSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType)
      // age, email, and phone columns are removed

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)

    // Should have 3 changes - deleting 'age', 'email', and 'phone' columns
    assert(changes.length === 3)

    // Verify all changes are DeleteColumn operations
    val deleteChanges = changes.collect { case dc: TableChange.DeleteColumn => dc }
    assert(deleteChanges.length === 3)

    // Verify the specific columns being deleted
    val columnNames = deleteChanges.map(_.fieldNames()(0)).toSet
    assert(columnNames === Set("age", "email", "phone"))
  }

  test("determineColumnChanges - mixed additions and deletions") {
    val currentSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("first_name", StringType)
      .add("last_name", StringType)
      .add("age", IntegerType)

    val targetSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("full_name", StringType) // New column
      .add("email", StringType) // New column
      .add("age", IntegerType) // Unchanged
      // first_name and last_name are removed

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)

    // Should have 4 changes:
    // - 2 additions (full_name, email)
    // - 2 deletions (first_name, last_name)
    assert(changes.length === 4)

    // Count the types of changes
    val addChanges = changes.collect { case ac: TableChange.AddColumn => ac }
    val deleteChanges = changes.collect { case dc: TableChange.DeleteColumn => dc }

    assert(addChanges.length === 2)
    assert(deleteChanges.length === 2)

    // Verify the specific columns being added and deleted
    val addedColumnNames = addChanges.map(_.fieldNames()(0)).toSet
    val deletedColumnNames = deleteChanges.map(_.fieldNames()(0)).toSet

    assert(addedColumnNames === Set("full_name", "email"))
    assert(deletedColumnNames === Set("first_name", "last_name"))
  }

  test("determineColumnChanges - a case-only difference is a drop-then-add, not a match") {
    // diffSchemas keys column identity on the EXACT field name, with no case normalization. So a
    // target `Value` against a persisted `value` is a distinct column: `value` is dropped and
    // `Value` added. This is what makes a case-only rename visible on the non-merging paths
    // (materialized views, full refresh), where targetSchema is the declared schema as-is.
    val currentSchema = new StructType().add("id", IntegerType).add("value", StringType)
    val targetSchema = new StructType().add("id", IntegerType).add("Value", StringType)

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)

    val addChanges = changes.collect { case ac: TableChange.AddColumn => ac.fieldNames()(0) }
    val deleteChanges = changes.collect { case dc: TableChange.DeleteColumn => dc.fieldNames()(0) }
    assert(addChanges === Seq("Value"))
    assert(deleteChanges === Seq("value"))
  }

  test("determineColumnChanges - two declared columns differing only in case are both kept") {
    // A declared schema carrying both `value` and `Value` reaches diffSchemas verbatim (nothing on
    // the create path rejects duplicate-cased columns). Exact-name keying must surface BOTH as
    // additions; normalizing the lookup key would collapse them and silently keep an arbitrary one
    // (whichever came last), losing a column the user declared.
    val currentSchema = new StructType().add("id", IntegerType)
    val targetSchema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
      .add("Value", IntegerType)

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)

    val added = changes.collect { case ac: TableChange.AddColumn =>
      ac.fieldNames()(0) -> ac.dataType()
    }.toMap
    assert(added === Map("value" -> StringType, "Value" -> IntegerType))
  }

  test("mergeSchemas - a nested field differing only in case folds onto the existing field when " +
    "case-insensitive") {
    // The nested analog of the top-level case-only fold. `StructType.merge` propagates the
    // case-sensitivity flag into nested struct merges (SPARK-58525), so the incoming `s.Value` is
    // matched to the existing `s.value` and the struct keeps a single field with the persisted
    // (left) spelling -- rather than growing a second, case-differing nested field.
    val currentSchema = new StructType()
      .add("id", IntegerType)
      .add("s", new StructType().add("value", StringType))
    val dataSchema = new StructType()
      .add("id", IntegerType)
      .add("s", new StructType().add("Value", StringType))

    val merged =
      SchemaMergingUtils.mergeSchemas(currentSchema, dataSchema, caseSensitive = false)
    assert(merged === currentSchema)

    // Because the merge is a no-op, evolution derives no table changes at all: in particular the
    // nested struct is NOT rewritten (which would be an UpdateColumnType on `s`).
    assert(
      SchemaInferenceUtils.diffSchemas(currentSchema, merged).isEmpty)
  }

  test("mergeSchemas - a nested field differing only in case stays distinct when case-sensitive") {
    // The case-sensitive control: `s.value` and `s.Value` are different fields, so the merged
    // struct carries both.
    val currentSchema = new StructType().add("s", new StructType().add("value", StringType))
    val dataSchema = new StructType().add("s", new StructType().add("Value", StringType))

    val merged = SchemaMergingUtils.mergeSchemas(currentSchema, dataSchema, caseSensitive = true)
    val expectedStruct = new StructType().add("value", StringType).add("Value", StringType)
    assert(merged === new StructType().add("s", expectedStruct))

    // Unlike the case-insensitive test above (where the merge is a no-op and no changes are
    // derived), evolution here must grow the `s` struct. The growth surfaces as an add of the
    // nested leaf `s.Value`, which is the portable shape; retyping `s` with the whole new struct
    // would be rejected by `CheckAnalysis`, which fails ALTER COLUMN ... TYPE on a struct.
    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, merged)
    assert(changes.length === 1)
    val addChange = changes.collect { case ac: TableChange.AddColumn => ac }
    assert(addChange.length === 1)
    assert(addChange.head.fieldNames() === Array("s", "Value"))
    assert(addChange.head.dataType() === StringType)
  }

  test("mergeSchemas - a nested case-only field whose type also changes fails to merge, and " +
    "diffSchemas reports it as a type change") {
    // A nested field that differs only in case AND changes type is rejected rather than silently
    // resolved. Note this is a *type* incompatibility, not a case one: `StructType.merge` never
    // widens numeric types, so `int` -> `long` fails identically for a same-cased field and at the
    // top level. The value of pinning it here is that case-insensitive matching does not turn an
    // incompatible type change into a silent merge -- the run still fails loudly, and the user's
    // remedy is a full refresh.
    val currentSchema = new StructType().add("s", new StructType().add("value", IntegerType))
    val dataSchema = new StructType().add("s", new StructType().add("Value", LongType))

    val ex = intercept[SparkException] {
      SchemaMergingUtils.mergeSchemas(currentSchema, dataSchema, caseSensitive = false)
    }
    assert(ex.getCondition === "CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE")

    // Same-cased and top-level widening fail the same way, confirming the rejection is about the
    // type change rather than the case difference.
    intercept[SparkException] {
      SchemaMergingUtils.mergeSchemas(
        currentSchema,
        new StructType().add("s", new StructType().add("value", LongType)),
        caseSensitive = false)
    }
    intercept[SparkException] {
      SchemaMergingUtils.mergeSchemas(
        new StructType().add("v", IntegerType),
        new StructType().add("v", LongType),
        caseSensitive = false)
    }

    // Diffing the two schemas directly (rather than diffing against their merge, which fails
    // above) reports a drop-then-add of the nested leaf, since `diffSchemas` keys column identity
    // on the exact name at every level. This is the nested analog of the top-level rule pinned by
    // "a case-only difference is a drop-then-add, not a match", and it is only reachable on the
    // non-merging paths, where the declared schema is used as-is.
    {
      val changes = SchemaInferenceUtils.diffSchemas(currentSchema, dataSchema)
      assert(changes.length === 2, s"changes=$changes")
      val added = changes.collect { case ac: TableChange.AddColumn => ac }
      assert(added.length === 1, s"changes=$changes")
      assert(added.head.fieldNames() === Array("s", "Value"))
      assert(added.head.dataType() === LongType)
      val deleted = changes.collect { case dc: TableChange.DeleteColumn => dc }
      assert(deleted.length === 1, s"changes=$changes")
      assert(deleted.head.fieldNames() === Array("s", "value"))
      assert(!changes.exists(_.isInstanceOf[TableChange.UpdateColumnType]))
    }
  }

  test("diffSchemas - a leaf added to a struct is a nested add, not a retype of the parent") {
    val currentSchema = new StructType()
      .add("id", IntegerType)
      .add("point", new StructType().add("x", DoubleType).add("y", DoubleType))
    val targetSchema = new StructType()
      .add("id", IntegerType)
      .add(
        "point",
        new StructType()
          .add("x", DoubleType)
          .add("y", DoubleType)
          .add("z", DoubleType))

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)
    assert(changes.length === 1, s"changes=$changes")
    assert(addsOf(changes) === Map(Seq("point", "z") -> ((DoubleType, true, null))))
  }

  test("diffSchemas - a leaf added to a deeply nested struct carries the full path") {
    val inner = new StructType().add("c", IntegerType)
    val currentSchema = new StructType().add("a", new StructType().add("b", inner))
    val targetSchema = new StructType()
      .add("a", new StructType().add("b", inner.add("d", StringType)))

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)
    assert(addsOf(changes).keySet === Set(Seq("a", "b", "d")))
  }

  test("diffSchemas - an added nested leaf keeps its declared nullability and comment") {
    val currentSchema = new StructType()
      .add("s", new StructType().add("a", IntegerType))
    val targetSchema = new StructType().add(
      "s",
      new StructType()
        .add("a", IntegerType)
        .add("b", StringType, nullable = false, "a comment"))

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)
    assert(addsOf(changes) === Map(Seq("s", "b") -> ((StringType, false, "a comment"))))
  }

  test("diffSchemas - a leaf removed from a struct is a nested delete") {
    val currentSchema = new StructType()
      .add("s", new StructType().add("a", IntegerType).add("b", StringType))
    val targetSchema = new StructType()
      .add("s", new StructType().add("a", IntegerType))

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)
    assert(changes.length === 1, s"changes=$changes")
    assert(deletesOf(changes) === Set(Seq("s", "b")))
  }

  test("diffSchemas - a leaf type change inside a struct is a nested type update") {
    val currentSchema = new StructType()
      .add("s", new StructType().add("a", IntegerType))
    val targetSchema = new StructType()
      .add("s", new StructType().add("a", LongType))

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)
    assert(changes.length === 1, s"changes=$changes")
    assert(typeUpdatesOf(changes) === Map(Seq("s", "a") -> LongType))
  }

  test("diffSchemas - nested leaf nullability and comment changes are emitted at the leaf") {
    val currentSchema = new StructType()
      .add("s", new StructType().add("a", IntegerType, nullable = true, "old"))
    val targetSchema = new StructType()
      .add("s", new StructType().add("a", IntegerType, nullable = false, "new"))

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)
    assert(changes.length === 2, s"changes=$changes")
    assert(nullabilityUpdatesOf(changes) === Map(Seq("s", "a") -> false))
    assert(commentUpdatesOf(changes) === Map(Seq("s", "a") -> "new"))
  }

  test("diffSchemas - a field added to a struct inside an array uses the element path") {
    val currentSchema = new StructType()
      .add("points", ArrayType(new StructType().add("x", DoubleType)))
    val targetSchema = new StructType()
      .add("points", ArrayType(new StructType().add("x", DoubleType).add("y", DoubleType)))

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)
    assert(addsOf(changes).keySet === Set(Seq("points", "element", "y")))
  }

  test("diffSchemas - a field added to a struct inside a map value uses the value path") {
    val currentSchema = new StructType().add(
      "points",
      MapType(StringType, new StructType().add("x", DoubleType)))
    val targetSchema = new StructType()
      .add(
        "points",
        MapType(StringType, new StructType().add("x", DoubleType).add("y", DoubleType)))

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)
    assert(addsOf(changes).keySet === Set(Seq("points", "value", "y")))
  }

  test("diffSchemas - a field added to a struct inside a map key uses the key path") {
    val currentSchema = new StructType().add(
      "points",
      MapType(new StructType().add("x", DoubleType), LongType))
    val targetSchema = new StructType()
      .add(
        "points",
        MapType(new StructType().add("x", DoubleType).add("y", DoubleType), LongType))

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)
    assert(addsOf(changes).keySet === Set(Seq("points", "key", "y")))
  }

  test("diffSchemas - an element type change inside an array uses the element path") {
    val currentSchema = new StructType().add("vals", ArrayType(IntegerType))
    val targetSchema = new StructType().add("vals", ArrayType(LongType))

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)
    assert(typeUpdatesOf(changes) === Map(Seq("vals", "element") -> LongType))
  }

  test("diffSchemas - a map value type change uses the value path") {
    val currentSchema = new StructType()
      .add("m", MapType(StringType, IntegerType))
    val targetSchema = new StructType()
      .add("m", MapType(StringType, LongType))

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)
    assert(typeUpdatesOf(changes) === Map(Seq("m", "value") -> LongType))
  }

  test("diffSchemas - an array containsNull change is a nullability update on the element") {
    val currentSchema = new StructType()
      .add("vals", ArrayType(IntegerType, containsNull = false))
    val targetSchema = new StructType()
      .add("vals", ArrayType(IntegerType, containsNull = true))

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)
    assert(changes.length === 1, s"changes=$changes")
    assert(nullabilityUpdatesOf(changes) === Map(Seq("vals", "element") -> true))
  }

  test("diffSchemas - a map valueContainsNull change is a nullability update on the value") {
    val currentSchema = new StructType().add(
      "m", MapType(StringType, IntegerType, valueContainsNull = false))
    val targetSchema = new StructType().add(
      "m", MapType(StringType, IntegerType, valueContainsNull = true))

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)
    assert(changes.length === 1, s"changes=$changes")
    assert(nullabilityUpdatesOf(changes) === Map(Seq("m", "value") -> true))
  }

  test("diffSchemas - a wholly new struct column stays a single top-level add") {
    val newStruct = new StructType().add("x", DoubleType).add("y", DoubleType)
    val currentSchema = new StructType().add("id", IntegerType)
    val targetSchema = new StructType()
      .add("id", IntegerType)
      .add("point", newStruct)

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)
    assert(changes.length === 1, s"changes=$changes")
    assert(addsOf(changes) === Map(Seq("point") -> ((newStruct, true, null))))
  }

  test("diffSchemas - a whole struct column removed stays a single top-level delete") {
    val currentSchema = new StructType()
      .add("id", IntegerType)
      .add("point", new StructType().add("x", DoubleType))
    val targetSchema = new StructType().add("id", IntegerType)

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)
    assert(changes.length === 1, s"changes=$changes")
    assert(deletesOf(changes) === Set(Seq("point")))
  }

  test("diffSchemas - a struct replaced by an atomic type is a type update on the column") {
    val currentSchema = new StructType()
      .add("s", new StructType().add("a", IntegerType))
    val targetSchema = new StructType().add("s", StringType)

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)
    assert(changes.length === 1, s"changes=$changes")
    assert(typeUpdatesOf(changes) === Map(Seq("s") -> StringType))
  }

  test("diffSchemas - identical nested schemas produce no changes") {
    val schema = new StructType()
      .add("s", new StructType().add("a", IntegerType).add("b", StringType))
      .add("arr", ArrayType(new StructType().add("x", DoubleType)))
      .add("m", MapType(StringType, new StructType().add("y", DoubleType)))

    assert(SchemaInferenceUtils.diffSchemas(schema, schema).isEmpty)
  }

  test("diffSchemas - independent nested changes are all emitted at their own leaves") {
    val currentSchema = new StructType()
      .add(
        "s",
        new StructType()
          .add("a", IntegerType)
          .add("gone", StringType))
      .add("arr", ArrayType(new StructType().add("x", DoubleType)))
      .add("top", IntegerType)
    val targetSchema = new StructType()
      .add(
        "s",
        new StructType()
          .add("a", IntegerType)
          .add("added", StringType))
      .add("arr", ArrayType(new StructType().add("x", DoubleType).add("z", DoubleType)))
      .add("top", IntegerType)
      .add("brand_new", BooleanType)

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)
    assert(
      addsOf(changes).keySet === Set(
        Seq("s", "added"),
        Seq("arr", "element", "z"),
        Seq("brand_new")),
      s"changes=$changes")
    assert(
      deletesOf(changes) === Set(Seq("s", "gone")),
      s"changes=$changes")
    assert(typeUpdatesOf(changes).isEmpty, s"changes=$changes")
  }

  test("diffSchemas - DSv2 catalog respects emitted nested changes") {
    val currentNestedStruct =
      new StructType().add("x", DoubleType, nullable = false, "old comment")
    val currentSchema = new StructType()
      .add("struct", currentNestedStruct)
      .add("array", ArrayType(currentNestedStruct))
      .add("map", MapType(currentNestedStruct, currentNestedStruct))

    val targetNestedStruct =
      new StructType()
        .add("x", DoubleType, nullable = true, "new comment")
        .add("y", DoubleType)
    // Add `y`, make `x` nullable, and update its comment in every nested struct.
    val targetSchema = new StructType()
      .add("struct", targetNestedStruct)
      .add("array", ArrayType(targetNestedStruct))
      .add("map", MapType(targetNestedStruct, targetNestedStruct))

    val catalog = new InMemoryTableCatalog
    catalog.initialize("test", CaseInsensitiveStringMap.empty())
    val ident = Identifier.of(Array.empty, "t")
    catalog.createTable(ident, new TableInfo.Builder().withSchema(currentSchema).build())

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)
    val updated = catalog.alterTable(ident, changes: _*)

    assert(
      CatalogV2Util.clearIds(updated.columns()) ===
        CatalogV2Util.structTypeToV2Columns(targetSchema, keepIds = false))
  }

  test("inferSchemaFromFlows folds a case-only column to the same spelling regardless of flow " +
    "order, even when identifier names contain dots") {
    // The merge order decides which spelling of a case-only-differing column survives, so it must
    // not depend on the incoming flow order (the nondeterministic flow-resolution completion
    // order). The two identifiers below differ only in where the dot falls, so a dot-joined sort
    // key would render them identical; sorting on the identifier parts keeps them distinct.
    val destination = TableIdentifier("t", Some("d"), Some("c"))
    val flowA = resolvedFlow(
      identifier = TableIdentifier("x", Some("a.b"), Some("c")),
      destination = destination,
      schema = new StructType().add("id", IntegerType).add("value", StringType))
    val flowB = resolvedFlow(
      identifier = TableIdentifier("b.x", Some("a"), Some("c")),
      destination = destination,
      schema = new StructType().add("id", IntegerType).add("Value", StringType))

    // The lower identifier (flowB: database "a" precedes "a.b") supplies the surviving spelling, in
    // either input order.
    val expected = new StructType().add("id", IntegerType).add("Value", StringType)
    Seq(Seq(flowA, flowB), Seq(flowB, flowA)).foreach { flows =>
      val inferred = SchemaInferenceUtils.inferSchemaFromFlows(
        tableIdentifier = destination,
        flows = flows,
        userSpecifiedSchema = None,
        sessionCaseSensitive = false)
      assert(inferred === expected, s"unexpected schema for input order $flows")
    }
  }
}

private[util] object TableChangeExtractors {
  /** Added columns as path -> (type, nullable, comment). */
  def addsOf(changes: Seq[TableChange]): Map[Seq[String], (DataType, Boolean, String)] =
    changes.collect {
      case ac: TableChange.AddColumn =>
        ac.fieldNames().toSeq -> ((ac.dataType(), ac.isNullable(), ac.comment()))
    }.toMap

  def deletesOf(changes: Seq[TableChange]): Set[Seq[String]] =
    changes.collect {
      case dc: TableChange.DeleteColumn => dc.fieldNames().toSeq
    }.toSet

  def typeUpdatesOf(changes: Seq[TableChange]): Map[Seq[String], DataType] =
    changes.collect {
      case tc: TableChange.UpdateColumnType =>
        tc.fieldNames().toSeq -> tc.newDataType()
    }.toMap

  def nullabilityUpdatesOf(changes: Seq[TableChange]): Map[Seq[String], Boolean] =
    changes.collect {
      case nc: TableChange.UpdateColumnNullability =>
        nc.fieldNames().toSeq -> nc.nullable()
    }.toMap

  def commentUpdatesOf(changes: Seq[TableChange]): Map[Seq[String], String] =
    changes.collect {
      case cc: TableChange.UpdateColumnComment =>
        cc.fieldNames().toSeq -> cc.newComment()
    }.toMap
}
