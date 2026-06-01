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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.internal.SQLConf.CASE_SENSITIVE
import org.apache.spark.sql.types._

class SchemaPruningSuite extends SparkFunSuite with SQLHelper {
  private def testPrunedSchema(
      schema: StructType,
      requestedFields: Seq[StructField],
      expectedSchema: StructType): Unit = {
    val requestedRootFields = requestedFields.map { f =>
      // `derivedFromAtt` doesn't affect the result of pruned schema.
      SchemaPruning.RootField(field = f, derivedFromAtt = true)
    }
    val prunedSchema = SchemaPruning.pruneSchema(schema, requestedRootFields)
    assert(prunedSchema === expectedSchema)
  }

  test("prune schema by the requested fields") {
    testPrunedSchema(
      StructType.fromDDL("a int, b int"),
      Seq(StructField("a", IntegerType)),
      StructType.fromDDL("a int, b int"))

    val structOfStruct = StructType.fromDDL("a struct<a:int, b:int>, b int")
    testPrunedSchema(structOfStruct,
      Seq(StructField("a", StructType.fromDDL("a int")), StructField("b", IntegerType)),
      StructType.fromDDL("a struct<a:int>, b int"))
    testPrunedSchema(structOfStruct,
      Seq(StructField("a", StructType.fromDDL("a int"))),
      StructType.fromDDL("a struct<a:int>, b int"))

    val arrayOfStruct = StructField("a", ArrayType(StructType.fromDDL("a int, b int, c string")))
    val mapOfStruct = StructField("d", MapType(StructType.fromDDL("a int, b int, c string"),
      StructType.fromDDL("d int, e int, f string")))

    val complexStruct = StructType(
      arrayOfStruct :: StructField("b", structOfStruct) :: StructField("c", IntegerType) ::
        mapOfStruct :: Nil)

    testPrunedSchema(complexStruct,
      Seq(StructField("a", ArrayType(StructType.fromDDL("b int"))),
        StructField("b", StructType.fromDDL("a int"))),
      StructType(
        StructField("a", ArrayType(StructType.fromDDL("b int"))) ::
          StructField("b", StructType.fromDDL("a int")) ::
          StructField("c", IntegerType) ::
          mapOfStruct :: Nil))
    testPrunedSchema(complexStruct,
      Seq(StructField("a", ArrayType(StructType.fromDDL("b int, c string"))),
        StructField("b", StructType.fromDDL("b int"))),
      StructType(
        StructField("a", ArrayType(StructType.fromDDL("b int, c string"))) ::
          StructField("b", StructType.fromDDL("b int")) ::
          StructField("c", IntegerType) ::
          mapOfStruct :: Nil))

    val selectFieldInMap = StructField("d", MapType(StructType.fromDDL("a int, b int"),
      StructType.fromDDL("e int, f string")))
    testPrunedSchema(complexStruct,
      Seq(StructField("c", IntegerType), selectFieldInMap),
      StructType(
        arrayOfStruct ::
          StructField("b", structOfStruct) ::
          StructField("c", IntegerType) ::
          selectFieldInMap :: Nil))
  }

  test("SPARK-35096: test case insensitivity of pruned schema") {
    val upperCaseSchema = StructType.fromDDL("A struct<A:int, B:int>, B int")
    val lowerCaseSchema = StructType.fromDDL("a struct<a:int, b:int>, b int")
    val upperCaseRequestedFields = Seq(StructField("A", StructType.fromDDL("A int")))
    val lowerCaseRequestedFields = Seq(StructField("a", StructType.fromDDL("a int")))

    Seq(true, false).foreach { isCaseSensitive =>
      withSQLConf(CASE_SENSITIVE.key -> isCaseSensitive.toString) {
        if (isCaseSensitive) {
          testPrunedSchema(
            upperCaseSchema,
            upperCaseRequestedFields,
            StructType.fromDDL("A struct<A:int>, B int"))
          testPrunedSchema(
            upperCaseSchema,
            lowerCaseRequestedFields,
            upperCaseSchema)

          testPrunedSchema(
            lowerCaseSchema,
            upperCaseRequestedFields,
            lowerCaseSchema)
          testPrunedSchema(
            lowerCaseSchema,
            lowerCaseRequestedFields,
            StructType.fromDDL("a struct<a:int>, b int"))
        } else {
          Seq(upperCaseRequestedFields, lowerCaseRequestedFields).foreach { requestedFields =>
            testPrunedSchema(
              upperCaseSchema,
              requestedFields,
              StructType.fromDDL("A struct<A:int>, B int"))
          }

          Seq(upperCaseRequestedFields, lowerCaseRequestedFields).foreach { requestedFields =>
            testPrunedSchema(
              lowerCaseSchema,
              requestedFields,
              StructType.fromDDL("a struct<a:int>, b int"))
          }
        }
      }
    }
  }

  test("SPARK-35232: getRootFields/pruneDataSchema should retain attribute metadata") {
    val metadata = new MetadataBuilder().putString("foo", "bar").build()
    val attr = AttributeReference("my_attr", IntegerType, metadata = metadata)()

    val rootFields = SchemaPruning.getRootFields(attr)
    assert(rootFields.length == 1)
    val field = rootFields.head.field
    assert(field.metadata.getString("foo") == "bar")

    val schema = StructType(Seq(field))
    val prunedSchema = SchemaPruning.pruneSchema(schema, rootFields)
    assert(prunedSchema.head.metadata.getString("foo") == "bar")
  }

  test("collect nested fields used by ArrayTransform lambda") {
    val elementType = StructType.fromDDL("a int, b int, c int")
    val eventType = StructType(Seq(
      StructField("rules", ArrayType(elementType, containsNull = true))))
    val event = AttributeReference("event", eventType)()
    val element = NamedLambdaVariable("x", elementType, nullable = true)
    val transformed = ArrayTransform(
      GetStructField(event, 0, Some("rules")),
      LambdaFunction(
        CreateNamedStruct(Seq(
          Literal("a"),
          GetStructField(element, 0, Some("a")),
          Literal("c"),
          GetStructField(element, 2, Some("c")))),
        Seq(element)))

    val rootFields = SchemaPruning.getRootFields(transformed)
    val prunedSchema = SchemaPruning.pruneSchema(
      StructType(Seq(StructField("event", eventType))),
      rootFields)

    assert(prunedSchema === StructType.fromDDL(
      "event struct<rules:array<struct<a:int,c:int>>>"))
  }

  test("do not collect ArrayTransform lambda fields when the whole element is used") {
    val elementType = StructType.fromDDL("a int, b int")
    val eventType = StructType(Seq(
      StructField("rules", ArrayType(elementType, containsNull = true))))
    val event = AttributeReference("event", eventType)()
    val element = NamedLambdaVariable("x", elementType, nullable = true)
    val transformed = ArrayTransform(
      GetStructField(event, 0, Some("rules")),
      LambdaFunction(element, Seq(element)))

    val rootFields = SchemaPruning.getRootFields(transformed)

    assert(rootFields === Seq(
      SchemaPruning.RootField(
        StructField("event", eventType, nullable = true),
        derivedFromAtt = false)))
  }

  test("collect nested fields used by ArrayExists and ArrayForAll lambdas") {
    val elementType = StructType.fromDDL("a int, b int, c int")
    val eventType = StructType(Seq(
      StructField("rules", ArrayType(elementType, containsNull = true))))
    val event = AttributeReference("event", eventType)()
    val argument = GetStructField(event, 0, Some("rules"))
    val element = NamedLambdaVariable("x", elementType, nullable = true)
    val predicate = LambdaFunction(
      GreaterThan(GetStructField(element, 2, Some("c")), Literal(0)),
      Seq(element))

    Seq(ArrayExists(argument, predicate), ArrayForAll(argument, predicate)).foreach { function =>
      val rootFields = SchemaPruning.getRootFields(function)
      val prunedSchema = SchemaPruning.pruneSchema(
        StructType(Seq(StructField("event", eventType))),
        rootFields)

      assert(prunedSchema === StructType.fromDDL(
        "event struct<rules:array<struct<c:int>>>"))
    }
  }

  test("merge returned and lambda fields for array higher-order functions") {
    val elementType = StructType.fromDDL("a int, b int, c int")
    val eventType = StructType(Seq(
      StructField("rules", ArrayType(elementType, containsNull = true))))
    val event = AttributeReference("event", eventType)()
    val argument = GetStructField(event, 0, Some("rules"))
    val element = NamedLambdaVariable("x", elementType, nullable = true)
    val predicate = LambdaFunction(
      GreaterThan(GetStructField(element, 2, Some("c")), Literal(0)),
      Seq(element))
    val left = NamedLambdaVariable("left", elementType, nullable = true)
    val right = NamedLambdaVariable("right", elementType, nullable = true)
    val comparator = LambdaFunction(
      Subtract(
        GetStructField(left, 2, Some("c")),
        GetStructField(right, 2, Some("c"))),
      Seq(left, right))

    Seq(ArrayFilter(argument, predicate), ArraySort(argument, comparator)).foreach { function =>
      val selected = GetArrayStructFields(
        function,
        elementType(0),
        ordinal = 0,
        numFields = elementType.length,
        containsNull = true)
      val rootFields = SchemaPruning.getRootFields(selected)
      val prunedSchema = SchemaPruning.pruneSchema(
        StructType(Seq(StructField("event", eventType))),
        rootFields)

      assert(prunedSchema === StructType.fromDDL(
        "event struct<rules:array<struct<a:int,c:int>>>"))
    }
  }

  test("do not collect ArrayExists and ArrayForAll lambda fields when the whole element is used") {
    val elementType = StructType.fromDDL("a int, b int")
    val eventType = StructType(Seq(
      StructField("rules", ArrayType(elementType, containsNull = true))))
    val event = AttributeReference("event", eventType)()
    val argument = GetStructField(event, 0, Some("rules"))
    val element = NamedLambdaVariable("x", elementType, nullable = true)
    val predicate = LambdaFunction(IsNotNull(element), Seq(element))

    Seq(ArrayExists(argument, predicate), ArrayForAll(argument, predicate)).foreach { function =>
      assert(SchemaPruning.getRootFields(function) === Seq(
        SchemaPruning.RootField(
          StructField("event", eventType, nullable = true),
          derivedFromAtt = false)))
    }
  }

  test("do not prune ArrayFilter when the whole result is used") {
    val elementType = StructType.fromDDL("a int, b int")
    val eventType = StructType(Seq(
      StructField("rules", ArrayType(elementType, containsNull = true))))
    val event = AttributeReference("event", eventType)()
    val element = NamedLambdaVariable("x", elementType, nullable = true)
    val filtered = ArrayFilter(
      GetStructField(event, 0, Some("rules")),
      LambdaFunction(
        GreaterThan(GetStructField(element, 0, Some("a")), Literal(0)),
        Seq(element)))

    val rootFields = SchemaPruning.getRootFields(filtered)

    assert(rootFields.contains(
      SchemaPruning.RootField(
        StructField("event", eventType, nullable = true),
        derivedFromAtt = false)))
  }

  test("retain input array nullability when pruning through KnownNotContainsNull") {
    val elementType = StructType.fromDDL("a int, b int")
    val eventType = StructType(Seq(
      StructField("rules", ArrayType(elementType, containsNull = true))))
    val event = AttributeReference("event", eventType)()
    val element = NamedLambdaVariable("x", elementType, nullable = true)
    val compacted = KnownNotContainsNull(ArrayFilter(
      GetStructField(event, 0, Some("rules")),
      LambdaFunction(IsNotNull(element), Seq(element))))
    val selected = GetArrayStructFields(
      compacted,
      elementType(0),
      ordinal = 0,
      numFields = elementType.length,
      containsNull = false)

    val rootFields = SchemaPruning.getRootFields(selected)
    val prunedSchema = SchemaPruning.pruneSchema(
      StructType(Seq(StructField("event", eventType))),
      rootFields)
    val prunedEventType = prunedSchema("event").dataType.asInstanceOf[StructType]

    assert(prunedEventType("rules").dataType ===
      ArrayType(StructType.fromDDL("a int"), containsNull = true))
  }
}
