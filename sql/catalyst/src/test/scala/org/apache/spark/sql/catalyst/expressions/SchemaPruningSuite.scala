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

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData, MapData, ResolveDefaultColumns}
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

  for ((dataType, prunedType, defaultSQL, expectedSQL) <- Seq(
      ("STRUCT<a:INT,b:INT>", "STRUCT<b:INT>",
        "named_struct('a',11,'b',22)", "named_struct('b',22)"),
      ("ARRAY<STRUCT<a:INT,b:INT>>", "ARRAY<STRUCT<b:INT>>",
        "array(named_struct('a',11,'b',22),CAST(NULL AS STRUCT<a:INT,b:INT>)," +
          "named_struct('a',0,'b',CAST(NULL AS INT)))",
        "array(named_struct('b',22),CAST(NULL AS STRUCT<b:INT>)," +
          "named_struct('b',CAST(NULL AS INT)))"),
      ("MAP<STRING,STRUCT<a:INT,b:INT>>", "MAP<STRING,STRUCT<b:INT>>",
        "map('key',named_struct('a',11,'b',22),'nil',CAST(NULL AS STRUCT<a:INT,b:INT>))",
        "map('key',named_struct('b',22),'nil',CAST(NULL AS STRUCT<b:INT>))"),
      ("STRUCT<a:INT,b:INT>", "STRUCT<b:INT>",
        "CAST(NULL AS STRUCT<a:INT,b:INT>)", "CAST(NULL AS STRUCT<b:INT>)"),
      ("STRUCT<a:INT,b:ARRAY<STRUCT<x:INT,y:STRING>>>",
        "STRUCT<b:ARRAY<STRUCT<y:STRING>>>",
        "named_struct('a',11,'b',array(named_struct('x',1,'y','kept')))",
        "named_struct('b',array(named_struct('y','kept')))"),
      ("MAP<STRUCT<a:INT,b:INT>,STRUCT<c:INT,d:INT>>",
        "MAP<STRUCT<b:INT>,STRUCT<d:INT>>",
        "map(named_struct('a',1,'b',2),named_struct('c',3,'d',4))",
        "map(named_struct('b',2),named_struct('d',4))"),
      ("ARRAY<STRUCT<a:INT,b:INT>>", "ARRAY<STRUCT<b:INT>>", "array()", "array()"),
      ("MAP<STRING,STRUCT<a:INT,b:INT>>", "MAP<STRING,STRUCT<b:INT>>", "map()", "map()"))) {
    test(s"SPARK-60790: prune default $defaultSQL to $prunedType") {
      val original = StructType.fromDDL(s"v $dataType").head
        .withCurrentDefaultValue(defaultSQL)
        .withExistenceDefaultValue(defaultSQL)
      val requested = StructType.fromDDL(s"v $prunedType").head.copy(metadata = original.metadata)
      val schema = SchemaPruning.pruneSchema(StructType(Seq(original)), Seq(
        SchemaPruning.RootField(requested, derivedFromAtt = false)))
      val field = schema.head
      val expected = requested.withExistenceDefaultValue(expectedSQL)
      val actualValue = ResolveDefaultColumns.getExistenceDefaultValue(field)
      val expectedValue = ResolveDefaultColumns.getExistenceDefaultValue(expected)
      assert(Literal.create(actualValue, field.dataType).sql ==
        Literal.create(expectedValue, field.dataType).sql)
      assert(field.getCurrentDefaultValue() == original.getCurrentDefaultValue())
      assert(original.getExistenceDefaultValue().contains(defaultSQL))
      assert(SchemaPruning.pruneSchema(schema, Seq(
        SchemaPruning.RootField(requested, derivedFromAtt = false))) === schema)
    }
  }

  test("SPARK-60790: preserve distinct map keys after default projection") {
    val field = StructType.fromDDL("v MAP<STRUCT<a:INT,b:INT>,STRUCT<c:INT,d:INT>>").head
      .withExistenceDefaultValue(
        "map(named_struct('a',1,'b',2),named_struct('c',3,'d',4)," +
          "named_struct('a',5,'b',2),named_struct('c',7,'d',6))")
    val requested = StructType.fromDDL("v MAP<STRUCT<b:INT>,STRUCT<d:INT>>").head
      .copy(metadata = field.metadata)
    val pruned = SchemaPruning.pruneSchema(StructType(Seq(field)), Seq(
      SchemaPruning.RootField(requested, derivedFromAtt = false))).head
    val value = ResolveDefaultColumns.getExistenceDefaultValue(pruned).asInstanceOf[MapData]
    assert(value.numElements() == 2)
    assert((0 until 2).map(i => value.keyArray().getStruct(i, 1).getInt(0)) == Seq(2, 2))
    assert((0 until 2).map(i => value.valueArray().getStruct(i, 1).getInt(0)) == Seq(4, 6))
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

  test("SPARK-60979: prefer the exact name when field names differ only by case") {
    withSQLConf(CASE_SENSITIVE.key -> "false") {
      // top-level fields
      testPrunedSchema(
        StructType.fromDDL("pk INT NOT NULL, salary INT NOT NULL, SALARY STRING"),
        Seq(StructField("SALARY", StringType)),
        StructType.fromDDL("pk INT NOT NULL, salary INT NOT NULL, SALARY STRING"))

      // nested fields
      testPrunedSchema(
        StructType.fromDDL("s struct<a: INT NOT NULL, A: STRING, b: INT>"),
        Seq(StructField("s", StructType.fromDDL("A STRING"))),
        StructType.fromDDL("s struct<A: STRING>"))
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
      Coalesce(Seq(
        Subtract(
          GetStructField(left, 2, Some("c")),
          GetStructField(right, 2, Some("c"))),
        Literal(0))),
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

  test("match separately instantiated array lambda variables by exprId") {
    val elementType = StructType.fromDDL("a int, b int, c int")
    val sourceSchema = StructType(Seq(
      StructField("rules", ArrayType(elementType, containsNull = true))))
    val rules = AttributeReference("rules", ArrayType(elementType, containsNull = true))()

    def selectFirstField(function: Expression): GetArrayStructFields = {
      GetArrayStructFields(
        function,
        elementType(0),
        ordinal = 0,
        numFields = elementType.length,
        containsNull = true)
    }

    def evaluateSelectedFields(selected: GetArrayStructFields): Seq[Int] = {
      val rootFields = SchemaPruning.identifyRootFields(Seq(Alias(selected, "out")()), Seq.empty)
      val prunedSchema = SchemaPruning.pruneSchema(sourceSchema, rootFields)
      assert(prunedSchema === StructType.fromDDL("rules array<struct<a:int,c:int>>"))

      val projected = ProjectionOverSchema(prunedSchema, AttributeSet(Seq(rules)))
        .unapply(selected).get
      val bound = BindReferences.bindReference(projected, Seq(rules))
      val array = new GenericArrayData(Array[Any](InternalRow(1, 3), InternalRow(2, -1)))
      val result = bound.eval(InternalRow(array)).asInstanceOf[ArrayData]
      (0 until result.numElements()).map(result.getInt)
    }

    val filterArgument = NamedLambdaVariable("x", elementType, nullable = true)
    val filterReference = filterArgument.copy(value = new AtomicReference[Any]())
    val filter = ArrayFilter(
      rules,
      LambdaFunction(
        GreaterThan(GetStructField(filterReference, 2, Some("c")), Literal(0)),
        Seq(filterArgument)))
    assert(evaluateSelectedFields(selectFirstField(filter)) === Seq(1))

    val leftArgument = NamedLambdaVariable("left", elementType, nullable = true)
    val rightArgument = NamedLambdaVariable("right", elementType, nullable = true)
    val leftReference = leftArgument.copy(value = new AtomicReference[Any]())
    val rightReference = rightArgument.copy(value = new AtomicReference[Any]())
    val sort = ArraySort(
      rules,
      LambdaFunction(
        Coalesce(Seq(
          Subtract(
            GetStructField(leftReference, 2, Some("c")),
            GetStructField(rightReference, 2, Some("c"))),
          Literal(0))),
        Seq(leftArgument, rightArgument)),
      allowNullComparisonResult = false)
    assert(evaluateSelectedFields(selectFirstField(sort)) === Seq(2, 1))
  }

  test("retain full nested array elements for array-returning higher-order functions") {
    val structType = StructType.fromDDL("a int, c int")
    val arrayType = ArrayType(structType, containsNull = true)
    val nestedArrayType = ArrayType(arrayType, containsNull = true)
    val sourceSchema = StructType(Seq(StructField("rules", nestedArrayType)))
    val rules = AttributeReference("rules", nestedArrayType)()

    def selectFirstField(function: Expression): GetArrayStructFields = {
      GetArrayStructFields(
        GetArrayItem(function, Literal(0)),
        structType(0),
        ordinal = 0,
        numFields = structType.length,
        containsNull = true)
    }

    val element = NamedLambdaVariable("x", arrayType, nullable = true)
    val elementC = GetArrayStructFields(
      element,
      structType(1),
      ordinal = 1,
      numFields = structType.length,
      containsNull = true)
    val filter = ArrayFilter(
      rules,
      LambdaFunction(GreaterThan(GetArrayItem(elementC, Literal(0)), Literal(0)), Seq(element)))

    val left = NamedLambdaVariable("left", arrayType, nullable = true)
    val right = NamedLambdaVariable("right", arrayType, nullable = true)
    def firstC(variable: NamedLambdaVariable): Expression = {
      GetArrayItem(
        GetArrayStructFields(
          variable,
          structType(1),
          ordinal = 1,
          numFields = structType.length,
          containsNull = true),
        Literal(0))
    }
    val sort = ArraySort(
      rules,
      LambdaFunction(
        Coalesce(Seq(Subtract(firstC(left), firstC(right)), Literal(0))),
        Seq(left, right)),
      allowNullComparisonResult = false)

    Seq(filter, sort).foreach { function =>
      val selected = Alias(selectFirstField(function), "out")()
      val rootFields = SchemaPruning.identifyRootFields(Seq(selected), Seq.empty)
      assert(SchemaPruning.pruneSchema(sourceSchema, rootFields) === sourceSchema)
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

  test("do not prune strict ArraySort when the comparator can return null") {
    val elementType = StructType.fromDDL("a int, b int")
    val eventType = StructType(Seq(
      StructField("rules", ArrayType(elementType, containsNull = true))))
    val event = AttributeReference("event", eventType)()
    val argument = GetStructField(event, 0, Some("rules"))
    val left = NamedLambdaVariable("left", elementType, nullable = true)
    val right = NamedLambdaVariable("right", elementType, nullable = true)
    val comparator = LambdaFunction(Literal.create(null, IntegerType), Seq(left, right))
    val sorted = ArraySort(argument, comparator, allowNullComparisonResult = false)
    val selected = GetArrayStructFields(
      sorted,
      elementType(0),
      ordinal = 0,
      numFields = elementType.length,
      containsNull = true)

    val rootFields = SchemaPruning.getRootFields(selected)
    val prunedSchema = SchemaPruning.pruneSchema(
      StructType(Seq(StructField("event", eventType))),
      rootFields)

    assert(prunedSchema === StructType(Seq(StructField("event", eventType))))
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
