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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Range}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._

/**
* SPARK-18601 discusses simplification direct access to complex types creators.
* i.e. {{{create_named_struct(square, `x` * `x`).square}}} can be simplified to {{{`x` * `x`}}}.
* sam applies to create_array and create_map
*/
class ComplexTypesSuite extends PlanTest with ExpressionEvalHelper {

  object Optimizer extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("collapse projections", FixedPoint(10),
        CollapseProject) ::
      Batch("Constant Folding", FixedPoint(10),
         NullPropagation,
         ConstantFolding,
         BooleanSimplification,
         SimplifyConditionals,
         SimplifyBinaryComparison,
         SimplifyExtractValueOps,
         CombineUpdateFields) :: Nil
  }

  private val idAtt = ('id).long.notNull
  private val nullableIdAtt = ('nullable_id).long

  private val relation = LocalRelation(idAtt, nullableIdAtt)
  private val testRelation = LocalRelation('a.int, 'b.int, 'c.int, 'd.double, 'e.int)

  private def checkRule(originalQuery: LogicalPlan, correctAnswer: LogicalPlan) = {
    val optimized = Optimizer.execute(originalQuery.analyze)
    assert(optimized.resolved, "optimized plans must be still resolvable")
    comparePlans(optimized, correctAnswer.analyze)
  }

  test("explicit get from namedStruct") {
    val query = relation
      .select(
        GetStructField(
          CreateNamedStruct(Seq("att", 'id )),
          0,
          None) as "outerAtt")
    val expected = relation.select('id as "outerAtt")

    checkRule(query, expected)
  }

  test("explicit get from named_struct- expression maintains original deduced alias") {
    val query = relation
      .select(GetStructField(CreateNamedStruct(Seq("att", 'id)), 0, None))

    val expected = relation
      .select('id as "named_struct(att, id).att")

    checkRule(query, expected)
  }

  test("collapsed getStructField ontop of namedStruct") {
    val query = relation
      .select(CreateNamedStruct(Seq("att", 'id)) as "struct1")
      .select(GetStructField('struct1, 0, None) as "struct1Att")
    val expected = relation.select('id as "struct1Att")
    checkRule(query, expected)
  }

  test("collapse multiple CreateNamedStruct/GetStructField pairs") {
    val query = relation
      .select(
        CreateNamedStruct(Seq(
          "att1", 'id,
          "att2", 'id * 'id)) as "struct1")
      .select(
        GetStructField('struct1, 0, None) as "struct1Att1",
        GetStructField('struct1, 1, None) as "struct1Att2")

    val expected =
      relation.
        select(
          'id as "struct1Att1",
          ('id * 'id) as "struct1Att2")

    checkRule(query, expected)
  }

  test("collapsed2 - deduced names") {
    val query = relation
      .select(
        CreateNamedStruct(Seq(
          "att1", 'id,
          "att2", 'id * 'id)) as "struct1")
      .select(
        GetStructField('struct1, 0, None),
        GetStructField('struct1, 1, None))

    val expected =
      relation.
        select(
          'id as "struct1.att1",
          ('id * 'id) as "struct1.att2")

    checkRule(query, expected)
  }

  test("simplified array ops") {
    val rel = relation.select(
      CreateArray(Seq(
        CreateNamedStruct(Seq(
          "att1", 'id,
          "att2", 'id * 'id)),
        CreateNamedStruct(Seq(
          "att1", 'id + 1,
          "att2", ('id + 1) * ('id + 1))
       ))
      ) as "arr"
    )
    val query = rel
      .select(
        GetArrayStructFields('arr, StructField("att1", LongType, false), 0, 1, false) as "a1",
        GetArrayItem('arr, 1) as "a2",
        GetStructField(GetArrayItem('arr, 1), 0, None) as "a3",
        GetArrayItem(
          GetArrayStructFields('arr,
            StructField("att1", LongType, false),
            0,
            1,
            false),
          1) as "a4")

    val expected = relation
      .select(
        CreateArray(Seq('id, 'id + 1L)) as "a1",
        CreateNamedStruct(Seq(
          "att1", ('id + 1L),
          "att2", (('id + 1L) * ('id + 1L)))) as "a2",
        ('id + 1L) as "a3",
        ('id + 1L) as "a4")
    checkRule(query, expected)
  }

  test("SPARK-22570: CreateArray should not create a lot of global variables") {
    val ctx = new CodegenContext
    CreateArray(Seq(Literal(1))).genCode(ctx)
    assert(ctx.inlinedMutableStates.length == 0)
  }

  test("SPARK-23208: Test code splitting for create array related methods") {
    val inputs = (1 to 2500).map(x => Literal(s"l_$x"))
    checkEvaluation(CreateArray(inputs), new GenericArrayData(inputs.map(_.eval())))
  }

  test("simplify map ops") {
    val rel = relation
      .select(
        CreateMap(Seq(
          "r1", CreateNamedStruct(Seq("att1", 'id)),
          "r2", CreateNamedStruct(Seq("att1", ('id + 1L))))) as "m")
    val query = rel
      .select(
        GetMapValue('m, "r1") as "a1",
        GetStructField(GetMapValue('m, "r1"), 0, None) as "a2",
        GetMapValue('m, "r32") as "a3",
        GetStructField(GetMapValue('m, "r32"), 0, None) as "a4")

    val expected =
      relation.select(
        CreateNamedStruct(Seq("att1", 'id)) as "a1",
        'id as "a2",
        Literal.create(
          null,
          StructType(
            StructField("att1", LongType, nullable = false) :: Nil
          )
        ) as "a3",
        Literal.create(null, LongType) as "a4")
    checkRule(query, expected)
  }

  test("simplify map ops, constant lookup, dynamic keys") {
    val query = relation.select(
      GetMapValue(
        CreateMap(Seq(
          'id, ('id + 1L),
          ('id + 1L), ('id + 2L),
          ('id + 2L), ('id + 3L),
          Literal(13L), 'id,
          ('id + 3L), ('id + 4L),
          ('id + 4L), ('id + 5L))),
        13L) as "a")

    val expected = relation
      .select(
        CaseWhen(Seq(
          (EqualTo(13L, 'id), ('id + 1L)),
          (EqualTo(13L, ('id + 1L)), ('id + 2L)),
          (EqualTo(13L, ('id + 2L)), ('id + 3L)),
          (Literal(true), 'id))) as "a")
    checkRule(query, expected)
  }

  test("simplify map ops, dynamic lookup, dynamic keys, lookup is equivalent to one of the keys") {
    val query = relation
      .select(
        GetMapValue(
          CreateMap(Seq(
            'id, ('id + 1L),
            ('id + 1L), ('id + 2L),
            ('id + 2L), ('id + 3L),
            ('id + 3L), ('id + 4L),
            ('id + 4L), ('id + 5L))),
            ('id + 3L)) as "a")
    val expected = relation
      .select(
        CaseWhen(Seq(
          (EqualTo('id + 3L, 'id), ('id + 1L)),
          (EqualTo('id + 3L, ('id + 1L)), ('id + 2L)),
          (EqualTo('id + 3L, ('id + 2L)), ('id + 3L)),
          (Literal(true), ('id + 4L)))) as "a")
    checkRule(query, expected)
  }

  test("simplify map ops, no positive match") {
    val rel = relation
      .select(
        GetMapValue(
          CreateMap(Seq(
            'id, ('id + 1L),
            ('id + 1L), ('id + 2L),
            ('id + 2L), ('id + 3L),
            ('id + 3L), ('id + 4L),
            ('id + 4L), ('id + 5L))),
          'id + 30L) as "a")
    val expected = relation.select(
      CaseWhen(Seq(
        (EqualTo('id + 30L, 'id), ('id + 1L)),
        (EqualTo('id + 30L, ('id + 1L)), ('id + 2L)),
        (EqualTo('id + 30L, ('id + 2L)), ('id + 3L)),
        (EqualTo('id + 30L, ('id + 3L)), ('id + 4L)),
        (EqualTo('id + 30L, ('id + 4L)), ('id + 5L)))) as "a")
    checkRule(rel, expected)
  }

  test("simplify map ops, constant lookup, mixed keys, eliminated constants") {
    val rel = relation
      .select(
        GetMapValue(
          CreateMap(Seq(
            'id, ('id + 1L),
            ('id + 1L), ('id + 2L),
            ('id + 2L), ('id + 3L),
            Literal(14L), 'id,
            ('id + 3L), ('id + 4L),
            ('id + 4L), ('id + 5L))),
          13L) as "a")

    val expected = relation
      .select(
        CaseKeyWhen(13L,
          Seq('id, ('id + 1L),
            ('id + 1L), ('id + 2L),
            ('id + 2L), ('id + 3L),
            ('id + 3L), ('id + 4L),
            ('id + 4L), ('id + 5L))) as "a")

    checkRule(rel, expected)
  }

  test("simplify map ops, potential dynamic match with null value + an absolute constant match") {
    val rel = relation
      .select(
        GetMapValue(
          CreateMap(Seq(
            'id, ('id + 1L),
            ('id + 1L), ('id + 2L),
            ('id + 2L), Literal.create(null, LongType),
            Literal(2L), 'id,
            ('id + 3L), ('id + 4L),
            ('id + 4L), ('id + 5L))),
          2L ) as "a")

    val expected = relation
      .select(
        CaseWhen(Seq(
          (EqualTo(2L, 'id), ('id + 1L)),
          // these two are possible matches, we can't tell until runtime
          (EqualTo(2L, ('id + 1L)), ('id + 2L)),
          (EqualTo(2L, 'id + 2L), Literal.create(null, LongType)),
          // this is a definite match (two constants),
          // but it cannot override a potential match with ('id + 2L),
          // which is exactly what [[Coalesce]] would do in this case.
          (Literal.TrueLiteral, 'id))) as "a")
    checkRule(rel, expected)
  }

  test("SPARK-23500: Simplify array ops that are not at the top node") {
    val query = LocalRelation('id.long)
      .select(
        CreateArray(Seq(
          CreateNamedStruct(Seq(
            "att1", 'id,
            "att2", 'id * 'id)),
          CreateNamedStruct(Seq(
            "att1", 'id + 1,
            "att2", ('id + 1) * ('id + 1))
          ))
        ) as "arr")
      .select(
        GetStructField(GetArrayItem('arr, 1), 0, None) as "a1",
        GetArrayItem(
          GetArrayStructFields('arr,
            StructField("att1", LongType, nullable = false),
            ordinal = 0,
            numFields = 1,
            containsNull = false),
          ordinal = 1) as "a2")
      .orderBy('id.asc)

    val expected = LocalRelation('id.long)
      .select(
        ('id + 1L) as "a1",
        ('id + 1L) as "a2")
      .orderBy('id.asc)
    checkRule(query, expected)
  }

  test("SPARK-23500: Simplify map ops that are not top nodes") {
    val query =
      LocalRelation('id.long)
        .select(
          CreateMap(Seq(
            "r1", 'id,
            "r2", 'id + 1L)) as "m")
        .select(
          GetMapValue('m, "r1") as "a1",
          GetMapValue('m, "r32") as "a2")
        .orderBy('id.asc)
        .select('a1, 'a2)

    val expected =
      LocalRelation('id.long).select(
        'id as "a1",
        Literal.create(null, LongType) as "a2")
        .orderBy('id.asc)
    checkRule(query, expected)
  }

  test("SPARK-23500: Simplify complex ops that aren't at the plan root") {
    val structRel = relation
      .select(GetStructField(CreateNamedStruct(Seq("att1", 'nullable_id)), 0, None) as "foo")
      .groupBy($"foo")("1")
    val structExpected = relation
      .select('nullable_id as "foo")
      .groupBy($"foo")("1")
    checkRule(structRel, structExpected)

    val arrayRel = relation
      .select(GetArrayItem(CreateArray(Seq('nullable_id, 'nullable_id + 1L)), 0) as "a1")
      .groupBy($"a1")("1")
    val arrayExpected = relation.select('nullable_id as "a1").groupBy($"a1")("1")
    checkRule(arrayRel, arrayExpected)

    val mapRel = relation
      .select(GetMapValue(CreateMap(Seq("id", 'nullable_id)), "id") as "m1")
      .groupBy($"m1")("1")
    val mapExpected = relation
      .select('nullable_id as "m1")
      .groupBy($"m1")("1")
    checkRule(mapRel, mapExpected)
  }

  test("SPARK-23500: Ensure that aggregation expressions are not simplified") {
    // Make sure that aggregation exprs are correctly ignored. Maps can't be used in
    // grouping exprs so aren't tested here.
    val structAggRel = relation.groupBy(
      CreateNamedStruct(Seq("att1", 'nullable_id)))(
      GetStructField(CreateNamedStruct(Seq("att1", 'nullable_id)), 0, None))
    checkRule(structAggRel, structAggRel)

    val arrayAggRel = relation.groupBy(
      CreateArray(Seq('nullable_id)))(GetArrayItem(CreateArray(Seq('nullable_id)), 0))
    checkRule(arrayAggRel, arrayAggRel)

    // This could be done if we had a more complex rule that checks that
    // the CreateMap does not come from key.
    val originalQuery = relation
      .groupBy('id)(
        GetMapValue(CreateMap(Seq('id, 'id + 1L)), 0L) as "a"
      )
    checkRule(originalQuery, originalQuery)
  }

  test("SPARK-23500: namedStruct and getField in the same Project #1") {
    val originalQuery =
      testRelation
        .select(
          namedStruct("col1", 'b, "col2", 'c).as("s1"), 'a, 'b)
        .select('s1 getField "col2" as 's1Col2,
          namedStruct("col1", 'a, "col2", 'b).as("s2"))
        .select('s1Col2, 's2 getField "col2" as 's2Col2)
    val correctAnswer =
      testRelation
        .select('c as 's1Col2, 'b as 's2Col2)
    checkRule(originalQuery, correctAnswer)
  }

  test("SPARK-23500: namedStruct and getField in the same Project #2") {
    val originalQuery =
      testRelation
        .select(
          namedStruct("col1", 'b, "col2", 'c) getField "col2" as 'sCol2,
          namedStruct("col1", 'a, "col2", 'c) getField "col1" as 'sCol1)
    val correctAnswer =
      testRelation
        .select('c as 'sCol2, 'a as 'sCol1)
    checkRule(originalQuery, correctAnswer)
  }

  test("SPARK-24313: support binary type as map keys in GetMapValue") {
    val mb0 = Literal.create(
      Map(Array[Byte](1, 2) -> "1", Array[Byte](3, 4) -> null, Array[Byte](2, 1) -> "2"),
      MapType(BinaryType, StringType))
    val mb1 = Literal.create(Map[Array[Byte], String](), MapType(BinaryType, StringType))

    checkEvaluation(GetMapValue(mb0, Literal(Array[Byte](1, 2, 3))), null)

    checkEvaluation(GetMapValue(mb1, Literal(Array[Byte](1, 2))), null)
    checkEvaluation(GetMapValue(mb0, Literal(Array[Byte](2, 1), BinaryType)), "2")
    checkEvaluation(GetMapValue(mb0, Literal(Array[Byte](3, 4))), null)
  }

  private val structAttr = 'struct1.struct('a.int, 'b.int).withNullability(false)
  private val testStructRelation = LocalRelation(structAttr)

  private val nullableStructAttr = 'struct1.struct('a.int, 'b.int)
  private val testNullableStructRelation = LocalRelation(nullableStructAttr)

  test("simplify GetStructField on UpdateFields that is not changing the attribute being " +
    "extracted") {
    def query(relation: LocalRelation): LogicalPlan = relation.select(GetStructField(
      UpdateFields('struct1, Seq(WithField("b", Literal(1)))), 0, Some("a")) as "outerAttr")

    checkRule(
      query(testStructRelation),
      testStructRelation.select(GetStructField('struct1, 0) as "outerAttr"))

    checkRule(
      query(testNullableStructRelation),
      testNullableStructRelation.select(GetStructField('struct1, 0) as "outerAttr"))
  }

  test("simplify GetStructField on UpdateFields that is changing the attribute being extracted") {
    def query(relation: LocalRelation): LogicalPlan = relation.select(GetStructField(
      UpdateFields('struct1, Seq(WithField("b", Literal(1)))), 1, Some("b")) as "res")

    checkRule(
      query(testStructRelation),
      testStructRelation.select(Literal(1) as "res"))

    checkRule(
      query(testNullableStructRelation),
      testNullableStructRelation.select(
        If(IsNull('struct1), Literal(null, IntegerType), Literal(1)) as "res"))
  }

  test("simplify GetStructField on UpdateFields that is changing the attribute being extracted " +
    "twice") {
    def query(relation: LocalRelation): LogicalPlan = relation.select(GetStructField(
      UpdateFields('struct1, Seq(WithField("b", Literal(1)), WithField("b", Literal(2)))),
      1, Some("b")) as "outerAtt")

    checkRule(
      query(testStructRelation),
      testStructRelation.select(Literal(2) as "outerAtt"))

    checkRule(
      query(testNullableStructRelation),
      testNullableStructRelation.select(
        If(IsNull('struct1), Literal(null, IntegerType), Literal(2)) as "outerAtt"))
  }

  test("collapse multiple GetStructField on the same UpdateFields") {
    def query(relation: LocalRelation): LogicalPlan = relation
      .select(UpdateFields('struct1, Seq(WithField("b", Literal(2)))) as "struct2")
      .select(
        GetStructField('struct2, 0, Some("a")) as "struct1A",
        GetStructField('struct2, 1, Some("b")) as "struct1B")

    checkRule(
      query(testStructRelation),
      testStructRelation.select(
        GetStructField('struct1, 0) as "struct1A",
        Literal(2) as "struct1B"))

    checkRule(
      query(testNullableStructRelation),
      testNullableStructRelation.select(
        GetStructField('struct1, 0) as "struct1A",
        If(IsNull('struct1), Literal(null, IntegerType), Literal(2)) as "struct1B"))
  }

  test("collapse multiple GetStructField on different UpdateFields") {
    def query(relation: LocalRelation): LogicalPlan = relation
      .select(
        UpdateFields('struct1, Seq(WithField("b", Literal(2)))) as "struct2",
        UpdateFields('struct1, Seq(WithField("b", Literal(3)))) as "struct3")
      .select(
        GetStructField('struct2, 0, Some("a")) as "struct2A",
        GetStructField('struct2, 1, Some("b")) as "struct2B",
        GetStructField('struct3, 0, Some("a")) as "struct3A",
        GetStructField('struct3, 1, Some("b")) as "struct3B")

    checkRule(
      query(testStructRelation),
      testStructRelation
        .select(
          GetStructField('struct1, 0) as "struct2A",
          Literal(2) as "struct2B",
          GetStructField('struct1, 0) as "struct3A",
          Literal(3) as "struct3B"))

    checkRule(
      query(testNullableStructRelation),
      testNullableStructRelation
        .select(
          GetStructField('struct1, 0) as "struct2A",
          If(IsNull('struct1), Literal(null, IntegerType), Literal(2)) as "struct2B",
          GetStructField('struct1, 0) as "struct3A",
          If(IsNull('struct1), Literal(null, IntegerType), Literal(3)) as "struct3B"))
  }

  test("simplify GetStructField on UpdateFields with multiple WithField and extract new column") {
    def query(relation: LocalRelation): LogicalPlan = relation.select(GetStructField(
      UpdateFields('struct1, ('a' to 'z').zipWithIndex.map { case (char, i) =>
        WithField(char.toString, Literal(i))
      }), 2) as "res")

    checkRule(
      query(testStructRelation),
      testStructRelation.select(Literal(2) as "res"))

    checkRule(
      query(testNullableStructRelation),
      testNullableStructRelation.select(
        If(IsNull('struct1), Literal(null, IntegerType), Literal(2)) as "res"))
  }

  test("Combine multiple UpdateFields and extract newly added field") {
    def query(relation: LocalRelation): LogicalPlan = relation.select(GetStructField(
      UpdateFields(
        UpdateFields(
          UpdateFields(
            UpdateFields(
              'struct1,
              Seq(WithField("col1", Literal(1)))),
            Seq(WithField("col2", Literal(2)))),
          Seq(WithField("col3", Literal(3)))),
        Seq(WithField("col4", Literal(4)))),
      5, Some("col4")) as "res")

    checkRule(
      query(testStructRelation),
      testStructRelation.select(Literal(4) as "res"))

    checkRule(
      query(testNullableStructRelation),
      testNullableStructRelation.select(
        If(IsNull('struct1), Literal(null, IntegerType), Literal(4)) as "res"))
  }

  test("should correctly handle different WithField + DropField + GetStructField combinations") {
    def check(fieldOps: Seq[StructFieldsOperation], ordinal: Int, expected: Expression): Unit = {
      def query(relation: LocalRelation): LogicalPlan =
        relation.select(GetStructField(UpdateFields('struct1, fieldOps), ordinal).as("res"))

      checkRule(
        query(testStructRelation),
        testStructRelation.select(expected.as("res")))

      checkRule(
        query(testNullableStructRelation),
        testNullableStructRelation.select((expected match {
          case expr: GetStructField if expr.child.fastEquals('struct1.expr) => expr
          case expr => If(IsNull('struct1), Literal(null, expr.dataType), expr)
        }).as("res")))
    }

    // add attribute, extract an attribute from the original struct
    check(WithField("c", Literal(3)) :: Nil, 0, GetStructField('struct1, 0))

    // drop attribute, extract an attribute from the original struct
    check(DropField("b") :: Nil, 0, GetStructField('struct1, 0))
    check(DropField("a") :: Nil, 0, GetStructField('struct1, 1))

    // drop attribute, add attribute, extract an attribute from the original struct
    check(DropField("b") :: WithField("c", Literal(2)) :: Nil, 0, GetStructField('struct1, 0))
    check(DropField("a") :: WithField("c", Literal(2)) :: Nil, 0, GetStructField('struct1, 1))

    // add attribute, drop attribute, extract an attribute from the original struct
    check(WithField("c", Literal(3)) :: DropField("a") :: Nil, 0, GetStructField('struct1, 1))
    check(WithField("c", Literal(3)) :: DropField("b") :: Nil, 0, GetStructField('struct1, 0))
    check(WithField("b", Literal(3)) :: DropField("b") :: Nil, 0, GetStructField('struct1, 0))
    check(WithField("a", Literal(3)) :: DropField("a") :: Nil, 0, GetStructField('struct1, 1))

    // add attribute, drop same attribute, extract an attribute from the original struct
    check(WithField("c", Literal(3)) :: DropField("c") :: Nil, 0, GetStructField('struct1, 0))

    // add attribute, drop another attribute, extract added attribute
    check(WithField("b", Literal(3)) :: DropField("a") :: Nil, 0, Literal(3))
    check(WithField("a", Literal(3)) :: DropField("b") :: Nil, 0, Literal(3))

    // drop attribute, add same attribute, extract added attribute
    check(DropField("a") :: WithField("c", Literal(3)) :: Nil, 1, Literal(3))
    check(DropField("b") :: WithField("c", Literal(3)) :: Nil, 1, Literal(3))
    check(DropField("b") :: WithField("b", Literal(3)) :: Nil, 1, Literal(3))
    check(DropField("a") :: WithField("a", Literal(3)) :: Nil, 1, Literal(3))
    check(DropField("c") :: WithField("c", Literal(3)) :: Nil, 2, Literal(3))
    check(WithField("c", Literal(3)) :: DropField("c") :: WithField("c", Literal(4)) :: Nil, 2,
      Literal(4))

    // drop earlier attribute, add attribute, extract added attribute
    check(DropField("a") :: WithField("c", Literal(3)) :: Nil, 1, Literal(3))

    // drop later attribute, add attribute, extract added attribute
    check(DropField("b") :: WithField("c", Literal(3)) :: Nil, 1, Literal(3))
  }

  test("should only transform GetStructField calls if not GetStructField on same struct") {
    val struct2 = 'struct2.struct('b.int)
    val testStructRelation = LocalRelation(structAttr, struct2)
    val testNullableStructRelation = LocalRelation(nullableStructAttr, struct2)

    def addFieldFromAnotherStructAndThenExtractIt(relation: LocalRelation): LogicalPlan =
      relation.select(GetStructField(
        UpdateFields('struct1, Seq(WithField("b", GetStructField('struct2, 0)))), 1).as("res"))

    checkRule(
      addFieldFromAnotherStructAndThenExtractIt(testStructRelation),
      testStructRelation.select(GetStructField('struct2, 0).as("res")))

    checkRule(
      addFieldFromAnotherStructAndThenExtractIt(testNullableStructRelation),
      testNullableStructRelation.select(
        If(IsNull('struct1), Literal(null, IntegerType), GetStructField('struct2, 0)).as("res")))

    def addFieldFromSameStructAndThenExtractIt(relation: LocalRelation): LogicalPlan =
      relation.select(GetStructField(
        UpdateFields('struct1, Seq(WithField("b", GetStructField('struct1, 0)))), 1).as("res"))

    checkRule(
      addFieldFromSameStructAndThenExtractIt(testStructRelation),
      testStructRelation.select(GetStructField('struct1, 0).as("res")))

    checkRule(
      addFieldFromSameStructAndThenExtractIt(testNullableStructRelation),
      testNullableStructRelation.select(GetStructField('struct1, 0).as("res")))
  }

  test("simplify if extract value is from inside struct") {
    val nullableStructAttr = 'struct1a.struct(
      'struct2a.struct(
        'struct3a.struct('a.int, 'b.int)))
    val structAttr = nullableStructAttr.withNullability(false)
    val testStructRelation = LocalRelation(structAttr)
    val testNullableStructRelation = LocalRelation(nullableStructAttr)

    Seq(testStructRelation, testNullableStructRelation).foreach { relation =>
      checkRule(
        relation.select(GetStructField(
          UpdateFields('struct1a, Seq(
            WithField("struct2b", GetStructField(GetStructField('struct1a, 0), 0)))), 1).as("res")),
        relation.select(GetStructField(GetStructField('struct1a, 0), 0).as("res")))
    }
  }

  test("simplify add multiple nested fields") {
    // this scenario is possible if users add multiple nested columns via the Column.withField API
    // ideally, users should not be doing this.
    val nullableStructLevel2 = LocalRelation(
      'a1.struct(
        'a2.struct('a3.int)).withNullability(false))

    val addB3toA1A2 = UpdateFields('a1, Seq(WithField("a2",
      UpdateFields(GetStructField('a1, 0), Seq(WithField("b3", Literal(2)))))))

    val query = nullableStructLevel2.select(
      UpdateFields(
        addB3toA1A2,
        Seq(WithField("a2", UpdateFields(
          GetStructField(addB3toA1A2, 0), Seq(WithField("c3", Literal(3))))))).as("a1"))

    val expected = nullableStructLevel2.select(
      UpdateFields('a1, Seq(
        WithField("a2", UpdateFields(GetStructField('a1, 0), Seq(WithField("b3", 2)))),
        WithField("a2", UpdateFields(GetStructField('a1, 0), Seq(
          WithField("b3", 2),
          WithField("c3", 3))))
      )).as("a1"))

    // TODO: how to make this a reality
    val idealExpected = nullableStructLevel2.select(
      UpdateFields('a1, Seq(
        WithField("a2", UpdateFields(GetStructField('a1, 0), Seq(
          WithField("b3", 2),
          WithField("c3", 3))))
      )).as("a1"))

    checkRule(query, expected)
  }

  test("simplify drop multiple nested fields") {
    // this scenario is possible if users drop multiple nested columns via the Column.dropFields API
    // ideally, users should not be doing this.
    val df = LocalRelation(
      'a1.struct(
        'a2.struct('a3.int, 'b3.int, 'c3.int).withNullability(false)
      ).withNullability(false))

    val dropA1A2B = UpdateFields('a1, Seq(WithField("a2", UpdateFields(
      GetStructField('a1, 0), Seq(DropField("b3"))))))

    val query = df.select(
      UpdateFields(
        dropA1A2B,
        Seq(WithField("a2", UpdateFields(
          GetStructField(dropA1A2B, 0), Seq(DropField("c3")))))).as("a1"))

    val expected = df.select(
      UpdateFields('a1, Seq(
        WithField("a2", UpdateFields(GetStructField('a1, 0), Seq(DropField("b3")))),
        WithField("a2", UpdateFields(GetStructField('a1, 0), Seq(DropField("b3"), DropField("c3"))))
      )).as("a1"))

    // TODO: how to make this a reality
    val idealExpected = df.select(
      UpdateFields('a1, Seq(
        WithField("a2", UpdateFields(GetStructField('a1, 0), Seq(DropField("b3"), DropField("c3"))))
      )).as("a1"))

    checkRule(query, expected)
  }
}
