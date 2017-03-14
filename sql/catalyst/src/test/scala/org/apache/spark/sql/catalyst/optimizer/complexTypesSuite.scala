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
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Range}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types._

/**
* SPARK-18601 discusses simplification direct access to complex types creators.
* i.e. {{{create_named_struct(square, `x` * `x`).square}}} can be simplified to {{{`x` * `x`}}}.
* sam applies to create_array and create_map
*/
class ComplexTypesSuite extends PlanTest{

  object Optimizer extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("collapse projections", FixedPoint(10),
          CollapseProject) ::
      Batch("Constant Folding", FixedPoint(10),
          NullPropagation(conf),
          ConstantFolding,
          BooleanSimplification,
          SimplifyConditionals,
          SimplifyBinaryComparison,
          SimplifyCreateStructOps,
          SimplifyCreateArrayOps,
          SimplifyCreateMapOps) :: Nil
  }

  val idAtt = ('id).long.notNull

  lazy val relation = LocalRelation(idAtt )

  test("explicit get from namedStruct") {
    val query = relation
      .select(
        GetStructField(
          CreateNamedStruct(Seq("att", 'id )),
          0,
          None) as "outerAtt").analyze
    val expected = relation.select('id as "outerAtt").analyze

    comparePlans(Optimizer execute query, expected)
  }

  test("explicit get from named_struct- expression maintains original deduced alias") {
    val query = relation
      .select(GetStructField(CreateNamedStruct(Seq("att", 'id)), 0, None))
      .analyze

    val expected = relation
      .select('id as "named_struct(att, id).att")
      .analyze

    comparePlans(Optimizer execute query, expected)
  }

  test("collapsed getStructField ontop of namedStruct") {
    val query = relation
      .select(CreateNamedStruct(Seq("att", 'id)) as "struct1")
      .select(GetStructField('struct1, 0, None) as "struct1Att")
      .analyze
    val expected = relation.select('id as "struct1Att").analyze
    comparePlans(Optimizer execute query, expected)
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
      .analyze

    val expected =
      relation.
        select(
          'id as "struct1Att1",
          ('id * 'id) as "struct1Att2")
      .analyze

    comparePlans(Optimizer execute query, expected)
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
      .analyze

    val expected =
      relation.
        select(
          'id as "struct1.att1",
          ('id * 'id) as "struct1.att2")
        .analyze

    comparePlans(Optimizer execute query, expected)
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
      .analyze

    val expected = relation
      .select(
        CreateArray(Seq('id, 'id + 1L)) as "a1",
        CreateNamedStruct(Seq(
          "att1", ('id + 1L),
          "att2", (('id + 1L) * ('id + 1L)))) as "a2",
        ('id + 1L) as "a3",
        ('id + 1L) as "a4")
      .analyze
    comparePlans(Optimizer execute query, expected)
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
      .analyze

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
      .analyze
    comparePlans(Optimizer execute query, expected)
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
      .analyze

    val expected = relation
      .select(
        CaseWhen(Seq(
          (EqualTo(13L, 'id), ('id + 1L)),
          (EqualTo(13L, ('id + 1L)), ('id + 2L)),
          (EqualTo(13L, ('id + 2L)), ('id + 3L)),
          (Literal(true), 'id))) as "a")
      .analyze
    comparePlans(Optimizer execute query, expected)
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
      .analyze
    val expected = relation
      .select(
        CaseWhen(Seq(
          (EqualTo('id + 3L, 'id), ('id + 1L)),
          (EqualTo('id + 3L, ('id + 1L)), ('id + 2L)),
          (EqualTo('id + 3L, ('id + 2L)), ('id + 3L)),
          (Literal(true), ('id + 4L)))) as "a")
      .analyze
    comparePlans(Optimizer execute query, expected)
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
      .analyze
    val expected = relation.select(
      CaseWhen(Seq(
        (EqualTo('id + 30L, 'id), ('id + 1L)),
        (EqualTo('id + 30L, ('id + 1L)), ('id + 2L)),
        (EqualTo('id + 30L, ('id + 2L)), ('id + 3L)),
        (EqualTo('id + 30L, ('id + 3L)), ('id + 4L)),
        (EqualTo('id + 30L, ('id + 4L)), ('id + 5L)))) as "a")
      .analyze
    comparePlans(Optimizer execute rel, expected)
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
      .analyze

    val expected = relation
      .select(
        CaseKeyWhen(13L,
          Seq('id, ('id + 1L),
            ('id + 1L), ('id + 2L),
            ('id + 2L), ('id + 3L),
            ('id + 3L), ('id + 4L),
            ('id + 4L), ('id + 5L))) as "a")
      .analyze

    comparePlans(Optimizer execute rel, expected)
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
      .analyze

    val expected = relation
      .select(
        CaseWhen(Seq(
          (EqualTo(2L, 'id), ('id + 1L)),
          // these two are possible matches, we can't tell untill runtime
          (EqualTo(2L, ('id + 1L)), ('id + 2L)),
          (EqualTo(2L, 'id + 2L), Literal.create(null, LongType)),
          // this is a definite match (two constants),
          // but it cannot override a potential match with ('id + 2L),
          // which is exactly what [[Coalesce]] would do in this case.
          (Literal.TrueLiteral, 'id))) as "a")
      .analyze
    comparePlans(Optimizer execute rel, expected)
  }
}
