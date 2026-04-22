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

package org.apache.spark.sql.connector

import org.apache.spark.sql.types._

/**
 * Evolution control, source-missing-fields, and null struct semantics tests
 * for nested struct/array schema evolution.
 */
trait MergeIntoSchemaEvolutionMissingFieldAndNullStructTests
  extends MergeIntoSchemaEvolutionSuiteBase {

  import testImplicits._

  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.types.StructType

  // scalastyle:off argcount
  override protected def testEvolution(name: String)(
      targetData: => DataFrame,
      sourceData: => DataFrame,
      cond: String = "t.pk = s.pk",
      clauses: Seq[MergeClause] = Seq.empty,
      expected: => DataFrame = null,
      expectedWithoutEvolution: => DataFrame = null,
      expectedSchema: StructType = null,
      expectedSchemaWithoutEvolution: StructType = null,
      expectErrorContains: String = null,
      expectErrorWithoutEvolutionContains: String = null,
      confs: Seq[(String, String)] = Seq.empty,
      partitionCols: Seq[String] = Seq("dep"),
      disableAutoSchemaEvolution: Boolean = false,
      requiresNestedTypeCoercion: Boolean = false): Unit = {
    super.testEvolution(name)(
      targetData = targetData,
      sourceData = sourceData,
      cond = cond,
      clauses = clauses,
      expected = expected,
      expectedWithoutEvolution = expectedWithoutEvolution,
      expectedSchema = expectedSchema,
      expectedSchemaWithoutEvolution = expectedSchemaWithoutEvolution,
      expectErrorContains = expectErrorContains,
      expectErrorWithoutEvolutionContains = expectErrorWithoutEvolutionContains,
      confs = confs,
      partitionCols = partitionCols,
      disableAutoSchemaEvolution = disableAutoSchemaEvolution,
      requiresNestedTypeCoercion = requiresNestedTypeCoercion
    )
  }
  // scalastyle:on argcount

  // Null target struct should become struct of nulls because source had a missing field
  testNestedStructsEvolution("source has missing field in nested struct containing array - " +
      "target null struct becomes struct of nulls")(
    target = Seq(
      // Target has struct with array and extra field 'c2' at nested struct level
      """{ "pk": 0, "s": { "c1": 1, "arr": [{ "x": 100 }, { "x": 101 }], "c2": "foo" },
        | "dep": "sales" }""".stripMargin.replace("\n", ""),
      // c2 is null in this row
      """{ "pk": 1, "s": { "c1": 2, "arr": [{ "x": 200 }, { "x": 201 }], "c2": null },
        | "dep": "hr" }""".stripMargin.replace("\n", "")
    ),
    source = Seq(
      // Source has null struct, schema missing field 'c2'
      """{ "pk": 1, "s": null, "dep": "engineering" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("arr", ArrayType(StructType(Seq(
          StructField("x", IntegerType)
        )))),
        StructField("c2", StringType) // extra field at nested struct level
      ))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("arr", ArrayType(StructType(Seq(
          StructField("x", IntegerType)
        ))))
        // missing field 'c2'
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insertAll()),
    result = Seq(
      """{ "pk": 0, "s": { "c1": 1, "arr": [{ "x": 100 }, { "x": 101 }], "c2": "foo" },
        | "dep": "sales" }""".stripMargin.replace("\n", ""),
      // Target had extra field 'c2', preserve struct of nulls
      """{ "pk": 1, "s": { "c1": null, "arr": null, "c2": null }, "dep": "engineering" }"""
    ),
    expectErrorWithoutEvolutionContains = "Cannot find data for the output column",
    requiresNestedTypeCoercion = true
  )

  // Both with/without evolution fail - non-existent column errors regardless of evolution
  testEvolution("error on non-existent column in UPDATE")(
    targetData = Seq(
      (1, 100, "hr"),
      (2, 200, "software")
    ).toDF("pk", "salary", "dep"),
    sourceData = Seq(
      (2, 250, "engineering"),
      (3, 300, "finance")
    ).toDF("pk", "salary", "dep"),
    clauses = Seq(update("non_existent = s.nonexistent_column")),
    expectErrorContains = "cannot be resolved",
    expectErrorWithoutEvolutionContains = "cannot be resolved"
  )

  testEvolution("error on non-existent column in INSERT")(
    targetData = Seq(
      (1, 100, "hr"),
      (2, 200, "software")
    ).toDF("pk", "salary", "dep"),
    sourceData = Seq(
      (2, 250, "engineering"),
      (3, 300, "finance")
    ).toDF("pk", "salary", "dep"),
    clauses = Seq(insert("(pk, salary, dep, non_existent) VALUES (s.pk, s.salary, s.dep, s.dep)")),
    expectErrorContains = "cannot be resolved",
    expectErrorWithoutEvolutionContains = "cannot be resolved"
  )
}
