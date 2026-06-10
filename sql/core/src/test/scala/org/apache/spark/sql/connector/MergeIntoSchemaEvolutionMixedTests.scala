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

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * Tests that combine multiple schema-evolution scenarios (e.g. source has both extra
 * and missing fields, or type widening together with extra fields), and null-struct
 * semantics where the source and target schemas match (top-level and nested).
 */
trait MergeIntoSchemaEvolutionMixedTests extends MergeIntoSchemaEvolutionSuiteBase {

  import testImplicits._

  // Source missing 'dep' column that exists in target - replaced with 'active'
  testEvolution("source has extra and missing column with set all column")(
    targetData = Seq(
      (1, 100, "hr"),
      (2, 200, "software"),
      (3, 300, "hr"),
      (4, 400, "marketing"),
      (5, 500, "executive")
    ).toDF("pk", "salary", "dep"),
    sourceData = Seq(
      (4, 150, true),
      (5, 250, true),
      (6, 350, false)
    ).toDF("pk", "salary", "active"),
    clauses = Seq(
      updateAll(),
      insertAll()
    ),
    expected = Seq[(Int, Int, String, java.lang.Boolean)](
      (1, 100, "hr", null),
      (2, 200, "software", null),
      (3, 300, "hr", null),
      (4, 150, "marketing", true),
      (5, 250, "executive", true),
      (6, 350, null, false)).toDF("pk", "salary", "dep", "active"),
    expectErrorWithoutEvolutionContains = "A column, variable, or function parameter with name " +
      "`dep` cannot be resolved"
  )

  testEvolution("source has missing column with default value and extra column" +
    " and set all columns")(
    targetData = {
      val schema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("salary", IntegerType),
        StructField("dep", StringType).withCurrentDefaultValue("'unknown'")
      ))
      val data = Seq(
        Row(1, 100, "hr"),
        Row(2, 200, "software"),
        Row(3, 300, "hr"),
        Row(4, 400, "marketing"),
        Row(5, 500, "executive"))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    },
    sourceData = Seq(
      (4, 150, true),
      (5, 250, true),
      (6, 350, false)
    ).toDF("pk", "salary", "active"),
    clauses = Seq(updateAll(), insertAll()),
    // With evolution: active column added, matched rows keep dep, inserted rows get default
    expected = Seq[(Int, Int, String, java.lang.Boolean)](
      (1, 100, "hr", null),
      (2, 200, "software", null),
      (3, 300, "hr", null),
      (4, 150, "marketing", true),
      (5, 250, "executive", true),
      (6, 350, "unknown", false)).toDF("pk", "salary", "dep", "active"),
    expectErrorWithoutEvolutionContains = "A column, variable, or function parameter with name " +
      "`dep` cannot be resolved"
  )

  testEvolution("source has missing and extra column with set explicit column")(
    targetData = Seq(
      (1, 100, "hr"),
      (2, 200, "software"),
      (3, 300, "hr"),
      (4, 400, "marketing"),
      (5, 500, "executive")
    ).toDF("pk", "salary", "dep"),
    sourceData = Seq(
      (4, 150, true),
      (5, 250, true),
      (6, 350, false)
    ).toDF("pk", "salary", "active"),
    clauses = Seq(
      update(set = "dep = 'finance', active = s.active"),
      insert(values = "(pk, salary, dep, active) VALUES (s.pk, s.salary, 'finance', s.active)")
    ),
    expected = Seq[(Int, Int, String, java.lang.Boolean)](
      (1, 100, "hr", null),
      (2, 200, "software", null),
      (3, 300, "hr", null),
      (4, 400, "finance", true),
      (5, 500, "finance", true),
      (6, 350, "finance", false)).toDF("pk", "salary", "dep", "active"),
    expectErrorWithoutEvolutionContains = "A column, variable, or function parameter with name " +
      "`active` cannot be resolved"
  )

  // Type widening (INT->LONG, SHORT->INT) and adding new columns together
  testEvolution("type widening two types and adding two columns")(
    targetData = {
      val schema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("score", IntegerType),
        StructField("rating", ShortType),
        StructField("dep", StringType)
      ))
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(
        Row(1, 100, 45.toShort, "premium"),
        Row(2, 85, 38.toShort, "standard")
      )), schema)
    },
    sourceData = {
      val sourceSchema = StructType(Seq(
        StructField("pk", IntegerType),
        StructField("score", LongType),
        StructField("rating", IntegerType),
        StructField("dep", StringType),
        StructField("priority", StringType),
        StructField("region", StringType)
      ))
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(
        Row(1, 5000000000L, 485, "premium", "high", "west"),
        Row(3, 7500000000L, 495, "enterprise", "critical", "east")
      )), sourceSchema)
    },
    clauses = Seq(
      updateAll(),
      insertAll()
    ),
    expected = Seq(
      (1, 5000000000L, 485, "premium", "high", "west"),
      (2, 85L, 38, "standard", null, null),
      (3, 7500000000L, 495, "enterprise", "critical", "east"))
      .toDF("pk", "score", "rating", "dep", "priority", "region"),
    expectedSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("score", LongType),
      StructField("rating", IntegerType),
      StructField("dep", StringType),
      StructField("priority", StringType),
      StructField("region", StringType)
    )),
    expectErrorWithoutEvolutionContains = "Fail to assign a value of \"BIGINT\" type " +
      "to the \"INT\" type column or variable `score` due to an overflow."
  )

  testEvolution("type widening + extra field combined - UPDATE/INSERT *")(
    targetData = Seq((1, 100, "hr"), (2, 200, "software")).toDF("pk", "salary", "dep"),
    sourceData = Seq((2, Long.MaxValue, "software", true), (3, Long.MaxValue, "engineering", true))
      .toDF("pk", "salary", "dep", "active"),
    clauses = Seq(updateAll(), insertAll()),
    expected = {
      val schema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("salary", LongType, nullable = false),
        StructField("dep", StringType),
        StructField("active", BooleanType)))
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(
        Row(1, 100L, "hr", null),
        Row(2, Long.MaxValue, "software", true),
        Row(3, Long.MaxValue, "engineering", true)
      )), schema)
    },
    expectedSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("salary", LongType, nullable = false),
      StructField("dep", StringType),
      StructField("active", BooleanType))),
    expectErrorWithoutEvolutionContains = "CAST_OVERFLOW_IN_TABLE_INSERT"
  )

  testEvolution("type widening + extra field combined - direct assignment")(
    targetData = Seq((1, 100, "hr"), (2, 200, "software")).toDF("pk", "salary", "dep"),
    sourceData = Seq((2, Long.MaxValue, "software", true), (3, Long.MaxValue, "engineering", true))
      .toDF("pk", "salary", "dep", "active"),
    clauses = Seq(
      update(set = "salary = s.salary, active = s.active"),
      insert(values = "(pk, salary, dep, active) VALUES (s.pk, s.salary, s.dep, s.active)")
    ),
    expected = {
      val schema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("salary", LongType, nullable = false),
        StructField("dep", StringType),
        StructField("active", BooleanType)))
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(
        Row(1, 100L, "hr", null),
        Row(2, Long.MaxValue, "software", true),
        Row(3, Long.MaxValue, "engineering", true)
      )), schema)
    },
    expectedSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("salary", LongType, nullable = false),
      StructField("dep", StringType),
      StructField("active", BooleanType))),
    expectErrorWithoutEvolutionContains =
      "A column, variable, or function parameter with name `active` cannot be resolved"
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

  // Null struct handling tests - same result with/without evolution since schemas match
  testNestedStructsEvolution("schemas match - source has null struct")(
    target = Seq(
      """{ "pk": 0, "s": { "c1": 1, "c2": "a" }, "dep": "sales" }""",
      """{ "pk": 1, "s": { "c1": 2, "c2": "b" }, "dep": "hr" }"""
    ),
    source = Seq(
      // Source has null struct values
      """{ "pk": 1, "s": null, "dep": "engineering" }""",
      """{ "pk": 2, "s": null, "dep": "finance" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StringType)
      ))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StringType)
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insertAll()),
    result = Seq(
      """{ "pk": 0, "s": { "c1": 1, "c2": "a" }, "dep": "sales" }""",
      """{ "pk": 1, "s": null, "dep": "engineering" }""",
      """{ "pk": 2, "s": null, "dep": "finance" }"""
    ),
    resultWithoutEvolution = Seq(
      """{ "pk": 0, "s": { "c1": 1, "c2": "a" }, "dep": "sales" }""",
      """{ "pk": 1, "s": null, "dep": "engineering" }""",
      """{ "pk": 2, "s": null, "dep": "finance" }"""
    )
  )

  testNestedStructsEvolution("schemas match - source has struct of nulls")(
    target = Seq(
      """{ "pk": 0, "s": { "c1": 1, "c2": "a" }, "dep": "sales" }""",
      """{ "pk": 1, "s": { "c1": 2, "c2": "b" }, "dep": "hr" }"""
    ),
    source = Seq(
      // Source has a struct with null field values (not a null struct)
      """{ "pk": 1, "s": { "c1": null, "c2": null }, "dep": "engineering" }""",
      """{ "pk": 2, "s": { "c1": null, "c2": null }, "dep": "finance" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StringType)
      ))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StringType)
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insertAll()),
    // Struct of null values should be preserved, not converted to null struct
    result = Seq(
      """{ "pk": 0, "s": { "c1": 1, "c2": "a" }, "dep": "sales" }""",
      """{ "pk": 1, "s": { "c1": null, "c2": null }, "dep": "engineering" }""",
      """{ "pk": 2, "s": { "c1": null, "c2": null }, "dep": "finance" }"""
    ),
    resultWithoutEvolution = Seq(
      """{ "pk": 0, "s": { "c1": 1, "c2": "a" }, "dep": "sales" }""",
      """{ "pk": 1, "s": { "c1": null, "c2": null }, "dep": "engineering" }""",
      """{ "pk": 2, "s": { "c1": null, "c2": null }, "dep": "finance" }"""
    )
  )

  testNestedStructsEvolution("schemas match - source has null struct " +
    "and target has struct of nulls")(
    target = Seq(
      """{ "pk": 0, "s": { "c1": 1, "c2": "a" }, "dep": "sales" }""",
      """{ "pk": 1, "s": { "c1": null, "c2": null }, "dep": "hr" }"""
    ),
    source = Seq(
      // Source has a null struct (not a struct of nulls)
      """{ "pk": 1, "s": null, "dep": "engineering" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StringType)
      ))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StringType)
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insertAll()),
    // Null struct should override struct of nulls
    result = Seq(
      """{ "pk": 0, "s": { "c1": 1, "c2": "a" }, "dep": "sales" }""",
      """{ "pk": 1, "s": null, "dep": "engineering" }"""
    ),
    resultWithoutEvolution = Seq(
      """{ "pk": 0, "s": { "c1": 1, "c2": "a" }, "dep": "sales" }""",
      """{ "pk": 1, "s": null, "dep": "engineering" }"""
    )
  )

  // Both with/without evolution succeed with same result - null struct is preserved
  testNestedStructsEvolution("schemas match - source and target have null struct")(
    target = Seq(
      """{ "pk": 0, "s": { "c1": 1, "c2": "a" }, "dep": "sales" }""",
      """{ "pk": 1, "s": { "c1": 2, "c2": "b" }, "dep": "hr" }"""
    ),
    source = Seq(
      // Source has null for the struct column
      """{ "pk": 1, "s": null, "dep": "engineering" }""",
      """{ "pk": 2, "s": null, "dep": "finance" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StringType)
      ))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StringType)
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(update("s = s.s"), insertAll()),
    // Same result for both - null struct preserved
    result = Seq(
      """{ "pk": 0, "s": { "c1": 1, "c2": "a" }, "dep": "sales" }""",
      """{ "pk": 1, "s": null, "dep": "hr" }""",
      """{ "pk": 2, "s": null, "dep": "finance" }"""
    ),
    resultWithoutEvolution = Seq(
      """{ "pk": 0, "s": { "c1": 1, "c2": "a" }, "dep": "sales" }""",
      """{ "pk": 1, "s": null, "dep": "hr" }""",
      """{ "pk": 2, "s": null, "dep": "finance" }"""
    )
  )

  // Both with/without evolution succeed - null nested struct is preserved
  testNestedStructsEvolution("schemas match - source has null nested struct")(
    target = Seq(
      """{ "pk": 0, "s": { "c1": 1, "c2": { "a": 10, "b": "foo" } }, "dep": "sales" }""",
      """{ "pk": 1, "s": { "c1": 2, "c2": { "a": 20, "b": "bar" } }, "dep": "hr" }"""
    ),
    source = Seq(
      // Source has null for the nested struct (c2)
      """{ "pk": 1, "s": { "c1": 3, "c2": null }, "dep": "engineering" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", StringType)
        )))
      ))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", StringType)
        )))
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insertAll()),
    // Same result for both - null nested struct preserved
    result = Seq(
      """{ "pk": 0, "s": { "c1": 1, "c2": { "a": 10, "b": "foo" } }, "dep": "sales" }""",
      """{ "pk": 1, "s": { "c1": 3, "c2": null }, "dep": "engineering" }"""
    ),
    resultWithoutEvolution = Seq(
      """{ "pk": 0, "s": { "c1": 1, "c2": { "a": 10, "b": "foo" } }, "dep": "sales" }""",
      """{ "pk": 1, "s": { "c1": 3, "c2": null }, "dep": "engineering" }"""
    )
  )

  // Both with/without evolution fail - can't insert null into NOT NULL column
  testNestedStructsEvolution("schemas match - source has null struct" +
    " in target' non-nullable struct column")(
    target = Seq(
      """{ "pk": 0, "s": { "c1": 1, "c2": "a" }, "dep": "sales" }""",
      """{ "pk": 1, "s": { "c1": 2, "c2": "b" }, "dep": "hr" }"""
    ),
    source = Seq(
      // Source has null for the struct column
      """{ "pk": 1, "s": null, "dep": "engineering" }""",
      """{ "pk": 2, "s": null, "dep": "finance" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StringType)
      )), nullable = false), // NOT NULL
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StringType)
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insertAll()),
    // Both fail - can't put null in NOT NULL column
    expectErrorContains = "NULL value appeared in non-nullable field",
    expectErrorWithoutEvolutionContains = "NULL value appeared in non-nullable field"
  )

  testNestedStructsEvolution("source has missing and extra nested struct field" +
    "and set explicit columns")(
    target = Seq(
      """{ "pk": 1, "s": { "c1": 2, "c2": { "a": [1,2], "m": { "a": "b" } } }, "dep": "hr" }"""
    ),
    source = Seq(
      // Source is missing column 'a' in s.c2, and has new column 'c3'
      """{ "pk": 1, "s": { "c1": 10, "c2": { "m": { "c": "d" }, "c3": false } },
        | "dep": "sales" }""".stripMargin.replace("\n", ""),
      """{ "pk": 2, "s": { "c1": 20, "c2": { "m": { "e": "f" }, "c3": true } },
        | "dep": "engineering" }""".stripMargin.replace("\n", "")
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StructType(Seq(
          StructField("a", ArrayType(IntegerType)),
          StructField("m", MapType(StringType, StringType))
        )))
      ))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StructType(Seq(
          // missing column 'a'
          StructField("m", MapType(StringType, StringType)),
          StructField("c3", BooleanType)
        )))
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(
      update(set = "s.c1 = -1, s.c2.m = map('k', 'v'), s.c2.a = array(-1), s.c2.c3 = s.s.c2.c3"),
      insert(values = """(pk, s, dep) VALUES (s.pk,
        |named_struct('c1', s.s.c1, 'c2', named_struct('a', array(-2), 'm', map('g', 'h'),
        |'c3', true)), s.dep)""".stripMargin.replace("\n", " "))
    ),
    result = Seq(
      """{ "pk": 1, "s": { "c1": -1, "c2": { "a": [-1], "m": { "k": "v" }, "c3": false } },
        | "dep": "hr" }""".stripMargin.replace("\n", ""),
      """{ "pk": 2, "s": { "c1": 20, "c2": { "a": [-2], "m": { "g": "h" }, "c3": true } },
        | "dep": "engineering" }""".stripMargin.replace("\n", "")
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StructType(Seq(
          StructField("a", ArrayType(IntegerType)),
          StructField("m", MapType(StringType, StringType)),
          StructField("c3", BooleanType)
        )))
      ))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "No such struct field `c3` in `a`, `m`"
  )

  testNestedStructsEvolution("source has missing and extra nested struct field with " +
    "set all columns")(
    target = Seq(
      """{ "pk": 1, "s": { "c1": 2, "c2": { "a": [1,2], "m": { "a": "b" } } }, "dep": "hr" }"""
    ),
    source = Seq(
      // Source is missing column 'a' in s.c2, and has new column 'c3'
      """{ "pk": 1, "s": { "c1": 10, "c2": { "m": { "c": "d" }, "c3": false } },
        | "dep": "sales" }""".stripMargin.replace("\n", ""),
      """{ "pk": 2, "s": { "c1": 20, "c2": { "m": { "e": "f" }, "c3": true } },
        | "dep": "engineering" }""".stripMargin.replace("\n", "")
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StructType(Seq(
          StructField("a", ArrayType(IntegerType)),
          StructField("m", MapType(StringType, StringType))
        )))
      ))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StructType(Seq(
          // missing column 'a'
          StructField("m", MapType(StringType, StringType)),
          StructField("c3", BooleanType)
        )))
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insertAll()),
    result = Seq(
      // Matched row: 'a' is preserved from target, other fields from source
      """{ "pk": 1, "s": { "c1": 10, "c2": { "a": [1,2], "m": { "c": "d" }, "c3": false } },
        | "dep": "sales" }""".stripMargin.replace("\n", ""),
      // Not matched row: 'a' is null since source doesn't have it
      """{ "pk": 2, "s": { "c1": 20, "c2": { "a": null, "m": { "e": "f" }, "c3": true } },
        | "dep": "engineering" }""".stripMargin.replace("\n", "")
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StructType(Seq(
          StructField("a", ArrayType(IntegerType)),
          StructField("m", MapType(StringType, StringType)),
          StructField("c3", BooleanType)
        )))
      ))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains =
      "Cannot find data for the output column `s`.`c2`.`a`",
    requiresNestedTypeCoercion = true
  )

  testNestedStructsEvolution("source has missing and extra nested struct field with" +
    " assign top-level struct - UPDATE") (
    target = Seq(
      """{ "pk": 1, "s": { "c1": 2, "c2": { "a": [1,2], "m": { "a": "b" } } }, "dep": "hr" }"""
    ),
    source = Seq(
      // Source is missing column 'a' in s.c2, and has new column 'c3'
      """{ "pk": 1, "s": { "c1": 10, "c2": { "m": { "c": "d" }, "c3": false } },
        | "dep": "sales" }""".stripMargin.replace("\n", ""),
      """{ "pk": 2, "s": { "c1": 20, "c2": { "m": { "e": "f" }, "c3": true } },
        | "dep": "engineering" }""".stripMargin.replace("\n", "")
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StructType(Seq(
          StructField("a", ArrayType(IntegerType)),
          StructField("m", MapType(StringType, StringType))
        )))
      ))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StructType(Seq(
          // missing column 'a'
          StructField("m", MapType(StringType, StringType)),
          StructField("c3", BooleanType)
        )))
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(update(set = "s = s.s"), insertAll()),
    result = Seq(
      // Matched row: 'a' is null since we're setting whole struct from source
      """{ "pk": 1, "s": { "c1": 10, "c2": { "a": null, "m": { "c": "d" }, "c3": false } },
        | "dep": "hr" }""".stripMargin.replace("\n", ""),
      // Not matched row: 'a' is null since source doesn't have it
      """{ "pk": 2, "s": { "c1": 20, "c2": { "a": null, "m": { "e": "f" }, "c3": true } },
        | "dep": "engineering" }""".stripMargin.replace("\n", "")
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StructType(Seq(
          StructField("a", ArrayType(IntegerType)),
          StructField("m", MapType(StringType, StringType)),
          StructField("c3", BooleanType)
        )))
      ))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains =
      "Cannot find data for the output column `s`.`c2`.`a`",
    requiresNestedTypeCoercion = true
  )

  testEvolution("source has missing and extra field for struct in map and set all columns")(
    targetData = {
      val schema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("m", MapType(
          StructType(Seq(StructField("c1", IntegerType), StructField("c2", IntegerType))),
          StructType(Seq(StructField("c4", StringType), StructField("c5", StringType))))),
        StructField("dep", StringType)))
      val data = Seq(
        Row(0, Map(Row(10, 10) -> Row("c", "c")), "hr"),
        Row(1, Map(Row(20, 20) -> Row("d", "d")), "sales"))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    },
    sourceData = {
      val sourceSchema = StructType(Seq(
        StructField("pk", IntegerType),
        StructField("m", MapType(
          StructType(Seq(StructField("c1", IntegerType), StructField("c3", BooleanType))),
          StructType(Seq(StructField("c4", StringType), StructField("c6", BooleanType))))),
        StructField("dep", StringType)))
      val sourceData = Seq(
        Row(1, Map(Row(10, true) -> Row("y", false)), "sales"),
        Row(2, Map(Row(20, false) -> Row("z", true)), "engineering"))
      spark.createDataFrame(spark.sparkContext.parallelize(sourceData), sourceSchema)
    },
    clauses = Seq(updateAll(), insertAll()),
    expected = Seq(
      (0, Map((10, 10: java.lang.Integer, null: java.lang.Boolean) ->
        ("c", "c", null: java.lang.Boolean)), "hr"),
      (1, Map((10, null: java.lang.Integer, true: java.lang.Boolean) ->
        ("y", null: String, false: java.lang.Boolean)), "sales"),
      (2, Map((20, null: java.lang.Integer, false: java.lang.Boolean) ->
        ("z", null: String, true: java.lang.Boolean)), "engineering")
    ).toDF("pk", "m", "dep"),
    expectErrorWithoutEvolutionContains = "Cannot find data for the output column",
    requiresNestedTypeCoercion = true
  )

  testEvolution("source has missing and extra field for struct in map and set explicit columns")(
    targetData = {
      val schema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("m", MapType(
          StructType(Seq(StructField("c1", IntegerType), StructField("c2", IntegerType))),
          StructType(Seq(StructField("c4", StringType), StructField("c5", StringType))))),
        StructField("dep", StringType)))
      val data = Seq(
        Row(0, Map(Row(10, 10) -> Row("c", "c")), "hr"),
        Row(1, Map(Row(20, 20) -> Row("d", "d")), "sales"))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    },
    sourceData = {
      val sourceSchema = StructType(Seq(
        StructField("pk", IntegerType),
        StructField("m", MapType(
          StructType(Seq(StructField("c1", IntegerType), StructField("c3", BooleanType))),
          StructType(Seq(StructField("c4", StringType), StructField("c6", BooleanType))))),
        StructField("dep", StringType)))
      val sourceData = Seq(
        Row(1, Map(Row(10, true) -> Row("y", false)), "sales"),
        Row(2, Map(Row(20, false) -> Row("z", true)), "engineering"))
      spark.createDataFrame(spark.sparkContext.parallelize(sourceData), sourceSchema)
    },
    clauses = Seq(
      update("m = s.m, dep = 'my_old_dep'"),
      insert("(pk, m, dep) VALUES (s.pk, s.m, 'my_new_dep')")),
    expected = Seq(
      (0, Map((10, 10: java.lang.Integer, null: java.lang.Boolean) ->
        ("c", "c", null: java.lang.Boolean)), "hr"),
      (1, Map((10, null: java.lang.Integer, true: java.lang.Boolean) ->
        ("y", null: String, false: java.lang.Boolean)), "my_old_dep"),
      (2, Map((20, null: java.lang.Integer, false: java.lang.Boolean) ->
        ("z", null: String, true: java.lang.Boolean)), "my_new_dep")
    ).toDF("pk", "m", "dep"),
    expectErrorWithoutEvolutionContains = "Cannot find data for the output column",
    requiresNestedTypeCoercion = true
  )

  testNestedStructsEvolution("source has missing and extra field for struct in array" +
    " and set all columns")(
    target = Seq(
      """{ "pk": 0, "a": [{ "c1": 10, "c2": 10 }], "dep": "hr" }""",
      """{ "pk": 1, "a": [{ "c1": 20, "c2": 20 }], "dep": "sales" }"""
    ),
    source = Seq(
      // Source is missing c2, has new c3
      """{ "pk": 1, "a": [{ "c1": 10, "c3": true }], "dep": "sales" }""",
      """{ "pk": 2, "a": [{ "c1": 20, "c3": false }], "dep": "engineering" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("a", ArrayType(
        StructType(Seq(StructField("c1", IntegerType), StructField("c2", IntegerType))))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("a", ArrayType(
        StructType(Seq(StructField("c1", IntegerType), StructField("c3", BooleanType))))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insertAll()),
    result = Seq(
      // Unmatched target row: c3 added as null
      """{ "pk": 0, "a": [{ "c1": 10, "c2": 10, "c3": null }], "dep": "hr" }""",
      // Matched row: c2 becomes null (from source), c3 from source
      """{ "pk": 1, "a": [{ "c1": 10, "c2": null, "c3": true }], "dep": "sales" }""",
      // Not matched row: c2 is null since source doesn't have it
      """{ "pk": 2, "a": [{ "c1": 20, "c2": null, "c3": false }], "dep": "engineering" }"""
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("a", ArrayType(
        StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", IntegerType),
          StructField("c3", BooleanType))))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "Cannot find data for the output column",
    requiresNestedTypeCoercion = true
  )

  testNestedStructsEvolution("source has missing and extra column for struct in array" +
    "and set explicit columns")(
    target = Seq(
      """{ "pk": 0, "a": [{ "c1": 10, "c2": 10 }], "dep": "hr" }""",
      """{ "pk": 1, "a": [{ "c1": 20, "c2": 20 }], "dep": "sales" }"""
    ),
    source = Seq(
      // Source is missing c2, has new c3
      """{ "pk": 1, "a": [{ "c1": 10, "c3": true }], "dep": "sales" }""",
      """{ "pk": 2, "a": [{ "c1": 20, "c3": false }], "dep": "engineering" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("a", ArrayType(
        StructType(Seq(StructField("c1", IntegerType), StructField("c2", IntegerType))))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("a", ArrayType(
        StructType(Seq(StructField("c1", IntegerType), StructField("c3", BooleanType))))),
      StructField("dep", StringType)
    )),
    clauses = Seq(
      update(set = "a = s.a, dep = 'my_old_dep'"),
      insert(values = "(pk, a, dep) VALUES (s.pk, s.a, 'my_new_dep')")
    ),
    result = Seq(
      // Unmatched target row: c3 added as null
      """{ "pk": 0, "a": [{ "c1": 10, "c2": 10, "c3": null }], "dep": "hr" }""",
      // Matched row: c2 becomes null (from source), c3 from source, dep updated
      """{ "pk": 1, "a": [{ "c1": 10, "c2": null, "c3": true }], "dep": "my_old_dep" }""",
      // Not matched row: c2 is null since source doesn't have it
      """{ "pk": 2, "a": [{ "c1": 20, "c2": null, "c3": false }], "dep": "my_new_dep" }"""
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("a", ArrayType(
        StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", IntegerType),
          StructField("c3", BooleanType))))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "Cannot find data for the output column",
    requiresNestedTypeCoercion = true
  )

  testNestedStructsEvolution("source with missing and extra nested fields - null source struct")(
    target = Seq(
      """{ "pk": 0, "s": { "c1": 1, "c2": { "a": 10, "b": "x" } }, "dep": "sales" }""",
      """{ "pk": 1, "s": { "c1": 2, "c2": { "a": 20, "b": "y" } }, "dep": "hr" }"""
    ),
    source = Seq(
      // Source has null struct, missing 'b' and extra 'c' in schema
      """{ "pk": 1, "s": null, "dep": "engineering" }""",
      """{ "pk": 2, "s": null, "dep": "finance" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", StringType)
        )))
      ))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StructType(Seq(
          StructField("a", IntegerType),
          // missing field 'b'
          StructField("c", StringType) // extra field 'c'
        )))
      ))),
      StructField("dep", StringType)
    )),
    cond = "t.pk = s.pk",
    clauses = Seq(updateAll(), insertAll()),
    result = Seq(
      // Unmatched target row: 'c' added as null
      """{ "pk": 0, "s": { "c1": 1, "c2": { "a": 10, "b": "x", "c": null } }, "dep": "sales" }""",
      // Matched row: null struct from source, but s.c2.b preserved from target as "y"
      """{ "pk": 1, "s": { "c1": null, "c2": { "a": null, "b": "y", "c": null } },
        | "dep": "engineering" }""".stripMargin.replace("\n", ""),
      // Not matched row: null struct inserted
      """{ "pk": 2, "s": null, "dep": "finance" }"""
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", StringType),
          StructField("c", StringType)
        )))
      ))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "Cannot find data for the output column",
    requiresNestedTypeCoercion = true
  )

  // All combinations fail because target has non-nullable field 'b' that source doesn't provide
  testNestedStructsEvolution(
      "null struct with non-nullable nested field - source missing and extra fields")(
    target = Seq(
      """{ "pk": 0, "s": { "c1": 1, "c2": { "a": 10, "b": "x" } }, "dep": "sales" }""",
      """{ "pk": 1, "s": { "c1": 2, "c2": { "a": 20, "b": "y" } }, "dep": "hr" }"""
    ),
    source = Seq(
      // Source has null struct, schema missing non-nullable field 'b', has extra field 'c'
      """{ "pk": 1, "s": null, "dep": "engineering" }""",
      """{ "pk": 2, "s": null, "dep": "finance" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", StringType, nullable = false) // NOT NULL
        )))
      ))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StructType(Seq(
          StructField("a", IntegerType),
          StructField("c", StringType) // extra field, missing 'b'
        )))
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insertAll()),
    // All cases fail - can't provide null for non-nullable field 'b'
    expectErrorContains = "Cannot find data for the output column",
    expectErrorWithoutEvolutionContains = "Cannot find data for the output column"
  )
}
