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
 * Basic schema evolution tests.
 */
trait MergeIntoSchemaEvolutionBasicTests extends MergeIntoSchemaEvolutionSuiteBase {

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

  // ---------------------------------------------------------------------------
  // Regular schema evolution tests (with auto-schema-evolution capability enabled)
  // ---------------------------------------------------------------------------

  testEvolution("source has extra column with set explicit column")(
    targetData = Seq(
      (1, 100, "hr"),
      (2, 200, "software"),
      (3, 300, "hr"),
      (4, 400, "marketing"),
      (5, 500, "executive")
    ).toDF("pk", "salary", "dep"),
    sourceData = Seq(
      (4, 150, "dummy", true),
      (5, 250, "dummy", true),
      (6, 350, "dummy", false)
    ).toDF("pk", "salary", "dep", "active"),
    clauses = Seq(
      update(set = "dep='software', active=s.active"),
      insert(values = "(pk, salary, dep, active) VALUES (s.pk, 0, s.dep, s.active)")
    ),
    expected = Seq[(Int, Int, String, java.lang.Boolean)](
      (1, 100, "hr", null),
      (2, 200, "software", null),
      (3, 300, "hr", null),
      (4, 400, "software", true),
      (5, 500, "software", true),
      (6, 0, "dummy", false)).toDF("pk", "salary", "dep", "active"),
    expectErrorWithoutEvolutionContains = "A column, variable, or function parameter with name " +
      "`active` cannot be resolved"
  )

  testEvolution("source with extra column with conditions on update and insert")(
    targetData = Seq(
      (1, 100, "hr"),
      (2, 200, "software"),
      (3, 300, "hr"),
      (4, 400, "marketing"),
      (5, 500, "executive")
    ).toDF("pk", "salary", "dep"),
    sourceData = Seq(
      (4, 450, "finance", false),
      (5, 550, "finance", true),
      (6, 350, "sales", true),
      (7, 250, "sales", false)
    ).toDF("pk", "salary", "dep", "active"),
    clauses = Seq(
      update(set = "dep='updated', active=s.active", condition = "s.salary > 450"),
      insert(values = "(pk, salary, dep, active) VALUES (s.pk, s.salary, s.dep, s.active)",
        condition = "s.active = true")
    ),
    expected = Seq[(Int, Int, String, java.lang.Boolean)](
      (1, 100, "hr", null),
      (2, 200, "software", null),
      (3, 300, "hr", null),
      (4, 400, "marketing", null),  // pk=4 not updated (salary 450 is not > 450)
      (5, 500, "updated", true),    // pk=5 updated (salary 550 > 450)
      (6, 350, "sales", true)).toDF("pk", "salary", "dep", "active"), // pk=6 inserted
    // pk=7 not inserted (active = false)
    expectErrorWithoutEvolutionContains = "A column, variable, or function parameter with name " +
      "`active` cannot be resolved"
  )

  // Condition references t.active which doesn't exist yet in target
  testEvolution("source has extra column with condition on new column")(
    targetData = Seq(
      (1, 100, "hr"),
      (2, 200, "software"),
      (3, 300, "hr"),
      (4, 400, "marketing"),
      (5, 500, "executive")
    ).toDF("pk", "salary", "dep"),
    sourceData = Seq(
      (4, 450, "finance", true),
      (5, 550, "finance", false),
      (6, 350, "sales", true)
    ).toDF("pk", "salary", "dep", "active"),
    clauses = Seq(
      update(set = "salary=s.salary, dep=s.dep, active=s.active", condition = "t.active IS NULL"),
      insert(values = "(pk, salary, dep, active) VALUES (s.pk, s.salary, s.dep, s.active)")
    ),
    expected = Seq[(Int, Int, String, java.lang.Boolean)](
      (1, 100, "hr", null),
      (2, 200, "software", null),
      (3, 300, "hr", null),
      (4, 450, "finance", true),   // Updated (t.active was NULL)
      (5, 550, "finance", false),  // Updated (t.active was NULL)
      (6, 350, "sales", true)).toDF("pk", "salary", "dep", "active"),  // Inserted
    expectErrorWithoutEvolutionContains = "A column, variable, or function parameter with name " +
      "`active` cannot be resolved"
  )

  testEvolution("source has extra column with set all columns")(
    targetData = Seq(
      (1, 100, "hr"),
      (2, 200, "software"),
      (3, 300, "hr"),
      (4, 400, "marketing"),
      (5, 500, "executive")
    ).toDF("pk", "salary", "dep"),
    sourceData = Seq(
      (4, 150, "finance", true),
      (5, 250, "finance", false),
      (6, 350, "finance", true)
    ).toDF("pk", "salary", "dep", "active"),
    clauses = Seq(
      updateAll(),
      insertAll()
    ),
    expected = Seq[(Int, Int, String, java.lang.Boolean)](
      (1, 100, "hr", null),
      (2, 200, "software", null),
      (3, 300, "hr", null),
      (4, 150, "finance", true),
      (5, 250, "finance", false),
      (6, 350, "finance", true)).toDF("pk", "salary", "dep", "active"),
    // Without schema evolution clause, new columns are not added
    expectedWithoutEvolution = Seq(
      (1, 100, "hr"),
      (2, 200, "software"),
      (3, 300, "hr"),
      (4, 150, "finance"),
      (5, 250, "finance"),
      (6, 350, "finance")).toDF("pk", "salary", "dep")
  )

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

  testNestedStructsEvolution("source has extra nested struct field and set all columns")(
    target = Seq(
      """{ "pk": 1, "s": { "c1": 2, "c2": { "a": [1,2], "m": { "a": "b" } } }, "dep": "hr" }"""
    ),
    source = Seq(
      """{ "pk": 1, "s": { "c1": 10, "c2": { "a": [3,4], "m": { "c": "d" }, "c3": false } },
        | "dep": "sales" }""".stripMargin.replace("\n", ""),
      """{ "pk": 2, "s": { "c1": 20, "c2": { "a": [4,5], "m": { "e": "f" }, "c3": true } },
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
          StructField("a", ArrayType(IntegerType)),
          StructField("m", MapType(StringType, StringType)),
          StructField("c3", BooleanType)
        )))
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insertAll()),
    result = Seq(
      """{ "pk": 1, "s": { "c1": 10, "c2": { "a": [3,4], "m": { "c": "d" }, "c3": false } },
        | "dep": "sales" }""".stripMargin.replace("\n", ""),
      """{ "pk": 2, "s": { "c1": 20, "c2": { "a": [4,5], "m": { "e": "f" }, "c3": true } },
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
      "Cannot write extra fields `c3` to the struct `s`.`c2`"
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

  // Type widening from SMALLINT to INT
  testEvolution("type widening from short to int")(
    targetData = {
      val schema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("salary", ShortType),
        StructField("dep", StringType)
      ))
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(
        Row(1, 100.toShort, "hr"),
        Row(2, 200.toShort, "finance"),
        Row(3, 300.toShort, "engineering")
      )), schema)
    },
    sourceData = Seq(
      (1, 50000, "hr"),
      (4, 40000, "sales"),
      (5, 500, "marketing")
    ).toDF("pk", "salary", "dep"),
    clauses = Seq(
      update(set = "salary = s.salary"),
      insert(values = "(pk, salary, dep) VALUES (s.pk, s.salary, s.dep)")
    ),
    expected = Seq(
      (1, 50000, "hr"),
      (2, 200, "finance"),
      (3, 300, "engineering"),
      (4, 40000, "sales"),
      (5, 500, "marketing")).toDF("pk", "salary", "dep"),
    expectedSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("salary", IntegerType),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains =
      "Fail to assign a value of \"INT\" type to the \"SMALLINT\" " +
        "type column or variable `salary` due to an overflow"
  )

  testNestedStructsEvolution("type widening nested struct from int to long")(
    target = Seq(
      """{ "pk": 1, "employee": { "salary": 50000, "details": { "bonus": 5000, "years": 2 } },
        | "dep": "hr" }""".stripMargin.replace("\n", ""),
      """{ "pk": 2, "employee": { "salary": 60000, "details": { "bonus": 6000, "years": 3 } },
        | "dep": "finance" }""".stripMargin.replace("\n", "")
    ),
    source = Seq(
      // Source has long values that exceed int range for nested bonus field
      """{ "pk": 1, "employee": { "salary": 75000, "details": { "bonus": 3000000000, "years": 5 } },
        | "dep": "hr" }""".stripMargin.replace("\n", ""),
      """{ "pk": 3, "employee": { "salary": 80000, "details": { "bonus": 4000000000, "years": 1 } },
        | "dep": "engineering" }""".stripMargin.replace("\n", "")
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("employee", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("details", StructType(Seq(
          StructField("bonus", IntegerType),
          StructField("years", IntegerType)
        )))
      ))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("employee", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("details", StructType(Seq(
          StructField("bonus", LongType), // Changed from INT to LONG
          StructField("years", IntegerType)
        )))
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insert("(pk, employee, dep) VALUES (s.pk, s.employee, s.dep)")),
    result = Seq(
      """{ "pk": 1, "employee": { "salary": 75000, "details": { "bonus": 3000000000, "years": 5 } },
        | "dep": "hr" }""".stripMargin.replace("\n", ""),
      """{ "pk": 2, "employee": { "salary": 60000, "details": { "bonus": 6000, "years": 3 } },
        | "dep": "finance" }""".stripMargin.replace("\n", ""),
      """{ "pk": 3, "employee": { "salary": 80000, "details": { "bonus": 4000000000, "years": 1 } },
        | "dep": "engineering" }""".stripMargin.replace("\n", "")
    ),
    // Schema with bonus widened to LongType
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("employee", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("details", StructType(Seq(
          StructField("bonus", LongType),
          StructField("years", IntegerType)
        )))
      ))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains =
      "Fail to assign a value of \"BIGINT\" type to the \"INT\" type column or variable"
  )

  testNestedStructsEvolution("type widening in array from int to long")(
    target = Seq(
      """{ "pk": 1, "scores": [1000, 2000, 3000], "dep": "hr" }""",
      """{ "pk": 2, "scores": [4000, 5000, 6000], "dep": "finance" }"""
    ),
    source = Seq(
      // Source has array of long values that exceed int range
      """{ "pk": 1, "scores": [3000000000, 4000000000], "dep": "hr" }""",
      """{ "pk": 3, "scores": [5000000000, 6000000000], "dep": "engineering" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("scores", ArrayType(IntegerType)),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("scores", ArrayType(LongType)), // Changed from INT to LONG
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insert("(pk, scores, dep) VALUES (s.pk, s.scores, s.dep)")),
    result = Seq(
      """{ "pk": 1, "scores": [3000000000, 4000000000], "dep": "hr" }""",
      """{ "pk": 2, "scores": [4000, 5000, 6000], "dep": "finance" }""",
      """{ "pk": 3, "scores": [5000000000, 6000000000], "dep": "engineering" }"""
    ),
    // Schema with scores array element widened to LongType
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("scores", ArrayType(LongType)),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains =
      "Fail to assign a value of \"BIGINT\" type to the \"INT\" type column or variable"
  )

  testNestedStructsEvolution("type widening in map from int to long")(
    target = Seq(
      """{ "pk": 1, "metrics": {"revenue": 100000, "profit": 50000}, "dep": "hr" }""",
      """{ "pk": 2, "metrics": {"revenue": 200000, "profit": 80000}, "dep": "finance" }"""
    ),
    source = Seq(
      // Source has map values that exceed int range
      """{ "pk": 1, "metrics": {"revenue": 3000000000, "profit": 1500000000}, "dep": "hr" }""",
      """{ "pk": 3, "metrics": {"revenue": 4000000000, "profit": 2000000000},
        | "dep": "engineering" }""".stripMargin.replace("\n", "")
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("metrics", MapType(StringType, IntegerType)),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("metrics", MapType(StringType, LongType)), // Changed from INT to LONG
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insert("(pk, metrics, dep) VALUES (s.pk, s.metrics, s.dep)")),
    result = Seq(
      """{ "pk": 1, "metrics": {"revenue": 3000000000, "profit": 1500000000}, "dep": "hr" }""",
      """{ "pk": 2, "metrics": {"revenue": 200000, "profit": 80000}, "dep": "finance" }""",
      """{ "pk": 3, "metrics": {"revenue": 4000000000, "profit": 2000000000},
        | "dep": "engineering" }""".stripMargin.replace("\n", "")
    ),
    // Schema with map value type widened to LongType
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("metrics", MapType(StringType, LongType)),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains =
      "Fail to assign a value of \"BIGINT\" type to the \"INT\" type column or variable"
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

  // Both with and without evolution fail, but with different errors
  testNestedStructsEvolution("type promotion from int to struct not allowed")(
    target = Seq(
      """{ "pk": 1, "data": 100, "dep": "test" }""",
      """{ "pk": 2, "data": 200, "dep": "sample" }"""
    ),
    source = Seq(
      // Source tries to promote INT to STRUCT - not allowed
      """{ "pk": 1, "data": { "value": 150, "timestamp": 1634567890 }, "dep": "test" }""",
      """{ "pk": 3, "data": { "value": 300, "timestamp": 1634567900 }, "dep": "new" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("data", IntegerType),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("data", StructType(Seq(
        StructField("value", IntegerType),
        StructField("timestamp", LongType)
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insertAll()),
    // Both cases fail with different errors
    expectErrorContains = "Failed to merge incompatible schemas",
    expectErrorWithoutEvolutionContains = "Cannot write incompatible data for the table"
  )

  testNestedStructsEvolution("source has extra nested struct field and set explicit columns")(
    target = Seq(
      """{ "pk": 1, "s": { "c1": 2, "c2": { "a": [1,2], "m": { "a": "b" } } }, "dep": "hr" }"""
    ),
    source = Seq(
      """{ "pk": 1, "s": { "c1": 10, "c2": { "a": [3,4], "m": { "c": "d" }, "c3": false } },
        | "dep": "sales" }""".stripMargin.replace("\n", ""),
      """{ "pk": 2, "s": { "c1": 20, "c2": { "a": [4,5], "m": { "e": "f" }, "c3": true } },
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
          StructField("a", ArrayType(IntegerType)),
          StructField("m", MapType(StringType, StringType)),
          StructField("c3", BooleanType)
        )))
      ))),
      StructField("dep", StringType)
    )),
    cond = "t.pk = s.pk",
    clauses = Seq(
      update(set = "s.c1 = -1, s.c2.m = map('k', 'v'), s.c2.a = array(-1), s.c2.c3 = s.s.c2.c3"),
      insert(values = """(pk, s, dep) VALUES (s.pk,
        |named_struct('c1', s.s.c1, 'c2', named_struct('a', s.s.c2.a, 'm', map('g', 'h'),
        |'c3', true)), s.dep)""".stripMargin.replace("\n", " "))
    ),
    result = Seq(
      """{ "pk": 1, "s": { "c1": -1, "c2": { "a": [-1], "m": { "k": "v" }, "c3": false } },
        | "dep": "hr" }""".stripMargin.replace("\n", ""),
      """{ "pk": 2, "s": { "c1": 20, "c2": { "a": [4,5], "m": { "g": "h" }, "c3": true } },
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

  testNestedStructsEvolution("source has extra field for struct in array and set all columns")(
    target = Seq(
      """{ "pk": 0, "a": [{ "c1": 1, "c2": "a" }, { "c1": 2, "c2": "b" }], "dep": "sales" }""",
      """{ "pk": 1, "a": [{ "c1": 1, "c2": "a" }, { "c1": 2, "c2": "b" }], "dep": "hr" }"""
    ),
    source = Seq(
      // Source has new column c3 in array element struct
      """{ "pk": 1, "a": [{ "c1": 10, "c2": "c", "c3": true },
        | { "c1": 20, "c2": "d", "c3": false }], "dep": "hr" }""".stripMargin.replace("\n", ""),
      """{ "pk": 2, "a": [{ "c1": 30, "c2": "d", "c3": false },
        | { "c1": 40, "c2": "e", "c3": true }],
        | "dep": "engineering" }""".stripMargin.replace("\n", "")
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("a", ArrayType(StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StringType)
      )))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("a", ArrayType(StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StringType),
        StructField("c3", BooleanType) // new column
      )))),
      StructField("dep", StringType)
    )),
    cond = "t.pk = s.pk",
    clauses = Seq(updateAll(), insertAll()),
    result = Seq(
      """{ "pk": 0, "a": [{ "c1": 1, "c2": "a", "c3": null },
        | { "c1": 2, "c2": "b", "c3": null }], "dep": "sales" }""".stripMargin.replace("\n", ""),
      """{ "pk": 1, "a": [{ "c1": 10, "c2": "c", "c3": true },
        | { "c1": 20, "c2": "d", "c3": false }], "dep": "hr" }""".stripMargin.replace("\n", ""),
      """{ "pk": 2, "a": [{ "c1": 30, "c2": "d", "c3": false },
        | { "c1": 40, "c2": "e", "c3": true }],
        | "dep": "engineering" }""".stripMargin.replace("\n", "")
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("a", ArrayType(StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StringType),
        StructField("c3", BooleanType)
      )))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "Cannot write extra fields"
  )

  testEvolution("source has extra field for struct in map and set all columns")(
    targetData = {
      val schema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("m", MapType(
          StructType(Seq(StructField("c1", IntegerType))),
          StructType(Seq(StructField("c2", StringType))))),
        StructField("dep", StringType)))
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(
        Row(0, Map(Row(10) -> Row("c")), "hr"),
        Row(1, Map(Row(20) -> Row("d")), "sales")
      )), schema)
    },
    sourceData = {
      val schema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("m", MapType(
          StructType(Seq(StructField("c1", IntegerType), StructField("c3", BooleanType))),
          StructType(Seq(StructField("c2", StringType), StructField("c4", BooleanType))))),
        StructField("dep", StringType)))
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(
        Row(1, Map(Row(10, true) -> Row("y", false)), "sales"),
        Row(2, Map(Row(20, false) -> Row("z", true)), "engineering")
      )), schema)
    },
    clauses = Seq(updateAll(), insertAll()),
    expected = Seq(
      (0, Map((10, null: java.lang.Boolean) -> ("c", null: java.lang.Boolean)), "hr"),
      (1, Map((10, true: java.lang.Boolean) -> ("y", false: java.lang.Boolean)), "sales"),
      (2, Map((20, false: java.lang.Boolean) -> ("z", true: java.lang.Boolean)), "engineering")
    ).toDF("pk", "m", "dep"),
    expectErrorWithoutEvolutionContains = "Cannot write extra fields"
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

  testEvolution("source has extra column - empty table and insert")(
    targetData = Seq.empty[(Int, Int, String)].toDF("pk", "salary", "dep"),
    sourceData = Seq(
      (1, 100, "hr", true),
      (2, 200, "finance", false),
      (3, 300, "hr", true)
    ).toDF("pk", "salary", "dep", "active"),
    clauses = Seq(insertAll()),
    expected = Seq(
      (1, 100, "hr", true),
      (2, 200, "finance", false),
      (3, 300, "hr", true)).toDF("pk", "salary", "dep", "active"),
    expectedWithoutEvolution = Seq(
      (1, 100, "hr"),
      (2, 200, "finance"),
      (3, 300, "hr")).toDF("pk", "salary", "dep")
  )

  // Schema evolution should not evolve when referencing new column via transform (e.g., substring)
  testEvolution("source has extra column -" +
    "should not evolve referencing new column via transform")(
    targetData = Seq(
      (1, 100, "hr"),
      (2, 200, "software")
    ).toDF("pk", "salary", "dep"),
    sourceData = Seq(
      (2, 150, "dummy", "blah"),
      (3, 250, "dummy", "blah")
    ).toDF("pk", "salary", "dep", "extra"),
    clauses = Seq(
      update(set = "extra=substring(s.extra, 1, 2)")
    ),
    expectErrorContains = "A column, variable, or function parameter with name " +
      "`extra` cannot be resolved",
    expectErrorWithoutEvolutionContains = "A column, variable, or function parameter with name " +
      "`extra` cannot be resolved"
  )

  // Schema should not evolve when update doesn't reference new columns directly
  testEvolution("source has extra column -" +
    "should not evolve if not directly referencing new column: update")(
    targetData = Seq(
      (1, 100, "hr"),
      (2, 200, "software")
    ).toDF("pk", "salary", "dep"),
    sourceData = Seq(
      (2, 150, "dummy", "blah"),
      (3, 250, "dummy", "blah")
    ).toDF("pk", "salary", "dep", "extra"),
    clauses = Seq(
      update(set = "dep='software'")
    ),
    expected = Seq(
      (1, 100, "hr"),
      (2, 200, "software")).toDF("pk", "salary", "dep"),
    expectedWithoutEvolution = Seq(
      (1, 100, "hr"),
      (2, 200, "software")).toDF("pk", "salary", "dep")
  )

  // Schema should not evolve when insert doesn't reference new columns directly
  testEvolution("source has extra column -" +
    "should not evolve if not directly referencing new column: insert")(
    targetData = Seq(
      (1, 100, "hr"),
      (2, 200, "software")
    ).toDF("pk", "salary", "dep"),
    sourceData = Seq(
      (2, 150, "dummy", "blah"),
      (3, 250, "dummy", "blah")
    ).toDF("pk", "salary", "dep", "extra"),
    clauses = Seq(
      insert(values = "(pk, salary, dep) VALUES (s.pk, s.salary, 'newdep')")
    ),
    expected = Seq(
      (1, 100, "hr"),
      (2, 200, "software"),
      (3, 250, "newdep")).toDF("pk", "salary", "dep"),
    expectedWithoutEvolution = Seq(
      (1, 100, "hr"),
      (2, 200, "software"),
      (3, 250, "newdep")).toDF("pk", "salary", "dep")
  )

  // Schema should not evolve when neither update nor insert reference new columns directly
  testEvolution("source has extra column -" +
    "should not evolve if not directly referencing new column: update and insert")(
    targetData = Seq(
      (1, 100, "hr"),
      (2, 200, "software")
    ).toDF("pk", "salary", "dep"),
    sourceData = Seq(
      (2, 150, "dummy", "blah"),
      (3, 250, "dummy", "blah")
    ).toDF("pk", "salary", "dep", "extra"),
    clauses = Seq(
      update(set = "dep='software'"),
      insert(values = "(pk, salary, dep) VALUES (s.pk, s.salary, 'newdep')")
    ),
    expected = Seq(
      (1, 100, "hr"),
      (2, 200, "software"),
      (3, 250, "newdep")).toDF("pk", "salary", "dep"),
    expectedWithoutEvolution = Seq(
      (1, 100, "hr"),
      (2, 200, "software"),
      (3, 250, "newdep")).toDF("pk", "salary", "dep")
  )

  // Schema should not evolve when using qualified column name (t.extra instead of just extra)
  testEvolution("source has extra column" +
    "should not evolve if set not just column name: update")(
    targetData = Seq(
      (1, 100, "hr"),
      (2, 200, "software")
    ).toDF("pk", "salary", "dep"),
    sourceData = Seq(
      (2, 150, "dummy", "blah"),
      (3, 250, "dummy", "blah")
    ).toDF("pk", "salary", "dep", "extra"),
    clauses = Seq(
      update(set = "t.extra = s.extra")
    ),
    expectErrorContains = "A column, variable, or function parameter with name " +
      "`t`.`extra` cannot be resolved",
    expectErrorWithoutEvolutionContains = "A column, variable, or function parameter with name " +
      "`t`.`extra` cannot be resolved"
  )

  // Only referenced column (bonus) should be evolved, not extra
  testEvolution("source has multiple extra columns -" +
    "only evolve referenced column")(
    targetData = Seq(
      (1, 100, "hr"),
      (2, 200, "software")
    ).toDF("pk", "salary", "dep"),
    sourceData = Seq(
      (2, 150, "dummy", 50, "blah"),
      (3, 250, "dummy", 75, "blah")
    ).toDF("pk", "salary", "dep", "bonus", "extra"),
    clauses = Seq(
      update(set = "salary = s.salary, bonus = s.bonus"),
      insert(values = "(pk, salary, dep, bonus) VALUES (s.pk, s.salary, 'newdep', s.bonus)")
    ),
    expected = Seq[(Int, Int, String, java.lang.Integer)](
      (1, 100, "hr", null),
      (2, 150, "software", 50),
      (3, 250, "newdep", 75)).toDF("pk", "salary", "dep", "bonus"),
    expectErrorWithoutEvolutionContains = "A column, variable, or function parameter with name " +
      "`bonus` cannot be resolved"
  )
}
