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

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Base trait containing merge into schema evolution tests.
 */
trait MergeIntoSchemaEvolutionTests extends MergeIntoSchemaEvolutionSuiteBase {

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
  // Tests with auto-schema-evolution table property DISABLED
  // These test that evolution fails even with WITH SCHEMA EVOLUTION clause
  // when the table property 'auto-schema-evolution' is set to 'false'
  // ---------------------------------------------------------------------------

  testEvolution("source has extra column with set explicit column - " +
    "no auto-schema-evolution capability")(
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
    disableAutoSchemaEvolution = true,
    expectErrorContains = "A column, variable, or function parameter with name " +
      "`active` cannot be resolved",
    expectErrorWithoutEvolutionContains = "A column, variable, or function parameter with name " +
      "`active` cannot be resolved"
  )

  testEvolution("source has extra column with set all columns - " +
    "no auto-schema-evolution capability")(
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
    disableAutoSchemaEvolution = true,
    // Without property enabled, new columns are not added even with clause
    expected = Seq(
      (1, 100, "hr"),
      (2, 200, "software"),
      (3, 300, "hr"),
      (4, 150, "finance"),
      (5, 250, "finance"),
      (6, 350, "finance")).toDF("pk", "salary", "dep"),
    expectedWithoutEvolution = Seq(
      (1, 100, "hr"),
      (2, 200, "software"),
      (3, 300, "hr"),
      (4, 150, "finance"),
      (5, 250, "finance"),
      (6, 350, "finance")).toDF("pk", "salary", "dep")
  )

  testEvolution("source has extra and missing column with set all column -" +
    "no auto-schema-evolution capability")(
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
    disableAutoSchemaEvolution = true,
    expectErrorContains = "A column, variable, or function parameter with name " +
      "`dep` cannot be resolved",
    expectErrorWithoutEvolutionContains = "A column, variable, or function parameter with name " +
      "`dep` cannot be resolved"
  )

  testEvolution("source has extra and missing column with set explicit column -" +
    "no auto-schema-evolution capability")(
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
    disableAutoSchemaEvolution = true,
    expectErrorContains = "A column, variable, or function parameter with name " +
      "`active` cannot be resolved",
    expectErrorWithoutEvolutionContains = "A column, variable, or function parameter with name " +
      "`active` cannot be resolved"
  )

  testEvolution("type widening from short to int - no auto-schema-evolution capability")(
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
    disableAutoSchemaEvolution = true,
    expectErrorContains = "Fail to assign a value of \"INT\" type to the \"SMALLINT\" " +
      "type column or variable `salary` due to an overflow",
    expectErrorWithoutEvolutionContains =
      "Fail to assign a value of \"INT\" type to the \"SMALLINT\" " +
        "type column or variable `salary` due to an overflow"
  )

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

  // Only referenced struct field should be evolved, not all new fields
  testNestedStructsEvolution("source has extra struct fields -" +
    "only evolve referenced struct field")(
    target = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "inactive" }, "dep": "software" }"""
    ),
    source = Seq(
      // Source has two new fields: bonus and extra
      """{ "pk": 2, "info": { "salary": 150, "status": "dummy", "bonus": 50, "extra": "blah" },
        | "dep": "active" }""".stripMargin.replace("\n", ""),
      """{ "pk": 3, "info": { "salary": 250, "status": "dummy", "bonus": 75, "extra": "blah" },
        | "dep": "active" }""".stripMargin.replace("\n", "")
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("status", StringType)
      ))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("status", StringType),
        StructField("bonus", IntegerType), // extra field not in target
        StructField("extra", StringType) // extra field not in target
      ))),
      StructField("dep", StringType)
    )),
    // Only update the bonus field - should only add 'bonus', not 'extra'
    clauses = Seq(update("info.bonus = s.info.bonus")),
    result = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active", "bonus": null }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "inactive", "bonus": 50 },
        | "dep": "software" }""".stripMargin.replace("\n", "")
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("status", StringType),
        StructField("bonus", IntegerType) // only bonus added, not extra
      ))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "No such struct field"
  )

  // When assigning s.bonus to existing t.salary, bonus column should NOT be added to schema
  testEvolution("source has extra column -" +
    "should not evolve when assigning existing target column from extra source column")(
    targetData = Seq(
      (1, 100, "hr"),
      (2, 200, "software")
    ).toDF("pk", "salary", "dep"),
    sourceData = Seq(
      (2, 150, "dummy", 50),
      (3, 250, "dummy", 75)
    ).toDF("pk", "salary", "dep", "bonus"),
    clauses = Seq(
      update(set = "salary = s.bonus"),
      insert(values = "(pk, salary, dep) VALUES (s.pk, s.bonus, 'newdep')")
    ),
    expected = Seq(
      (1, 100, "hr"),
      (2, 50, "software"),
      (3, 75, "newdep")).toDF("pk", "salary", "dep"),
    expectedWithoutEvolution = Seq(
      (1, 100, "hr"),
      (2, 50, "software"),
      (3, 75, "newdep")).toDF("pk", "salary", "dep")
  )

  // No evolution when using named_struct to construct value without referencing new field
  testNestedStructsEvolution("source has extra struct field -" +
    "no evolution when not directly referencing new field - INSERT")(
    target = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "inactive" }, "dep": "software" }"""
    ),
    source = Seq(
      // Source has new 'bonus' field not in target
      """{ "pk": 2, "info": { "salary": 150, "status": "dummy", "bonus": 50 }, "dep": "active" }""",
      """{ "pk": 3, "info": { "salary": 250, "status": "dummy", "bonus": 75 }, "dep": "active" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("status", StringType)
      ))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("status", StringType),
        StructField("bonus", IntegerType) // extra field not in target
      ))),
      StructField("dep", StringType)
    )),
    // INSERT uses named_struct without referencing the new 'bonus' field
    clauses = Seq(insert(
      "(pk, info, dep) VALUES (s.pk, named_struct('salary', s.info.salary, 'status', 'active'), " +
        "'marketing')")),
    // Same result for both - no schema evolution because new field not directly referenced
    result = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "inactive" }, "dep": "software" }""",
      """{ "pk": 3, "info": { "salary": 250, "status": "active" }, "dep": "marketing" }"""
    ),
    resultWithoutEvolution = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "inactive" }, "dep": "software" }""",
      """{ "pk": 3, "info": { "salary": 250, "status": "active" }, "dep": "marketing" }"""
    )
  )

  // No schema evolution when not directly referencing new field
  testNestedStructsEvolution("source has extra struct field -" +
    "no evolution when not directly referencing new field - UPDATE")(
    target = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "inactive" }, "dep": "software" }"""
    ),
    source = Seq(
      // Source has new 'bonus' field not in target, but we only update info.status
      """{ "pk": 2, "info": { "salary": 150, "status": "dummy", "bonus": 50 }, "dep": "active" }""",
      """{ "pk": 3, "info": { "salary": 250, "status": "dummy", "bonus": 75 }, "dep": "active" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("status", StringType)
      ))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("status", StringType),
        StructField("bonus", IntegerType) // extra field not in target
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(update("info.status = 'inactive'")),
    // Same result for both - no schema evolution because new field not directly assigned
    result = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "inactive" }, "dep": "software" }"""
    ),
    resultWithoutEvolution = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "inactive" }, "dep": "software" }"""
    )
  )

  testNestedStructsEvolution("source has extra struct field -" +
    "evolve when directly assigning struct - UPDATE")(
    target = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "inactive" }, "dep": "software" }"""
    ),
    source = Seq(
      // Source has new 'bonus' field, and we assign the whole struct
      """{ "pk": 2, "info": { "salary": 150, "status": "updated", "bonus": 50 },
        | "dep": "engineering" }""".stripMargin.replace("\n", "")
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("status", StringType)
      ))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("status", StringType),
        StructField("bonus", IntegerType) // extra field not in target
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(update("info = s.info")),
    result = Seq(
      // Schema evolves - bonus field added, null for non-matched row
      """{ "pk": 1, "info": { "salary": 100, "status": "active", "bonus": null }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 150, "status": "updated", "bonus": 50 },
        | "dep": "software" }""".stripMargin.replace("\n", "")
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("status", StringType),
        StructField("bonus", IntegerType)
      ))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "Cannot write extra fields `bonus` to the struct `info`"
  )

  testNestedStructsEvolution("source has extra struct field -" +
    "evolve when directly assigning struct - INSERT")(
    target = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "inactive" }, "dep": "software" }"""
    ),
    source = Seq(
      // Source has new 'bonus' field, and we insert with explicit columns
      """{ "pk": 3, "info": { "salary": 150, "status": "new", "bonus": 50 },
        | "dep": "engineering" }""".stripMargin.replace("\n", "")
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("status", StringType)
      ))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("status", StringType),
        StructField("bonus", IntegerType) // extra field not in target
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(insert("(pk, info, dep) VALUES (s.pk, s.info, s.dep)")),
    result = Seq(
      // Schema evolves - bonus field added, null for existing rows
      """{ "pk": 1, "info": { "salary": 100, "status": "active", "bonus": null }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "inactive", "bonus": null },
        | "dep": "software" }""".stripMargin.replace("\n", ""),
      """{ "pk": 3, "info": { "salary": 150, "status": "new", "bonus": 50 },
        | "dep": "engineering" }""".stripMargin.replace("\n", "")
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("status", StringType),
        StructField("bonus", IntegerType)
      ))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "Cannot write extra fields `bonus` to the struct `info`"
  )

  testNestedStructsEvolution("source has extra struct field -" +
    "no evolution when not directly assigning struct - UPDATE")(
    target = Seq(
      """{ "pk": 1, "employee": { "name": "Alice", "details": { "salary": 100, "status": "active" }
        | }, "dep": "hr" }""".stripMargin.replace("\n", ""),
      """{ "pk": 2, "employee": { "name": "Bob", "details": { "salary": 200, "status": "active" }
        | }, "dep": "software" }""".stripMargin.replace("\n", "")
    ),
    source = Seq(
      // Source has new 'bonus' field in nested struct, but we only update employee.details.status
      """{ "pk": 2, "employee": { "name": "Bob", "details": { "salary": 150, "status": "active",
        | "bonus": 50 } }, "dep": "dummy" }""".stripMargin.replace("\n", ""),
      """{ "pk": 3, "employee": { "name": "Charlie", "details": { "salary": 250, "status": "active",
        | "bonus": 75 } }, "dep": "dummy" }""".stripMargin.replace("\n", "")
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("employee", StructType(Seq(
        StructField("name", StringType),
        StructField("details", StructType(Seq(
          StructField("salary", IntegerType),
          StructField("status", StringType)
        )))
      ))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("employee", StructType(Seq(
        StructField("name", StringType),
        StructField("details", StructType(Seq(
          StructField("salary", IntegerType),
          StructField("status", StringType),
          StructField("bonus", IntegerType) // extra field not in target
        )))
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(update("employee.details.status = 'inactive'")),
    // Same result for both - no schema evolution because new field not directly assigned
    result = Seq(
      """{ "pk": 1, "employee": { "name": "Alice", "details": { "salary": 100, "status": "active" }
        | }, "dep": "hr" }""".stripMargin.replace("\n", ""),
      """{ "pk": 2, "employee": { "name": "Bob", "details": { "salary": 200, "status": "inactive" }
        | }, "dep": "software" }""".stripMargin.replace("\n", "")
    ),
    resultWithoutEvolution = Seq(
      """{ "pk": 1, "employee": { "name": "Alice", "details": { "salary": 100, "status": "active" }
        | }, "dep": "hr" }""".stripMargin.replace("\n", ""),
      """{ "pk": 2, "employee": { "name": "Bob", "details": { "salary": 200, "status": "inactive" }
        | }, "dep": "software" }""".stripMargin.replace("\n", "")
    )
  )

  // Schema should not evolve when referencing source extra column
  // but not assigning from corresponding source column
  testEvolution("source has extra column " +
    "should not evolve when non-direct assignment")(
    targetData = Seq(
      (1, 100, "hr"),
      (2, 200, "software")
    ).toDF("pk", "salary", "dep"),
    sourceData = Seq(
      (2, 150, "dummy", "blah"),
      (3, 250, "dummy", "blah")
    ).toDF("pk", "salary", "dep", "extra"),
    clauses = Seq(
      update(set = "extra=s.dep")
    ),
    expectErrorContains = "A column, variable, or function parameter with name " +
      "`extra` cannot be resolved",
    expectErrorWithoutEvolutionContains = "A column, variable, or function parameter with name " +
      "`extra` cannot be resolved"
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

  testNestedStructsEvolution("source with missing field in struct nested in array")(
    target = Seq(
      // Target has struct with 3 fields (c1, c2, c3) in array
      """{ "pk": 0, "a": [ { "c1": 1, "c2": "a", "c3": true } ], "dep": "sales" }""",
      """{ "pk": 1, "a": [ { "c1": 2, "c2": "b", "c3": false } ], "dep": "sales" }"""
    ),
    source = Seq(
      // Source is missing c3 field
      """{ "pk": 1, "a": [ { "c1": 10, "c2": "c" } ], "dep": "hr" }""",
      """{ "pk": 2, "a": [ { "c1": 30, "c2": "e" } ], "dep": "engineering" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("a", ArrayType(
        StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StringType),
          StructField("c3", BooleanType))))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("a", ArrayType(
        StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StringType))))), // missing c3 field
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insertAll()),
    result = Seq(
      // Unmatched target row unchanged
      """{ "pk": 0, "a": [ { "c1": 1, "c2": "a", "c3": true } ], "dep": "sales" }""",
      // Matched row: c3 filled with null
      """{ "pk": 1, "a": [ { "c1": 10, "c2": "c", "c3": null } ], "dep": "hr" }""",
      // Not matched row: c3 filled with null
      """{ "pk": 2, "a": [ { "c1": 30, "c2": "e", "c3": null } ], "dep": "engineering" }"""
    ),
    expectErrorWithoutEvolutionContains = "Cannot find data for the output column",
    requiresNestedTypeCoercion = true
  )

  testEvolution("source missing field in struct nested in map key")(
    targetData = {
      val schema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("m", MapType(
          StructType(Seq(StructField("c1", IntegerType), StructField("c2", BooleanType))),
          StructType(Seq(StructField("c3", StringType))))),
        StructField("dep", StringType)))
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(
        Row(0, Map(Row(10, true) -> Row("x")), "hr"),
        Row(1, Map(Row(20, false) -> Row("y")), "sales")
      )), schema)
    },
    sourceData = {
      // Source has struct with only 1 field (c1) in map key - missing c2
      val schema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("m", MapType(
          StructType(Seq(StructField("c1", IntegerType))), // missing c2
          StructType(Seq(StructField("c3", StringType))))),
        StructField("dep", StringType)))
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(
        Row(1, Map(Row(10) -> Row("z")), "sales"),
        Row(2, Map(Row(20) -> Row("w")), "engineering")
      )), schema)
    },
    clauses = Seq(updateAll(), insertAll()),
    expected = Seq(
      // Missing field c2 filled with null
      (0, Map((10, true: java.lang.Boolean) -> Tuple1("x")), "hr"),
      (1, Map((10, null: java.lang.Boolean) -> Tuple1("z")), "sales"),
      (2, Map((20, null: java.lang.Boolean) -> Tuple1("w")), "engineering")
    ).toDF("pk", "m", "dep"),
    expectErrorWithoutEvolutionContains = "Cannot find data for the output column",
    requiresNestedTypeCoercion = true
  )

  testEvolution("source missing fields in struct nested in map value")(
    targetData = {
      val schema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("m", MapType(
          StructType(Seq(StructField("c1", IntegerType))),
          StructType(Seq(StructField("c1", StringType), StructField("c2", BooleanType))))),
        StructField("dep", StringType)))
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(
        Row(0, Map(Row(10) -> Row("x", true)), "hr"),
        Row(1, Map(Row(20) -> Row("y", false)), "sales")
      )), schema)
    },
    sourceData = {
      // Source has struct with only 1 field (c1) in map value - missing c2
      val schema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("m", MapType(
          StructType(Seq(StructField("c1", IntegerType))),
          StructType(Seq(StructField("c1", StringType))))), // missing c2
        StructField("dep", StringType)))
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(
        Row(1, Map(Row(10) -> Row("z")), "sales"),
        Row(2, Map(Row(20) -> Row("w")), "engineering")
      )), schema)
    },
    clauses = Seq(updateAll(), insertAll()),
    expected = Seq(
      // Missing field c2 filled with null
      (0, Map(Tuple1(10) -> ("x", true: java.lang.Boolean)), "hr"),
      (1, Map(Tuple1(10) -> ("z", null: java.lang.Boolean)), "sales"),
      (2, Map(Tuple1(20) -> ("w", null: java.lang.Boolean)), "engineering")
    ).toDF("pk", "m", "dep"),
    expectErrorWithoutEvolutionContains = "Cannot find data for the output column",
    requiresNestedTypeCoercion = true
  )

  testNestedStructsEvolution("source missing fields in top-level struct")(
    target = Seq(
      """{ "pk": 0, "s": { "c1": 1, "c2": "a", "c3": true }, "dep": "sales" }"""
    ),
    source = Seq(
      // Source is missing c3 field
      """{ "pk": 1, "s": { "c1": 10, "c2": "b" }, "dep": "hr" }""",
      """{ "pk": 2, "s": { "c1": 20, "c2": "c" }, "dep": "engineering" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StringType),
        StructField("c3", BooleanType)
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
      """{ "pk": 0, "s": { "c1": 1, "c2": "a", "c3": true }, "dep": "sales" }""",
      // Missing c3 filled with null
      """{ "pk": 1, "s": { "c1": 10, "c2": "b", "c3": null }, "dep": "hr" }""",
      """{ "pk": 2, "s": { "c1": 20, "c2": "c", "c3": null }, "dep": "engineering" }"""
    ),
    expectErrorWithoutEvolutionContains = "Cannot find data for the output column",
    requiresNestedTypeCoercion = true
  )

  // Source missing top-level column - with evolution preserves target value, without fails
  testEvolution("source missing top-level column")(
    targetData = {
      val schema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("salary", IntegerType, nullable = true),
        StructField("dep", StringType)
      ))
      val data = Seq(
        Row(0, 100, "sales"),
        Row(1, 200, "hr"))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    },
    sourceData = Seq(
      (1, "engineering"),
      (2, "finance")
    ).toDF("pk", "dep"),
    clauses = Seq(updateAll(), insertAll()),
    expected = Seq[(Int, java.lang.Integer, String)](
      (0, 100, "sales"),
      (1, 200, "engineering"),
      (2, null, "finance")).toDF("pk", "salary", "dep"),
    expectErrorWithoutEvolutionContains = "A column, variable, or function parameter with name " +
      "`salary` cannot be resolved"
  )

  testNestedStructsEvolution("source missing struct field - source has struct of nulls")(
    target = Seq(
      // Target has struct with 3 fields
      """{ "pk": 0, "s": { "c1": 1, "c2": "a", "c3": 10 }, "dep": "sales" }""",
      """{ "pk": 1, "s": { "c1": 2, "c2": "b", "c3": 20 }, "dep": "hr" }"""
    ),
    source = Seq(
      // Source has struct with null values (not a null struct), missing c3
      """{ "pk": 1, "s": { "c1": null, "c2": null }, "dep": "engineering" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StringType),
        StructField("c3", IntegerType)
      ))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StringType)
        // missing field c3
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insertAll()),
    result = Seq(
      """{ "pk": 0, "s": { "c1": 1, "c2": "a", "c3": 10 }, "dep": "sales" }""",
      // Struct of null values preserved, c3 preserved from target
      """{ "pk": 1, "s": { "c1": null, "c2": null, "c3": 20 }, "dep": "engineering" }"""
    ),
    expectErrorWithoutEvolutionContains = "Cannot find data for the output column",
    requiresNestedTypeCoercion = true
  )

  testNestedStructsEvolution("source has missing struct field - source has struct of nulls" +
    "and target has null struct")(
    target = Seq(
      // Target has struct with 3 fields, row 1 has null for c3
      """{ "pk": 0, "s": { "c1": 1, "c2": "a", "c3": 10 }, "dep": "sales" }""",
      """{ "pk": 1, "s": { "c1": 2, "c2": "b", "c3": null }, "dep": "hr" }"""
    ),
    source = Seq(
      // Source has struct with null values (not a null struct), missing c3
      """{ "pk": 1, "s": { "c1": null, "c2": null }, "dep": "engineering" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StringType),
        StructField("c3", IntegerType)
      ))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StringType)
        // missing field c3
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insertAll()),
    result = Seq(
      """{ "pk": 0, "s": { "c1": 1, "c2": "a", "c3": 10 }, "dep": "sales" }""",
      // Struct of null values preserved, c3 preserved from target (which was null)
      """{ "pk": 1, "s": { "c1": null, "c2": null, "c3": null }, "dep": "engineering" }"""
    ),
    expectErrorWithoutEvolutionContains = "Cannot find data for the output column",
    requiresNestedTypeCoercion = true
  )
  testNestedStructsEvolution("source missing nested struct field - source has null struct")(
    target = Seq(
      // Target has nested struct with fields c1 and c2
      """{ "pk": 0, "s": { "c1": 1, "c2": { "a": 10, "b": "x" } }, "dep": "sales" }""",
      """{ "pk": 1, "s": { "c1": 2, "c2": { "a": 20, "b": "y" } }, "dep": "hr" }"""
    ),
    source = Seq(
      // Source has null struct, schema missing field 'b'
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
          StructField("a", IntegerType)
          // missing field 'b'
        )))
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insertAll()),
    result = Seq(
      """{ "pk": 0, "s": { "c1": 1, "c2": { "a": 10, "b": "x" } }, "dep": "sales" }""",
      // Matched: null struct coerced, c2.b preserved from target
      """{ "pk": 1, "s": { "c1": null, "c2": { "a": null, "b": "y" } }, "dep": "engineering" }""",
      // Not matched: null struct inserted
      """{ "pk": 2, "s": null, "dep": "finance" }"""
    ),
    expectErrorWithoutEvolutionContains = "Cannot find data for the output column",
    requiresNestedTypeCoercion = true
  )

  // Source missing struct column - DEFAULT value should be used for INSERT
  testEvolution("source has missing nested struct field in struct with default value")(
    targetData = {
      val defaultExpr = "named_struct('c1', 999, 'c2', named_struct('a', 999, 'b', 'default'))"
      val schema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("s", StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StructType(Seq(
            StructField("a", IntegerType),
            StructField("b", StringType)
          )))
        ))).withCurrentDefaultValue(defaultExpr).withExistenceDefaultValue(defaultExpr),
        StructField("dep", StringType)
      ))
      val data = Seq(
        Row(0, Row(1, Row(10, "x")), "sales"),
        Row(1, Row(2, Row(20, "y")), "hr"))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    },
    sourceData = Seq(
      (1, "engineering"),
      (2, "finance")
    ).toDF("pk", "dep"),
    clauses = Seq(
      update("dep = s.dep"),
      insert("(pk, dep) VALUES (s.pk, s.dep)")),
    // When inserting without specifying the struct column, default should be used
    expected = Seq(
      (0, (1, (10, "x")), "sales"),
      (1, (2, (20, "y")), "engineering"),
      (2, (999, (999, "default")), "finance")
    ).toDF("pk", "s", "dep"),
    expectedWithoutEvolution = Seq(
      (0, (1, (10, "x")), "sales"),
      (1, (2, (20, "y")), "engineering"),
      (2, (999, (999, "default")), "finance")
    ).toDF("pk", "s", "dep")
  )

  testNestedStructsEvolution("source has missing nested struct fields")(
    target = Seq(
      // Target table has nested struct: s.c1, s.c2.a, s.c2.b
      """{ "pk": 1, "s": { "c1": 2, "c2": { "a": 10, "b": true } } }""",
      """{ "pk": 2, "s": { "c1": 2, "c2": { "a": 30, "b": false } } }"""
    ),
    source = Seq(
      // Source is missing field 'b' in nested struct s.c2
      """{ "pk": 1, "s": { "c1": 10, "c2": { "a": 20 } }, "dep": "sales" }""",
      """{ "pk": 2, "s": { "c1": 20, "c2": { "a": 30 } }, "dep": "engineering" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", BooleanType)
        )))
      ))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StructType(Seq(
          StructField("a", IntegerType)
          // missing field 'b'
        )))
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insertAll()),
    result = Seq(
      // Missing field 'b' preserved from target
      """{ "pk": 1, "s": { "c1": 10, "c2": { "a": 20, "b": true } }, "dep": "sales" }""",
      """{ "pk": 2, "s": { "c1": 20, "c2": { "a": 30, "b": false } }, "dep": "engineering" }"""
    ),
    expectErrorWithoutEvolutionContains = "Cannot find data for the output column",
    requiresNestedTypeCoercion = true
  )

  testNestedStructsEvolution(
    "source has missing non-nullable struct field")(
    target = Seq(
      """{ "pk": 0, "s": { "c1": 1, "c2": "a" }, "dep": "sales" }""",
      """{ "pk": 1, "s": { "c1": 2, "c2": "b" }, "dep": "hr" }"""
    ),
    source = Seq(
      """{ "pk": 1, "s": { "c1": 10, "c2": "a" }, "dep": "engineering" }""",
      """{ "pk": 2, "s": { "c1": 20, "c2": "b" }, "dep": "finance" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StringType, nullable = false)
      ))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StringType, nullable = false)
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(
      update(set = "s = named_struct('c1', s.s.c1), dep = s.dep"),
      insert(values = "(pk, s, dep) VALUES (s.pk, named_struct('c1', 1), s.dep)")
    ),
    expectErrorContains = "Cannot find data for the output column `s`.`c2`",
    expectErrorWithoutEvolutionContains = "Cannot find data for the output column `s`.`c2`"
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

  testNestedStructsEvolution("source has missing struct field - source has null struct" +
    " and target has struct of nulls")(
    target = Seq(
      // Target has struct with 3 fields, row 1 has all nulls including extra field c3
      """{ "pk": 0, "s": { "c1": 1, "c2": "a", "c3": 10 }, "dep": "sales" }""",
      """{ "pk": 1, "s": { "c1": null, "c2": null, "c3": null }, "dep": "hr" }"""
    ),
    source = Seq(
      // Source has a null struct (not a struct of nulls), missing c3 in schema
      """{ "pk": 1, "s": null, "dep": "engineering" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StringType),
        StructField("c3", IntegerType)
      ))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StringType)
        // missing field c3
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insertAll()),
    result = Seq(
      """{ "pk": 0, "s": { "c1": 1, "c2": "a", "c3": 10 }, "dep": "sales" }""",
      // Because target has extra field c3, we preserve struct of nulls
      """{ "pk": 1, "s": { "c1": null, "c2": null, "c3": null }, "dep": "engineering" }"""
    ),
    expectErrorWithoutEvolutionContains = "Cannot find data for the output column",
    requiresNestedTypeCoercion = true
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

  // Null struct with missing nested field and default value - requires coercion
  testEvolution("null struct with missing nested field using default value")(
    targetData = {
      val defaultExpr = "named_struct('c1', 999, 'c2', named_struct('a', 999, 'b', 'default'))"
      val schema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("s", StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StructType(Seq(
            StructField("a", IntegerType),
            StructField("b", StringType)
          )))
        ))).withCurrentDefaultValue(defaultExpr).withExistenceDefaultValue(defaultExpr),
        StructField("dep", StringType)
      ))
      val data = Seq(
        Row(0, Row(1, Row(10, "x")), "sales"),
        Row(1, Row(2, Row(20, "y")), "hr"))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    },
    sourceData = {
      // Source has struct missing field s.c2.b, and some rows have null struct
      val sourceSchema = StructType(Seq(
        StructField("pk", IntegerType),
        StructField("s", StructType(Seq(
          StructField("c1", IntegerType),
          StructField("c2", StructType(Seq(
            StructField("a", IntegerType)
          )))
        ))),
        StructField("dep", StringType)
      ))
      val data = Seq(
        Row(1, null, "engineering"),
        Row(2, null, "finance"))
      spark.createDataFrame(spark.sparkContext.parallelize(data), sourceSchema)
    },
    clauses = Seq(updateAll(), insertAll()),
    expected = Seq[(Int, (java.lang.Integer, (java.lang.Integer, String)), String)](
      (0, (1, (10, "x")), "sales"),
      (1, (null, (null, "y")), "engineering"),
      (2, null, "finance")
    ).toDF("pk", "s", "dep"),
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

  testNestedStructsEvolution("source has missing struct field -" +
    "target has null value for this field")(
    target = Seq(
      // Target has nested struct, row 1 has null for field 'b' (missing in source)
      """{ "pk": 0, "s": { "c1": 1, "c2": { "a": 10, "b": "x" } }, "dep": "sales" }""",
      """{ "pk": 1, "s": { "c1": 2, "c2": { "a": 20, "b": null } }, "dep": "hr" }"""
    ),
    source = Seq(
      // Source has null struct, schema missing field 'b'
      """{ "pk": 1, "s": null, "dep": "engineering" }"""
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
          StructField("a", IntegerType)
          // missing field 'b'
        )))
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insertAll()),
    result = Seq(
      """{ "pk": 0, "s": { "c1": 1, "c2": { "a": 10, "b": "x" } }, "dep": "sales" }""",
      // Target had extra field 'b' (null), preserve struct of nulls
      """{ "pk": 1, "s": { "c1": null, "c2": { "a": null, "b": null } }, "dep": "engineering" }"""
    ),
    expectErrorWithoutEvolutionContains = "Cannot find data for the output column",
    requiresNestedTypeCoercion = true
  )

  testNestedStructsEvolution("source has missing nested struct field - " +
    "target has nested struct of nulls")(
    target = Seq(
      // Target has doubly nested struct with extra field 'y' in innermost struct
      """{ "pk": 0, "s": { "c1": 1, "c2": { "a": 10, "b": { "x": 100, "y": "foo" } } },
        | "dep": "sales" }""".stripMargin.replace("\n", ""),
      """{ "pk": 1, "s": { "c1": 2, "c2": { "a": 20, "b": { "x": 200, "y": null } } },
        | "dep": "hr" }""".stripMargin.replace("\n", "")
    ),
    source = Seq(
      // Source has null struct, schema missing field 'y' in innermost struct
      """{ "pk": 1, "s": null, "dep": "engineering" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", StructType(Seq(
            StructField("x", IntegerType),
            StructField("y", StringType)
          )))
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
          StructField("b", StructType(Seq(
            StructField("x", IntegerType)
            // missing field 'y'
          )))
        )))
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insertAll()),
    result = Seq(
      """{ "pk": 0, "s": { "c1": 1, "c2": { "a": 10, "b": { "x": 100, "y": "foo" } } },
        | "dep": "sales" }""".stripMargin.replace("\n", ""),
      // Target had 'y', preserve struct of nulls
      """{ "pk": 1, "s": { "c1": null, "c2": { "a": null, "b": { "x": null, "y": null } } },
        | "dep": "engineering" }""".stripMargin.replace("\n", "")
    ),
    expectErrorWithoutEvolutionContains = "Cannot find data for the output column",
    requiresNestedTypeCoercion = true
  )

  testNestedStructsEvolution("source has missing nested struct field - " +
    "source and target have null values")(
    target = Seq(
      // Target has doubly nested struct with extra field 'y' in innermost struct
      """{ "pk": 0, "s": { "c1": 1, "c2": { "a": 10, "b": { "x": 100, "y": "foo" } } },
        | "dep": "sales" }""".stripMargin.replace("\n", ""),
      // Target row 1 has null for innermost struct 'b'
      """{ "pk": 1, "s": { "c1": 2, "c2": { "a": 20, "b": null } }, "dep": "hr" }"""
    ),
    source = Seq(
      // Source also has null for innermost struct 'b', schema missing 'y'
      """{ "pk": 1, "s": { "c1": 3, "c2": { "a": 30, "b": null } }, "dep": "engineering" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", StructType(Seq(
            StructField("x", IntegerType),
            StructField("y", StringType)
          )))
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
          StructField("b", StructType(Seq(
            StructField("x", IntegerType)
            // missing field 'y'
          )))
        )))
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insertAll()),
    result = Seq(
      """{ "pk": 0, "s": { "c1": 1, "c2": { "a": 10, "b": { "x": 100, "y": "foo" } } },
        | "dep": "sales" }""".stripMargin.replace("\n", ""),
      // Both source and target have null for 'b', remains null
      """{ "pk": 1, "s": { "c1": 3, "c2": { "a": 30, "b": null } }, "dep": "engineering" }"""
    ),
    expectErrorWithoutEvolutionContains = "Cannot find data for the output column",
    requiresNestedTypeCoercion = true
  )

  testNestedStructsEvolution("source has missing field in struct inside array -" +
    "target has null values")(
    target = Seq(
      // Target has struct with array of structs, with extra field 'y' in array element struct
      """{ "pk": 0, "s": { "c1": 1, "arr": [{ "x": 100, "y": "foo" }, { "x": 101, "y": "bar" }] },
        | "dep": "sales" }""".stripMargin.replace("\n", ""),
      """{ "pk": 1, "s": { "c1": 2, "arr": [{ "x": 200, "y": null }, { "x": 201, "y": null }] },
        | "dep": "hr" }""".stripMargin.replace("\n", "")
    ),
    source = Seq(
      // Source has null struct, schema missing field 'y' in array element struct
      """{ "pk": 1, "s": null, "dep": "engineering" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("arr", ArrayType(StructType(Seq(
          StructField("x", IntegerType),
          StructField("y", StringType)
        ))))
      ))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("s", StructType(Seq(
        StructField("c1", IntegerType),
        StructField("arr", ArrayType(StructType(Seq(
          StructField("x", IntegerType)
          // missing field 'y'
        ))))
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insertAll()),
    result = Seq(
      """{ "pk": 0, "s": { "c1": 1, "arr": [{ "x": 100, "y": "foo" }, { "x": 101, "y": "bar" }] },
        | "dep": "sales" }""".stripMargin.replace("\n", ""),
      // Target 'y' was in array, cannot be preserved, source null overrides
      """{ "pk": 1, "s": null, "dep": "engineering" }"""
    ),
    expectErrorWithoutEvolutionContains = "Cannot find data for the output column",
    requiresNestedTypeCoercion = true
  )

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

// Tests that cannot re-use helper functions as they have custom logic
trait MergeIntoSchemaEvolutionExtraSQLTests extends RowLevelOperationSuiteBase {

  test("source missing struct field violating check constraints") {
    Seq(true, false).foreach { withSchemaEvolution =>
      Seq(true, false).foreach { coercionEnabled =>
        withSQLConf(SQLConf.MERGE_INTO_NESTED_TYPE_COERCION_ENABLED.key ->
          coercionEnabled.toString) {
          withTempView("source") {
            // Target table has struct with nested field c2
            createAndInitTable(
              s"""pk INT NOT NULL,
                 |s STRUCT<c1: INT, c2: INT>,
                 |dep STRING""".stripMargin,
              """{ "pk": 0, "s": { "c1": 1, "c2": 10 }, "dep": "sales" }
                |{ "pk": 1, "s": { "c1": 2, "c2": 20 }, "dep": "hr" }"""
                .stripMargin)

            // Add CHECK constraint on nested field c2 using ALTER TABLE
            sql(s"ALTER TABLE $tableNameAsString ADD CONSTRAINT check_c2 CHECK " +
              s"(s.c2 IS NOT NULL AND s.c2 > 1)")

            // Source table schema with struct missing the c2 field
            val sourceTableSchema = StructType(Seq(
              StructField("pk", IntegerType),
              StructField("s", StructType(Seq(
                StructField("c1", IntegerType)
                // missing field 'c2' which has CHECK constraint IS NOT NULL AND > 1
              ))),
              StructField("dep", StringType)
            ))

            val data = Seq(
              Row(1, Row(100), "engineering"),
              Row(2, Row(200), "finance")
            )
            spark.createDataFrame(spark.sparkContext.parallelize(data), sourceTableSchema)
              .createOrReplaceTempView("source")

            val schemaEvolutionClause = if (withSchemaEvolution) "WITH SCHEMA EVOLUTION" else ""
            val mergeStmt =
              s"""MERGE $schemaEvolutionClause INTO $tableNameAsString t USING source
                 |ON t.pk = source.pk
                 |WHEN MATCHED THEN
                 | UPDATE SET s = source.s, dep = source.dep
                 |""".stripMargin

            if (withSchemaEvolution && coercionEnabled) {
              val error = intercept[SparkRuntimeException] {
                sql(mergeStmt)
              }
              assert(error.getCondition == "CHECK_CONSTRAINT_VIOLATION")
              assert(error.getMessage.contains("CHECK constraint check_c2 s.c2 IS NOT NULL AND " +
                "s.c2 > 1 violated by row with values:\n - s.c2 : null"))
            } else {
              // Without schema evolution or coercion, the schema mismatch is rejected
              val error = intercept[AnalysisException] {
                sql(mergeStmt)
              }
              assert(error.errorClass.get == "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA")
            }
          }
          sql(s"DROP TABLE IF EXISTS $tableNameAsString")
        }
      }
    }
  }
}
