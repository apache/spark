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
 * Tests where the source has extra column(s)/field(s) not present in the target
 * (top-level and nested), including cases that should NOT trigger schema evolution.
 */
trait MergeIntoSchemaEvolutionExtraSourceColumnTests extends MergeIntoSchemaEvolutionSuiteBase {

  import testImplicits._

  import org.apache.spark.sql.DataFrame

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

  testEvolution("source has extra column with delete action")(
    targetData = Seq((1, "hr"), (2, "software")).toDF("pk", "dep"),
    sourceData = Seq((2, "dummy", 200), (3, "dummy", 300)).toDF("pk", "dep", "salary"),
    clauses = Seq(delete(), insertAll()),
    expected = Seq[(Int, String, java.lang.Integer)](
      (1, "hr", null),
      (3, "dummy", 300)).toDF("pk", "dep", "salary"),
    expectedWithoutEvolution = Seq(
      (1, "hr"),
      (3, "dummy")).toDF("pk", "dep")
  )

  testEvolution("delete-only action does not trigger schema evolution")(
    targetData = Seq((1, "hr"), (2, "software")).toDF("pk", "dep"),
    sourceData = Seq((2, "dummy", 200)).toDF("pk", "dep", "salary"),
    clauses = Seq(delete()),
    expected = Seq((1, "hr")).toDF("pk", "dep"),
    expectedWithoutEvolution = Seq((1, "hr")).toDF("pk", "dep")
  )

  for (colName <- Seq("job.title", "job title")) {
    testEvolution(s"SPARK-56462: source has extra column with special-char name: $colName")(
      targetData = Seq(
        (1, 100, "hr"),
        (2, 200, "software"),
        (3, 300, "hr")
      ).toDF("pk", "salary", "dep"),
      sourceData = Seq(
        (2, 150, "finance", "engineer"),
        (4, 400, "finance", "manager")
      ).toDF("pk", "salary", "dep", colName),
      clauses = Seq(updateAll(), insertAll()),
      expected = Seq[(Int, Int, String, String)](
        (1, 100, "hr", null),
        (2, 150, "finance", "engineer"),
        (3, 300, "hr", null),
        (4, 400, "finance", "manager")
      ).toDF("pk", "salary", "dep", colName),
      expectedWithoutEvolution = Seq(
        (1, 100, "hr"),
        (2, 150, "finance"),
        (3, 300, "hr"),
        (4, 400, "finance")
      ).toDF("pk", "salary", "dep"),
      expectedSchema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("salary", IntegerType, nullable = false),
        StructField("dep", StringType),
        StructField(colName, StringType)
      )),
      expectedSchemaWithoutEvolution = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("salary", IntegerType, nullable = false),
        StructField("dep", StringType)
      ))
    )
  }

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

  // When assigning s.bonus to existing t.salary and source.salary has a wider type (long) than
  // target.salary (int), no evolution should occur because the assignment uses s.bonus, not
  // s.salary. The type mismatch on the same-named column should be irrelevant.
  testEvolution("source has extra column with type mismatch on existing column -" +
    "should not evolve when assigning from differently named source column")(
    targetData = {
      val schema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("salary", IntegerType),
        StructField("dep", StringType)
      ))
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(
        Row(1, 100, "hr"),
        Row(2, 200, "software")
      )), schema)
    },
    sourceData = {
      val schema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("salary", LongType),
        StructField("dep", StringType),
        StructField("bonus", LongType)
      ))
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(
        Row(2, 150L, "dummy", 50L),
        Row(3, 250L, "dummy", 75L)
      )), schema)
    },
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
      (3, 75, "newdep")).toDF("pk", "salary", "dep"),
    expectedSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("salary", IntegerType),
      StructField("dep", StringType)
    )),
    expectedSchemaWithoutEvolution = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("salary", IntegerType),
      StructField("dep", StringType)
    ))
  )

  // When assigning s.bonus (StringType) to target salary (IntegerType), the types are
  // incompatible. This should fail both with and without schema evolution because the explicit
  // assignment has mismatched types regardless of evolution.
  testEvolution("source has extra column with type mismatch on existing column -" +
    "should fail when assigning from incompatible source column")(
    targetData = {
      val schema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("salary", IntegerType),
        StructField("dep", StringType)
      ))
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(
        Row(1, 100, "hr"),
        Row(2, 200, "software")
      )), schema)
    },
    sourceData = {
      val schema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("salary", LongType),
        StructField("dep", StringType),
        StructField("bonus", StringType)
      ))
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(
        Row(2, 150L, "dummy", "fifty"),
        Row(3, 250L, "dummy", "seventy-five")
      )), schema)
    },
    clauses = Seq(
      update(set = "salary = s.bonus"),
      insert(values = "(pk, salary, dep) VALUES (s.pk, s.bonus, 'newdep')")
    ),
    expectErrorContains = "Cannot safely cast",
    expectErrorWithoutEvolutionContains = "Cannot safely cast"
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

  for (subFieldName <- Seq("job.title", "job title")) {
    testNestedStructsEvolution(
      s"source struct has extra nested field with special-char name: $subFieldName")(
      target = Seq(
        """{ "pk": 1, "info": { "name": "Alice" }, "dep": "hr" }""",
        """{ "pk": 2, "info": { "name": "Bob" }, "dep": "finance" }"""
      ),
      source = Seq(
        s"""{ "pk": 1, "info": { "name": "Alice2", "$subFieldName": "engineer" }, "dep": "hr" }""",
        s"""{ "pk": 3, "info": { "name": "Cathy", "$subFieldName": "manager" }, "dep": "sales" }"""
      ),
      targetSchema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("info", StructType(Seq(
          StructField("name", StringType)
        ))),
        StructField("dep", StringType)
      )),
      sourceSchema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("info", StructType(Seq(
          StructField("name", StringType),
          StructField(subFieldName, StringType)
        ))),
        StructField("dep", StringType)
      )),
      clauses = Seq(updateAll(), insertAll()),
      result = Seq(
        s"""{ "pk": 1, "info": { "name": "Alice2", "$subFieldName": "engineer" }, "dep": "hr" }""",
        s"""{ "pk": 2, "info": { "name": "Bob", "$subFieldName": null }, "dep": "finance" }""",
        s"""{ "pk": 3, "info": { "name": "Cathy", "$subFieldName": "manager" }, "dep": "sales" }"""
      ),
      resultSchema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("info", StructType(Seq(
          StructField("name", StringType),
          StructField(subFieldName, StringType)
        ))),
        StructField("dep", StringType)
      )),
      expectErrorWithoutEvolutionContains = "Cannot write extra fields"
    )
  }
}
