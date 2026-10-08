/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 *
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

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Tests for `spark.sql.schemaEvolution.preserveColumnOrder`: when enabled, fields
 * added by MERGE INTO schema evolution keep the position they occupy in the source
 * schema instead of being appended at the end.
 */
trait MergeIntoSchemaEvolutionColumnOrderTests extends MergeIntoSchemaEvolutionSuiteBase {

  import testImplicits._

  private val preserveOrderConfs =
    Seq(SQLConf.SCHEMA_EVOLUTION_PRESERVE_COLUMN_ORDER.key -> "true")

  testEvolution("preserve order - extra source column in the middle")(
    targetData = Seq(
      (1, 100, "hr"),
      (2, 200, "software")
    ).toDF("pk", "salary", "dep"),
    sourceData = Seq(
      (2, 150, true, "dummy"),
      (3, 250, false, "dummy")
    ).toDF("pk", "salary", "active", "dep"),
    clauses = Seq(
      updateAll(),
      insertAll()
    ),
    expected = Seq[(Int, Int, java.lang.Boolean, String)](
      (1, 100, null, "hr"),
      (2, 150, true, "dummy"),
      (3, 250, false, "dummy")).toDF("pk", "salary", "active", "dep"),
    expectedSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("salary", IntegerType, nullable = false),
      StructField("active", BooleanType),
      StructField("dep", StringType)
    )),
    expectedWithoutEvolution = Seq(
      (1, 100, "hr"),
      (2, 150, "dummy"),
      (3, 250, "dummy")).toDF("pk", "salary", "dep"),
    // Keep the default partitionCols = Seq("dep"): the new column is inserted before
    // the partition column, shifting its stored index.
    confs = preserveOrderConfs
  )

  testEvolution("preserve order off - extra source column in the middle appends at end")(
    targetData = Seq(
      (1, 100, "hr"),
      (2, 200, "software")
    ).toDF("pk", "salary", "dep"),
    sourceData = Seq(
      (2, 150, true, "dummy"),
      (3, 250, false, "dummy")
    ).toDF("pk", "salary", "active", "dep"),
    clauses = Seq(
      updateAll(),
      insertAll()
    ),
    expected = Seq[(Int, Int, String, java.lang.Boolean)](
      (1, 100, "hr", null),
      (2, 150, "dummy", true),
      (3, 250, "dummy", false)).toDF("pk", "salary", "dep", "active"),
    expectedSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("salary", IntegerType, nullable = false),
      StructField("dep", StringType),
      StructField("active", BooleanType)
    )),
    expectedWithoutEvolution = Seq(
      (1, 100, "hr"),
      (2, 150, "dummy"),
      (3, 250, "dummy")).toDF("pk", "salary", "dep")
  )

  testEvolution("preserve order - extra source column first")(
    targetData = Seq(
      (1, 100, "hr"),
      (2, 200, "software")
    ).toDF("pk", "salary", "dep"),
    sourceData = Seq(
      ("us", 2, 150, "dummy"),
      ("eu", 3, 250, "dummy")
    ).toDF("region", "pk", "salary", "dep"),
    clauses = Seq(
      updateAll(),
      insertAll()
    ),
    expected = Seq[(String, Int, Int, String)](
      (null, 1, 100, "hr"),
      ("us", 2, 150, "dummy"),
      ("eu", 3, 250, "dummy")).toDF("region", "pk", "salary", "dep"),
    expectedSchema = StructType(Seq(
      StructField("region", StringType),
      StructField("pk", IntegerType, nullable = false),
      StructField("salary", IntegerType, nullable = false),
      StructField("dep", StringType)
    )),
    expectedWithoutEvolution = Seq(
      (1, 100, "hr"),
      (2, 150, "dummy"),
      (3, 250, "dummy")).toDF("pk", "salary", "dep"),
    confs = preserveOrderConfs
  )

  // Each added column anchors after a preceding source sibling that is itself being
  // added earlier in the same change list.
  testEvolution("preserve order - consecutive new source columns")(
    targetData = Seq(
      (1, 100, "hr"),
      (2, 200, "software")
    ).toDF("pk", "salary", "dep"),
    sourceData = Seq(
      (2, 150, 50, true, "dummy"),
      (3, 250, 75, false, "dummy")
    ).toDF("pk", "salary", "bonus", "active", "dep"),
    clauses = Seq(
      updateAll(),
      insertAll()
    ),
    expected = Seq[(Int, Int, java.lang.Integer, java.lang.Boolean, String)](
      (1, 100, null, null, "hr"),
      (2, 150, 50, true, "dummy"),
      (3, 250, 75, false, "dummy")).toDF("pk", "salary", "bonus", "active", "dep"),
    expectedSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("salary", IntegerType, nullable = false),
      StructField("bonus", IntegerType),
      StructField("active", BooleanType),
      StructField("dep", StringType)
    )),
    expectedWithoutEvolution = Seq(
      (1, 100, "hr"),
      (2, 150, "dummy"),
      (3, 250, "dummy")).toDF("pk", "salary", "dep"),
    partitionCols = Seq.empty,
    confs = preserveOrderConfs
  )

  testEvolution("preserve order - insert assignments in a different order")(
    targetData = Seq(
      (1, 100, "hr"),
      (2, 200, "software")
    ).toDF("pk", "salary", "dep"),
    sourceData = Seq(
      (2, 150, 50, true, "dummy"),
      (3, 250, 75, false, "dummy")
    ).toDF("pk", "salary", "bonus", "active", "dep"),
    clauses = Seq(
      insert(values =
        "(pk, salary, active, bonus, dep) " +
          "VALUES (s.pk, s.salary, s.active, s.bonus, s.dep)")
    ),
    expected = Seq[(Int, Int, java.lang.Integer, java.lang.Boolean, String)](
      (1, 100, null, null, "hr"),
      (2, 200, null, null, "software"),
      (3, 250, 75, false, "dummy")).toDF("pk", "salary", "bonus", "active", "dep"),
    expectedSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("salary", IntegerType, nullable = false),
      StructField("bonus", IntegerType),
      StructField("active", BooleanType),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "cannot be resolved",
    partitionCols = Seq.empty,
    confs = preserveOrderConfs
  )

  testEvolution("preserve order - extra column with explicit assignments")(
    targetData = Seq(
      (1, 100, "hr"),
      (2, 200, "software")
    ).toDF("pk", "salary", "dep"),
    sourceData = Seq(
      (2, 150, true, "dummy"),
      (3, 250, false, "dummy")
    ).toDF("pk", "salary", "active", "dep"),
    clauses = Seq(
      update(set = "salary = s.salary, dep = s.dep, active = s.active"),
      insert(values = "(pk, salary, active, dep) VALUES (s.pk, s.salary, s.active, s.dep)")
    ),
    expected = Seq[(Int, Int, java.lang.Boolean, String)](
      (1, 100, null, "hr"),
      (2, 150, true, "dummy"),
      (3, 250, false, "dummy")).toDF("pk", "salary", "active", "dep"),
    expectedSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("salary", IntegerType, nullable = false),
      StructField("active", BooleanType),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains =
      "A column, variable, or function parameter with name `active` cannot be resolved",
    partitionCols = Seq.empty,
    confs = preserveOrderConfs
  )

  testNestedStructsEvolution("preserve order - extra nested struct field in the middle")(
    target = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "inactive" }, "dep": "software" }"""
    ),
    source = Seq(
      """{ "pk": 2, "info": { "salary": 150, "bonus": 50, "status": "dummy" },
        | "dep": "finance" }""".stripMargin.replace("\n", ""),
      """{ "pk": 3, "info": { "salary": 250, "bonus": 75, "status": "dummy" },
        | "dep": "finance" }""".stripMargin.replace("\n", "")
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
        StructField("bonus", IntegerType),
        StructField("status", StringType)
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insertAll()),
    result = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 150, "bonus": 50, "status": "dummy" },
        | "dep": "finance" }""".stripMargin.replace("\n", ""),
      """{ "pk": 3, "info": { "salary": 250, "bonus": 75, "status": "dummy" },
        | "dep": "finance" }""".stripMargin.replace("\n", "")
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("bonus", IntegerType),
        StructField("status", StringType)
      ))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "Cannot write extra fields",
    confs = preserveOrderConfs
  )

  testNestedStructsEvolution("preserve order off - extra nested field appends at end")(
    target = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "inactive" }, "dep": "software" }"""
    ),
    source = Seq(
      """{ "pk": 2, "info": { "salary": 150, "bonus": 50, "status": "dummy" },
        | "dep": "finance" }""".stripMargin.replace("\n", ""),
      """{ "pk": 3, "info": { "salary": 250, "bonus": 75, "status": "dummy" },
        | "dep": "finance" }""".stripMargin.replace("\n", "")
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
        StructField("bonus", IntegerType),
        StructField("status", StringType)
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insertAll()),
    result = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 150, "bonus": 50, "status": "dummy" },
        | "dep": "finance" }""".stripMargin.replace("\n", ""),
      """{ "pk": 3, "info": { "salary": 250, "bonus": 75, "status": "dummy" },
        | "dep": "finance" }""".stripMargin.replace("\n", "")
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
    expectErrorWithoutEvolutionContains = "Cannot write extra fields"
  )

  testNestedStructsEvolution("preserve order - nested leaf assignment in the middle")(
    target = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "inactive" }, "dep": "software" }"""
    ),
    source = Seq(
      """{ "pk": 2, "info": { "salary": 150, "bonus": 50, "status": "dummy" },
        | "dep": "finance" }""".stripMargin.replace("\n", "")
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
        StructField("bonus", IntegerType),
        StructField("status", StringType)
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(update("info.bonus = s.info.bonus")),
    result = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "bonus": 50, "status": "inactive" },
        | "dep": "software" }""".stripMargin.replace("\n", "")
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("bonus", IntegerType),
        StructField("status", StringType)
      ))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "No such struct field",
    confs = preserveOrderConfs
  )

  testEvolution("preserve order - anchor matches target field case")(
    targetData = Seq(
      (1, 100, "hr"),
      (2, 200, "software")
    ).toDF("pk", "Salary", "dep"),
    sourceData = Seq(
      (2, 150, true, "dummy"),
      (3, 250, false, "dummy")
    ).toDF("pk", "salary", "active", "dep"),
    clauses = Seq(
      updateAll(),
      insertAll()
    ),
    expected = Seq[(Int, Int, java.lang.Boolean, String)](
      (1, 100, null, "hr"),
      (2, 150, true, "dummy"),
      (3, 250, false, "dummy")).toDF("pk", "Salary", "active", "dep"),
    expectedSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("Salary", IntegerType, nullable = false),
      StructField("active", BooleanType),
      StructField("dep", StringType)
    )),
    expectedWithoutEvolution = Seq(
      (1, 100, "hr"),
      (2, 150, "dummy"),
      (3, 250, "dummy")).toDF("pk", "Salary", "dep"),
    partitionCols = Seq.empty,
    confs = preserveOrderConfs
  )

  // A top-level column added before an existing struct column shifts the struct's
  // stored position while the struct itself gains a nested field.
  testNestedStructsEvolution("preserve order - top-level add before an evolving struct")(
    target = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "inactive" }, "dep": "software" }"""
    ),
    source = Seq(
      """{ "pk": 2, "active": true, "info": { "salary": 150, "bonus": 50,
        | "status": "dummy" }, "dep": "finance" }""".stripMargin.replace("\n", ""),
      """{ "pk": 3, "active": false, "info": { "salary": 250, "bonus": 75,
        | "status": "dummy" }, "dep": "finance" }""".stripMargin.replace("\n", "")
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
      StructField("active", BooleanType),
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("bonus", IntegerType),
        StructField("status", StringType)
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insertAll()),
    result = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "active": true, "info": { "salary": 150, "bonus": 50,
        | "status": "dummy" }, "dep": "finance" }""".stripMargin.replace("\n", ""),
      """{ "pk": 3, "active": false, "info": { "salary": 250, "bonus": 75,
        | "status": "dummy" }, "dep": "finance" }""".stripMargin.replace("\n", "")
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("active", BooleanType),
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("bonus", IntegerType),
        StructField("status", StringType)
      ))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "Cannot write extra fields",
    confs = preserveOrderConfs
  )

  testNestedStructsEvolution("preserve order - new struct column in the middle")(
    target = Seq(
      """{ "pk": 1, "dep": "hr" }""",
      """{ "pk": 2, "dep": "software" }"""
    ),
    source = Seq(
      """{ "pk": 2, "info": { "salary": 150, "status": "dummy" }, "dep": "finance" }""",
      """{ "pk": 3, "info": { "salary": 250, "status": "dummy" }, "dep": "finance" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("status", StringType)
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insertAll()),
    result = Seq(
      """{ "pk": 1, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 150, "status": "dummy" }, "dep": "finance" }""",
      """{ "pk": 3, "info": { "salary": 250, "status": "dummy" }, "dep": "finance" }"""
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("status", StringType)
      ))),
      StructField("dep", StringType)
    )),
    resultWithoutEvolution = Seq(
      """{ "pk": 1, "dep": "hr" }""",
      """{ "pk": 2, "dep": "finance" }""",
      """{ "pk": 3, "dep": "finance" }"""
    ),
    confs = preserveOrderConfs
  )
}
