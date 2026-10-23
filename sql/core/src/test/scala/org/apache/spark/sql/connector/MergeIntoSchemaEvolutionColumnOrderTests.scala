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
import org.apache.spark.sql.connector.catalog.CatalogV2Util
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

  testEvolution("preserve order - extra source column in the middle shifts " +
      "partition column position")(
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
    partitionCols = Seq("dep"),
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

  testEvolution("preserve order - consecutive new source columns anchor on each other")(
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

  testNestedStructsEvolution("preserve order - top-level add before an evolving " +
      "struct shifts its stored position")(
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

  testNestedStructsEvolution("preserve order - new sibling anchors on the added " +
      "column's spelling")(
    target = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "inactive" }, "dep": "software" }"""
    ),
    source = Seq(
      """{ "pk": 2, "info": { "salary": 150, "bonus": 50, "extra": 10,
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
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("bonus", IntegerType),
        StructField("extra", IntegerType),
        StructField("status", StringType)
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(update("info.BONUS = s.info.BONUS, info.extra = s.info.extra")),
    result = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "BONUS": 50, "extra": 10,
        | "status": "inactive" }, "dep": "software" }""".stripMargin.replace("\n", "")
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("BONUS", IntegerType),
        StructField("extra", IntegerType),
        StructField("status", StringType)
      ))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "No such struct field",
    confs = preserveOrderConfs
  )

  // The leaf assignment spells the new field differently than the source, so only
  // the resolver-based dedup prevents a duplicate add; distinct AddColumn objects
  // with fieldNames [info, BONUS] and [info, bonus] would both survive .distinct.
  testNestedStructsEvolution("preserve order - field added by case-variant leaf " +
      "assignment and insert star is added once")(
    target = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "inactive" }, "dep": "software" }"""
    ),
    source = Seq(
      """{ "pk": 2, "info": { "salary": 150, "extra": 10, "bonus": 50,
        | "status": "dummy" }, "dep": "finance" }""".stripMargin.replace("\n", ""),
      """{ "pk": 3, "info": { "salary": 250, "extra": 30, "bonus": 75,
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
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("extra", IntegerType),
        StructField("bonus", IntegerType),
        StructField("status", StringType)
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(update("info.BONUS = s.info.BONUS"), insertAll()),
    result = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "extra": null, "BONUS": 50,
        | "status": "inactive" }, "dep": "software" }""".stripMargin.replace("\n", ""),
      """{ "pk": 3, "info": { "salary": 250, "extra": 30, "BONUS": 75,
        | "status": "dummy" }, "dep": "finance" }""".stripMargin.replace("\n", "")
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("extra", IntegerType),
        StructField("BONUS", IntegerType),
        StructField("status", StringType)
      ))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "No such struct field",
    confs = preserveOrderConfs
  )

  // Dedup is unconditional: with the flag off the field is still added once,
  // appended under the leaf assignment's spelling.
  testNestedStructsEvolution("field added by case-variant leaf assignment and " +
      "insert star is added once")(
    target = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "inactive" }, "dep": "software" }"""
    ),
    source = Seq(
      """{ "pk": 2, "info": { "salary": 150, "extra": 10, "bonus": 50,
        | "status": "dummy" }, "dep": "finance" }""".stripMargin.replace("\n", ""),
      """{ "pk": 3, "info": { "salary": 250, "extra": 30, "bonus": 75,
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
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("extra", IntegerType),
        StructField("bonus", IntegerType),
        StructField("status", StringType)
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(update("info.BONUS = s.info.BONUS"), insertAll()),
    result = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "inactive", "BONUS": 50 },
        | "dep": "software" }""".stripMargin.replace("\n", ""),
      """{ "pk": 3, "info": { "salary": 250, "status": "dummy", "BONUS": 75,
        | "extra": 30 }, "dep": "finance" }""".stripMargin.replace("\n", "")
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("status", StringType),
        StructField("BONUS", IntegerType),
        StructField("extra", IntegerType)
      ))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "No such struct field"
  )

  testNestedStructsEvolution("preserve order - extra field inside array element struct")(
    target = Seq(
      """{ "pk": 1, "arr": [ { "a": 1, "c": 3 } ], "dep": "hr" }""",
      """{ "pk": 2, "arr": [ { "a": 2, "c": 4 } ], "dep": "software" }"""
    ),
    source = Seq(
      """{ "pk": 2, "arr": [ { "a": 3, "b": 10, "c": 5 } ], "dep": "finance" }""",
      """{ "pk": 3, "arr": [ { "a": 4, "b": 20, "c": 6 } ], "dep": "finance" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("arr", ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("c", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("arr", ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insertAll()),
    result = Seq(
      """{ "pk": 1, "arr": [ { "a": 1, "c": 3 } ], "dep": "hr" }""",
      """{ "pk": 2, "arr": [ { "a": 3, "b": 10, "c": 5 } ], "dep": "finance" }""",
      """{ "pk": 3, "arr": [ { "a": 4, "b": 20, "c": 6 } ], "dep": "finance" }"""
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("arr", ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "Cannot write extra fields",
    confs = preserveOrderConfs
  )

  testNestedStructsEvolution("preserve order - extra field inside map value struct")(
    target = Seq(
      """{ "pk": 1, "m": { "k": { "a": 1, "c": 3 } }, "dep": "hr" }""",
      """{ "pk": 2, "m": { "k": { "a": 2, "c": 4 } }, "dep": "software" }"""
    ),
    source = Seq(
      """{ "pk": 2, "m": { "k": { "a": 3, "b": 10, "c": 5 } }, "dep": "finance" }""",
      """{ "pk": 3, "m": { "k": { "a": 4, "b": 20, "c": 6 } }, "dep": "finance" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("m", MapType(StringType, StructType(Seq(
        StructField("a", IntegerType),
        StructField("c", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("m", MapType(StringType, StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(), insertAll()),
    result = Seq(
      """{ "pk": 1, "m": { "k": { "a": 1, "c": 3 } }, "dep": "hr" }""",
      """{ "pk": 2, "m": { "k": { "a": 3, "b": 10, "c": 5 } }, "dep": "finance" }""",
      """{ "pk": 3, "m": { "k": { "a": 4, "b": 20, "c": 6 } }, "dep": "finance" }"""
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("m", MapType(StringType, StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "Cannot write extra fields",
    confs = preserveOrderConfs
  )

  // DataFrames instead of JSON rows: the JSON reader only supports string map keys.
  testEvolution("preserve order - extra field inside map key struct")(
    targetData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, Map(Row(1, 3) -> 5), "hr"),
        Row(2, Map(Row(2, 4) -> 6), "software"))),
      StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("m", MapType(
          StructType(Seq(StructField("a", IntegerType), StructField("c", IntegerType))),
          IntegerType)),
        StructField("dep", StringType)))),
    sourceData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(2, Map(Row(3, 10, 5) -> 7), "finance"),
        Row(3, Map(Row(4, 20, 6) -> 8), "finance"))),
      StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("m", MapType(
          StructType(Seq(
            StructField("a", IntegerType),
            StructField("b", IntegerType),
            StructField("c", IntegerType))),
          IntegerType)),
        StructField("dep", StringType)))),
    clauses = Seq(updateAll(), insertAll()),
    expected = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, Map(Row(1, null, 3) -> 5), "hr"),
        Row(2, Map(Row(3, 10, 5) -> 7), "finance"),
        Row(3, Map(Row(4, 20, 6) -> 8), "finance"))),
      StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("m", MapType(
          StructType(Seq(
            StructField("a", IntegerType),
            StructField("b", IntegerType),
            StructField("c", IntegerType))),
          IntegerType)),
        StructField("dep", StringType)))),
    expectedSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("m", MapType(
        StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", IntegerType),
          StructField("c", IntegerType))),
        IntegerType)),
      StructField("dep", StringType))),
    expectErrorWithoutEvolutionContains = "Cannot write extra fields",
    confs = preserveOrderConfs,
    partitionCols = Seq.empty
  )

  testNestedStructsEvolution("preserve order - matched condition on nested " +
      "field after positioned add")(
    target = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "inactive" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "active" }, "dep": "software" }"""
    ),
    source = Seq(
      """{ "pk": 2, "info": { "salary": 150, "bonus": 50, "status": "x" },
        | "dep": "finance" }""".stripMargin.replace("\n", ""),
      """{ "pk": 3, "info": { "salary": 250, "bonus": 75, "status": "y" },
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
    clauses = Seq(updateAll(condition = "t.info.status = 'active'"), insertAll()),
    result = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "inactive" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 150, "bonus": 50, "status": "x" },
        | "dep": "finance" }""".stripMargin.replace("\n", ""),
      """{ "pk": 3, "info": { "salary": 250, "bonus": 75, "status": "y" },
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

  // Same scenario with a string field inserted before status: a stale ordinal reads
  // an all-NULL field, so the row would silently fail the condition instead of erroring.
  testNestedStructsEvolution("preserve order - matched condition on nested " +
      "field after same-type positioned add")(
    target = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "inactive" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "active" }, "dep": "software" }"""
    ),
    source = Seq(
      """{ "pk": 2, "info": { "salary": 150, "region": "us", "status": "x" },
        | "dep": "finance" }""".stripMargin.replace("\n", ""),
      """{ "pk": 3, "info": { "salary": 250, "region": "eu", "status": "y" },
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
        StructField("region", StringType),
        StructField("status", StringType)
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(condition = "t.info.status = 'active'"), insertAll()),
    result = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "inactive" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 150, "region": "us", "status": "x" },
        | "dep": "finance" }""".stripMargin.replace("\n", ""),
      """{ "pk": 3, "info": { "salary": 250, "region": "eu", "status": "y" },
        | "dep": "finance" }""".stripMargin.replace("\n", "")
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("region", StringType),
        StructField("status", StringType)
      ))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "Cannot write extra fields",
    confs = preserveOrderConfs
  )

  testNestedStructsEvolution("preserve order - merge condition on nested " +
      "field after positioned add")(
    target = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "inactive" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "active" }, "dep": "software" }"""
    ),
    source = Seq(
      """{ "pk": 2, "info": { "salary": 150, "region": "us", "status": "x" },
        | "dep": "finance" }""".stripMargin.replace("\n", ""),
      """{ "pk": 3, "info": { "salary": 250, "region": "eu", "status": "y" },
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
        StructField("region", StringType),
        StructField("status", StringType)
      ))),
      StructField("dep", StringType)
    )),
    cond = "t.pk = s.pk AND t.info.status = 'active'",
    clauses = Seq(updateAll(), insertAll()),
    result = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "inactive" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 150, "region": "us", "status": "x" },
        | "dep": "finance" }""".stripMargin.replace("\n", ""),
      """{ "pk": 3, "info": { "salary": 250, "region": "eu", "status": "y" },
        | "dep": "finance" }""".stripMargin.replace("\n", "")
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("region", StringType),
        StructField("status", StringType)
      ))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "Cannot write extra fields",
    confs = preserveOrderConfs
  )

  testNestedStructsEvolution("preserve order - not matched by source condition " +
      "on nested field after positioned add")(
    target = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "inactive" }, "dep": "software" }"""
    ),
    source = Seq(
      """{ "pk": 2, "info": { "salary": 150, "region": "us", "status": "x" },
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
        StructField("region", StringType),
        StructField("status", StringType)
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(
      updateAll(),
      updateNotMatched(set = "dep = 'gone'", condition = "t.info.status = 'active'")
    ),
    result = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "gone" }""",
      """{ "pk": 2, "info": { "salary": 150, "region": "us", "status": "x" },
        | "dep": "finance" }""".stripMargin.replace("\n", "")
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("region", StringType),
        StructField("status", StringType)
      ))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "Cannot write extra fields",
    confs = preserveOrderConfs
  )

  testNestedStructsEvolution("preserve order - update assignment reads nested " +
      "field after positioned add")(
    target = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "inactive" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "active" }, "dep": "software" }"""
    ),
    source = Seq(
      """{ "pk": 2, "info": { "salary": 150, "bonus": 50, "status": "x" },
        | "dep": "finance" }""".stripMargin.replace("\n", ""),
      """{ "pk": 3, "info": { "salary": 250, "bonus": 75, "status": "y" },
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
    clauses = Seq(update(set = "dep = t.info.status"), insertAll()),
    result = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "inactive" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "active" }, "dep": "active" }""",
      """{ "pk": 3, "info": { "salary": 250, "bonus": 75, "status": "y" },
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

  testNestedStructsEvolution("preserve order - nested assignment key after " +
      "positioned add")(
    target = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "inactive" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "active" }, "dep": "software" }"""
    ),
    source = Seq(
      """{ "pk": 2, "info": { "salary": 150, "bonus": 50, "status": "x" },
        | "dep": "finance" }""".stripMargin.replace("\n", ""),
      """{ "pk": 3, "info": { "salary": 250, "bonus": 75, "status": "y" },
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
    clauses = Seq(update(set = "info.status = 'changed'"), insertAll()),
    result = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "inactive" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "changed" }, "dep": "software" }""",
      """{ "pk": 3, "info": { "salary": 250, "bonus": 75, "status": "y" },
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

  // mid lands inside info before addr and region inside addr before zip, so both
  // levels of the extraction chain shift and the inner one must rebind first.
  testNestedStructsEvolution("preserve order - chained nested field condition " +
      "after positioned adds")(
    target = Seq(
      """{ "pk": 1, "info": { "status": "inactive",
        | "addr": { "city": "la", "zip": "90001" } }, "dep": "hr" }"""
        .stripMargin.replace("\n", ""),
      """{ "pk": 2, "info": { "status": "active",
        | "addr": { "city": "sf", "zip": "94102" } }, "dep": "software" }"""
        .stripMargin.replace("\n", "")
    ),
    source = Seq(
      """{ "pk": 2, "info": { "status": "x", "mid": "m",
        | "addr": { "city": "ny", "region": "r", "zip": "10001" } },
        | "dep": "finance" }""".stripMargin.replace("\n", ""),
      """{ "pk": 3, "info": { "status": "y", "mid": "n",
        | "addr": { "city": "bo", "region": "e", "zip": "02101" } },
        | "dep": "finance" }""".stripMargin.replace("\n", "")
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("status", StringType),
        StructField("addr", StructType(Seq(
          StructField("city", StringType),
          StructField("zip", StringType)
        )))
      ))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("status", StringType),
        StructField("mid", StringType),
        StructField("addr", StructType(Seq(
          StructField("city", StringType),
          StructField("region", StringType),
          StructField("zip", StringType)
        )))
      ))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(condition = "t.info.addr.zip = '94102'"), insertAll()),
    result = Seq(
      """{ "pk": 1, "info": { "status": "inactive",
        | "addr": { "city": "la", "zip": "90001" } }, "dep": "hr" }"""
        .stripMargin.replace("\n", ""),
      """{ "pk": 2, "info": { "status": "x", "mid": "m",
        | "addr": { "city": "ny", "region": "r", "zip": "10001" } },
        | "dep": "finance" }""".stripMargin.replace("\n", ""),
      """{ "pk": 3, "info": { "status": "y", "mid": "n",
        | "addr": { "city": "bo", "region": "e", "zip": "02101" } },
        | "dep": "finance" }""".stripMargin.replace("\n", "")
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("status", StringType),
        StructField("mid", StringType),
        StructField("addr", StructType(Seq(
          StructField("city", StringType),
          StructField("region", StringType),
          StructField("zip", StringType)
        )))
      ))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "Cannot write extra fields",
    confs = preserveOrderConfs
  )

  testNestedStructsEvolution("preserve order - array element field condition " +
      "after positioned add")(
    target = Seq(
      """{ "pk": 1, "arr": [ { "a": 1, "c": 9 } ], "dep": "hr" }""",
      """{ "pk": 2, "arr": [ { "a": 2, "c": 3 } ], "dep": "software" }"""
    ),
    source = Seq(
      """{ "pk": 2, "arr": [ { "a": 3, "b": 10, "c": 5 } ], "dep": "finance" }""",
      """{ "pk": 3, "arr": [ { "a": 4, "b": 20, "c": 6 } ], "dep": "finance" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("arr", ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("c", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("arr", ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(condition = "array_contains(t.arr.c, 3)"), insertAll()),
    result = Seq(
      """{ "pk": 1, "arr": [ { "a": 1, "c": 9 } ], "dep": "hr" }""",
      """{ "pk": 2, "arr": [ { "a": 3, "b": 10, "c": 5 } ], "dep": "finance" }""",
      """{ "pk": 3, "arr": [ { "a": 4, "b": 20, "c": 6 } ], "dep": "finance" }"""
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("arr", ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "Cannot write extra fields",
    confs = preserveOrderConfs
  )

  // Lambda variables keep the element type bound before evolution, so a
  // positioned add inside the element struct shifts what x.c reads unless the
  // variable is rebound.
  testNestedStructsEvolution("preserve order - exists on array element field " +
      "after positioned add")(
    target = Seq(
      """{ "pk": 1, "arr": [ { "a": 1, "c": 9 } ], "dep": "hr" }""",
      """{ "pk": 2, "arr": [ { "a": 2, "c": 3 } ], "dep": "software" }"""
    ),
    source = Seq(
      """{ "pk": 2, "arr": [ { "a": 3, "b": "u", "c": 5 } ], "dep": "finance" }""",
      """{ "pk": 3, "arr": [ { "a": 4, "b": "v", "c": 6 } ], "dep": "finance" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("arr", ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("c", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("arr", ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", StringType),
        StructField("c", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(condition = "exists(t.arr, x -> x.c = 3)"), insertAll()),
    result = Seq(
      """{ "pk": 1, "arr": [ { "a": 1, "c": 9 } ], "dep": "hr" }""",
      """{ "pk": 2, "arr": [ { "a": 3, "b": "u", "c": 5 } ], "dep": "finance" }""",
      """{ "pk": 3, "arr": [ { "a": 4, "b": "v", "c": 6 } ], "dep": "finance" }"""
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("arr", ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", StringType),
        StructField("c", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "Cannot write extra fields",
    confs = preserveOrderConfs
  )

  // With an int inserted before c the stale variable reads a NULL b, so the
  // condition fails silently instead of erroring.
  testNestedStructsEvolution("preserve order - exists on array element field " +
      "after same-type positioned add")(
    target = Seq(
      """{ "pk": 1, "arr": [ { "a": 1, "c": 9 } ], "dep": "hr" }""",
      """{ "pk": 2, "arr": [ { "a": 2, "c": 3 } ], "dep": "software" }"""
    ),
    source = Seq(
      """{ "pk": 2, "arr": [ { "a": 3, "b": 10, "c": 5 } ], "dep": "finance" }""",
      """{ "pk": 3, "arr": [ { "a": 4, "b": 20, "c": 6 } ], "dep": "finance" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("arr", ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("c", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("arr", ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(condition = "exists(t.arr, x -> x.c = 3)"), insertAll()),
    result = Seq(
      """{ "pk": 1, "arr": [ { "a": 1, "c": 9 } ], "dep": "hr" }""",
      """{ "pk": 2, "arr": [ { "a": 3, "b": 10, "c": 5 } ], "dep": "finance" }""",
      """{ "pk": 3, "arr": [ { "a": 4, "b": 20, "c": 6 } ], "dep": "finance" }"""
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("arr", ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "Cannot write extra fields",
    confs = preserveOrderConfs
  )

  // Flag off the added field appends, so the bound element type stays a prefix
  // and the same statement is unaffected.
  testNestedStructsEvolution("exists on array element field after appended add")(
    target = Seq(
      """{ "pk": 1, "arr": [ { "a": 1, "c": 9 } ], "dep": "hr" }""",
      """{ "pk": 2, "arr": [ { "a": 2, "c": 3 } ], "dep": "software" }"""
    ),
    source = Seq(
      """{ "pk": 2, "arr": [ { "a": 3, "c": 5, "b": 10 } ], "dep": "finance" }""",
      """{ "pk": 3, "arr": [ { "a": 4, "c": 6, "b": 20 } ], "dep": "finance" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("arr", ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("c", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("arr", ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("c", IntegerType),
        StructField("b", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    clauses = Seq(updateAll(condition = "exists(t.arr, x -> x.c = 3)"), insertAll()),
    result = Seq(
      """{ "pk": 1, "arr": [ { "a": 1, "c": 9 } ], "dep": "hr" }""",
      """{ "pk": 2, "arr": [ { "a": 3, "c": 5, "b": 10 } ], "dep": "finance" }""",
      """{ "pk": 3, "arr": [ { "a": 4, "c": 6, "b": 20 } ], "dep": "finance" }"""
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("arr", ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("c", IntegerType),
        StructField("b", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "Cannot write extra fields"
  )

  testNestedStructsEvolution("preserve order - transform over array element " +
      "field in assignment")(
    target = Seq(
      """{ "pk": 1, "arr": [ { "a": 1, "c": 9 } ], "dep": "hr" }""",
      """{ "pk": 2, "arr": [ { "a": 2, "c": 3 } ], "dep": "software" }"""
    ),
    source = Seq(
      """{ "pk": 2, "arr": [ { "a": 3, "b": 10, "c": 5 } ], "dep": "finance" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("arr", ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("c", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("arr", ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    clauses = Seq(
      update(set = "dep = transform(t.arr, x -> CAST(x.c AS STRING))[0], arr = s.arr")),
    result = Seq(
      """{ "pk": 1, "arr": [ { "a": 1, "c": 9 } ], "dep": "hr" }""",
      """{ "pk": 2, "arr": [ { "a": 3, "b": 10, "c": 5 } ], "dep": "3" }"""
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("arr", ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "Cannot write extra fields",
    confs = preserveOrderConfs
  )

  testNestedStructsEvolution("preserve order - two argument transform over " +
      "array element field in assignment")(
    target = Seq(
      """{ "pk": 1, "arr": [ { "a": 1, "c": 9 } ], "dep": "hr" }""",
      """{ "pk": 2, "arr": [ { "a": 2, "c": 3 } ], "dep": "software" }"""
    ),
    source = Seq(
      """{ "pk": 2, "arr": [ { "a": 3, "b": 10, "c": 5 } ], "dep": "finance" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("arr", ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("c", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("arr", ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    clauses = Seq(
      update(set = "dep = transform(t.arr, (x, i) -> CAST(x.c + i AS STRING))[0], arr = s.arr")),
    result = Seq(
      """{ "pk": 1, "arr": [ { "a": 1, "c": 9 } ], "dep": "hr" }""",
      """{ "pk": 2, "arr": [ { "a": 3, "b": 10, "c": 5 } ], "dep": "3" }"""
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("arr", ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "Cannot write extra fields",
    confs = preserveOrderConfs
  )

  testNestedStructsEvolution("preserve order - aggregate over array element " +
      "field in assignment")(
    target = Seq(
      """{ "pk": 1, "arr": [ { "a": 1, "c": 9 } ], "dep": "hr" }""",
      """{ "pk": 2, "arr": [ { "a": 2, "c": 3 } ], "dep": "software" }"""
    ),
    source = Seq(
      """{ "pk": 2, "arr": [ { "a": 3, "b": 10, "c": 5 } ], "dep": "finance" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("arr", ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("c", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("arr", ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    clauses = Seq(
      update(set = "dep = CAST(aggregate(t.arr, 0, (acc, x) -> acc + x.c) " +
        "AS STRING), arr = s.arr")),
    result = Seq(
      """{ "pk": 1, "arr": [ { "a": 1, "c": 9 } ], "dep": "hr" }""",
      """{ "pk": 2, "arr": [ { "a": 3, "b": 10, "c": 5 } ], "dep": "3" }"""
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("arr", ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "Cannot write extra fields",
    confs = preserveOrderConfs
  )

  testNestedStructsEvolution("preserve order - transform_values over map " +
      "value struct field in assignment")(
    target = Seq(
      """{ "pk": 1, "m": { "k": { "a": 1, "c": 9 } }, "dep": "hr" }""",
      """{ "pk": 2, "m": { "k": { "a": 2, "c": 3 } }, "dep": "software" }"""
    ),
    source = Seq(
      """{ "pk": 2, "m": { "k": { "a": 3, "b": 10, "c": 5 } }, "dep": "finance" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("m", MapType(StringType, StructType(Seq(
        StructField("a", IntegerType),
        StructField("c", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("m", MapType(StringType, StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    clauses = Seq(
      update(set = "dep = transform_values(t.m, (k, v) -> CAST(v.c AS STRING))['k'], m = s.m")),
    result = Seq(
      """{ "pk": 1, "m": { "k": { "a": 1, "c": 9 } }, "dep": "hr" }""",
      """{ "pk": 2, "m": { "k": { "a": 3, "b": 10, "c": 5 } }, "dep": "3" }"""
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("m", MapType(StringType, StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType)
      )))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "Cannot write extra fields",
    confs = preserveOrderConfs
  )

  // The inner lambda's argument derives from the refreshed outer variable, so
  // inner variables pick up the evolved inner element type as well.
  testNestedStructsEvolution("preserve order - nested higher order function " +
      "condition after positioned adds")(
    target = Seq(
      """{ "pk": 1, "arr": [ { "a": 1, "c": 9, "inner": [ { "f": "w" } ] } ],
        | "dep": "hr" }""".stripMargin.replace("\n", ""),
      """{ "pk": 2, "arr": [ { "a": 2, "c": 3, "inner": [ { "f": "z" } ] } ],
        | "dep": "software" }""".stripMargin.replace("\n", "")
    ),
    source = Seq(
      """{ "pk": 2, "arr": [ { "a": 3, "b": "u", "c": 5,
        | "inner": [ { "e": 1, "f": "z" } ] } ], "dep": "finance" }"""
          .stripMargin.replace("\n", "")
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("arr", ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("c", IntegerType),
        StructField("inner", ArrayType(StructType(Seq(
          StructField("f", StringType)
        ))))
      )))),
      StructField("dep", StringType)
    )),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("arr", ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", StringType),
        StructField("c", IntegerType),
        StructField("inner", ArrayType(StructType(Seq(
          StructField("e", IntegerType),
          StructField("f", StringType)
        ))))
      )))),
      StructField("dep", StringType)
    )),
    clauses = Seq(
      updateAll(condition = "exists(t.arr, x -> forall(x.inner, y -> y.f = 'z'))"),
      insertAll()),
    result = Seq(
      """{ "pk": 1, "arr": [ { "a": 1, "c": 9, "inner": [ { "f": "w" } ] } ],
        | "dep": "hr" }""".stripMargin.replace("\n", ""),
      """{ "pk": 2, "arr": [ { "a": 3, "b": "u", "c": 5,
        | "inner": [ { "e": 1, "f": "z" } ] } ], "dep": "finance" }"""
          .stripMargin.replace("\n", "")
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("arr", ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", StringType),
        StructField("c", IntegerType),
        StructField("inner", ArrayType(StructType(Seq(
          StructField("e", IntegerType),
          StructField("f", StringType)
        ))))
      )))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "Cannot write extra fields",
    confs = preserveOrderConfs
  )

  testEvolution("preserve order - ill-typed higher order function in matched " +
      "condition reports data type mismatch")(
    targetData = Seq(
      (1, 100, "hr"),
      (2, 200, "software")
    ).toDF("pk", "salary", "dep"),
    sourceData = Seq(
      (2, 150, "finance", 50),
      (3, 250, "eng", 75)
    ).toDF("pk", "salary", "dep", "bonus"),
    clauses = Seq(
      updateAll(condition = "exists(t.salary, x -> x > 0)"),
      insertAll()
    ),
    expectErrorContains = "DATATYPE_MISMATCH",
    expectErrorWithoutEvolutionContains = "DATATYPE_MISMATCH",
    partitionCols = Seq.empty,
    confs = preserveOrderConfs
  )

  testEvolution("ill-typed higher order function in matched condition reports " +
      "data type mismatch")(
    targetData = Seq(
      (1, 100, "hr"),
      (2, 200, "software")
    ).toDF("pk", "salary", "dep"),
    sourceData = Seq(
      (2, 150, "finance", 50),
      (3, 250, "eng", 75)
    ).toDF("pk", "salary", "dep", "bonus"),
    clauses = Seq(
      updateAll(condition = "exists(t.salary, x -> x > 0)"),
      insertAll()
    ),
    expectErrorContains = "DATATYPE_MISMATCH",
    expectErrorWithoutEvolutionContains = "DATATYPE_MISMATCH",
    partitionCols = Seq.empty
  )

  testEvolution("preserve order - new column anchors on a reordered source")(
    targetData = Seq(
      (1, "hr"),
      (2, "software")
    ).toDF("pk", "data"),
    sourceData = Seq(
      ("finance", true, 2),
      ("sales", false, 3)
    ).toDF("data", "active", "pk"),
    clauses = Seq(
      updateAll(),
      insertAll()
    ),
    expected = Seq[(Int, String, java.lang.Boolean)](
      (1, "hr", null),
      (2, "finance", true),
      (3, "sales", false)).toDF("pk", "data", "active"),
    expectedSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("data", StringType),
      StructField("active", BooleanType)
    )),
    expectedWithoutEvolution = Seq(
      (1, "hr"),
      (2, "finance"),
      (3, "sales")).toDF("pk", "data"),
    partitionCols = Seq.empty,
    confs = preserveOrderConfs
  )

  // Each clause adds [info.BONUS] or [info.bonus] through the unresolved-key
  // path, so the resolver dedup decides whether one field or two are added.
  testNestedStructsEvolution("preserve order - field added by two case-variant " +
      "leaf assignments is added once")(
    target = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "inactive" }, "dep": "software" }"""
    ),
    source = Seq(
      """{ "pk": 2, "info": { "salary": 150, "bonus": 50, "status": "x" },
        | "dep": "finance" }""".stripMargin.replace("\n", ""),
      """{ "pk": 3, "info": { "salary": 250, "bonus": 75, "status": "y" },
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
    clauses = Seq(
      update(set = "info.BONUS = s.info.BONUS", condition = "s.pk = 2"),
      update(set = "info.bonus = s.info.bonus", condition = "s.pk = 3")
    ),
    result = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "BONUS": 50, "status": "inactive" },
        | "dep": "software" }""".stripMargin.replace("\n", "")
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("BONUS", IntegerType),
        StructField("status", StringType)
      ))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "No such struct field",
    confs = preserveOrderConfs
  )

  testNestedStructsEvolution("field added by two case-variant leaf " +
      "assignments is added once")(
    target = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "inactive" }, "dep": "software" }"""
    ),
    source = Seq(
      """{ "pk": 2, "info": { "salary": 150, "bonus": 50, "status": "x" },
        | "dep": "finance" }""".stripMargin.replace("\n", ""),
      """{ "pk": 3, "info": { "salary": 250, "bonus": 75, "status": "y" },
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
    clauses = Seq(
      update(set = "info.BONUS = s.info.BONUS", condition = "s.pk = 2"),
      update(set = "info.bonus = s.info.bonus", condition = "s.pk = 3")
    ),
    result = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "inactive", "BONUS": 50 },
        | "dep": "software" }""".stripMargin.replace("\n", "")
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("status", StringType),
        StructField("BONUS", IntegerType)
      ))),
      StructField("dep", StringType)
    )),
    expectErrorWithoutEvolutionContains = "No such struct field"
  )

  test("preserve order - add rejected by connector is skipped when anchoring") {
    val targetData = Seq((1, "hr")).toDF("id", "data")
    val sourceData = Seq((2, 5, 9, "eng")).toDF("id", "v", "b", "data")

    var error = ""
    withSQLConf(SQLConf.SCHEMA_EVOLUTION_PRESERVE_COLUMN_ORDER.key -> "true") {
      withTable(tableNameAsString) {
        withTempView("source") {
          val columns = CatalogV2Util.structTypeToV2Columns(targetData.schema)
          createTable(columns, Seq.empty)
          targetData.writeTo(tableNameAsString).append()
          sql(s"ALTER TABLE $tableNameAsString SET TBLPROPERTIES " +
            "('reject-added-columns' = 'v')")
          sourceData.createOrReplaceTempView("source")
          error = intercept[Exception] {
            executeMerge(true, tableNameAsString, "source", "t.id = s.id", Seq(insertAll()))
          }.getMessage
          // The merge must fail on the unresolved `v` column, not inside
          // alterTable on a dangling AFTER anchor.
          assert(error.contains("cannot be resolved"))
          // The failed merge aborts the transaction, so `b` is only visible in
          // the staged schema, anchored on `id` rather than first or at the end.
          val stagedSchema =
            catalog.lastTransaction.catalog.txnTables(ident).schema
          assert(stagedSchema.fieldNames.toSeq === Seq("id", "b", "data"))
        }
      }
    }
  }
}
