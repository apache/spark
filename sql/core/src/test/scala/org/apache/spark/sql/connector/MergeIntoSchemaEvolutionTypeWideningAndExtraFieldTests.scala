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
 * Type widening and source-has-extra-fields tests for nested struct/array schema evolution.
 */
trait MergeIntoSchemaEvolutionTypeWideningAndExtraFieldTests
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
}
