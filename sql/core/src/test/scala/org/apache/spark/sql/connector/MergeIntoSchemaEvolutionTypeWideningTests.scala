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
 * Type widening and type promotion tests (top-level and nested).
 */
trait MergeIntoSchemaEvolutionTypeWideningTests extends MergeIntoSchemaEvolutionSuiteBase {

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

  for (colName <- Seq("job.title", "job title")) {
    testEvolution(
      s"special-char column already in target gets updated with type widening: $colName")(
      targetData = {
        val schema = StructType(Seq(
          StructField("pk", IntegerType, nullable = false),
          StructField("salary", IntegerType),
          StructField("dep", StringType),
          StructField(colName, ShortType)
        ))
        spark.createDataFrame(spark.sparkContext.parallelize(Seq(
          Row(1, 100, "hr", 1.toShort),
          Row(2, 200, "software", 2.toShort),
          Row(3, 300, "hr", 3.toShort)
        )), schema)
      },
      sourceData = {
        val schema = StructType(Seq(
          StructField("pk", IntegerType, nullable = false),
          StructField("salary", IntegerType),
          StructField("dep", StringType),
          StructField(colName, IntegerType)
        ))
        spark.createDataFrame(spark.sparkContext.parallelize(Seq(
          Row(2, 150, "finance", 50000),
          Row(4, 400, "finance", 60000)
        )), schema)
      },
      clauses = Seq(updateAll(), insertAll()),
      expected = Seq(
        (1, 100, "hr", 1),
        (2, 150, "finance", 50000),
        (3, 300, "hr", 3),
        (4, 400, "finance", 60000)
      ).toDF("pk", "salary", "dep", colName),
      expectedSchema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("salary", IntegerType),
        StructField("dep", StringType),
        StructField(colName, IntegerType)
      )),
      expectErrorWithoutEvolutionContains =
        "Fail to assign a value of \"INT\" type to the \"SMALLINT\" type column or variable"
    )
  }

  testEvolution("type widening top-level field - int to long")(
    targetData = Seq((1, 100, "hr"), (2, 200, "software")).toDF("pk", "salary", "dep"),
    // Use narrower type for 'pk': type should stay int
    sourceData = Seq(
        (2.toShort, Long.MaxValue, "software"),
        (3.toShort, Long.MaxValue, "engineering")
      ).toDF("pk", "salary", "dep"),
    clauses = Seq(updateAll(), insertAll()),
    expected = Seq(
        (1, 100L, "hr"),
        (2, Long.MaxValue, "software"),
        (3, Long.MaxValue, "engineering")
      ).toDF("pk", "salary", "dep"),
    expectedSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("salary", LongType, nullable = false),
      StructField("dep", StringType))),
    expectErrorWithoutEvolutionContains = "CAST_OVERFLOW_IN_TABLE_INSERT"
  )

  testEvolution("type widening - column not used in assignments")(
    targetData = Seq((1, 100, "hr"), (2, 200, "software")).toDF("pk", "salary", "dep"),
    sourceData = Seq((2, Long.MaxValue, "software"), (3, Long.MaxValue, "engineering"))
      .toDF("pk", "salary", "dep"),
    clauses = Seq(
      update(set = "dep = s.dep")
    ),
    // Type stays int since the salary column isn't used in assignments.
    expected = Seq((1, 100, "hr"), (2, 200, "software")).toDF("pk", "salary", "dep"),
    expectedSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("salary", IntegerType, nullable = false),
      StructField("dep", StringType))),
    expectedWithoutEvolution =
      Seq((1, 100, "hr"), (2, 200, "software")).toDF("pk", "salary", "dep"),
    expectedSchemaWithoutEvolution = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("salary", IntegerType, nullable = false),
      StructField("dep", StringType)))
  )

  testNestedStructsEvolution("type widening nested struct field - int to long")(
    target = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 200, "status": "inactive" }, "dep": "software" }"""
    ),
    source = Seq(
      """{ "pk": 2, "info": { "salary": 9999999999, "status": "updated" }, "dep": "software" }""",
      """{ "pk": 3, "info": { "salary": 9999999999, "status": "new" }, "dep": "engineering" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("salary", IntegerType),
        StructField("status", StringType)))),
      StructField("dep", StringType))),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("salary", LongType),
        StructField("status", StringType)))),
      StructField("dep", StringType))),
    clauses = Seq(updateAll(), insertAll()),
    result = Seq(
      """{ "pk": 1, "info": { "salary": 100, "status": "active" }, "dep": "hr" }""",
      """{ "pk": 2, "info": { "salary": 9999999999, "status": "updated" }, "dep": "software" }""",
      """{ "pk": 3, "info": { "salary": 9999999999, "status": "new" }, "dep": "engineering" }"""
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("info", StructType(Seq(
        StructField("salary", LongType),
        StructField("status", StringType)))),
      StructField("dep", StringType))),
    expectErrorWithoutEvolutionContains = "CAST_OVERFLOW_IN_TABLE_INSERT"
  )

  testNestedStructsEvolution("type widening in struct inside array")(
    target = Seq(
      """{ "pk": 0, "a": [ { "c1": 1, "c2": "x" } ], "dep": "sales" }""",
      """{ "pk": 1, "a": [ { "c1": 2, "c2": "y" } ], "dep": "hr" }"""
    ),
    source = Seq(
      """{ "pk": 1, "a": [ { "c1": 9999999999, "c2": "z" } ], "dep": "hr" }""",
      """{ "pk": 2, "a": [ { "c1": 9999999999, "c2": "w" } ], "dep": "engineering" }"""
    ),
    targetSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("a", ArrayType(StructType(Seq(
        StructField("c1", IntegerType),
        StructField("c2", StringType))))),
      StructField("dep", StringType))),
    sourceSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("a", ArrayType(StructType(Seq(
        StructField("c1", LongType),
        StructField("c2", StringType))))),
      StructField("dep", StringType))),
    clauses = Seq(updateAll(), insertAll()),
    result = Seq(
      """{ "pk": 0, "a": [ { "c1": 1, "c2": "x" } ], "dep": "sales" }""",
      """{ "pk": 1, "a": [ { "c1": 9999999999, "c2": "z" } ], "dep": "hr" }""",
      """{ "pk": 2, "a": [ { "c1": 9999999999, "c2": "w" } ], "dep": "engineering" }"""
    ),
    resultSchema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("a", ArrayType(StructType(Seq(
        StructField("c1", LongType),
        StructField("c2", StringType))))),
      StructField("dep", StringType))),
    expectErrorWithoutEvolutionContains = "CAST_OVERFLOW_IN_TABLE_INSERT"
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

  for (subFieldName <- Seq("job.title", "job title")) {
    testNestedStructsEvolution(
      s"nested special-char field already in target gets updated with type widening:" +
        s" $subFieldName")(
      target = Seq(
        s"""{ "pk": 1, "info": { "name": "Alice", "$subFieldName": 1 }, "dep": "hr" }""",
        s"""{ "pk": 2, "info": { "name": "Bob", "$subFieldName": 2 }, "dep": "software" }""",
        s"""{ "pk": 3, "info": { "name": "Charlie", "$subFieldName": 3 }, "dep": "hr" }"""
      ),
      source = Seq(
        s"""{ "pk": 2, "info": { "name": "Bob2", "$subFieldName": 50000 }, "dep": "finance" }""",
        s"""{ "pk": 4, "info": { "name": "Diana", "$subFieldName": 60000 }, "dep": "finance" }"""
      ),
      targetSchema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("info", StructType(Seq(
          StructField("name", StringType),
          StructField(subFieldName, ShortType)
        ))),
        StructField("dep", StringType)
      )),
      sourceSchema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("info", StructType(Seq(
          StructField("name", StringType),
          StructField(subFieldName, IntegerType)
        ))),
        StructField("dep", StringType)
      )),
      clauses = Seq(updateAll(), insertAll()),
      result = Seq(
        s"""{ "pk": 1, "info": { "name": "Alice", "$subFieldName": 1 }, "dep": "hr" }""",
        s"""{ "pk": 2, "info": { "name": "Bob2", "$subFieldName": 50000 }, "dep": "finance" }""",
        s"""{ "pk": 3, "info": { "name": "Charlie", "$subFieldName": 3 }, "dep": "hr" }""",
        s"""{ "pk": 4, "info": { "name": "Diana", "$subFieldName": 60000 }, "dep": "finance" }"""
      ),
      resultSchema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("info", StructType(Seq(
          StructField("name", StringType),
          StructField(subFieldName, IntegerType)
        ))),
        StructField("dep", StringType)
      )),
      expectErrorWithoutEvolutionContains =
        "Fail to assign a value of \"INT\" type to the \"SMALLINT\" type column or variable"
    )
  }
}
