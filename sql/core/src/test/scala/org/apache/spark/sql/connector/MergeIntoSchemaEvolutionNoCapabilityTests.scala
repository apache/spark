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
 * Tests with auto-schema-evolution table property DISABLED.
 */
trait MergeIntoSchemaEvolutionNoCapabilityTests extends MergeIntoSchemaEvolutionSuiteBase {

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

}
