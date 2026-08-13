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

package org.apache.spark.sql.pipelines.graph

import java.util.Locale

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.pipelines.autocdc.{AutoCdcReservedNames, Scd1BatchProcessor}
import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

/**
 * Materialization-level tests for AUTO CDC's engine-owned reserved metadata column (SPARK-58118).
 *
 * The reserved `__spark_autocdc_metadata` column is engine-owned: a user-declared schema may omit
 * it, and materialization appends the engine-owned shape so the created table matches what the
 * AUTO CDC MERGE writes at runtime. Reserved-column matching goes through the flow's effective
 * case sensitivity -- a pipeline-level `SET spark.sql.caseSensitive` can differ from the session --
 * so these tests inspect the created table's schema rather than only validation.
 */
class AutoCdcReservedColumnMaterializationSuite
    extends ExecutionTest
    with SharedSparkSession
    with AutoCdcGraphExecutionTestMixin {

  test("materialization appends the engine-owned reserved metadata column when the user " +
    "schema omits it") {
    val session = spark
    import session.implicits._

    // The user declares only the logical data columns and omits the engine-owned reserved
    // metadata column. Materialization must append it so the created target has exactly what the
    // AUTO CDC MERGE writes; otherwise the MERGE fails with an unresolved metadata column.
    val declaredSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType)
      .add("version", LongType, nullable = false)

    val stream = MemoryStream[(Int, String, Long)]
    stream.addData((1, "alice", 5L))
    val ctx = new TestGraphRegistrationContext(spark) {
      registerTable(
        "target",
        catalog = Some(catalog),
        database = Some(namespace),
        specifiedSchema = Some(declaredSchema))
      registerFlow(autoCdcFlow(
        name = "auto_cdc_flow",
        target = "target",
        query = dfFlowFunc(stream.toDF().toDF("id", "name", "version")),
        keys = Seq("id"),
        sequencing = functions.col("version")))
    }
    runPipeline(ctx)

    val targetSchema = spark.table(s"$catalog.$namespace.target").schema
    assert(
      targetSchema.fieldNames.contains(AutoCdcReservedNames.cdcMetadataColName),
      "target should carry the engine-owned reserved metadata column, got " +
        targetSchema.fieldNames.mkString(", "))
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(1, "alice", 5L, cdcMeta(None, Some(5L))))
    )
  }

  test("materialization matches the reserved metadata column through the flow's effective case " +
    "sensitivity, not the session's") {
    // The session is case-sensitive, but the flow sets spark.sql.caseSensitive=false, so the
    // flow's effective analysis is case-insensitive -- the same resolver the AUTO CDC MERGE uses.
    // A user who declares the reserved column as __SPARK_AUTOCDC_METADATA (upper case) is then
    // declaring the engine-owned column, so materialization must recognize it as reserved and end
    // up with a single engine-owned column, not keep the upper-case one and append a lower-case
    // duplicate. Matching against the session resolver (case-sensitive) here would leave two.
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val session = spark
      import session.implicits._

      val metadataType = new StructType()
        .add(Scd1BatchProcessor.cdcDeleteSequenceFieldName, LongType)
        .add(Scd1BatchProcessor.cdcUpsertSequenceFieldName, LongType)
      val declaredSchema = new StructType()
        .add("id", IntegerType, nullable = false)
        .add("version", LongType, nullable = false)
        .add(AutoCdcReservedNames.cdcMetadataColName.toUpperCase(Locale.ROOT), metadataType)

      val stream = MemoryStream[(Int, Long)]
      stream.addData((1, 5L))
      val ctx = new TestGraphRegistrationContext(
        spark,
        Map(SQLConf.CASE_SENSITIVE.key -> "false")) {
        registerTable(
          "target",
          catalog = Some(catalog),
          database = Some(namespace),
          specifiedSchema = Some(declaredSchema))
        registerFlow(autoCdcFlow(
          name = "auto_cdc_flow",
          target = "target",
          query = dfFlowFunc(stream.toDF().toDF("id", "version")),
          keys = Seq("id"),
          sequencing = functions.col("version")))
      }
      runPipeline(ctx)

      val reserved = spark.table(s"$catalog.$namespace.target").schema.fieldNames
        .filter(_.toLowerCase(Locale.ROOT).startsWith(AutoCdcReservedNames.prefix))
      assert(
        reserved.toSeq == Seq(AutoCdcReservedNames.cdcMetadataColName),
        "expected exactly the engine-owned metadata column, got " + reserved.mkString(", "))
      checkAnswer(
        spark.table(s"$catalog.$namespace.target"),
        Seq(Row(1, 5L, cdcMeta(None, Some(5L))))
      )
    }
  }
}
