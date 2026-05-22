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

package org.apache.spark.sql.streaming

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis.WidenStatefulOpNullability
import org.apache.spark.sql.execution.streaming.checkpointing.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.{StateSchemaCompatibilityChecker, StateStore}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

/**
 * Regression suite for stateful-operator nullability drift.
 *
 * Driver: `PropagateEmptyRelation` drops empty `Union` branches without a streaming
 * guard, so the surviving branch's per-column nullability becomes the Union's
 * nullability and propagates into a stateful operator above -- across microbatches or
 * restarts.
 *
 * Coverage:
 *   - New-query (default conf): originally-failing scenarios now complete cleanly.
 *   - Existing-query (conf forced false): pre-fix behavior preserved verbatim.
 *   - Helper invariant: `WidenStatefulOpNullability.deepWidenAttribute` recurses into
 *     nested types.
 */
class StreamingStatefulOperatorNullabilityDriftSuite extends StreamTest {

  import testImplicits._

  private def buildTwoSources(): (MemoryStream[Int], MemoryStream[Int], DataFrame, DataFrame) = {
    val inputA = MemoryStream[Int]
    val inputB = MemoryStream[Int]

    val dfA = inputA.toDF().select($"value".as("key"))
    val dfB = inputB.toDF()
      .select(when($"value" > Int.MinValue, $"value")
        .otherwise(lit(null).cast("int"))
        .as("key"))

    (inputA, inputB, dfA, dfB)
  }

  private def buildTwoSourcesWithWatermark()
      : (MemoryStream[Int], MemoryStream[Int], DataFrame, DataFrame) = {
    val inputA = MemoryStream[Int]
    val inputB = MemoryStream[Int]

    val dfA = inputA.toDF()
      .select($"value".as("key"),
        current_timestamp().cast("timestamp").as("ts"))
      .withWatermark("ts", "1 minute")
    val dfB = inputB.toDF()
      .select(when($"value" > Int.MinValue, $"value")
        .otherwise(lit(null).cast("int")).as("key"),
        current_timestamp().cast("timestamp").as("ts"))
      .withWatermark("ts", "1 minute")

    (inputA, inputB, dfA, dfB)
  }

  private def runUnionBranchDropRestart(
      buildSources: () => (MemoryStream[Int], MemoryStream[Int], DataFrame, DataFrame),
      buildQuery: (DataFrame, DataFrame) => DataFrame,
      outputMode: OutputMode,
      nullableToNonNullable: Boolean): Unit = {
    withTempDir { checkpointDir =>
      val checkpointPath = checkpointDir.getAbsolutePath

      val (inputA, inputB, dfA, dfB) = buildSources()
      val q = buildQuery(dfA, dfB)

      if (nullableToNonNullable) {
        testStream(q, outputMode)(
          StartStream(checkpointLocation = checkpointPath),
          MultiAddData(inputA, 1, 2, 3)(inputB, 4, 5),
          ProcessAllAvailable(),
          StopStream
        )
      } else {
        testStream(q, outputMode)(
          StartStream(checkpointLocation = checkpointPath),
          AddData(inputA, 1, 2, 3),
          ProcessAllAvailable(),
          StopStream
        )
      }

      assertJournaledStateSchemaAllNullable(checkpointPath)

      if (nullableToNonNullable) {
        testStream(q, outputMode)(
          StartStream(checkpointLocation = checkpointPath),
          AddData(inputA, 6),
          ProcessAllAvailable()
        )
      } else {
        testStream(q, outputMode)(
          StartStream(checkpointLocation = checkpointPath),
          MultiAddData(inputA, 6)(inputB, 7),
          ProcessAllAvailable()
        )
      }
    }
  }

  private def assertJournaledStateSchemaAllNullable(checkpointPath: String): Unit = {
    val schemaFilePath = new Path(checkpointPath,
      s"state/0/${StateStore.PARTITION_ID_TO_CHECK_SCHEMA}/_metadata/schema")
    val hadoopConf = spark.sessionState.newHadoopConf()
    val fm = CheckpointFileManager.create(schemaFilePath, hadoopConf)
    val inStream = fm.open(schemaFilePath)
    try {
      val schemas = StateSchemaCompatibilityChecker.readSchemaFile(inStream)
      assert(schemas.nonEmpty, "expected at least one persisted state column family schema")
      schemas.foreach { s =>
        assertSchemaAllNullable(s.keySchema, s"key schema for col family ${s.colFamilyName}")
        assertSchemaAllNullable(s.valueSchema, s"value schema for col family ${s.colFamilyName}")
      }
    } finally inStream.close()
  }

  private def assertSchemaAllNullable(schema: StructType, label: String): Unit = {
    schema.fields.foreach { f =>
      assert(f.nullable, s"$label: field ${f.name} should be nullable")
      assertDataTypeAllNullable(f.dataType, s"$label.${f.name}")
    }
  }

  private def assertDataTypeAllNullable(dataType: DataType, label: String): Unit = dataType match {
    case s: StructType => assertSchemaAllNullable(s, label)
    case ArrayType(elementType, containsNull) =>
      assert(containsNull, s"$label: array element should be nullable")
      assertDataTypeAllNullable(elementType, s"$label[]")
    case MapType(keyType, valueType, valueContainsNull) =>
      assert(valueContainsNull, s"$label: map value should be nullable")
      assertDataTypeAllNullable(keyType, s"$label.key")
      assertDataTypeAllNullable(valueType, s"$label.value")
    case _ =>
  }

  test("streaming aggregate: non-nullable -> nullable widening remains restart-compatible") {
    runUnionBranchDropRestart(
      buildSources = () => buildTwoSources(),
      buildQuery = (dfA, dfB) => dfA.union(dfB).groupBy($"key").count(),
      outputMode = OutputMode.Update(),
      nullableToNonNullable = false)
  }

  test("streaming aggregate: nullable -> non-nullable narrowing remains restart-compatible") {
    runUnionBranchDropRestart(
      buildSources = () => buildTwoSources(),
      buildQuery = (dfA, dfB) => dfA.union(dfB).groupBy($"key").count(),
      outputMode = OutputMode.Update(),
      nullableToNonNullable = true)
  }

  test("streaming dropDuplicates: non-nullable -> nullable widening remains restart-compatible") {
    runUnionBranchDropRestart(
      buildSources = () => buildTwoSources(),
      buildQuery = (dfA, dfB) => dfA.union(dfB).dropDuplicates(Seq("key")),
      outputMode = OutputMode.Append(),
      nullableToNonNullable = false)
  }

  test("streaming dropDuplicatesWithinWatermark: " +
    "non-nullable -> nullable widening remains restart-compatible") {
    runUnionBranchDropRestart(
      buildSources = () => buildTwoSourcesWithWatermark(),
      buildQuery = (dfA, dfB) => dfA.union(dfB).dropDuplicatesWithinWatermark(Seq("key")),
      outputMode = OutputMode.Append(),
      nullableToNonNullable = false)
  }

  test("streaming aggregate (Complete mode): no codegen NPE on state-restored null " +
    "struct grouping key after fix") {
    import org.apache.spark.sql.functions.struct

    def mkQuery(inNullableK: MemoryStream[Int], inNonNullK: MemoryStream[Int]): DataFrame = {
      val dfNullable = inNullableK.toDF()
        .select(
          when($"value" > 0, struct($"value".as("v")))
            .otherwise(lit(null).cast("struct<v:int>"))
            .as("key"),
          lit(1).as("metric"))

      val dfNonNull = inNonNullK.toDF()
        .select(
          struct($"value".as("v")).as("key"),
          lit(1).as("metric"))

      dfNullable.union(dfNonNull)
        .groupBy($"key")
        .agg(sum($"metric").as("c"))
        .select($"key.v".as("v"), $"c")
    }

    withTempDir { checkpointDir =>
      withSQLConf(
        SQLConf.STATE_SCHEMA_CHECK_ENABLED.key -> "false",
        SQLConf.STATE_STORE_FORMAT_VALIDATION_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
        val inNullable = MemoryStream[Int]
        val inNonNull = MemoryStream[Int]
        val q = mkQuery(inNullable, inNonNull)
        testStream(q, OutputMode.Complete())(
          StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
          AddData(inNullable, 0),
          ProcessAllAvailable(),
          StopStream
        )

        testStream(q, OutputMode.Complete())(
          StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
          AddData(inNonNull, 1),
          ProcessAllAvailable()
        )
      }
    }
  }

  test("streaming aggregate: with widening forced off (existing-query path), " +
    "STATE_STORE_KEY_SCHEMA_NOT_COMPATIBLE still triggers on restart") {
    withTempDir { checkpointDir =>
      withSQLConf(
        SQLConf.STATEFUL_OPERATOR_ALWAYS_NULLABLE_OUTPUT.key -> "false") {
        val (inputA, inputB, dfA, dfB) = buildTwoSources()
        val aggregated = dfA.union(dfB).groupBy($"key").count()
        testStream(aggregated, OutputMode.Update())(
          StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
          AddData(inputA, 1, 2, 3),
          ProcessAllAvailable(),
          StopStream
        )

        inputA.addData(4)
        inputB.addData(5)

        val ex = intercept[SparkUnsupportedOperationException] {
          testStream(aggregated, OutputMode.Update())(
            StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
            ProcessAllAvailable()
          )
        }

        checkError(
          ex,
          condition = "STATE_STORE_KEY_SCHEMA_NOT_COMPATIBLE.NULLABILITY_CHANGED",
          parameters = Map(
            "changedFields" -> ".*",
            "storedKeySchema" -> ".*",
            "newKeySchema" -> ".*"),
          matchPVals = true
        )
      }
    }
  }

  test("rule skips non-stateful nodes whose subtree has no stateful operator") {
    import org.apache.spark.sql.catalyst.analysis.WidenStatefulOperatorAttributeNullability
    import org.apache.spark.sql.catalyst.expressions.{AttributeReference, NamedExpression}
    import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LocalRelation, Project}
    import org.apache.spark.sql.types.IntegerType

    withSQLConf(SQLConf.STATEFUL_OPERATOR_ALWAYS_NULLABLE_OUTPUT.key -> "true") {
      val key = AttributeReference("key", IntegerType, nullable = false)()
      val payload = AttributeReference("payload", IntegerType, nullable = false)()
      val source = LocalRelation(Seq(key, payload), isStreaming = true)
      val project = Project(Seq(key, payload), source)
      val agg = Aggregate(
        groupingExpressions = Seq(key),
        aggregateExpressions = Seq(key.asInstanceOf[NamedExpression]),
        child = project)

      val widened = WidenStatefulOperatorAttributeNullability(agg)

      val projectAfter = widened.collectFirst { case p: Project => p }.getOrElse(
        fail(s"expected to find a Project node in the rewritten plan: $widened"))
      assert(projectAfter.projectList.forall {
        case ar: AttributeReference => !ar.nullable
        case _ => true
      }, s"Project.projectList below a stateful op should remain non-nullable: " +
         s"${projectAfter.projectList}")

      val aggAfter = widened.asInstanceOf[Aggregate]
      assert(aggAfter.aggregateExpressions.forall {
        case ar: AttributeReference => ar.nullable
        case _ => true
      }, s"Aggregate.aggregateExpressions should be widened to nullable: " +
         s"${aggAfter.aggregateExpressions}")
      assert(aggAfter.groupingExpressions.forall {
        case ar: AttributeReference => ar.nullable
        case _ => true
      }, s"Aggregate.groupingExpressions should be widened to nullable: " +
         s"${aggAfter.groupingExpressions}")
    }
  }

  test("deepWidenAttribute recurses into struct fields, array elements, map values") {
    import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId}
    import org.apache.spark.sql.types._

    val nestedStruct = StructType(Seq(
      StructField("inner_nn", IntegerType, nullable = false),
      StructField("inner_nl", StringType, nullable = true)))
    val arrayOfNonNull = ArrayType(IntegerType, containsNull = false)
    val mapWithNonNullValue = MapType(StringType, IntegerType, valueContainsNull = false)
    val combined = StructType(Seq(
      StructField("s", nestedStruct, nullable = false),
      StructField("a", arrayOfNonNull, nullable = false),
      StructField("m", mapWithNonNullValue, nullable = false)))

    val attr = AttributeReference("complex", combined, nullable = false)(ExprId(42L))
    val widened = WidenStatefulOpNullability.deepWidenAttribute(attr)

    assert(widened.nullable, "outer attribute should be widened to nullable")
    val widenedStruct = widened.dataType.asInstanceOf[StructType]
    val widenedNested = widenedStruct("s").dataType.asInstanceOf[StructType]
    assert(
      widenedStruct("s").nullable && widenedStruct("a").nullable && widenedStruct("m").nullable,
      "all top-level fields should be widened to nullable")
    assert(widenedNested("inner_nn").nullable && widenedNested("inner_nl").nullable,
      "nested struct fields should be widened to nullable")
    val widenedArray = widenedStruct("a").dataType.asInstanceOf[ArrayType]
    assert(widenedArray.containsNull, "array element nullability should be widened")
    val widenedMap = widenedStruct("m").dataType.asInstanceOf[MapType]
    assert(widenedMap.valueContainsNull, "map value nullability should be widened")

    assert(widened.exprId == attr.exprId)
    assert(widened.name == attr.name)
    assert(widened.qualifier == attr.qualifier)
  }
}
