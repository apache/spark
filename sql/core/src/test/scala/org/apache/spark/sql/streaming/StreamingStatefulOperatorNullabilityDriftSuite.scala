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
import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.sql.catalyst.analysis.WidenStatefulOpNullability
import org.apache.spark.sql.execution.streaming.checkpointing.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.{RocksDBStateStoreProvider, StateSchemaCompatibilityChecker, StateStore}
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
        timestamp_seconds($"value").as("ts"))
      .withWatermark("ts", "1 minute")
    val dfB = inputB.toDF()
      .select(when($"value" > Int.MinValue, $"value")
        .otherwise(lit(null).cast("int")).as("key"),
        timestamp_seconds($"value").as("ts"))
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
    val partId = StateStore.PARTITION_ID_TO_CHECK_SCHEMA
    val operatorRoot = new Path(checkpointPath, "state/0")
    val partitionRoot = new Path(operatorRoot, s"$partId")
    val hadoopConf = spark.sessionState.newHadoopConf()
    val fm = CheckpointFileManager.create(operatorRoot, hadoopConf)
    val fs = operatorRoot.getFileSystem(hadoopConf)

    def collectSchemaFiles(dir: Path): Seq[Path] = {
      if (!fm.exists(dir)) return Seq.empty
      if (fs.getFileStatus(dir).isDirectory) {
        fs.listStatus(dir).filter(_.isFile).map(_.getPath).toSeq
      } else {
        Seq(dir)
      }
    }

    val schemaFiles = scala.collection.mutable.ArrayBuffer.empty[Path]

    val storeDirs = scala.collection.mutable.ArrayBuffer(partitionRoot)
    if (fs.exists(partitionRoot)) {
      fs.listStatus(partitionRoot)
        .filter(_.isDirectory)
        .filterNot(_.getPath.getName.startsWith("_"))
        .foreach(d => storeDirs += d.getPath)
    }
    storeDirs.foreach { storeDir =>
      schemaFiles ++= collectSchemaFiles(
        new Path(storeDir, "_metadata/schema"))
    }

    val stateSchemaRoot = new Path(operatorRoot, "_stateSchema")
    if (fs.exists(stateSchemaRoot)) {
      fs.listStatus(stateSchemaRoot)
        .filter(_.isDirectory)
        .foreach { storeDir =>
          schemaFiles ++= collectSchemaFiles(storeDir.getPath)
        }
    }

    assert(schemaFiles.nonEmpty,
      s"expected at least one schema file under $operatorRoot")
    schemaFiles.foreach { schemaFile =>
      val inStream = fm.open(schemaFile)
      try {
        val schemas = StateSchemaCompatibilityChecker.readSchemaFile(inStream)
        schemas.foreach { s =>
          assertSchemaAllNullable(s.keySchema,
            s"$schemaFile: key schema for col family ${s.colFamilyName}")
          assertSchemaAllNullable(s.valueSchema,
            s"$schemaFile: value schema for col family ${s.colFamilyName}")
        }
      } finally inStream.close()
    }
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
          condition = "STATE_STORE_KEY_SCHEMA_NOT_COMPATIBLE",
          parameters = Map(
            "storedKeySchema" -> ".*",
            "newKeySchema" -> ".*"),
          matchPVals = true
        )
      }
    }
  }

  test("stream-stream join: non-nullable -> nullable widening remains restart-compatible") {
    withTempDir { checkpointDir =>
      val checkpointPath = checkpointDir.getAbsolutePath

      def buildJoinQuery(): (MemoryStream[Int], MemoryStream[Int], DataFrame) = {
        val leftInput = MemoryStream[Int]
        val rightInput = MemoryStream[Int]

        val left = leftInput.toDF()
          .select($"value".as("key"),
            timestamp_seconds($"value").as("leftTime"))
          .withWatermark("leftTime", "10 seconds")
        val right = rightInput.toDF()
          .select(
            when($"value" > Int.MinValue, $"value")
              .otherwise(lit(null).cast("int")).as("key"),
            timestamp_seconds($"value").as("rightTime"))
          .withWatermark("rightTime", "10 seconds")

        val joined = left.join(right,
          left("key") === right("key") &&
            left("leftTime") > right("rightTime") - expr("INTERVAL 10 SECONDS") &&
            left("leftTime") < right("rightTime") + expr("INTERVAL 10 SECONDS"),
          "inner")
        (leftInput, rightInput, joined)
      }

      val (leftInput1, rightInput1, joined1) = buildJoinQuery()
      testStream(joined1, OutputMode.Append())(
        StartStream(checkpointLocation = checkpointPath),
        MultiAddData(leftInput1, 1, 2, 3)(rightInput1, 1, 2),
        ProcessAllAvailable(),
        StopStream
      )

      assertJournaledStateSchemaAllNullable(checkpointPath)

      val (leftInput2, rightInput2, joined2) = buildJoinQuery()
      testStream(joined2, OutputMode.Append())(
        StartStream(checkpointLocation = checkpointPath),
        MultiAddData(leftInput2, 4)(rightInput2, 5),
        ProcessAllAvailable()
      )
    }
  }

  test("streaming flatMapGroupsWithState: " +
    "non-nullable -> nullable widening remains restart-compatible") {
    val stateFunc = (key: Int, values: Iterator[Int], state: GroupState[Int]) => {
      val sum = values.sum + state.getOption.getOrElse(0)
      state.update(sum)
      Iterator((key, sum))
    }

    withTempDir { checkpointDir =>
      val checkpointPath = checkpointDir.getAbsolutePath

      def buildFmgwsQuery()
          : (MemoryStream[Int], MemoryStream[Int], DataFrame) = {
        val (inputA, inputB, dfA, dfB) = buildTwoSources()
        val result = dfA.union(dfB)
          .as[Int]
          .groupByKey(identity)
          .flatMapGroupsWithState(
            OutputMode.Update(), GroupStateTimeout.NoTimeout())(stateFunc)
          .toDF("key", "sum")
        (inputA, inputB, result)
      }

      val (inputA1, inputB1, q1) = buildFmgwsQuery()
      testStream(q1, OutputMode.Update())(
        StartStream(checkpointLocation = checkpointPath),
        AddData(inputA1, 1, 2, 3),
        ProcessAllAvailable(),
        StopStream
      )

      assertJournaledStateSchemaAllNullable(checkpointPath)

      val (inputA2, inputB2, q2) = buildFmgwsQuery()
      testStream(q2, OutputMode.Update())(
        StartStream(checkpointLocation = checkpointPath),
        MultiAddData(inputA2, 4)(inputB2, 5),
        ProcessAllAvailable()
      )
    }
  }

  test("streaming transformWithState: " +
    "non-nullable -> nullable widening remains restart-compatible") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
    withTempDir { checkpointDir =>
      val checkpointPath = checkpointDir.getAbsolutePath

      def buildTwsQuery()
          : (MemoryStream[Int], MemoryStream[Int], DataFrame) = {
        val (inputA, inputB, dfA, dfB) = buildTwoSources()
        val result = dfA.union(dfB)
          .as[Int]
          .groupByKey(identity)
          .transformWithState(
            new NullabilityDriftCountProcessor(),
            TimeMode.None(),
            OutputMode.Update())
        (inputA, inputB, result.toDF())
      }

      val (inputA1, inputB1, q1) = buildTwsQuery()
      testStream(q1, OutputMode.Update())(
        StartStream(checkpointLocation = checkpointPath),
        AddData(inputA1, 1, 2, 3),
        ProcessAllAvailable(),
        StopStream
      )

      assertJournaledStateSchemaAllNullable(checkpointPath)

      val (inputA2, inputB2, q2) = buildTwsQuery()
      testStream(q2, OutputMode.Update())(
        StartStream(checkpointLocation = checkpointPath),
        MultiAddData(inputA2, 4)(inputB2, 5),
        ProcessAllAvailable()
      )
    }
    }
  }

  test("rule skips non-stateful nodes whose subtree has no stateful operator") {
    import org.apache.spark.sql.catalyst.analysis.WidenStatefulOperatorAttributeNullability
    import org.apache.spark.sql.catalyst.expressions.{AttributeReference, NamedExpression}
    import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LocalRelation, Project}
    import org.apache.spark.sql.types.IntegerType

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

class NullabilityDriftCountProcessor
    extends StatefulProcessor[Int, Int, (Int, Long)] {
  @transient private var countState: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    countState = getHandle.getValueState[Long](
      "count", Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: Int,
      rows: Iterator[Int],
      timerValues: TimerValues): Iterator[(Int, Long)] = {
    val count = (if (countState.exists()) countState.get() else 0L) + rows.size
    countState.update(count)
    Iterator((key, count))
  }
}
