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

package org.apache.spark.sql.catalyst.analysis

import java.util.UUID

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkFunSuite, TaskContext}
import org.apache.spark.internal.config.ConfigBindingPolicy
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.catalog.SQLFunction
import org.apache.spark.sql.catalyst.plans.logical.View
import org.apache.spark.sql.internal.{ReadOnlySQLConf, SQLConf}
import org.apache.spark.sql.types.IntegerType

class RetainedResolutionConfigsSuite extends SparkFunSuite {
  private val sessionBoundKey = SQLConf.STRICT_DATAFRAME_COLUMN_RESOLUTION.key
  // Defined in a .textproto file with BINDING_POLICY_NOT_APPLICABLE.
  private val protoDefinedKey = "spark.sql.optimizer.maxIterations"
  private val catalogKey = "spark.sql.catalog.memo_test_catalog"
  private val memoizeKey = SQLConf.ANALYZER_MEMOIZE_RETAINED_RESOLUTION_CONFIGS.key

  /**
   * A stand-in for a session conf holding one setting per retention rule: retained SESSION-bound,
   * proto-defined NOT_APPLICABLE-bound, and catalog keys, plus a policy-less (ANSI) and an
   * unregistered key that are dropped. It also pins the policy-less flags that the view and SQL
   * function conf builds read from it; `withSQLConf` would not reach them under
   * `SQLConf.withExistingConf`.
   */
  private def newSessionConf(extraSettings: (String, String)*): SQLConf = {
    val sessionConf = new SQLConf
    (Seq(
      sessionBoundKey -> "false",
      protoDefinedKey -> "50",
      catalogKey -> "com.example.MemoTestCatalog",
      SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.ASSUME_ANSI_FALSE_IF_NOT_PERSISTED.key -> "true",
      SQLConf.APPLY_SESSION_CONF_OVERRIDES_TO_FUNCTION_RESOLUTION.key -> "true",
      SQLConf.USE_CURRENT_SQL_CONFIGS_FOR_VIEW.key -> "false",
      "spark.sql.memoTest.unregistered" -> "dropped") ++ extraSettings).foreach {
      case (key, value) => sessionConf.setConfString(key, value)
    }
    sessionConf
  }

  private val retainedFromSessionConf = Map(
    sessionBoundKey -> "false",
    protoDefinedKey -> "50",
    catalogKey -> "com.example.MemoTestCatalog")

  private def buildViewConf(
      sessionConf: SQLConf,
      capturedConfigs: Map[String, String],
      createSparkVersion: String): Map[String, String] = {
    SQLConf.withExistingConf(sessionConf) {
      View.effectiveSQLConf(
        capturedConfigs, isTempView = false, createSparkVersion = createSparkVersion).getAllConfs
    }
  }

  gridTest("view body conf is identical with and without memoized retained configs")(
    for {
      capturedAnsi <- Seq(None, Some("false"), Some("true"))
      createSparkVersion <- Seq("3.5.0", "4.0.0")
    } yield (capturedAnsi, createSparkVersion)) { case (capturedAnsi, createSparkVersion) =>
    val capturedConfigs =
      Map(sessionBoundKey -> "true", "spark.sql.memoTest.captured" -> "kept") ++
        capturedAnsi.map(SQLConf.ANSI_ENABLED.key -> _)
    val expected = capturedConfigs ++ retainedFromSessionConf +
      (SQLConf.ANSI_ENABLED.key -> capturedAnsi.getOrElse((createSparkVersion == "4.0.0").toString))

    val memoizedSessionConf = newSessionConf()
    val firstBuild = buildViewConf(memoizedSessionConf, capturedConfigs, createSparkVersion)
    val memoHit = memoizedSessionConf.retainedResolutionConfigs
    val secondBuild = buildViewConf(memoizedSessionConf, capturedConfigs, createSparkVersion)
    assert(memoizedSessionConf.retainedResolutionConfigs eq memoHit)
    val unmemoizedBuild = buildViewConf(
      newSessionConf(memoizeKey -> "false"), capturedConfigs, createSparkVersion)

    assert(firstBuild == expected)
    assert(secondBuild == expected)
    assert(unmemoizedBuild == expected + (memoizeKey -> "false"))
  }

  gridTest("SQL function body conf is identical with and without memoized retained configs")(
    for {
      capturedAnsi <- Seq(None, Some("true"))
      alwaysSetAnsiValue <- Seq(false, true)
    } yield (capturedAnsi, alwaysSetAnsiValue)) { case (capturedAnsi, alwaysSetAnsiValue) =>
    val capturedConfigs =
      Map(sessionBoundKey -> "true") ++ capturedAnsi.map(SQLConf.ANSI_ENABLED.key -> _)
    val function = SQLFunction(
      name = FunctionIdentifier("memo_test_func"),
      inputParam = None,
      returnType = Left(IntegerType),
      exprText = Some("1"),
      queryText = None,
      comment = None,
      collation = None,
      deterministic = Some(true),
      containsSQL = Some(false),
      isTableFunc = false,
      properties = capturedConfigs.map { case (key, value) => s"sqlConfig.$key" -> value })
    def buildFunctionConf(sessionConf: SQLConf): Map[String, String] =
      SQLConf.withExistingConf(sessionConf) {
        Analyzer.buildSQLFunctionConf(
          function = function,
          applySessionOverrides = true,
          alwaysSetAnsiValue = alwaysSetAnsiValue).getAllConfs
      }
    val expected = retainedFromSessionConf +
      (SQLConf.ANSI_ENABLED.key -> capturedAnsi.getOrElse("false"))

    val memoizedSessionConf = newSessionConf()
    val firstBuild = buildFunctionConf(memoizedSessionConf)
    val memoHit = memoizedSessionConf.retainedResolutionConfigs
    val secondBuild = buildFunctionConf(memoizedSessionConf)
    assert(memoizedSessionConf.retainedResolutionConfigs eq memoHit)
    val unmemoizedBuild = buildFunctionConf(newSessionConf(memoizeKey -> "false"))

    assert(firstBuild == expected)
    assert(secondBuild == expected)
    assert(unmemoizedBuild == expected + (memoizeKey -> "false"))
  }

  test("retained configs are recomputed after every change to the session conf") {
    val sessionConf = newSessionConf()
    def retainedValue: Option[String] =
      Option(sessionConf.retainedResolutionConfigs.get(sessionBoundKey))

    assert(retainedValue.contains("false"))
    sessionConf.setConfString(sessionBoundKey, "true")
    assert(retainedValue.contains("true"))
    sessionConf.settings.put(sessionBoundKey, "false")
    assert(retainedValue.contains("false"))
    sessionConf.unsetConf(sessionBoundKey)
    assert(retainedValue.isEmpty)
    sessionConf.settings.putAll(Map(sessionBoundKey -> "true").asJava)
    assert(retainedValue.contains("true"))
    sessionConf.settings.asScala -= sessionBoundKey
    assert(retainedValue.isEmpty)
    sessionConf.clear()
    assert(sessionConf.retainedResolutionConfigs.isEmpty)
  }

  test("retained configs are recomputed after a config entry is registered") {
    val lateKey = s"spark.sql.memoTest.lateRegistered.${UUID.randomUUID()}"
    val sessionConf = newSessionConf(lateKey -> "true")
    assert(!sessionConf.retainedResolutionConfigs.containsKey(lateKey))

    val lateEntry = SQLConf.buildConf(lateKey)
      .internal()
      .withBindingPolicy(ConfigBindingPolicy.SESSION)
      .booleanConf
      .createWithDefault(false)
    try {
      assert(sessionConf.retainedResolutionConfigs.get(lateKey) == "true")
    } finally {
      SQLConf.unregister(lateEntry)
    }
    assert(!sessionConf.retainedResolutionConfigs.containsKey(lateKey))
  }

  test("nested existing confs memoize their own retained configs") {
    val sessionConf = newSessionConf()
    val sessionRetained = sessionConf.retainedResolutionConfigs
    val outerViewConf = SQLConf.withExistingConf(sessionConf) {
      View.effectiveSQLConf(Map.empty, isTempView = false)
    }
    outerViewConf.settings.put(sessionBoundKey, "true")

    val nestedViewConf = SQLConf.withExistingConf(outerViewConf) {
      View.effectiveSQLConf(Map.empty, isTempView = false)
    }

    assert(nestedViewConf.getConfString(sessionBoundKey) == "true")
    assert(outerViewConf.retainedResolutionConfigs.get(sessionBoundKey) == "true")
    assert(sessionConf.retainedResolutionConfigs eq sessionRetained)
    assert(sessionRetained.get(sessionBoundKey) == "false")
  }

  test("with the memo disabled, body confs are built without consulting it") {
    // A session conf whose memo throws, so a build that succeeds did not consult it.
    def sessionConfWithThrowingMemo(memoize: Boolean): SQLConf = {
      val sessionConf = new SQLConf {
        override private[sql] def retainedResolutionConfigs: java.util.Map[String, String] =
          throw new IllegalStateException("memo consulted")
      }
      newSessionConf(memoizeKey -> memoize.toString).getAllConfs.foreach {
        case (key, value) => sessionConf.setConfString(key, value)
      }
      sessionConf
    }

    intercept[IllegalStateException] {
      buildViewConf(sessionConfWithThrowingMemo(memoize = true), Map.empty, "4.0.0")
    }
    assert(buildViewConf(sessionConfWithThrowingMemo(memoize = false), Map.empty, "4.0.0") ==
      retainedFromSessionConf + (memoizeKey -> "false") + (SQLConf.ANSI_ENABLED.key -> "true"))
  }

  test("an executor-side read-only conf filters its settings without memoizing them") {
    val taskContext = TaskContext.empty()
    (retainedFromSessionConf + (SQLConf.ANSI_ENABLED.key -> "true")).foreach {
      case (key, value) => taskContext.getLocalProperties.setProperty(key, value)
    }
    val readOnlyConf = new ReadOnlySQLConf(taskContext)

    val first = readOnlyConf.retainedResolutionConfigs
    assert(first.asScala == retainedFromSessionConf)
    assert(readOnlyConf.retainedResolutionConfigs ne first)
  }
}
