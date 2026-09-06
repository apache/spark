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

package org.apache.spark.sql.execution.python

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkConf, SparkEnv, SparkException}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType, SimplePythonFunction}
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * Unit coverage for the session Python worker environment: how it is read from the session
 * configurations, validated, merged, and installed by the runners that launch Python workers.
 *
 * End-to-end coverage -- that a worker process really observes the variables -- lives in the
 * PySpark suites, since it needs a Python worker.
 */
class PythonWorkerEnvironmentSuite extends QueryTest with SharedSparkSession {

  private def key(name: String): String = PythonWorkerEnvironment.confPrefix + name

  /** A Python function carrying `env`, as a classic session's `sc.environment` supplies it. */
  private def functionWith(env: Map[String, String]): SimplePythonFunction = {
    SimplePythonFunction(
      command = Seq.empty[Byte],
      envVars = new java.util.HashMap[String, String](env.asJava),
      pythonIncludes = new java.util.ArrayList[String](),
      pythonExec = "python3",
      pythonVer = "3.11",
      broadcastVars = new java.util.ArrayList(),
      accumulator = null)
  }

  private def chained(env: Map[String, String]): Seq[(ChainedPythonFunctions, Long)] =
    Seq((ChainedPythonFunctions(Seq(functionWith(env))), 0L))

  // The runners below are the ones the physical operators actually instantiate --
  // `BatchEvalPythonExec`, `ArrowEvalPythonExec` and `ColumnarArrowEvalPythonEvaluatorFactory`
  // respectively. They are used in preference to their simpler siblings because overriding
  // `envVars` in a subclass is an established pattern in this package (`ArrowPythonUDTFRunner`
  // and `CoGroupedArrowPythonRunner` both do it), so testing a sibling would not catch a
  // production runner that stopped inheriting the merge.

  private val udfSchema = StructType(Seq(StructField("value", StringType)))
  private val argMetas = Array(Array(ArgumentMetadata(0, None)))

  /** The environment the non-Arrow scalar UDF runner would hand to a worker. */
  private def batchedRunner(
      evalType: Int = PythonEvalType.SQL_BATCHED_UDF,
      funcs: Seq[(ChainedPythonFunctions, Long)] = chained(Map.empty),
      sessionUUID: Option[String] = None): BasePythonUDFRunner = {
    new PythonUDFWithNamedArgumentsRunner(
      funcs = funcs,
      evalType = evalType,
      argMetas = argMetas,
      pythonMetrics = Map.empty[String, SQLMetric],
      jobArtifactUUID = None,
      sessionUUID = sessionUUID)
  }

  private def batchedRunnerEnv(
      evalType: Int = PythonEvalType.SQL_BATCHED_UDF,
      functionEnv: Map[String, String] = Map.empty,
      sessionUUID: Option[String] = None): Map[String, String] =
    batchedRunner(evalType, chained(functionEnv), sessionUUID).envVars.asScala.toMap

  /** The environment the Arrow scalar UDF runner would hand to a worker. */
  private def arrowRunnerEnv(
      evalType: Int = PythonEvalType.SQL_ARROW_BATCHED_UDF,
      functionEnv: Map[String, String] = Map.empty,
      sessionUUID: Option[String] = None): Map[String, String] = {
    new ArrowPythonWithNamedArgumentRunner(
      funcs = chained(functionEnv),
      evalType = evalType,
      argMetas = argMetas,
      schema = udfSchema,
      timeZoneId = "UTC",
      largeVarTypes = false,
      pythonRunnerConf = Map.empty,
      pythonMetrics = Map.empty[String, SQLMetric],
      jobArtifactUUID = None,
      sessionUUID = sessionUUID).envVars.asScala.toMap
  }

  /**
   * The environment the columnar Arrow scalar UDF runner would hand to a worker. It inherits the
   * merge from `BaseArrowPythonRunner` rather than declaring anything of its own, so without a test
   * this production path would be covered only by that inheritance holding.
   */
  private def columnarArrowRunnerEnv(
      evalType: Int = PythonEvalType.SQL_ARROW_BATCHED_UDF,
      functionEnv: Map[String, String] = Map.empty,
      sessionUUID: Option[String] = None): Map[String, String] = {
    new ColumnarArrowPythonWithNamedArgumentRunner(
      funcs = chained(functionEnv),
      evalType = evalType,
      argMetas = argMetas,
      schema = udfSchema,
      timeZoneId = "UTC",
      largeVarTypes = false,
      pythonRunnerConf = Map.empty,
      pythonMetrics = Map.empty[String, SQLMetric],
      jobArtifactUUID = None,
      sessionUUID = sessionUUID,
      inputColumnIndices = Array(0)).envVars.asScala.toMap
  }

  /** Overrides a cluster-level limit for the duration of `body`. */
  private def withLimit[T](entry: ConfigEntry[T], value: T)(body: => Unit): Unit = {
    val conf = SparkEnv.get.conf
    val previous = conf.get(entry)
    conf.set(entry, value)
    try body
    finally conf.set(entry, previous)
  }

  // ---------------------------------------------------------------------------
  // Reading the environment from session configurations
  // ---------------------------------------------------------------------------

  test("SPARK-58752: no configurations means an empty environment") {
    assert(PythonWorkerEnvironment.read(spark.sessionState.conf) === Map.empty)
  }

  test("SPARK-58752: configurations under the prefix become environment variables") {
    withSQLConf(key("FOO") -> "1", key("BAR") -> "2") {
      assert(
        PythonWorkerEnvironment.read(spark.sessionState.conf) ===
          Map("FOO" -> "1", "BAR" -> "2"))
    }
  }

  test("SPARK-58752: configurations outside the prefix are ignored") {
    withSQLConf(key("FOO") -> "1", "spark.sql.unrelated.setting" -> "2") {
      assert(PythonWorkerEnvironment.read(spark.sessionState.conf) === Map("FOO" -> "1"))
    }
  }

  test("SPARK-58752: an empty value is allowed") {
    withSQLConf(key("FOO") -> "") {
      assert(PythonWorkerEnvironment.readValidated(spark.sessionState.conf) === Map("FOO" -> ""))
    }
  }

  test("SPARK-58752: variable names are case-sensitive") {
    withSQLConf(key("FOO") -> "upper", key("foo") -> "lower") {
      assert(
        PythonWorkerEnvironment.read(spark.sessionState.conf) ===
          Map("FOO" -> "upper", "foo" -> "lower"))
    }
  }

  test("SPARK-58752: read does not validate") {
    withSQLConf(key("1INVALID") -> "x") {
      // Reading has to stay usable on an invalid environment: only the queries that would install
      // it in a worker may fail, not every query in the session.
      assert(PythonWorkerEnvironment.read(spark.sessionState.conf) === Map("1INVALID" -> "x"))
    }
  }

  test("SPARK-58752: the limit configurations are not read back as environment variables") {
    // Every SparkConf entry is copied into a new session's SQLConf, so a limit named under the
    // reserved prefix would come back out of `read` as a variable. The keys are deliberately not
    // under it; this pins that.
    Seq(
      StaticSQLConf.PYTHON_WORKER_ENV_MAX_VARIABLES,
      StaticSQLConf.PYTHON_WORKER_ENV_MAX_NAME_LENGTH,
      StaticSQLConf.PYTHON_WORKER_ENV_MAX_TOTAL_SIZE_BYTES).map(_.key).foreach { limitKey =>
      assert(
        !limitKey.startsWith(PythonWorkerEnvironment.confPrefix),
        s"$limitKey would be read back as an environment variable")
    }
  }

  // ---------------------------------------------------------------------------
  // Validation
  // ---------------------------------------------------------------------------

  test("SPARK-58752: an invalid variable name is rejected") {
    withSQLConf(key("1INVALID") -> "x") {
      val ex = intercept[SparkException] {
        PythonWorkerEnvironment.readValidated(spark.sessionState.conf)
      }
      assert(ex.getCondition === "INVALID_SPARK_CONFIG.INVALID_PYTHON_WORKER_ENV_VAR_NAME")
    }
  }

  test("SPARK-58752: a rejected name cannot inject control characters into the message") {
    val newline = 0x0a.toChar
    val escape = 0x1b.toChar
    val name = s"BAD${newline}name${escape}[31m"
    withSQLConf(key(name) -> "x") {
      val ex = intercept[SparkException] {
        PythonWorkerEnvironment.readValidated(spark.sessionState.conf)
      }
      assert(!ex.getMessage.contains(newline.toString))
      assert(!ex.getMessage.contains(escape.toString))
      assert(ex.getMessage.contains("\\x0a"))
    }
  }

  test("SPARK-58752: a value containing NUL is rejected without quoting the value") {
    val nul = 0.toChar
    withSQLConf(key("WITH_NUL") -> s"abc${nul}def") {
      val ex = intercept[SparkException] {
        PythonWorkerEnvironment.readValidated(spark.sessionState.conf)
      }
      assert(ex.getCondition === "INVALID_SPARK_CONFIG.INVALID_PYTHON_WORKER_ENV_VAR_VALUE")
      // A value can be a secret, so a rejection names the variable but never its value.
      assert(ex.getMessage.contains("WITH_NUL"))
      assert(!ex.getMessage.contains("abc"))
      assert(!ex.getMessage.contains("def"))
    }
  }

  test("SPARK-58752: a name over the length limit is rejected, and exactly the limit passes") {
    withLimit(StaticSQLConf.PYTHON_WORKER_ENV_MAX_NAME_LENGTH, 4) {
      // Exactly at the limit, so the comparison has to be `>` rather than `>=`.
      withSQLConf(key("ABCD") -> "x") {
        assert(
          PythonWorkerEnvironment.readValidated(spark.sessionState.conf) === Map("ABCD" -> "x"))
      }
      withSQLConf(key("TOOLONG") -> "x") {
        val ex = intercept[SparkException] {
          PythonWorkerEnvironment.readValidated(spark.sessionState.conf)
        }
        assert(ex.getCondition === "INVALID_SPARK_CONFIG.INVALID_PYTHON_WORKER_ENV_VAR_NAME")
      }
    }
  }

  test("SPARK-58752: more variables than the limit is rejected, and exactly the limit passes") {
    withLimit(StaticSQLConf.PYTHON_WORKER_ENV_MAX_VARIABLES, 1) {
      withSQLConf(key("FIRST") -> "1") {
        assert(PythonWorkerEnvironment.readValidated(spark.sessionState.conf).size === 1)
      }
      withSQLConf(key("FIRST") -> "1", key("SECOND") -> "2") {
        val ex = intercept[SparkException] {
          PythonWorkerEnvironment.readValidated(spark.sessionState.conf)
        }
        assert(ex.getCondition === "INVALID_SPARK_CONFIG.PYTHON_WORKER_ENV_TOO_MANY_VARIABLES")
      }
    }
  }

  test("SPARK-58752: a zero variable limit accepts no environment at all") {
    withLimit(StaticSQLConf.PYTHON_WORKER_ENV_MAX_VARIABLES, 0) {
      withSQLConf(key("FOO") -> "1") {
        val ex = intercept[SparkException] {
          PythonWorkerEnvironment.readValidated(spark.sessionState.conf)
        }
        assert(ex.getCondition === "INVALID_SPARK_CONFIG.PYTHON_WORKER_ENV_TOO_MANY_VARIABLES")
      }
    }
  }

  test("SPARK-58752: total size counts UTF-8 bytes rather than characters") {
    // A character that needs 3 bytes in UTF-8, built from its code point so that this file stays
    // ASCII. Repeated enough to exceed the byte limit while the character count stays under it, so
    // the test fails if the limit is ever applied to characters instead of bytes.
    val threeByteChar = 0x4e2d.toChar.toString
    val value = threeByteChar * 64
    withLimit(StaticSQLConf.PYTHON_WORKER_ENV_MAX_TOTAL_SIZE_BYTES, 128L) {
      assert(value.length < 128, "character count must stay under the limit")
      withSQLConf(key("BIG") -> value) {
        val ex = intercept[SparkException] {
          PythonWorkerEnvironment.readValidated(spark.sessionState.conf)
        }
        assert(ex.getCondition === "INVALID_SPARK_CONFIG.PYTHON_WORKER_ENV_TOO_LARGE")
      }
    }
  }

  test("SPARK-58752: a total size over the limit is rejected, and exactly the limit passes") {
    withLimit(StaticSQLConf.PYTHON_WORKER_ENV_MAX_TOTAL_SIZE_BYTES, 128L) {
      // The size counted is the sum of the UTF-8 lengths of every name and value, so a one-byte
      // name with a 127-byte value sits exactly on the limit and must be accepted.
      withSQLConf(key("N") -> ("x" * 127)) {
        assert(
          PythonWorkerEnvironment.readValidated(spark.sessionState.conf) ===
            Map("N" -> ("x" * 127)))
      }
      withSQLConf(key("N") -> ("x" * 128)) {
        val ex = intercept[SparkException] {
          PythonWorkerEnvironment.readValidated(spark.sessionState.conf)
        }
        assert(ex.getCondition === "INVALID_SPARK_CONFIG.PYTHON_WORKER_ENV_TOO_LARGE")
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Write-time rejection at the shared `RuntimeConfig` boundary
  // ---------------------------------------------------------------------------

  test("SPARK-58752: a classic conf write is refused before it is stored") {
    // `RuntimeConfig.set` is the write path both front ends share, so an invalid variable fails at
    // the call instead of at the first query that would launch a worker. Nothing is stored, so the
    // session is not left carrying an environment its queries cannot use.
    val ex = intercept[SparkException] {
      spark.conf.set(key("1INVALID"), "x")
    }
    assert(ex.getCondition === "INVALID_SPARK_CONFIG.INVALID_PYTHON_WORKER_ENV_VAR_NAME")
    assert(spark.conf.getOption(key("1INVALID")).isEmpty)
  }

  test("SPARK-58752: a classic conf write is validated against the whole environment") {
    // The count limit is a property of the environment, not of the entry being written, so the
    // check has to read what the session already holds rather than only the incoming pair.
    withLimit(StaticSQLConf.PYTHON_WORKER_ENV_MAX_VARIABLES, 1) {
      spark.conf.set(key("FIRST"), "1")
      try {
        val ex = intercept[SparkException] {
          spark.conf.set(key("SECOND"), "2")
        }
        assert(ex.getCondition === "INVALID_SPARK_CONFIG.PYTHON_WORKER_ENV_TOO_MANY_VARIABLES")
        assert(spark.conf.getOption(key("SECOND")).isEmpty)
      } finally {
        spark.conf.unset(key("FIRST"))
      }
    }
  }

  test("SPARK-58752: a write outside the prefix is not validated as an environment") {
    // The check is scoped to the prefix so an ordinary configuration write neither validates an
    // environment nor takes the monitor. A name that would be rejected under the prefix is fine
    // here, because it is not an environment variable name at all.
    val plainKey = "spark.sql.pythonWorkerEnvNotAPrefixMatch"
    spark.conf.set(plainKey, "1INVALID")
    try {
      assert(spark.conf.get(plainKey) === "1INVALID")
    } finally {
      spark.conf.unset(plainKey)
    }
  }

  test("SPARK-58752: removing a variable is not validated") {
    // A removal can only shrink the environment, and it is how a session recovers from an invalid
    // one left behind by a write straight to `SQLConf`, which is how `SparkSession.builder` merges
    // its configurations and is the one path this boundary does not see.
    spark.sessionState.conf.setConfString(key("1INVALID"), "x")
    spark.conf.unset(key("1INVALID"))
    assert(spark.conf.getOption(key("1INVALID")).isEmpty)
  }

  // ---------------------------------------------------------------------------
  // Merging and precedence
  // ---------------------------------------------------------------------------

  test("SPARK-58752: the session environment overrides an executorEnv variable") {
    // `spark.executorEnv.*` is the pre-existing, application-scoped way to put a variable in a
    // Python worker: a classic `SparkContext` copies these into `sc.environment`, which becomes
    // the function's `envVars`. Build the original environment from that same API so the
    // precedence rule is pinned against it rather than against a hand-written map.
    val sparkConf = new SparkConf(false)
      .set("spark.executorEnv.SHARED", "from_application")
      .set("spark.executorEnv.ONLY_APPLICATION", "kept")
    val applicationEnv = sparkConf.getExecutorEnv.toMap
    assert(applicationEnv === Map("SHARED" -> "from_application", "ONLY_APPLICATION" -> "kept"))

    withSQLConf(key("SHARED") -> "from_session") {
      val merged = batchedRunnerEnv(functionEnv = applicationEnv)
      // The session's own configuration is the more specific statement of intent, so it wins.
      assert(merged("SHARED") === "from_session")
      // A variable only the application set survives; merging is not a replacement.
      assert(merged("ONLY_APPLICATION") === "kept")
    }
  }

  test("SPARK-58752: mergeToJavaMap returns an independent mutable copy") {
    val original = functionWith(Map("FOO" -> "1"))
    val first = PythonWorkerEnvironment.mergeToJavaMap(original.envVars, Map.empty)
    // The Python runners add their own entries to the map they are given before launching a
    // worker, so a shared map would leak entries between functions.
    first.put("ADDED_BY_RUNNER", "2")
    val second = PythonWorkerEnvironment.mergeToJavaMap(original.envVars, Map.empty)
    assert(!second.containsKey("ADDED_BY_RUNNER"))
    assert(second.get("FOO") === "1")
    assert(!original.envVars.containsKey("ADDED_BY_RUNNER"))
  }

  test("SPARK-58752: mergeValidated rejects a malformed environment") {
    withSQLConf(key("1INVALID") -> "x") {
      val ex = intercept[SparkException] {
        PythonWorkerEnvironment.mergeValidated(
          new java.util.HashMap[String, String](), spark.sessionState.conf)
      }
      assert(ex.getCondition === "INVALID_SPARK_CONFIG.INVALID_PYTHON_WORKER_ENV_VAR_NAME")
    }
  }

  // ---------------------------------------------------------------------------
  // The evaluation types in scope
  // ---------------------------------------------------------------------------

  test("SPARK-58752: appliesTo covers every form of the regular scalar Python UDF") {
    // A UDF inside a higher-order function's lambda is lifted to the element-wise type by
    // `ExtractPythonUDFFromLambda`, and that is on by default, so it is the same UDF to the user.
    assert(PythonWorkerEnvironment.appliesTo(PythonEvalType.SQL_BATCHED_UDF))
    assert(PythonWorkerEnvironment.appliesTo(PythonEvalType.SQL_ARROW_BATCHED_UDF))
    assert(PythonWorkerEnvironment.appliesTo(PythonEvalType.SQL_ARROW_ELEMENTWISE_UDF))
  }

  test("SPARK-58752: an element-wise scalar UDF runner installs the session environment") {
    withSQLConf(key("FOO") -> "bar") {
      val evalType = PythonEvalType.SQL_ARROW_ELEMENTWISE_UDF
      assert(arrowRunnerEnv(evalType = evalType).get("FOO") === Some("bar"))
      assert(columnarArrowRunnerEnv(evalType = evalType).get("FOO") === Some("bar"))
    }
  }

  test("SPARK-58752: the pandas element-wise types stay out of scope") {
    // Lifting a pandas or Arrow UDF produces its own element-wise type, which keeps that family's
    // batching contract and is not covered here.
    Seq(
      PythonEvalType.SQL_SCALAR_PANDAS_ELEMENTWISE_UDF,
      PythonEvalType.SQL_SCALAR_ARROW_ELEMENTWISE_UDF).foreach { evalType =>
      assert(!PythonWorkerEnvironment.appliesTo(evalType), s"evalType $evalType is not in scope")
    }
  }

  // ---------------------------------------------------------------------------
  // Spark's own namespace
  // ---------------------------------------------------------------------------

  test("SPARK-58752: a name in Spark's namespace is rejected") {
    // Write order protects only the variables Spark sets unconditionally. Several are conditional
    // -- SPARK_REUSE_WORKER, SPARK_HIDE_TRACEBACK, PYTHON_FAULTHANDLER_DIR, SPARK_PIPELINED_UDF,
    // PYSPARK_SPARK_SESSION_UUID -- so with the condition false a session's value would otherwise
    // reach the worker. SPARK_PIPELINED_UDF is read by the worker to pick its wire protocol.
    Seq(
      "SPARK_PIPELINED_UDF",
      "SPARK_REUSE_WORKER",
      "SPARK_HIDE_TRACEBACK",
      "PYSPARK_SPARK_SESSION_UUID",
      "PYTHON_FAULTHANDLER_DIR",
      "PYTHON_TRACEBACK_DUMP_INTERVAL_SECONDS",
      "PYTHON_DAEMON_KILL_WORKER_ON_FLUSH_FAILURE",
      "PYTHON_UNIX_DOMAIN_ENABLED",
      "PYTHON_WORKER_FACTORY_SECRET",
      // Set only on the daemon's Unix-domain-socket branch. Enumerating this family missed it, so
      // the whole `PYTHON_WORKER_FACTORY_` prefix is reserved and this name pins that.
      "PYTHON_WORKER_FACTORY_SOCK_DIR",
      "OMP_NUM_THREADS").foreach { name =>
      withSQLConf(key(name) -> "1") {
        val ex = intercept[SparkException] {
          PythonWorkerEnvironment.readValidated(spark.sessionState.conf)
        }
        assert(
          ex.getCondition === "INVALID_SPARK_CONFIG.RESERVED_PYTHON_WORKER_ENV_VAR_NAME",
          s"$name should be rejected as reserved")
        assert(ex.getMessage.contains(name))
      }
    }
  }

  test("SPARK-58752: a reserved name is rejected before a worker is launched") {
    withSQLConf(key("SPARK_PIPELINED_UDF") -> "1") {
      val ex = intercept[SparkException](batchedRunnerEnv())
      assert(ex.getCondition === "INVALID_SPARK_CONFIG.RESERVED_PYTHON_WORKER_ENV_VAR_NAME")
    }
  }

  test("SPARK-58752: a name Spark sets unconditionally is still the session's to set") {
    // Write order already protects these -- the runner and the worker factory apply them after the
    // session's environment -- so they are deliberately not reserved. A session setting one is
    // harmless: Spark's value still reaches the worker.
    withSQLConf(
      key("PYTHONUNBUFFERED") -> "NO",
      key("PYTHON_UDF_BATCH_SIZE") -> "1",
      key("PYTHONWARNINGS") -> "ignore",
      key("MY_SPARK_SETTING") -> "1") {
      val env = PythonWorkerEnvironment.readValidated(spark.sessionState.conf)
      assert(env.keySet === Set(
        "PYTHONUNBUFFERED",
        "PYTHON_UDF_BATCH_SIZE",
        "PYTHONWARNINGS",
        "MY_SPARK_SETTING"))
    }
  }

  test("SPARK-58752: PYTHONPATH is accepted, because Spark merges it rather than replacing it") {
    // The only name Spark neither reserves nor overwrites: `PythonWorkerFactory` folds the
    // session's value into the path it computes. Accepting the name is deliberate, not an oversight
    // in the reserved list -- a session already chooses the code its own worker runs. Spark's own
    // entries are not guaranteed to come first, so nothing here asserts an order. This pins only
    // that validation lets the name through; the merge needs a running worker, so the end-to-end
    // `test_pythonpath_reaches_the_worker_import_path` covers that.
    withSQLConf(key("PYTHONPATH") -> "/tmp/extra-modules") {
      assert(
        PythonWorkerEnvironment.readValidated(spark.sessionState.conf) ===
          Map("PYTHONPATH" -> "/tmp/extra-modules"))
    }
  }

  test("SPARK-58752: the reserved set is pinned, and write-order names stay settable") {
    // The reserved list is a hand-maintained mirror of the `envVars.put` calls in
    // `BasePythonRunner.compute` and `PythonWorkerFactory`. Restating it here makes an edit
    // deliberate rather than incidental; it cannot catch a name Spark adds upstream later, which
    // is the standing cost of a list and the gap that left `PYTHON_WORKER_FACTORY_SOCK_DIR`
    // settable until the family became a prefix.
    assert(PythonWorkerEnvironment.reservedNames === Set(
      "OMP_NUM_THREADS",
      "PYTHON_DAEMON_KILL_WORKER_ON_FLUSH_FAILURE",
      "PYTHON_FAULTHANDLER_DIR",
      "PYTHON_TRACEBACK_DUMP_INTERVAL_SECONDS",
      "PYTHON_UNIX_DOMAIN_ENABLED"))
    // Unconditional writes are protected by write order and must stay settable, so they must not
    // creep into the reserved set.
    Seq("PYTHONUNBUFFERED", "PYTHON_UDF_BATCH_SIZE", "PYTHONPATH").foreach { name =>
      assert(!PythonWorkerEnvironment.reservedNames.contains(name))
      assert(!PythonWorkerEnvironment.reservedNamePrefixes.exists(name.startsWith))
    }
  }

  test("SPARK-58752: appliesTo excludes the families this change does not cover") {
    // Widening the scope has to come with a test on the widened path, so these are pinned here to
    // make an accidental widening fail rather than ship silently.
    Seq(
      PythonEvalType.SQL_SCALAR_PANDAS_UDF,
      PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF,
      PythonEvalType.SQL_TABLE_UDF,
      PythonEvalType.SQL_ARROW_TABLE_UDF).foreach { evalType =>
      assert(!PythonWorkerEnvironment.appliesTo(evalType), s"evalType $evalType is not in scope")
    }
  }

  // ---------------------------------------------------------------------------
  // Installation by the runners
  // ---------------------------------------------------------------------------

  test("SPARK-58752: a non-Arrow scalar UDF runner installs the session environment") {
    withSQLConf(key("FOO") -> "bar") {
      assert(batchedRunnerEnv().get("FOO") === Some("bar"))
    }
  }

  test("SPARK-58752: an Arrow scalar UDF runner installs the session environment") {
    // Arrow is the default for a regular Python UDF, so this is the ordinary path.
    withSQLConf(key("FOO") -> "bar") {
      assert(arrowRunnerEnv().get("FOO") === Some("bar"))
    }
  }

  test("SPARK-58752: the columnar Arrow scalar UDF runner installs the session environment") {
    withSQLConf(key("FOO") -> "bar") {
      assert(columnarArrowRunnerEnv().get("FOO") === Some("bar"))
    }
  }

  test("SPARK-58752: a runner outside the scope installs nothing") {
    withSQLConf(key("FOO") -> "bar") {
      // Same class as the scalar UDF runner, so only the evaluation type keeps UDTFs out.
      assert(batchedRunnerEnv(evalType = PythonEvalType.SQL_TABLE_UDF).get("FOO").isEmpty)
      // Same for the Arrow hierarchy, which the pandas and window paths share.
      assert(arrowRunnerEnv(evalType = PythonEvalType.SQL_SCALAR_PANDAS_UDF).get("FOO").isEmpty)
      assert(
        columnarArrowRunnerEnv(evalType = PythonEvalType.SQL_SCALAR_PANDAS_UDF).get("FOO").isEmpty)
    }
  }

  test("SPARK-58752: a runner installs nothing when no environment is set") {
    assert(batchedRunnerEnv() === Map.empty)
    assert(arrowRunnerEnv().get("FOO").isEmpty)
  }

  test("SPARK-58752: a Spark-owned name is refused rather than silently overridden") {
    // Superseded by the reserved-name rule: `PYSPARK_SPARK_SESSION_UUID` is only written when a
    // session UUID is present, so relying on write order left a session's value in place whenever
    // it was absent. Rejecting the name removes that gap for the conditional and unconditional
    // cases alike.
    withSQLConf(key("PYSPARK_SPARK_SESSION_UUID") -> "forged") {
      Seq(
        () => batchedRunnerEnv(sessionUUID = None),
        () => batchedRunnerEnv(sessionUUID = Some("real")),
        () => arrowRunnerEnv(sessionUUID = None)).foreach { build =>
        val ex = intercept[SparkException](build())
        assert(ex.getCondition === "INVALID_SPARK_CONFIG.RESERVED_PYTHON_WORKER_ENV_VAR_NAME")
      }
    }
  }

  test("SPARK-58752: an invalid environment fails the runner rather than reaching a worker") {
    withSQLConf(key("1INVALID") -> "x") {
      Seq(
        () => batchedRunnerEnv(),
        () => arrowRunnerEnv(),
        () => columnarArrowRunnerEnv()).foreach { build =>
        val ex = intercept[SparkException](build())
        assert(ex.getCondition === "INVALID_SPARK_CONFIG.INVALID_PYTHON_WORKER_ENV_VAR_NAME")
      }
    }
  }

  test("SPARK-58752: an invalid environment does not fail a runner outside the scope") {
    withSQLConf(key("1INVALID") -> "x") {
      // Validation is part of installing the environment, so a family that receives none is
      // unaffected by an environment it will never see.
      assert(batchedRunnerEnv(evalType = PythonEvalType.SQL_TABLE_UDF).isEmpty)
    }
  }

  test("SPARK-58752: each runner gets its own mutable copy") {
    withSQLConf(key("FOO") -> "bar") {
      val funcs = chained(Map.empty)
      val first = batchedRunner(funcs = funcs).envVars
      first.put("ADDED_BY_RUNNER", "1")
      assert(!batchedRunner(funcs = funcs).envVars.containsKey("ADDED_BY_RUNNER"))
      // The function the runners share must not have been mutated either.
      assert(!funcs.head._1.funcs.head.envVars.containsKey("ADDED_BY_RUNNER"))
    }
  }

  // ---------------------------------------------------------------------------
  // Session scoping
  // ---------------------------------------------------------------------------

  test("SPARK-58752: the environment changes between actions in one session") {
    // The environment is installed when a worker is launched, so a change takes effect on the next
    // action without rebuilding the function or invalidating a cached plan.
    val funcs = chained(Map.empty)
    def envOf(): Map[String, String] = batchedRunner(funcs = funcs).envVars.asScala.toMap
    withSQLConf(key("FOO") -> "first") {
      assert(envOf().get("FOO") === Some("first"))
    }
    withSQLConf(key("FOO") -> "second") {
      assert(envOf().get("FOO") === Some("second"))
    }
    assert(envOf().get("FOO").isEmpty, "unsetting the configuration removes the variable")
  }

  test("SPARK-58752: separate sessions on one SparkContext keep separate environments") {
    val other = spark.newSession()
    assert(other.sparkContext eq spark.sparkContext)
    other.conf.set(key("FOO"), "from_other")
    try {
      assert(other.withActive(batchedRunnerEnv()).get("FOO") === Some("from_other"))
      assert(spark.withActive(batchedRunnerEnv()).get("FOO").isEmpty)
    } finally {
      other.conf.unset(key("FOO"))
    }
  }

  test("SPARK-58752: a cloned session copies the environment and then diverges") {
    withSQLConf(key("FOO") -> "original") {
      val cloned = spark.cloneSession()
      assert(cloned.withActive(batchedRunnerEnv()).get("FOO") === Some("original"))
      // A clone copies the configurations; later writes on either side are independent.
      cloned.conf.set(key("FOO"), "changed")
      assert(cloned.withActive(batchedRunnerEnv()).get("FOO") === Some("changed"))
      assert(spark.withActive(batchedRunnerEnv()) === Map("FOO" -> "original"))
    }
  }
}
