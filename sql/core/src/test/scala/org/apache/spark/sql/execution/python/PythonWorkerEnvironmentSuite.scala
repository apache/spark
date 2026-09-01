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

  /** A Python function carrying `env`, as a front end that supplies one would build it. */
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

  /** The environment a non-Arrow scalar runner would hand to a worker. */
  private def batchedRunner(
      evalType: Int = PythonEvalType.SQL_BATCHED_UDF,
      funcs: Seq[(ChainedPythonFunctions, Long)] = chained(Map.empty),
      sessionUUID: Option[String] = None): PythonUDFRunner = {
    new PythonUDFRunner(
      funcs = funcs,
      evalType = evalType,
      argOffsets = Array(Array(0)),
      pythonMetrics = Map.empty[String, SQLMetric],
      jobArtifactUUID = None,
      sessionUUID = sessionUUID)
  }

  private def batchedRunnerEnv(
      evalType: Int = PythonEvalType.SQL_BATCHED_UDF,
      functionEnv: Map[String, String] = Map.empty,
      sessionUUID: Option[String] = None): Map[String, String] =
    batchedRunner(evalType, chained(functionEnv), sessionUUID).envVars.asScala.toMap

  /** The environment an Arrow scalar runner would hand to a worker. */
  private def arrowRunnerEnv(
      evalType: Int = PythonEvalType.SQL_ARROW_BATCHED_UDF,
      functionEnv: Map[String, String] = Map.empty,
      sessionUUID: Option[String] = None): Map[String, String] = {
    new ArrowPythonRunner(
      funcs = chained(functionEnv),
      evalType = evalType,
      argOffsets = Array(Array(0)),
      schema = StructType(Seq(StructField("value", StringType))),
      timeZoneId = "UTC",
      largeVarTypes = false,
      pythonRunnerConf = Map.empty,
      pythonMetrics = Map.empty[String, SQLMetric],
      jobArtifactUUID = None,
      sessionUUID = sessionUUID).envVars.asScala.toMap
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

  test("SPARK-58752: a name over the length limit is rejected") {
    withLimit(StaticSQLConf.PYTHON_WORKER_ENV_MAX_NAME_LENGTH, 4) {
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

  test("SPARK-58752: appliesTo covers the regular scalar Python UDF in both forms") {
    assert(PythonWorkerEnvironment.appliesTo(PythonEvalType.SQL_BATCHED_UDF))
    assert(PythonWorkerEnvironment.appliesTo(PythonEvalType.SQL_ARROW_BATCHED_UDF))
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

  test("SPARK-58752: a runner outside the scope installs nothing") {
    withSQLConf(key("FOO") -> "bar") {
      // Same class as the scalar UDF runner, so only the evaluation type keeps UDTFs out.
      assert(batchedRunnerEnv(evalType = PythonEvalType.SQL_TABLE_UDF).get("FOO").isEmpty)
      // Same for the Arrow hierarchy, which the pandas and window paths share.
      assert(arrowRunnerEnv(evalType = PythonEvalType.SQL_SCALAR_PANDAS_UDF).get("FOO").isEmpty)
    }
  }

  test("SPARK-58752: a runner installs nothing when no environment is set") {
    assert(batchedRunnerEnv() === Map.empty)
    assert(arrowRunnerEnv().get("FOO").isEmpty)
  }

  test("SPARK-58752: a session cannot override a Spark-owned variable") {
    withSQLConf(key("PYSPARK_SPARK_SESSION_UUID") -> "forged") {
      // The runner applies its own variables after the session's, so Spark wins.
      assert(batchedRunnerEnv(sessionUUID = Some("real")) ===
        Map("PYSPARK_SPARK_SESSION_UUID" -> "real"))
      assert(arrowRunnerEnv(sessionUUID = Some("real")) ===
        Map("PYSPARK_SPARK_SESSION_UUID" -> "real"))
    }
  }

  test("SPARK-58752: an invalid environment fails the runner rather than reaching a worker") {
    withSQLConf(key("1INVALID") -> "x") {
      val ex = intercept[SparkException](batchedRunnerEnv())
      assert(ex.getCondition === "INVALID_SPARK_CONFIG.INVALID_PYTHON_WORKER_ENV_VAR_NAME")
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
