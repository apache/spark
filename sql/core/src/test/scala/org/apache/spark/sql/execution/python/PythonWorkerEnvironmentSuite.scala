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

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.api.python.{PythonBroadcast, PythonFunction, SimplePythonFunction}
import org.apache.spark.api.python.PythonFunction.PythonAccumulator
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.test.SharedSparkSession

class PythonWorkerEnvironmentSuite extends SharedSparkSession {

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

  // ---------------------------------------------------------------------------
  // Reading
  // ---------------------------------------------------------------------------

  test("SPARK-58752: readValidated returns the environment under the prefix") {
    withSQLConf(key("FOO") -> "1", "spark.sql.unrelated.setting" -> "2") {
      assert(PythonWorkerEnvironment.readValidated(spark.sessionState.conf) === Map("FOO" -> "1"))
    }
  }

  test("SPARK-58752: readValidated rejects a malformed environment") {
    withSQLConf(key("1INVALID") -> "x") {
      val ex = intercept[SparkException] {
        PythonWorkerEnvironment.readValidated(spark.sessionState.conf)
      }
      assert(ex.getCondition === "INVALID_SPARK_CONFIG.INVALID_PYTHON_WORKER_ENV_VAR_NAME")
    }
  }

  test("SPARK-58752: the limit configurations are not read back as environment variables") {
    // Every SparkConf entry is copied into a new session's SQLConf, so a limit named under the
    // reserved prefix would come back out of `read` as a variable. The keys are deliberately not
    // under it; this pins that.
    val limitKeys = Seq(
      StaticSQLConf.PYTHON_WORKER_ENV_MAX_VARIABLES,
      StaticSQLConf.PYTHON_WORKER_ENV_MAX_NAME_LENGTH,
      StaticSQLConf.PYTHON_WORKER_ENV_MAX_TOTAL_SIZE_BYTES).map(_.key)
    limitKeys.foreach { limitKey =>
      assert(
        !limitKey.startsWith(PythonWorkerEnvironment.confPrefix),
        s"$limitKey would be read back as an environment variable")
    }
  }

  // ---------------------------------------------------------------------------
  // Precedence against the application-scoped `spark.executorEnv.*`
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

    val merged = PythonWorkerEnvironment
      .merge(functionWith(applicationEnv), Map("SHARED" -> "from_session"))
      .envVars
      .asScala

    // The session's own configuration is the more specific statement of intent, so it wins.
    assert(merged("SHARED") === "from_session")
    // A variable only the application set survives; merging is not a replacement.
    assert(merged("ONLY_APPLICATION") === "kept")
  }

  test("SPARK-58752: merge leaves the original function's environment untouched") {
    val original = functionWith(Map("SHARED" -> "from_application"))
    PythonWorkerEnvironment.merge(original, Map("SHARED" -> "from_session"))
    assert(original.envVars.get("SHARED") === "from_application")
  }

  test("SPARK-58752: each merge produces an independent mutable map") {
    val original = functionWith(Map("FOO" -> "1"))
    val first = PythonWorkerEnvironment.merge(original, Map.empty).envVars
    // The Python runners add their own entries to the map they are given before launching a
    // worker, so a shared map would leak entries between functions.
    first.put("ADDED_BY_RUNNER", "2")
    val second = PythonWorkerEnvironment.merge(original, Map.empty).envVars
    assert(!second.containsKey("ADDED_BY_RUNNER"))
    assert(second.get("FOO") === "1")
  }

  test("SPARK-58752: merge fails loudly for a function it cannot rewrite") {
    // Returning the input unchanged would drop the environment silently, so a UDF would run
    // without a variable it was told to have.
    val other = new PythonFunction {
      override def command: Seq[Byte] = Seq.empty
      override def envVars: java.util.Map[String, String] = new java.util.HashMap()
      override def pythonIncludes: java.util.List[String] = new java.util.ArrayList()
      override def pythonExec: String = "python3"
      override def pythonVer: String = "3.11"
      override def broadcastVars: java.util.List[Broadcast[PythonBroadcast]] =
        new java.util.ArrayList()
      override def accumulator: PythonAccumulator = null
    }
    val ex = intercept[SparkException] {
      PythonWorkerEnvironment.merge(other, Map("FOO" -> "1"))
    }
    assert(ex.getMessage.contains("Cannot install a Python worker environment"))
  }
}
