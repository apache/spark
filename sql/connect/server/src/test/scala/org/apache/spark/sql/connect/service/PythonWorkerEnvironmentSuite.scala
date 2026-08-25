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

package org.apache.spark.sql.connect.service

import com.google.protobuf.ByteString

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.api.python.{PythonEvalType, PythonFunction}
import org.apache.spark.connect.proto
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, PythonUDF}
import org.apache.spark.sql.connect.SparkConnectTestUtils
import org.apache.spark.sql.connect.common.DataTypeProtoConverter
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.planner.SparkConnectPlanTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}

class PythonWorkerEnvironmentSuite extends SparkConnectPlanTest {

  private def sessionConf: SQLConf = spark.sessionState.conf

  private def key(name: String): String = PythonWorkerEnvironment.confPrefix + name

  /**
   * Reads and validates, the way the planner does for a request that builds a Python function.
   */
  private def readValidated(conf: SQLConf): Map[String, String] = {
    val variables = PythonWorkerEnvironment.read(conf)
    PythonWorkerEnvironment.validate(variables)
    variables
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
    assert(readValidated(sessionConf).isEmpty)
  }

  test("SPARK-58752: configurations under the prefix become environment variables") {
    withSQLConf(key("FOO") -> "1", key("BAR") -> "2") {
      assert(readValidated(sessionConf) === Map("FOO" -> "1", "BAR" -> "2"))
    }
  }

  test("SPARK-58752: configurations outside the prefix are ignored") {
    withSQLConf(key("FOO") -> "1", "spark.pythonWorkerEnvOther.BAR" -> "2") {
      assert(readValidated(sessionConf) === Map("FOO" -> "1"))
    }
  }

  test("SPARK-58752: an empty value is allowed") {
    withSQLConf(key("FOO") -> "") {
      assert(readValidated(sessionConf) === Map("FOO" -> ""))
    }
  }

  test("SPARK-58752: variable names are case-sensitive") {
    withSQLConf(key("FOO") -> "upper", key("foo") -> "lower") {
      assert(readValidated(sessionConf) === Map("FOO" -> "upper", "foo" -> "lower"))
    }
  }

  test("SPARK-58752: a null value cannot be installed at all") {
    // Nothing has to handle a null value downstream: SQLConf refuses one on the way in, so an
    // absent value in a config request fails rather than storing null.
    intercept[IllegalArgumentException] {
      spark.conf.set(key("FOO"), null.asInstanceOf[String])
    }
  }

  // ---------------------------------------------------------------------------
  // Validation
  // ---------------------------------------------------------------------------

  test("SPARK-58752: an invalid variable name is rejected") {
    // "FOO\n" is included because an anchored pattern that is searched for rather than matched
    // against the whole name would accept a trailing newline.
    Seq("1FOO", "FOO-BAR", "FOO BAR", "FOO.BAR", "FOO\n", "\nFOO", "").foreach { name =>
      withSQLConf(key(name) -> "1") {
        val ex = intercept[SparkException](readValidated(sessionConf))
        assert(
          ex.getCondition === "INVALID_SPARK_CONFIG.INVALID_PYTHON_WORKER_ENV_VAR_NAME",
          s"unexpected condition for name '$name'")
      }
    }
  }

  test("SPARK-58752: a rejected name cannot inject control characters into the message") {
    // The name is user-controlled, so a message that passed it through verbatim would let a
    // rejection forge log lines.
    val newline = 10.toChar
    val tab = 9.toChar
    val delete = 127.toChar
    val escape = 27.toChar
    val name = s"FOO${newline}fake${tab}line${delete}${escape}[31m"
    withSQLConf(key(name) -> "1") {
      val ex = intercept[SparkException](readValidated(sessionConf))
      assert(ex.getCondition === "INVALID_SPARK_CONFIG.INVALID_PYTHON_WORKER_ENV_VAR_NAME")
      val message = ex.getMessage
      assert(
        !message.exists(c => c < 0x20 || (c >= 0x7f && c <= 0x9f)),
        "the message must not carry control characters")
      // The escaped form is present instead, so the name is still diagnosable.
      assert(message.contains("FOO\\x0afake\\x09line\\x7f\\x1b[31m"))
    }
  }

  test("SPARK-58752: a variable name over the length limit is rejected") {
    val longName = "A" * 513
    withSQLConf(key(longName) -> "1") {
      val ex = intercept[SparkException](readValidated(sessionConf))
      assert(ex.getCondition === "INVALID_SPARK_CONFIG.INVALID_PYTHON_WORKER_ENV_VAR_NAME")
      // The name is bounded in the message rather than echoed whole.
      assert(!ex.getMessage.contains(longName))
    }
  }

  test("SPARK-58752: a value containing NUL is rejected") {
    // A process environment cannot carry NUL, and the JDK's own rejection embeds the value in its
    // message, so this has to be caught before a worker launch is attempted.
    val nul = 0.toChar
    val secret = s"abc${nul}def"
    withSQLConf(key("FOO") -> secret) {
      val ex = intercept[SparkException](readValidated(sessionConf))
      assert(ex.getCondition === "INVALID_SPARK_CONFIG.INVALID_PYTHON_WORKER_ENV_VAR_VALUE")
      assert(ex.getMessage.contains("FOO"))
      // The value never reaches the message.
      assert(!ex.getMessage.contains("abc"))
      assert(!ex.getMessage.contains("def"))
    }
  }

  test("SPARK-58752: more variables than the limit is rejected") {
    val tooMany = (0 until 101).map(i => key(s"VAR_$i") -> "1")
    withSQLConf(tooMany: _*) {
      val ex = intercept[SparkException](readValidated(sessionConf))
      assert(ex.getCondition === "INVALID_SPARK_CONFIG.PYTHON_WORKER_ENV_TOO_MANY_VARIABLES")
      assert(ex.getMessage.contains("101"))
    }
  }

  test("SPARK-58752: exactly the variable limit is accepted") {
    val atLimit = (0 until 100).map(i => key(s"VAR_$i") -> "1")
    withSQLConf(atLimit: _*) {
      assert(readValidated(sessionConf).size === 100)
    }
  }

  test("SPARK-58752: a total size over the limit is rejected") {
    // Two values that together exceed 128 KiB, while each name stays within its own limit.
    val half = "x" * (65 * 1024)
    withSQLConf(key("A") -> half, key("B") -> half) {
      val ex = intercept[SparkException](readValidated(sessionConf))
      assert(ex.getCondition === "INVALID_SPARK_CONFIG.PYTHON_WORKER_ENV_TOO_LARGE")
      // Neither the names nor the values are echoed into the message.
      assert(!ex.getMessage.contains(half))
    }
  }

  test("SPARK-58752: total size counts UTF-8 bytes rather than characters") {
    // A character that needs 3 bytes in UTF-8, built from its code point so that this file stays
    // ASCII. Repeated enough to pass the byte limit while the character count stays under it, so
    // the test fails if the limit is ever applied to characters instead of bytes.
    val threeByteChar = 0x4e2d.toChar.toString
    val value = threeByteChar * (50 * 1024)
    assert(value.length < 128 * 1024, "character count must stay under the limit")
    withSQLConf(key("A") -> value) {
      val ex = intercept[SparkException](readValidated(sessionConf))
      assert(ex.getCondition === "INVALID_SPARK_CONFIG.PYTHON_WORKER_ENV_TOO_LARGE")
    }
  }

  // ---------------------------------------------------------------------------
  // Validation against non-default limits
  // ---------------------------------------------------------------------------

  test("SPARK-58752: the variable limit is configurable") {
    withLimit(Connect.CONNECT_PYTHON_WORKER_ENV_MAX_VARIABLES, 1) {
      withSQLConf(key("FOO") -> "1") {
        assert(readValidated(sessionConf).size === 1)
      }
      withSQLConf(key("FOO") -> "1", key("BAR") -> "2") {
        val ex = intercept[SparkException](readValidated(sessionConf))
        assert(ex.getCondition === "INVALID_SPARK_CONFIG.PYTHON_WORKER_ENV_TOO_MANY_VARIABLES")
      }
    }
  }

  test("SPARK-58752: a zero variable limit accepts no environment at all") {
    withLimit(Connect.CONNECT_PYTHON_WORKER_ENV_MAX_VARIABLES, 0) {
      assert(readValidated(sessionConf).isEmpty)
      withSQLConf(key("FOO") -> "1") {
        val ex = intercept[SparkException](readValidated(sessionConf))
        assert(ex.getCondition === "INVALID_SPARK_CONFIG.PYTHON_WORKER_ENV_TOO_MANY_VARIABLES")
      }
    }
  }

  test("SPARK-58752: the name length limit is configurable") {
    withLimit(Connect.CONNECT_PYTHON_WORKER_ENV_MAX_NAME_LENGTH, 3) {
      withSQLConf(key("FOO") -> "1") {
        assert(readValidated(sessionConf) === Map("FOO" -> "1"))
      }
      withSQLConf(key("FOUR") -> "1") {
        val ex = intercept[SparkException](readValidated(sessionConf))
        assert(ex.getCondition === "INVALID_SPARK_CONFIG.INVALID_PYTHON_WORKER_ENV_VAR_NAME")
      }
    }
  }

  test("SPARK-58752: the total size limit is configurable") {
    withLimit(Connect.CONNECT_PYTHON_WORKER_ENV_MAX_TOTAL_SIZE_BYTES, 8L) {
      withSQLConf(key("FOO") -> "12") {
        assert(readValidated(sessionConf) === Map("FOO" -> "12"))
      }
      withSQLConf(key("FOO") -> "123456") {
        val ex = intercept[SparkException](readValidated(sessionConf))
        assert(ex.getCondition === "INVALID_SPARK_CONFIG.PYTHON_WORKER_ENV_TOO_LARGE")
      }
    }
  }

  test("SPARK-58752: a negative limit is rejected by the configuration itself") {
    Seq[ConfigEntry[_]](
      Connect.CONNECT_PYTHON_WORKER_ENV_MAX_VARIABLES,
      Connect.CONNECT_PYTHON_WORKER_ENV_MAX_NAME_LENGTH,
      Connect.CONNECT_PYTHON_WORKER_ENV_MAX_TOTAL_SIZE_BYTES).foreach { entry =>
      val ex = intercept[IllegalArgumentException] {
        new org.apache.spark.SparkConf().set(entry.key, "-1").get(entry)
      }
      assert(
        ex.getMessage.contains("must not be negative"),
        s"unexpected message for ${entry.key}")
    }
  }

  // ---------------------------------------------------------------------------
  // Reading without validation, and the fingerprint
  // ---------------------------------------------------------------------------

  test("SPARK-58752: read does not validate") {
    // The plan cache key only needs to notice a change, so it reads without validating. An invalid
    // entry must fail the queries that would install it, not every query that consults the cache.
    withSQLConf(key("1INVALID") -> "1") {
      assert(PythonWorkerEnvironment.read(sessionConf) === Map("1INVALID" -> "1"))
    }
  }

  test("SPARK-58752: the fingerprint of an empty environment is empty") {
    assert(PythonWorkerEnvironment.fingerprint(Map.empty) === "")
  }

  test("SPARK-58752: the fingerprint is stable, bounded, and distinguishes environments") {
    val a = Map("FOO" -> "1", "BAR" -> "2")
    val b = Map("FOO" -> "1", "BAR" -> "3")
    val fingerprintA = PythonWorkerEnvironment.fingerprint(a)
    // Stable across calls and independent of iteration order.
    assert(fingerprintA === PythonWorkerEnvironment.fingerprint(Map("BAR" -> "2", "FOO" -> "1")))
    // A different environment gets a different fingerprint.
    assert(fingerprintA !== PythonWorkerEnvironment.fingerprint(b))
    // Bounded: a large environment does not produce a large key component.
    val large = (0 until 100).map(i => s"VAR_$i" -> ("x" * 1024)).toMap
    assert(PythonWorkerEnvironment.fingerprint(large).length === fingerprintA.length)
    assert(fingerprintA.length === 64)
  }

  test("SPARK-58752: the fingerprint does not confuse a shifted name and value boundary") {
    // Without folding lengths into the digest, these two would hash the same bytes.
    val first = PythonWorkerEnvironment.fingerprint(Map("AB" -> "C"))
    val second = PythonWorkerEnvironment.fingerprint(Map("A" -> "BC"))
    assert(first !== second)
  }

  // ---------------------------------------------------------------------------
  // Copying
  // ---------------------------------------------------------------------------

  test("SPARK-58752: toMutableJavaMap returns an independent mutable copy") {
    val variables = Map("FOO" -> "1")
    val first = PythonWorkerEnvironment.toMutableJavaMap(variables)
    first.put("ADDED_BY_RUNNER", "2")
    val second = PythonWorkerEnvironment.toMutableJavaMap(variables)
    // Entries added to one copy do not reach the environment or a later copy.
    assert(!second.containsKey("ADDED_BY_RUNNER"))
    assert(second.get("FOO") === "1")
  }

  // ---------------------------------------------------------------------------
  // Session lifecycle
  // ---------------------------------------------------------------------------

  test("SPARK-58752: a cloned session inherits the environment") {
    withSQLConf(key("FOO") -> "1") {
      val cloned = spark.cloneSession()
      assert(readValidated(cloned.sessionState.conf) === Map("FOO" -> "1"))
      // The clone is independent: a later change on the source does not reach it.
      spark.conf.set(key("FOO"), "changed")
      assert(readValidated(cloned.sessionState.conf) === Map("FOO" -> "1"))
    }
  }

  test("SPARK-58752: a new session does not inherit the environment") {
    withSQLConf(key("FOO") -> "1") {
      val fresh = spark.newSession()
      assert(readValidated(fresh.sessionState.conf).isEmpty)
    }
  }

  // ---------------------------------------------------------------------------
  // Delivery into a Python function
  // ---------------------------------------------------------------------------

  private def inputRelation: proto.Relation =
    createLocalRelationProto(
      Seq(AttributeReference("id", IntegerType)(), AttributeReference("s", StringType)()),
      Seq.empty)

  private def pythonFunctionProto(
      evalType: Int,
      outputType: DataType = IntegerType): proto.CommonInlineUserDefinedFunction =
    proto.CommonInlineUserDefinedFunction
      .newBuilder()
      .setFunctionName("f")
      .setDeterministic(true)
      .setPythonUdf(
        proto.PythonUDF
          .newBuilder()
          .setEvalType(evalType)
          .setCommand(ByteString.copyFrom(Array[Byte](1, 2, 3)))
          .setOutputType(DataTypeProtoConverter.toConnectProtoType(outputType))
          .setPythonVer("3.12")
          .build())
      .build()

  /** A `Project` carrying a Python function, the path scalar and pandas UDFs take. */
  private def projectRelation(evalType: Int): proto.Relation =
    proto.Relation
      .newBuilder()
      .setProject(
        proto.Project
          .newBuilder()
          .setInput(inputRelation)
          .addExpressions(proto.Expression
            .newBuilder()
            .setCommonInlineUserDefinedFunction(pythonFunctionProto(evalType))))
      .build()

  /** A `MapPartitions`, the path `mapInPandas` and `mapInArrow` take. */
  private def mapPartitionsRelation(evalType: Int): proto.Relation =
    proto.Relation
      .newBuilder()
      .setMapPartitions(
        proto.MapPartitions
          .newBuilder()
          .setInput(inputRelation)
          .setFunc(
            // mapInPandas and mapInArrow return a struct, which the planner requires here.
            pythonFunctionProto(evalType, StructType(Seq(StructField("id", IntegerType))))))
      .build()

  private def pythonFunctionsOf(rel: proto.Relation): Seq[PythonFunction] = {
    val plan = transform(rel)
    plan
      .collect { case node => node.expressions }
      .flatten
      .flatMap(_.collect { case udf: PythonUDF => udf.func })
  }

  private def onlyPythonFunction(rel: proto.Relation): PythonFunction = {
    val functions = pythonFunctionsOf(rel)
    assert(functions.size === 1, s"expected exactly one Python function in $rel")
    functions.head
  }

  test("SPARK-58752: a Python UDF receives the session environment") {
    withSQLConf(key("FOO") -> "1", key("BAR") -> "2") {
      val envVars = onlyPythonFunction(projectRelation(PythonEvalType.SQL_BATCHED_UDF)).envVars
      assert(envVars.get("FOO") === "1")
      assert(envVars.get("BAR") === "2")
    }
  }

  test("SPARK-58752: a Python UDF receives an empty environment when none is set") {
    assert(onlyPythonFunction(projectRelation(PythonEvalType.SQL_BATCHED_UDF)).envVars.isEmpty)
  }

  test("SPARK-58752: every scalar Python function family receives the environment") {
    // These share one construction site; the test pins that, so moving one of them to another site
    // does not silently drop its environment.
    Seq(
      PythonEvalType.SQL_BATCHED_UDF,
      PythonEvalType.SQL_ARROW_BATCHED_UDF,
      PythonEvalType.SQL_SCALAR_PANDAS_UDF,
      PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF).foreach { evalType =>
      withSQLConf(key("FOO") -> "1") {
        val envVars = onlyPythonFunction(projectRelation(evalType)).envVars
        assert(envVars.get("FOO") === "1", s"eval type $evalType did not receive the environment")
      }
    }
  }

  test("SPARK-58752: mapInPandas and mapInArrow receive the environment") {
    Seq(PythonEvalType.SQL_MAP_PANDAS_ITER_UDF, PythonEvalType.SQL_MAP_ARROW_ITER_UDF).foreach {
      evalType =>
        withSQLConf(key("FOO") -> "1") {
          val functions = pythonFunctionsOf(mapPartitionsRelation(evalType))
          assert(functions.nonEmpty, s"eval type $evalType produced no Python function")
          functions.foreach { function =>
            assert(function.envVars.get("FOO") === "1")
          }
        }
    }
  }

  test("SPARK-58752: each Python function receives its own mutable copy") {
    withSQLConf(key("FOO") -> "1") {
      val first = onlyPythonFunction(projectRelation(PythonEvalType.SQL_BATCHED_UDF)).envVars
      first.put("ADDED_BY_RUNNER", "2")
      val second = onlyPythonFunction(projectRelation(PythonEvalType.SQL_BATCHED_UDF)).envVars
      assert(!second.containsKey("ADDED_BY_RUNNER"))
    }
  }

  test("SPARK-58752: an invalid environment fails planning of a Python function") {
    withSQLConf(key("1INVALID") -> "1") {
      val ex = intercept[SparkException] {
        onlyPythonFunction(projectRelation(PythonEvalType.SQL_BATCHED_UDF))
      }
      assert(ex.getCondition === "INVALID_SPARK_CONFIG.INVALID_PYTHON_WORKER_ENV_VAR_NAME")
    }
  }

  test("SPARK-58752: an invalid environment does not fail a plan without a Python function") {
    // Validation is deferred to building a Python function, so an invalid entry must not take the
    // whole session down with it.
    withSQLConf(key("1INVALID") -> "1") {
      transform(inputRelation)
    }
  }

  // ---------------------------------------------------------------------------
  // Plan cache
  // ---------------------------------------------------------------------------

  test("SPARK-58752: a plan cached under one environment is not reused under another") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val relation = proto.Relation
      .newBuilder()
      .setSql(proto.SQL.newBuilder().setQuery("select 1").build())
      .setCommon(proto.RelationCommon.newBuilder().setPlanId(1L).build())
      .build()

    var transformCount = 0
    def transformOnce(): Unit = {
      val fingerprint =
        PythonWorkerEnvironment.fingerprint(PythonWorkerEnvironment.read(sessionConf))
      sessionHolder.usePlanCache(relation, cachePlan = true, fingerprint) { _ =>
        transformCount += 1
        spark.sql("select 1").logicalPlan
      }
    }

    try {
      spark.conf.set(key("FOO"), "1")
      transformOnce()
      assert(transformCount === 1)
      // Same relation and same environment: served from the cache.
      transformOnce()
      assert(transformCount === 1)
      // Same relation, different environment: the cached plan holds the old values, so it must not
      // be reused.
      spark.conf.set(key("FOO"), "2")
      transformOnce()
      assert(transformCount === 2)
    } finally {
      spark.conf.unset(key("FOO"))
    }
  }

  test("SPARK-58752: the plan cache key does not hold environment values") {
    // The key must stay bounded: a session cannot grow the memory the cache holds by setting a
    // large environment.
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val relation = proto.Relation
      .newBuilder()
      .setSql(proto.SQL.newBuilder().setQuery("select 1").build())
      .setCommon(proto.RelationCommon.newBuilder().setPlanId(2L).build())
      .build()

    val secret = "s3cr3t" * 1024
    try {
      spark.conf.set(key("FOO"), secret)
      val fingerprint =
        PythonWorkerEnvironment.fingerprint(PythonWorkerEnvironment.read(sessionConf))
      sessionHolder.usePlanCache(relation, cachePlan = true, fingerprint) { _ =>
        spark.sql("select 1").logicalPlan
      }
      val keys = sessionHolder.getPlanCache.get.asMap().keySet()
      assert(!keys.isEmpty)
      keys.forEach { cacheKey =>
        assert(!cacheKey.pythonWorkerEnvFingerprint.contains("s3cr3t"))
        assert(cacheKey.pythonWorkerEnvFingerprint.length === 64)
      }
    } finally {
      spark.conf.unset(key("FOO"))
    }
  }
}
