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

import org.apache.spark.SparkException
import org.apache.spark.api.python.{PythonEvalType, PythonFunction}
import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, PythonUDF}
import org.apache.spark.sql.connect.SparkConnectTestUtils
import org.apache.spark.sql.connect.common.DataTypeProtoConverter
import org.apache.spark.sql.connect.planner.SparkConnectPlanTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType}

class PythonWorkerEnvironmentSuite extends SparkConnectPlanTest {

  private def sessionConf: SQLConf = spark.sessionState.conf

  private def key(name: String): String = PythonWorkerEnvironment.confPrefix + name

  // ---------------------------------------------------------------------------
  // Reading the environment from session configurations
  // ---------------------------------------------------------------------------

  test("SPARK-58752: no configurations means an empty environment") {
    assert(PythonWorkerEnvironment.readValidated(sessionConf).isEmpty)
  }

  test("SPARK-58752: configurations under the prefix become environment variables") {
    withSQLConf(key("FOO") -> "1", key("BAR") -> "2") {
      assert(PythonWorkerEnvironment.readValidated(sessionConf) === Map("FOO" -> "1", "BAR" -> "2"))
    }
  }

  test("SPARK-58752: configurations outside the prefix are ignored") {
    withSQLConf(key("FOO") -> "1", "spark.pythonWorkerEnvOther.BAR" -> "2") {
      assert(PythonWorkerEnvironment.readValidated(sessionConf) === Map("FOO" -> "1"))
    }
  }

  test("SPARK-58752: an empty value is allowed") {
    withSQLConf(key("FOO") -> "") {
      assert(PythonWorkerEnvironment.readValidated(sessionConf) === Map("FOO" -> ""))
    }
  }

  test("SPARK-58752: variable names are case-sensitive") {
    withSQLConf(key("FOO") -> "upper", key("foo") -> "lower") {
      val env = PythonWorkerEnvironment.readValidated(sessionConf)
      assert(env === Map("FOO" -> "upper", "foo" -> "lower"))
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
        val ex = intercept[SparkException] {
          PythonWorkerEnvironment.readValidated(sessionConf)
        }
        assert(
          ex.getCondition === "INVALID_SPARK_CONFIG.INVALID_PYTHON_WORKER_ENV_VAR_NAME",
          s"unexpected condition for name '$name'")
      }
    }
  }

  test("SPARK-58752: a variable name over the length limit is rejected") {
    val longName = "A" * 513
    withSQLConf(key(longName) -> "1") {
      val ex = intercept[SparkException] {
        PythonWorkerEnvironment.readValidated(sessionConf)
      }
      assert(ex.getCondition === "INVALID_SPARK_CONFIG.INVALID_PYTHON_WORKER_ENV_VAR_NAME")
      // The name is bounded in the message rather than echoed whole.
      assert(!ex.getMessage.contains(longName))
    }
  }

  test("SPARK-58752: more variables than the limit is rejected") {
    val tooMany = (0 until 101).map(i => key(s"VAR_$i") -> "1")
    withSQLConf(tooMany: _*) {
      val ex = intercept[SparkException] {
        PythonWorkerEnvironment.readValidated(sessionConf)
      }
      assert(ex.getCondition === "INVALID_SPARK_CONFIG.PYTHON_WORKER_ENV_TOO_MANY_VARIABLES")
      assert(ex.getMessage.contains("101"))
    }
  }

  test("SPARK-58752: exactly the variable limit is accepted") {
    val atLimit = (0 until 100).map(i => key(s"VAR_$i") -> "1")
    withSQLConf(atLimit: _*) {
      assert(PythonWorkerEnvironment.readValidated(sessionConf).size === 100)
    }
  }

  test("SPARK-58752: a total size over the limit is rejected") {
    // Two values that together exceed 128 KiB, while each name stays within its own limit.
    val half = "x" * (65 * 1024)
    withSQLConf(key("A") -> half, key("B") -> half) {
      val ex = intercept[SparkException] {
        PythonWorkerEnvironment.readValidated(sessionConf)
      }
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
      val ex = intercept[SparkException] {
        PythonWorkerEnvironment.readValidated(sessionConf)
      }
      assert(ex.getCondition === "INVALID_SPARK_CONFIG.PYTHON_WORKER_ENV_TOO_LARGE")
    }
  }

  test("SPARK-58752: read does not validate") {
    // The plan cache key only needs to notice a change, so it reads without validating. An invalid
    // entry must fail the queries that would install it, not every query that consults the cache.
    withSQLConf(key("1INVALID") -> "1") {
      assert(PythonWorkerEnvironment.read(sessionConf) === Map("1INVALID" -> "1"))
    }
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
      assert(PythonWorkerEnvironment.readValidated(cloned.sessionState.conf) === Map("FOO" -> "1"))
      // The clone is independent: a later change on the source does not reach it.
      spark.conf.set(key("FOO"), "changed")
      assert(PythonWorkerEnvironment.readValidated(cloned.sessionState.conf) === Map("FOO" -> "1"))
    }
  }

  test("SPARK-58752: a new session does not inherit the environment") {
    withSQLConf(key("FOO") -> "1") {
      val fresh = spark.newSession()
      assert(PythonWorkerEnvironment.readValidated(fresh.sessionState.conf).isEmpty)
    }
  }

  // ---------------------------------------------------------------------------
  // Delivery into a Python function
  // ---------------------------------------------------------------------------

  private def pythonUdfRelation(): proto.Relation = {
    val udf = proto.PythonUDF
      .newBuilder()
      .setEvalType(PythonEvalType.SQL_BATCHED_UDF)
      .setCommand(ByteString.copyFrom(Array[Byte](1, 2, 3)))
      .setOutputType(DataTypeProtoConverter.toConnectProtoType(IntegerType))
      .setPythonVer("3.12")
      .build()
    val function = proto.CommonInlineUserDefinedFunction
      .newBuilder()
      .setFunctionName("f")
      .setDeterministic(true)
      .setPythonUdf(udf)
      .build()
    proto.Relation
      .newBuilder()
      .setProject(
        proto.Project
          .newBuilder()
          .setInput(
            createLocalRelationProto(
              Seq(AttributeReference("id", IntegerType)(), AttributeReference("s", StringType)()),
              Seq.empty))
          .addExpressions(
            proto.Expression
              .newBuilder()
              .setCommonInlineUserDefinedFunction(function)))
      .build()
  }

  private def plannedPythonFunction(): PythonFunction = {
    val plan = transform(pythonUdfRelation())
    val udfs = plan.expressions.flatMap(_.collect { case udf: PythonUDF => udf })
    assert(udfs.size === 1, s"expected exactly one Python UDF in $plan")
    udfs.head.func
  }

  test("SPARK-58752: a Python UDF receives the session environment") {
    withSQLConf(key("FOO") -> "1", key("BAR") -> "2") {
      val envVars = plannedPythonFunction().envVars
      assert(envVars.get("FOO") === "1")
      assert(envVars.get("BAR") === "2")
    }
  }

  test("SPARK-58752: a Python UDF receives an empty environment when none is set") {
    assert(plannedPythonFunction().envVars.isEmpty)
  }

  test("SPARK-58752: each Python function receives its own mutable copy") {
    withSQLConf(key("FOO") -> "1") {
      val first = plannedPythonFunction().envVars
      first.put("ADDED_BY_RUNNER", "2")
      val second = plannedPythonFunction().envVars
      assert(!second.containsKey("ADDED_BY_RUNNER"))
    }
  }

  test("SPARK-58752: an invalid environment fails planning of a Python function") {
    withSQLConf(key("1INVALID") -> "1") {
      val ex = intercept[SparkException] {
        plannedPythonFunction()
      }
      assert(ex.getCondition === "INVALID_SPARK_CONFIG.INVALID_PYTHON_WORKER_ENV_VAR_NAME")
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
      sessionHolder.usePlanCache(relation, cachePlan = true) { _ =>
        transformCount += 1
        spark.sql("select 1").logicalPlan
      }
    }

    sessionHolder.session.conf.set(key("FOO"), "1")
    transformOnce()
    assert(transformCount === 1)
    // Same relation and same environment: served from the cache.
    transformOnce()
    assert(transformCount === 1)
    // Same relation, different environment: the cached plan holds the old values, so it must not
    // be reused.
    sessionHolder.session.conf.set(key("FOO"), "2")
    transformOnce()
    assert(transformCount === 2)
    sessionHolder.session.conf.unset(key("FOO"))
  }
}
