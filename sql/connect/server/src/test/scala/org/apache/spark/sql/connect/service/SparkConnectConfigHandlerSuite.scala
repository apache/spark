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

import java.util.concurrent.{ConcurrentLinkedQueue, CyclicBarrier, TimeUnit}

import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Try

import io.grpc.stub.StreamObserver

import org.apache.spark.{SparkEnv, SparkException, SparkNoSuchElementException}
import org.apache.spark.connect.proto
import org.apache.spark.internal.config.{ConfigEntry, SECRET_REDACTION_PATTERN}
import org.apache.spark.sql.connect.SparkConnectTestUtils
import org.apache.spark.sql.execution.python.PythonWorkerEnvironment
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ThreadUtils

class SparkConnectConfigHandlerSuite extends SharedSparkSession {

  // Matches the default spark.redaction.regex on "password".
  private val secretKey = "spark.test.connect.password"
  private val secretValue = "hunter2"
  private val plainKey = "spark.test.connect.endpoint"
  private val plainValue = "localhost:15002"

  protected override def afterEach(): Unit = {
    super.afterEach()
    SparkConnectService.sessionManager.invalidateAllSessions()
  }

  private def sendConfigRequest(
      sessionHolder: SessionHolder,
      customize: proto.ConfigRequest.Operation.Builder => Unit): proto.ConfigResponse = {
    val operation = proto.ConfigRequest.Operation.newBuilder()
    customize(operation)
    val request = proto.ConfigRequest
      .newBuilder()
      .setUserContext(proto.UserContext.newBuilder().setUserId(sessionHolder.userId).build())
      .setSessionId(sessionHolder.sessionId)
      .setOperation(operation)
      .build()
    val responseObserver = new ConfigResponseObserver()
    new SparkConnectConfigHandler(responseObserver).handle(request)
    ThreadUtils.awaitResult(responseObserver.promise.future, 10.seconds)
  }

  /** The returned pairs, with an absent value mapped to None. */
  private def pairs(response: proto.ConfigResponse): Map[String, Option[String]] = {
    response.getPairsList.asScala
      .map(pair => pair.getKey -> (if (pair.hasValue) Some(pair.getValue) else None))
      .toMap
  }

  test("GetAll does not return keys matching the redaction pattern") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    withSQLConf(secretKey -> secretValue, plainKey -> plainValue) {
      val returned = pairs(sendConfigRequest(sessionHolder, _.getGetAllBuilder))
      assert(!returned.contains(secretKey))
      assert(returned(plainKey) === Some(plainValue))
    }
  }

  test("GetAll matches the redaction pattern on the full key, not the prefixed one") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    // Stripping the prefix leaves "value", which no longer matches the pattern. The filter has to
    // run before the prefix comes off.
    val prefix = "spark.test.connect.secret."
    withSQLConf(prefix + "value" -> secretValue) {
      val returned = pairs(sendConfigRequest(sessionHolder, _.getGetAllBuilder.setPrefix(prefix)))
      assert(returned.isEmpty)
    }
  }

  test("a secret carried by an innocuous key is withheld too") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    // `SET spark.test.connect.jdbc.url` already masks this value, because SetCommand redacts
    // through Utils.redact, which matches the value as well. The Config RPC has to agree.
    val urlKey = "spark.test.connect.jdbc.url"
    val urlValue = "jdbc:postgresql://db:5432/app?user=app&password=hunter2"
    withSQLConf(urlKey -> urlValue) {
      assert(!pairs(sendConfigRequest(sessionHolder, _.getGetAllBuilder)).contains(urlKey))
      val returned =
        pairs(sendConfigRequest(sessionHolder, _.getGetOptionBuilder.addKeys(urlKey)))
      assert(returned(urlKey) === None)
    }
  }

  test("Get reports a redacted key the way it reports an unset one") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    withSQLConf(secretKey -> secretValue) {
      val redacted = intercept[SparkNoSuchElementException] {
        sendConfigRequest(sessionHolder, _.getGetBuilder.addKeys(secretKey))
      }
      val unset = intercept[SparkNoSuchElementException] {
        sendConfigRequest(sessionHolder, _.getGetBuilder.addKeys("spark.test.connect.absent"))
      }
      assert(redacted.getCondition === unset.getCondition)
    }
  }

  test("GetOption returns no value for a redacted key") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    withSQLConf(secretKey -> secretValue, plainKey -> plainValue) {
      val returned = pairs(
        sendConfigRequest(
          sessionHolder,
          _.getGetOptionBuilder.addKeys(secretKey).addKeys(plainKey)))
      assert(returned(secretKey) === None)
      assert(returned(plainKey) === Some(plainValue))
    }
  }

  test("GetWithDefault returns the caller's default for a redacted key") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    withSQLConf(secretKey -> secretValue) {
      val returned = pairs(
        sendConfigRequest(
          sessionHolder,
          _.getGetWithDefaultBuilder.addPairsBuilder().setKey(secretKey).setValue("fallback")))
      assert(returned(secretKey) === Some("fallback"))
    }
  }

  test("the redaction pattern is not read from the session config") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    // Both of these are reachable through the Set operation: the legacy flag is what otherwise
    // stops a client from writing spark.redaction.regex into its own session config.
    withSQLConf(
      SQLConf.SET_COMMAND_REJECTS_SPARK_CORE_CONFS.key -> "false",
      secretKey -> secretValue) {
      spark.conf.set(SECRET_REDACTION_PATTERN.key, "matches-no-key")
      try {
        val returned = pairs(sendConfigRequest(sessionHolder, _.getGetAllBuilder))
        assert(!returned.contains(secretKey))
      } finally {
        spark.conf.unset(SECRET_REDACTION_PATTERN.key)
      }
    }
  }

  test("IsModifiable still answers for a redacted key") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    withSQLConf(secretKey -> secretValue) {
      // Whether a key is modifiable is a property of the key, so it discloses no value.
      val returned =
        pairs(sendConfigRequest(sessionHolder, _.getIsModifiableBuilder.addKeys(secretKey)))
      assert(returned(secretKey) === Some("false"))
    }
  }

  // ---------------------------------------------------------------------------
  // Validating a write to the session's Python worker environment
  // ---------------------------------------------------------------------------

  private def envKey(name: String): String = PythonWorkerEnvironment.confPrefix + name

  private def sendSet(
      sessionHolder: SessionHolder,
      key: String,
      value: String,
      silent: Boolean = false): proto.ConfigResponse = {
    sendConfigRequest(
      sessionHolder,
      _.getSetBuilder.setSilent(silent).addPairsBuilder().setKey(key).setValue(value))
  }

  /**
   * Runs `body` with a limit temporarily lowered. The limits are cluster-level static configs.
   */
  private def withLimit[T](entry: ConfigEntry[T], value: T)(body: => Unit): Unit = {
    val conf = SparkEnv.get.conf
    val previous = conf.get(entry)
    conf.set(entry, value)
    try body
    finally conf.set(entry, previous)
  }

  private def withEnvKeys(keys: String*)(body: => Unit): Unit = {
    try body
    finally keys.foreach(spark.conf.unset)
  }

  test("SPARK-58752: Set stores a valid environment variable") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    withEnvKeys(envKey("MY_SETTING")) {
      sendSet(sessionHolder, envKey("MY_SETTING"), "abc")
      assert(spark.conf.getOption(envKey("MY_SETTING")) === Some("abc"))
    }
  }

  test("SPARK-58752: Set rejects an invalid variable name without storing it") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val key = envKey("1INVALID")
    withEnvKeys(key) {
      val ex = intercept[SparkException](sendSet(sessionHolder, key, "x"))
      assert(ex.getCondition === "INVALID_SPARK_CONFIG.INVALID_PYTHON_WORKER_ENV_VAR_NAME")
      // The point of checking here rather than only when a worker is launched: the write is
      // refused, so the session is not left carrying an environment none of its queries can use.
      assert(spark.conf.getOption(key).isEmpty)
    }
  }

  test("SPARK-58752: Set rejects a value a process environment cannot carry") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val nul = 0.toChar
    val key = envKey("WITH_NUL")
    withEnvKeys(key) {
      val ex = intercept[SparkException](sendSet(sessionHolder, key, s"abc${nul}def"))
      assert(ex.getCondition === "INVALID_SPARK_CONFIG.INVALID_PYTHON_WORKER_ENV_VAR_VALUE")
      assert(spark.conf.getOption(key).isEmpty)
    }
  }

  test("SPARK-58752: Set is validated against the environment the write would produce") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    // A limit on the collection cannot be checked against the written entry alone, so the check
    // has to consider the whole environment the session would end up with.
    withLimit(StaticSQLConf.PYTHON_WORKER_ENV_MAX_VARIABLES, 1) {
      withEnvKeys(envKey("FIRST"), envKey("SECOND")) {
        sendSet(sessionHolder, envKey("FIRST"), "1")
        val ex = intercept[SparkException](sendSet(sessionHolder, envKey("SECOND"), "2"))
        assert(ex.getCondition === "INVALID_SPARK_CONFIG.PYTHON_WORKER_ENV_TOO_MANY_VARIABLES")
        assert(spark.conf.getOption(envKey("SECOND")).isEmpty)
        // The write that was already accepted stays.
        assert(spark.conf.getOption(envKey("FIRST")) === Some("1"))
      }
    }
  }

  test("SPARK-58752: a silent Set reports a rejected variable as a warning") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val key = envKey("1INVALID")
    withEnvKeys(key) {
      // This is the path `SparkSession.builder.config` takes: it applies its options silently, so a
      // rejected one has to come back as a warning rather than failing session creation.
      val response = sendSet(sessionHolder, key, "x", silent = true)
      assert(response.getWarningsCount === 1)
      assert(response.getWarnings(0).contains(key))
      assert(spark.conf.getOption(key).isEmpty)
    }
  }

  test("SPARK-58752: Unset is not validated, so a session can leave an invalid environment") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val key = envKey("1INVALID")
    withEnvKeys(key) {
      // Installed the way SQL `SET` installs one, which the config RPC never sees.
      spark.sessionState.conf.setConfString(key, "x")
      sendConfigRequest(sessionHolder, _.getUnsetBuilder.addKeys(key))
      assert(spark.conf.getOption(key).isEmpty)
    }
  }

  test("SPARK-58752: a silent rejection does not carry the rejected value") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val nul = 0.toChar
    val secret = s"abc${nul}def"
    val key = envKey("WITH_NUL")
    withEnvKeys(key) {
      val response = sendSet(sessionHolder, key, secret, silent = true)
      assert(response.getWarningsCount === 1)
      val warning = response.getWarnings(0)
      // The Scala client logs this warning and the Python client raises it, so it outlives the
      // response. A rejected value must not travel in it, just as it does not reach the message of
      // the rejection itself.
      assert(warning.contains(key))
      assert(!warning.contains("abc"))
      assert(!warning.contains("def"))
      assert(!warning.contains(nul.toString))
      assert(spark.conf.getOption(key).isEmpty)
    }
  }

  test("SPARK-58752: a silent rejection of any config does not carry the rejected value") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    // Not an environment variable: any typed config whose conversion fails produces an
    // INVALID_CONF_VALUE error, and that message quotes the offending value. The warning must
    // report the condition instead, since a value rejected for the wrong type can still be a
    // secret that a caller pasted into the wrong key.
    val sentinel = "not-a-number-hunter2"
    val response = sendSet(sessionHolder, SQLConf.SHUFFLE_PARTITIONS.key, sentinel, silent = true)
    assert(response.getWarningsCount === 1)
    val warning = response.getWarnings(0)
    assert(warning.contains(SQLConf.SHUFFLE_PARTITIONS.key))
    assert(!warning.contains(sentinel))
    assert(warning.contains("INVALID_CONF_VALUE"))
  }

  test("SPARK-58752: concurrent writes cannot jointly exceed a limit") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    withLimit(StaticSQLConf.PYTHON_WORKER_ENV_MAX_VARIABLES, 1) {
      withEnvKeys(envKey("FIRST"), envKey("SECOND")) {
        // Both writers read the environment, validate, and write. Unless the check and the write it
        // validated are one unit, both observe an empty environment, both accept, and the session
        // is left holding two variables against a limit of one.
        val barrier = new CyclicBarrier(2)
        val outcomes = new ConcurrentLinkedQueue[Try[Unit]]()
        val threads = Seq("FIRST", "SECOND").map { name =>
          new Thread(() => {
            barrier.await(10, TimeUnit.SECONDS)
            outcomes.add(Try(sendSet(sessionHolder, envKey(name), "1")).map(_ => ()))
          })
        }
        threads.foreach(_.start())
        threads.foreach(_.join(TimeUnit.SECONDS.toMillis(30)))
        assert(outcomes.size === 2)
        val accepted = outcomes.asScala.count(_.isSuccess)
        assert(accepted === 1, s"expected exactly one write to be accepted, got $accepted")
        val stored = PythonWorkerEnvironment.read(spark.sessionState.conf)
        assert(stored.size === 1, s"the limit was exceeded: $stored")
      }
    }
  }

  test("SPARK-58752: a key outside the reserved prefix is not validated as a variable") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    // Shaped like an invalid variable name, but the prefix is not the reserved one.
    val key = "spark.test.connect.pythonWorkerEnv.1INVALID"
    withEnvKeys(key) {
      sendSet(sessionHolder, key, "x")
      assert(spark.conf.getOption(key) === Some("x"))
    }
  }
}

private class ConfigResponseObserver extends StreamObserver[proto.ConfigResponse] {
  val promise: Promise[proto.ConfigResponse] = Promise()
  override def onNext(value: proto.ConfigResponse): Unit = promise.success(value)
  override def onError(t: Throwable): Unit = promise.failure(t)
  override def onCompleted(): Unit = {}
}
