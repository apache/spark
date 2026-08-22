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

import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import io.grpc.stub.StreamObserver

import org.apache.spark.SparkNoSuchElementException
import org.apache.spark.connect.proto
import org.apache.spark.internal.config.SECRET_REDACTION_PATTERN
import org.apache.spark.sql.connect.SparkConnectTestUtils
import org.apache.spark.sql.internal.SQLConf
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
}

private class ConfigResponseObserver extends StreamObserver[proto.ConfigResponse] {
  val promise: Promise[proto.ConfigResponse] = Promise()
  override def onNext(value: proto.ConfigResponse): Unit = promise.success(value)
  override def onError(t: Throwable): Unit = promise.failure(t)
  override def onCompleted(): Unit = {}
}
