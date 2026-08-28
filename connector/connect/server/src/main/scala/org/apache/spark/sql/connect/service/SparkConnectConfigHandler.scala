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

import scala.collection.JavaConverters._
import scala.util.matching.Regex

import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.SECRET_REDACTION_PATTERN
import org.apache.spark.sql.RuntimeConfig
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf

class SparkConnectConfigHandler(responseObserver: StreamObserver[proto.ConfigResponse])
    extends Logging {

  def handle(request: proto.ConfigRequest): Unit = {
    val session =
      SparkConnectService
        .getOrCreateIsolatedSession(request.getUserContext.getUserId, request.getSessionId)
        .session

    // Read the pattern from the SparkConf rather than from the session config. The session config
    // is writable by the client, so taking the pattern from there would let a client widen its own
    // view of the configuration before reading it back.
    val redactionPattern = session.sparkContext.conf.get(SECRET_REDACTION_PATTERN)
    val builder = request.getOperation.getOpTypeCase match {
      case proto.ConfigRequest.Operation.OpTypeCase.SET =>
        handleSet(request.getOperation.getSet, session.conf)
      case proto.ConfigRequest.Operation.OpTypeCase.GET =>
        handleGet(request.getOperation.getGet, session.conf, redactionPattern)
      case proto.ConfigRequest.Operation.OpTypeCase.GET_WITH_DEFAULT =>
        handleGetWithDefault(
          request.getOperation.getGetWithDefault,
          session.conf,
          redactionPattern)
      case proto.ConfigRequest.Operation.OpTypeCase.GET_OPTION =>
        handleGetOption(request.getOperation.getGetOption, session.conf, redactionPattern)
      case proto.ConfigRequest.Operation.OpTypeCase.GET_ALL =>
        handleGetAll(request.getOperation.getGetAll, session.conf, redactionPattern)
      case proto.ConfigRequest.Operation.OpTypeCase.UNSET =>
        handleUnset(request.getOperation.getUnset, session.conf)
      case proto.ConfigRequest.Operation.OpTypeCase.IS_MODIFIABLE =>
        handleIsModifiable(request.getOperation.getIsModifiable, session.conf)
      case _ => throw new UnsupportedOperationException(s"${request.getOperation} not supported.")
    }

    builder.setSessionId(request.getSessionId)
    responseObserver.onNext(builder.build())
    responseObserver.onCompleted()
  }

  private def handleSet(
      operation: proto.ConfigRequest.Set,
      conf: RuntimeConfig): proto.ConfigResponse.Builder = {
    val builder = proto.ConfigResponse.newBuilder()
    operation.getPairsList.asScala.iterator.foreach { pair =>
      val (key, value) = SparkConnectConfigHandler.toKeyValue(pair)
      conf.set(key, value.orNull)
      getWarning(key).foreach(builder.addWarnings)
    }
    builder
  }

  private def handleGet(
      operation: proto.ConfigRequest.Get,
      conf: RuntimeConfig,
      redactionPattern: Regex): proto.ConfigResponse.Builder = {
    val builder = proto.ConfigResponse.newBuilder()
    operation.getKeysList.asScala.iterator.foreach { key =>
      val value = conf.get(key)
      if (SparkConnectConfigHandler.isRedacted(key, Option(value), redactionPattern)) {
        // This operation reports an unset key by failing, so a redacted key fails the same way.
        throw QueryExecutionErrors.sqlConfigNotFoundError(key)
      }
      builder.addPairs(SparkConnectConfigHandler.toProtoKeyValue(key, Option(value)))
      getWarning(key).foreach(builder.addWarnings)
    }
    builder
  }

  private def handleGetWithDefault(
      operation: proto.ConfigRequest.GetWithDefault,
      conf: RuntimeConfig,
      redactionPattern: Regex): proto.ConfigResponse.Builder = {
    val builder = proto.ConfigResponse.newBuilder()
    operation.getPairsList.asScala.iterator.foreach { pair =>
      val (key, default) = SparkConnectConfigHandler.toKeyValue(pair)
      val stored = Option(conf.get(key, default.orNull))
      // A redacted entry falls back to the caller's default, as an unset key does.
      val value = if (SparkConnectConfigHandler.isRedacted(key, stored, redactionPattern)) {
        default
      } else {
        stored
      }
      builder.addPairs(SparkConnectConfigHandler.toProtoKeyValue(key, value))
      getWarning(key).foreach(builder.addWarnings)
    }
    builder
  }

  private def handleGetOption(
      operation: proto.ConfigRequest.GetOption,
      conf: RuntimeConfig,
      redactionPattern: Regex): proto.ConfigResponse.Builder = {
    val builder = proto.ConfigResponse.newBuilder()
    operation.getKeysList.asScala.iterator.foreach { key =>
      val stored = conf.getOption(key)
      val value = if (SparkConnectConfigHandler.isRedacted(key, stored, redactionPattern)) {
        None
      } else {
        stored
      }
      builder.addPairs(SparkConnectConfigHandler.toProtoKeyValue(key, value))
      getWarning(key).foreach(builder.addWarnings)
    }
    builder
  }

  private def handleGetAll(
      operation: proto.ConfigRequest.GetAll,
      conf: RuntimeConfig,
      redactionPattern: Regex): proto.ConfigResponse.Builder = {
    val builder = proto.ConfigResponse.newBuilder()
    // Drop redacted entries before the prefix is stripped below. Matching afterwards would let a
    // GetAll with prefix `spark.my.secret.` return `spark.my.secret.value` as `value`, which no
    // longer matches the pattern.
    val visible = conf.getAll.iterator.filterNot { case (key, value) =>
      SparkConnectConfigHandler.isRedacted(key, Option(value), redactionPattern)
    }
    val results = if (operation.hasPrefix) {
      val prefix = operation.getPrefix
      visible
        .filter { case (key, _) => key.startsWith(prefix) }
        .map { case (key, value) => (key.substring(prefix.length), value) }
    } else {
      visible
    }
    results.foreach { case (key, value) =>
      builder.addPairs(SparkConnectConfigHandler.toProtoKeyValue(key, Option(value)))
      getWarning(key).foreach(builder.addWarnings)
    }
    builder
  }

  private def handleUnset(
      operation: proto.ConfigRequest.Unset,
      conf: RuntimeConfig): proto.ConfigResponse.Builder = {
    val builder = proto.ConfigResponse.newBuilder()
    operation.getKeysList.asScala.iterator.foreach { key =>
      conf.unset(key)
      getWarning(key).foreach(builder.addWarnings)
    }
    builder
  }

  private def handleIsModifiable(
      operation: proto.ConfigRequest.IsModifiable,
      conf: RuntimeConfig): proto.ConfigResponse.Builder = {
    val builder = proto.ConfigResponse.newBuilder()
    operation.getKeysList.asScala.iterator.foreach { key =>
      val value = conf.isModifiable(key)
      builder.addPairs(SparkConnectConfigHandler.toProtoKeyValue(key, Option(value.toString)))
      getWarning(key).foreach(builder.addWarnings)
    }
    builder
  }

  private def getWarning(key: String): Option[String] = {
    if (SparkConnectConfigHandler.unsupportedConfigurations.contains(key)) {
      Some(s"The SQL config '$key' is NOT supported in Spark Connect")
    } else {
      SQLConf.deprecatedSQLConfigs.get(key).map(_.toDeprecationString)
    }
  }
}

object SparkConnectConfigHandler {

  private[connect] val unsupportedConfigurations = Set("spark.sql.execution.arrow.enabled")

  /**
   * Whether a configuration entry is considered sensitive and must not be disclosed by the Config
   * RPC. Follows `spark.redaction.regex` and matches the key or the value, the way `Utils.redact`
   * does for the environment UI and event logs and `SetCommand` does for `SET`. Matching the
   * value is what covers a secret carried by an innocuous key, such as a password inside a JDBC
   * URL.
   */
  private[connect] def isRedacted(
      key: String,
      value: Option[String],
      redactionPattern: Regex): Boolean = {
    redactionPattern.findFirstIn(key).isDefined ||
    value.exists(redactionPattern.findFirstIn(_).isDefined)
  }

  def toKeyValue(pair: proto.KeyValue): (String, Option[String]) = {
    val key = pair.getKey
    val value = if (pair.hasValue) {
      Some(pair.getValue)
    } else {
      None
    }
    (key, value)
  }

  def toProtoKeyValue(key: String, value: Option[String]): proto.KeyValue = {
    val builder = proto.KeyValue.newBuilder()
    builder.setKey(key)
    value.foreach(builder.setValue)
    builder.build()
  }
}
