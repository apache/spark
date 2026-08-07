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

import scala.jdk.CollectionConverters._

import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.sql.RuntimeConfig
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.internal.SQLConf

class SparkConnectConfigHandler(responseObserver: StreamObserver[proto.ConfigResponse])
    extends Logging {

  def handle(request: proto.ConfigRequest): Unit = {
    val previousSessionId = request.hasClientObservedServerSideSessionId match {
      case true => Some(request.getClientObservedServerSideSessionId)
      case false => None
    }
    doHandle(
      request,
      SparkConnectService
        .getOrCreateIsolatedSession(
          request.getUserContext.getUserId,
          request.getSessionId,
          previousSessionId))
  }

  private def doHandle(r: proto.ConfigRequest, h: SessionHolder): Unit = h.withSession { s =>
    // Make sure we're using the current running session.
    val builder = r.getOperation.getOpTypeCase match {
      case proto.ConfigRequest.Operation.OpTypeCase.SET =>
        handleSet(r.getOperation.getSet, s.conf)
      case proto.ConfigRequest.Operation.OpTypeCase.GET =>
        handleGet(r.getOperation.getGet, s.conf)
      case proto.ConfigRequest.Operation.OpTypeCase.GET_WITH_DEFAULT =>
        handleGetWithDefault(r.getOperation.getGetWithDefault, s.conf)
      case proto.ConfigRequest.Operation.OpTypeCase.GET_OPTION =>
        handleGetOption(r.getOperation.getGetOption, s.conf)
      case proto.ConfigRequest.Operation.OpTypeCase.GET_ALL =>
        handleGetAll(r.getOperation.getGetAll, s.conf)
      case proto.ConfigRequest.Operation.OpTypeCase.UNSET =>
        handleUnset(r.getOperation.getUnset, s.conf)
      case proto.ConfigRequest.Operation.OpTypeCase.IS_MODIFIABLE =>
        handleIsModifiable(r.getOperation.getIsModifiable, s.conf)
      case _ =>
        throw new UnsupportedOperationException(s"${r.getOperation} not supported.")
    }

    builder.setSessionId(r.getSessionId)
    builder.setServerSideSessionId(h.serverSessionId)
    responseObserver.onNext(builder.build())
    responseObserver.onCompleted()
  }

  private def handleSet(
      operation: proto.ConfigRequest.Set,
      conf: RuntimeConfig): proto.ConfigResponse.Builder = {
    val silent = operation.hasSilent && operation.getSilent
    val builder = proto.ConfigResponse.newBuilder()
    operation.getPairsList.asScala.iterator.foreach { pair =>
      val (key, value) = SparkConnectConfigHandler.toKeyValue(pair)
      try {
        conf.set(key, value.orNull)
        getWarning(key).foreach(builder.addWarnings)
      } catch {
        case e: Throwable =>
          if (silent) {
            builder.addWarnings(s"Failed to set $key to $value due to ${e.getMessage}")
          } else {
            throw e
          }
      }
    }
    builder
  }

  private def handleGet(
      operation: proto.ConfigRequest.Get,
      conf: RuntimeConfig): proto.ConfigResponse.Builder = {
    val builder = proto.ConfigResponse.newBuilder()
    operation.getKeysList.asScala.iterator.foreach { key =>
      val value = if (SparkConnectConfigHandler.isUndisclosed(key)) null else conf.get(key)
      builder.addPairs(SparkConnectConfigHandler.toProtoKeyValue(key, Option(value)))
      getWarning(key).foreach(builder.addWarnings)
    }
    builder
  }

  private def handleGetWithDefault(
      operation: proto.ConfigRequest.GetWithDefault,
      conf: RuntimeConfig): proto.ConfigResponse.Builder = {
    val builder = proto.ConfigResponse.newBuilder()
    operation.getPairsList.asScala.iterator.foreach { pair =>
      val (key, default) = SparkConnectConfigHandler.toKeyValue(pair)
      // An undisclosed key falls back to the caller's default, which is
      // what an unset key would do.
      val value = if (SparkConnectConfigHandler.isUndisclosed(key)) {
        default.orNull
      } else {
        conf.get(key, default.orNull)
      }
      builder.addPairs(SparkConnectConfigHandler.toProtoKeyValue(key, Option(value)))
      getWarning(key).foreach(builder.addWarnings)
    }
    builder
  }

  private def handleGetOption(
      operation: proto.ConfigRequest.GetOption,
      conf: RuntimeConfig): proto.ConfigResponse.Builder = {
    val builder = proto.ConfigResponse.newBuilder()
    operation.getKeysList.asScala.iterator.foreach { key =>
      val value = if (SparkConnectConfigHandler.isUndisclosed(key)) None else conf.getOption(key)
      builder.addPairs(SparkConnectConfigHandler.toProtoKeyValue(key, value))
      getWarning(key).foreach(builder.addWarnings)
    }
    builder
  }

  private def handleGetAll(
      operation: proto.ConfigRequest.GetAll,
      conf: RuntimeConfig): proto.ConfigResponse.Builder = {
    val builder = proto.ConfigResponse.newBuilder()
    // Filtering happens on the full key, before the prefix is stripped
    // off below.
    val disclosed = conf.getAll.iterator.filterNot { case (key, _) =>
      SparkConnectConfigHandler.isUndisclosed(key)
    }
    val results = if (operation.hasPrefix) {
      val prefix = operation.getPrefix
      disclosed
        .filter { case (key, _) => key.startsWith(prefix) }
        .map { case (key, value) => (key.substring(prefix.length), value) }
    } else {
      disclosed
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

  private[connect] val unsupportedConfigurations =
    Set("spark.sql.execution.arrow.enabled", "spark.sql.execution.arrow.pyspark.fallback.enabled")

  /**
   * Configurations the server holds but never hands back to a client. Reads
   * report them as unset: `Get` and `GetOption` return no value,
   * `GetWithDefault` returns the caller's default, and `GetAll` omits them.
   *
   * `SQLConf.mergeSparkConf` copies every `SparkConf` entry into the session
   * config, static ones included, so without this the pre-shared
   * authentication token is readable through the Config RPC. That discloses
   * nothing to a client of a standalone server, which had to present the
   * token to connect at all. It does matter to deployments that place a
   * proxy in front of Spark Connect and treat the token as a secret shared
   * between the proxy and the servers, with end users authenticating by
   * other means: there, any client the proxy admits could read the token and
   * then reach a server directly. Not disclosing a server-side credential is
   * the right default either way.
   */
  private[connect] val undisclosedConfigurations = Set(Connect.CONNECT_AUTHENTICATE_TOKEN.key)

  private[connect] def isUndisclosed(key: String): Boolean =
    undisclosedConfigurations.contains(key)

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
