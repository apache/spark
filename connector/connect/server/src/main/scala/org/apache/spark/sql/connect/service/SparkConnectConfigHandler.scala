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
import org.apache.spark.sql.internal.SQLConf

class SparkConnectConfigHandler(responseObserver: StreamObserver[proto.ConfigResponse])
    extends Logging {

  def handle(request: proto.ConfigRequest): Unit = {
    val previousSessionId = request.hasClientObservedServerSideSessionId match {
      case true => Some(request.getClientObservedServerSideSessionId)
      case false => None
    }
    val holder =
      SparkConnectService
        .getOrCreateIsolatedSession(
          request.getUserContext.getUserId,
          request.getSessionId,
          previousSessionId)
    val session = holder.session

    val builder = request.getOperation.getOpTypeCase match {
      case proto.ConfigRequest.Operation.OpTypeCase.SET =>
        handleSet(request.getOperation.getSet, session.conf)
      case proto.ConfigRequest.Operation.OpTypeCase.GET =>
        handleGet(request.getOperation.getGet, session.conf)
      case proto.ConfigRequest.Operation.OpTypeCase.GET_WITH_DEFAULT =>
        handleGetWithDefault(request.getOperation.getGetWithDefault, session.conf)
      case proto.ConfigRequest.Operation.OpTypeCase.GET_OPTION =>
        handleGetOption(request.getOperation.getGetOption, session.conf)
      case proto.ConfigRequest.Operation.OpTypeCase.GET_ALL =>
        handleGetAll(request.getOperation.getGetAll, session.conf)
      case proto.ConfigRequest.Operation.OpTypeCase.UNSET =>
        handleUnset(request.getOperation.getUnset, session.conf)
      case proto.ConfigRequest.Operation.OpTypeCase.IS_MODIFIABLE =>
        handleIsModifiable(request.getOperation.getIsModifiable, session.conf)
      case _ => throw new UnsupportedOperationException(s"${request.getOperation} not supported.")
    }

    builder.setSessionId(request.getSessionId)
    builder.setServerSideSessionId(holder.serverSessionId)
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
      conf: RuntimeConfig): proto.ConfigResponse.Builder = {
    val builder = proto.ConfigResponse.newBuilder()
    operation.getKeysList.asScala.iterator.foreach { key =>
      val value = conf.get(key)
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
      val value = conf.get(key, default.orNull)
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
      val value = conf.getOption(key)
      builder.addPairs(SparkConnectConfigHandler.toProtoKeyValue(key, value))
      getWarning(key).foreach(builder.addWarnings)
    }
    builder
  }

  private def handleGetAll(
      operation: proto.ConfigRequest.GetAll,
      conf: RuntimeConfig): proto.ConfigResponse.Builder = {
    val builder = proto.ConfigResponse.newBuilder()
    val results = if (operation.hasPrefix) {
      val prefix = operation.getPrefix
      conf.getAll.iterator
        .filter { case (key, _) => key.startsWith(prefix) }
        .map { case (key, value) => (key.substring(prefix.length), value) }
    } else {
      conf.getAll.iterator
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
