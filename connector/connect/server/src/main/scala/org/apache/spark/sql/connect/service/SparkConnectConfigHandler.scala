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

import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.sql.RuntimeConfig
import org.apache.spark.sql.internal.SQLConf

class SparkConnectConfigHandler(responseObserver: StreamObserver[proto.ConfigResponse])
    extends Logging {

  def handle(request: proto.ConfigRequest): Unit = {
    val session =
      SparkConnectService
        .getOrCreateIsolatedSession(request.getUserContext.getUserId, request.getClientId)
        .session

    val (operation, warnings) = request.getOperation.getOpTypeCase match {
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
      case proto.ConfigRequest.Operation.OpTypeCase.CONTAINS =>
        handleContains(request.getOperation.getContains, session.conf)
      case proto.ConfigRequest.Operation.OpTypeCase.IS_MODIFIABLE =>
        handleIsModifiable(request.getOperation.getIsModifiable, session.conf)
      case _ => throw new UnsupportedOperationException(s"${request.getOperation} not supported.")
    }

    val response = proto.ConfigResponse
      .newBuilder()
      .setClientId(request.getClientId)
      .setOperation(operation)
      .addAllWarnings(warnings.asJava)
      .build()
    responseObserver.onNext(response)
    responseObserver.onCompleted()
  }

  private def handleSet(
      operation: proto.ConfigRequest.Set,
      conf: RuntimeConfig): (proto.ConfigResponse.Operation, Seq[String]) = {
    val builder = proto.ConfigResponse.Operation.newBuilder()
    val response = proto.ConfigResponse.Set.newBuilder()
    val warnings = operation.getPairsList.asScala.flatMap { pair =>
      val key = pair.getKey
      val value = pair.getValue
      conf.set(key, value)
      getWarning(key)
    }
    builder.setSet(response.build())
    (builder.build(), warnings)
  }

  private def handleGet(
      operation: proto.ConfigRequest.Get,
      conf: RuntimeConfig): (proto.ConfigResponse.Operation, Seq[String]) = {
    val builder = proto.ConfigResponse.Operation.newBuilder()
    val response = proto.ConfigResponse.Get.newBuilder()
    val warnings = operation.getKeysList.asScala.flatMap { key =>
      response.addValues(conf.get(key))
      getWarning(key)
    }
    builder.setGet(response.build())
    (builder.build(), warnings)
  }

  private def handleGetWithDefault(
      operation: proto.ConfigRequest.GetWithDefault,
      conf: RuntimeConfig): (proto.ConfigResponse.Operation, Seq[String]) = {
    val builder = proto.ConfigResponse.Operation.newBuilder()
    val response = proto.ConfigResponse.GetWithDefault.newBuilder()
    val warnings = operation.getPairsList.asScala.flatMap { pair =>
      val key = pair.getKey
      val value = SparkConnectConfigHandler.toOption(pair.getValue).orNull
      response.addValues(
        SparkConnectConfigHandler.toProtoOptionalValue(Option(conf.get(key, value))))
      getWarning(key)
    }
    builder.setGetWithDefault(response.build())
    (builder.build(), warnings)
  }

  private def handleGetOption(
      operation: proto.ConfigRequest.GetOption,
      conf: RuntimeConfig): (proto.ConfigResponse.Operation, Seq[String]) = {
    val builder = proto.ConfigResponse.Operation.newBuilder()
    val response = proto.ConfigResponse.GetOption.newBuilder()
    val warnings = operation.getKeysList.asScala.flatMap { key =>
      response.addValues(SparkConnectConfigHandler.toProtoOptionalValue(conf.getOption(key)))
      getWarning(key)
    }
    builder.setGetOption(response.build())
    (builder.build(), warnings)
  }

  private def handleGetAll(
      operation: proto.ConfigRequest.GetAll,
      conf: RuntimeConfig): (proto.ConfigResponse.Operation, Seq[String]) = {
    val builder = proto.ConfigResponse.Operation.newBuilder()
    val response = proto.ConfigResponse.GetAll.newBuilder()
    val results = if (operation.hasPrefix) {
      val prefix = operation.getPrefix
      conf.getAll.iterator
        .filter { case (key, _) => key.startsWith(prefix) }
        .map { case (key, value) => (key.substring(prefix.length), value) }
    } else {
      conf.getAll.iterator
    }
    val warnings = results.flatMap { case (key, value) =>
      response.addPairs(proto.KeyValue.newBuilder().setKey(key).setValue(value).build())
      getWarning(key)
    }.toSeq
    builder.setGetAll(response.build())
    (builder.build(), warnings)
  }

  private def handleUnset(
      operation: proto.ConfigRequest.Unset,
      conf: RuntimeConfig): (proto.ConfigResponse.Operation, Seq[String]) = {
    val builder = proto.ConfigResponse.Operation.newBuilder()
    val response = proto.ConfigResponse.Unset.newBuilder()
    val warnings = operation.getKeysList.asScala.flatMap { key =>
      conf.unset(key)
      getWarning(key)
    }
    builder.setUnset(response.build())
    (builder.build(), warnings)
  }

  private def handleContains(
      operation: proto.ConfigRequest.Contains,
      conf: RuntimeConfig): (proto.ConfigResponse.Operation, Seq[String]) = {
    val builder = proto.ConfigResponse.Operation.newBuilder()
    val response = proto.ConfigResponse.Contains.newBuilder()
    val warnings = operation.getKeysList.asScala.flatMap { key =>
      response.addBools(conf.contains(key))
      getWarning(key)
    }
    builder.setContains(response.build())
    (builder.build(), warnings)
  }

  private def handleIsModifiable(
      operation: proto.ConfigRequest.IsModifiable,
      conf: RuntimeConfig): (proto.ConfigResponse.Operation, Seq[String]) = {
    val builder = proto.ConfigResponse.Operation.newBuilder()
    val response = proto.ConfigResponse.IsModifiable.newBuilder()
    val warnings = operation.getKeysList.asScala.flatMap { key =>
      response.addBools(conf.isModifiable(key))
      getWarning(key)
    }
    builder.setIsModifiable(response.build())
    (builder.build(), warnings)
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

  def toOption(value: proto.OptionalValue): Option[String] = {
    if (value.hasValue) {
      Some(value.getValue)
    } else {
      None
    }
  }

  def toProtoOptionalValue(value: Option[String]): proto.OptionalValue = {
    val builder = proto.OptionalValue.newBuilder()
    value.foreach(builder.setValue(_))
    builder.build()
  }
}
