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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

class SparkConnectConfigHandler(responseObserver: StreamObserver[proto.ConfigResponse])
    extends Logging {

  def handle(request: proto.ConfigRequest): Unit = {
    val session =
      SparkConnectService
        .getOrCreateIsolatedSession(request.getUserContext.getUserId, request.getClientId)
        .session

    val builder = handleSQLConf(request, session)
    builder.setClientId(request.getClientId)
    responseObserver.onNext(builder.build())
    responseObserver.onCompleted()
  }

  private def handleSQLConf(request: proto.ConfigRequest, session: SparkSession) = {
    val conf = session.conf
    val builder = proto.ConfigResponse.newBuilder()

    request.getOperation match {
      case proto.ConfigRequest.Operation.OPERATION_SET =>
        if (request.getKeysCount != request.getValuesCount) {
          throw new UnsupportedOperationException("Keys and values should have the same length!")
        }
        request.getKeysList.asScala.iterator
          .zip(request.getValuesList.asScala.iterator)
          .foreach { case (key, value) =>
            conf.set(key, value)
            getWarning(key).foreach(builder.addWarnings)
          }

      case proto.ConfigRequest.Operation.OPERATION_GET =>
        if (request.getValuesCount == 0) {
          request.getKeysList.asScala.iterator.foreach { key =>
            builder.addValues(conf.get(key))
            getWarning(key).foreach(builder.addWarnings)
          }
        } else {
          if (request.getKeysCount != request.getValuesCount) {
            throw new UnsupportedOperationException(
              "Keys and values should have the same length!")
          }
          request.getKeysList.asScala.iterator
            .zip(request.getValuesList.asScala.iterator)
            .foreach { case (key, value) =>
              builder.addValues(conf.get(key, value))
              getWarning(key).foreach(builder.addWarnings)
            }
        }

      case proto.ConfigRequest.Operation.OPERATION_GET_OPTION =>
        request.getKeysList.asScala.iterator.foreach { key =>
          conf.getOption(key) match {
            case Some(value) =>
              builder.addOptionalValues(
                proto.ConfigResponse.OptionalValue.newBuilder().setValue(value).build())
            case _ =>
              builder.addOptionalValues(proto.ConfigResponse.OptionalValue.newBuilder().build())
          }
          getWarning(key).foreach(builder.addWarnings)
        }

      case proto.ConfigRequest.Operation.OPERATION_GET_ALL =>
        val results = if (request.hasPrefix) {
          val prefix = request.getPrefix
          conf.getAll.iterator
            .filter { case (key, value) => key.startsWith(prefix) }
            .map { case (key, value) => (key.substring(prefix.length), value) }
        } else {
          conf.getAll.iterator
        }
        results.foreach { case (key, value) =>
          builder.addKeys(key).addValues(value)
          getWarning(key).foreach(builder.addWarnings)
        }

      case proto.ConfigRequest.Operation.OPERATION_UNSET =>
        request.getKeysList.asScala.iterator.foreach { key =>
          conf.unset(key)
          getWarning(key).foreach(builder.addWarnings)
        }

      case proto.ConfigRequest.Operation.OPERATION_CONTAINS =>
        request.getKeysList.asScala.iterator.foreach { key =>
          builder.addBools(conf.contains(key))
          getWarning(key).foreach(builder.addWarnings)
        }

      case proto.ConfigRequest.Operation.OPERATION_IS_MODIFIABLE =>
        request.getKeysList.asScala.iterator.foreach { key =>
          builder.addBools(conf.isModifiable(key))
          getWarning(key).foreach(builder.addWarnings)
        }

      case other =>
        throw new UnsupportedOperationException(s"Unsupported operation $other in SQLConf.")
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
}
