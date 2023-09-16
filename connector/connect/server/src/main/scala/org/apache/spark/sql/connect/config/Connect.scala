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
package org.apache.spark.sql.connect.config

import java.util.concurrent.TimeUnit

import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.connect.common.config.ConnectCommon

object Connect {
  import org.apache.spark.sql.internal.SQLConf.buildStaticConf

  val CONNECT_GRPC_BINDING_PORT =
    buildStaticConf("spark.connect.grpc.binding.port")
      .version("3.4.0")
      .intConf
      .createWithDefault(ConnectCommon.CONNECT_GRPC_BINDING_PORT)

  val CONNECT_GRPC_INTERCEPTOR_CLASSES =
    buildStaticConf("spark.connect.grpc.interceptor.classes")
      .doc(
        "Comma separated list of class names that must " +
          "implement the io.grpc.ServerInterceptor interface.")
      .version("3.4.0")
      .stringConf
      .createOptional

  val CONNECT_GRPC_ARROW_MAX_BATCH_SIZE =
    buildStaticConf("spark.connect.grpc.arrow.maxBatchSize")
      .doc(
        "When using Apache Arrow, limit the maximum size of one arrow batch, in bytes unless " +
          "otherwise specified, that can be sent from server side to client side. Currently, we " +
          "conservatively use 70% of it because the size is not accurate but estimated.")
      .version("3.4.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(4 * 1024 * 1024)

  val CONNECT_GRPC_MAX_INBOUND_MESSAGE_SIZE =
    buildStaticConf("spark.connect.grpc.maxInboundMessageSize")
      .doc("Sets the maximum inbound message in bytes size for the gRPC requests." +
        "Requests with a larger payload will fail.")
      .version("3.4.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(ConnectCommon.CONNECT_GRPC_MAX_MESSAGE_SIZE)

  val CONNECT_GRPC_MARSHALLER_RECURSION_LIMIT =
    buildStaticConf("spark.connect.grpc.marshallerRecursionLimit")
      .internal()
      .doc("""
          |Sets the recursion limit to grpc protobuf messages.
          |""".stripMargin)
      .version("3.5.0")
      .intConf
      .createWithDefault(1024)

  val CONNECT_EXECUTE_MANAGER_DETACHED_TIMEOUT =
    buildStaticConf("spark.connect.execute.manager.detachedTimeout")
      .internal()
      .doc("Timeout after which executions without an attached RPC will be removed.")
      .version("3.5.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("5m")

  val CONNECT_EXECUTE_MANAGER_MAINTENANCE_INTERVAL =
    buildStaticConf("spark.connect.execute.manager.maintenanceInterval")
      .internal()
      .doc("Interval at which execution manager will search for abandoned executions to remove.")
      .version("3.5.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("30s")

  val CONNECT_EXECUTE_MANAGER_ABANDONED_TOMBSTONES_SIZE =
    buildStaticConf("spark.connect.execute.manager.abandonedTombstonesSize")
      .internal()
      .doc("Maximum size of the cache of abandoned executions.")
      .version("3.5.0")
      .intConf
      .createWithDefaultString("10000")

  val CONNECT_EXECUTE_REATTACHABLE_ENABLED =
    buildStaticConf("spark.connect.execute.reattachable.enabled")
      .internal()
      .doc("Enables reattachable execution on the server. If disabled and a client requests it, " +
        "non-reattachable execution will follow and should run until query completion. This will " +
        "work, unless there is a GRPC stream error, in which case the client will discover that " +
        "execution is not reattachable when trying to reattach fails.")
      .version("3.5.0")
      .booleanConf
      .createWithDefault(true)

  val CONNECT_EXECUTE_REATTACHABLE_SENDER_MAX_STREAM_DURATION =
    buildStaticConf("spark.connect.execute.reattachable.senderMaxStreamDuration")
      .internal()
      .doc("For reattachable execution, after this amount of time the response stream will be " +
        "automatically completed and client needs to send a new ReattachExecute RPC to continue. " +
        "Set to 0 for unlimited.")
      .version("3.5.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("2m")

  val CONNECT_EXECUTE_REATTACHABLE_SENDER_MAX_STREAM_SIZE =
    buildStaticConf("spark.connect.execute.reattachable.senderMaxStreamSize")
      .internal()
      .doc(
        "For reattachable execution, after total responses size exceeds this value, the " +
          "response stream will be automatically completed and client needs to send a new " +
          "ReattachExecute RPC to continue. Set to 0 for unlimited.")
      .version("3.5.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1g")

  val CONNECT_EXECUTE_REATTACHABLE_OBSERVER_RETRY_BUFFER_SIZE =
    buildStaticConf("spark.connect.execute.reattachable.observerRetryBufferSize")
      .internal()
      .doc(
        "For reattachable execution, the total size of responses that were already sent to be " +
          "kept in the buffer in case of connection error and client needing to retry. " +
          "Set 0 to don't buffer anything (even last sent response)." +
          "With any value greater than 0, the last sent response will always be buffered.")
      .version("3.5.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("10m")

  val CONNECT_EXTENSIONS_RELATION_CLASSES =
    buildStaticConf("spark.connect.extensions.relation.classes")
      .doc("""
          |Comma separated list of classes that implement the trait
          |org.apache.spark.sql.connect.plugin.RelationPlugin to support custom
          |Relation types in proto.
          |""".stripMargin)
      .version("3.4.0")
      .stringConf
      .toSequence
      .createWithDefault(Nil)

  val CONNECT_EXTENSIONS_EXPRESSION_CLASSES =
    buildStaticConf("spark.connect.extensions.expression.classes")
      .doc("""
          |Comma separated list of classes that implement the trait
          |org.apache.spark.sql.connect.plugin.ExpressionPlugin to support custom
          |Expression types in proto.
          |""".stripMargin)
      .version("3.4.0")
      .stringConf
      .toSequence
      .createWithDefault(Nil)

  val CONNECT_EXTENSIONS_COMMAND_CLASSES =
    buildStaticConf("spark.connect.extensions.command.classes")
      .doc("""
             |Comma separated list of classes that implement the trait
             |org.apache.spark.sql.connect.plugin.CommandPlugin to support custom
             |Command types in proto.
             |""".stripMargin)
      .version("3.4.0")
      .stringConf
      .toSequence
      .createWithDefault(Nil)

  val CONNECT_JVM_STACK_TRACE_MAX_SIZE =
    buildStaticConf("spark.connect.jvmStacktrace.maxSize")
      .doc("""
          |Sets the maximum stack trace size to display when
          |`spark.sql.pyspark.jvmStacktrace.enabled` is true.
          |""".stripMargin)
      .version("3.5.0")
      .intConf
      .createWithDefault(2048)

  val CONNECT_COPY_FROM_LOCAL_TO_FS_ALLOW_DEST_LOCAL =
    buildStaticConf("spark.connect.copyFromLocalToFs.allowDestLocal")
      .internal()
      .doc("""
            |Allow `spark.copyFromLocalToFs` destination to be local file system
            | path on spark driver node when
            |`spark.connect.copyFromLocalToFs.allowDestLocal` is true.
            |This will allow user to overwrite arbitrary file on spark
            |driver node we should only enable it for testing purpose.
            |""".stripMargin)
      .version("3.5.0")
      .booleanConf
      .createWithDefault(false)

  val CONNECT_UI_STATEMENT_LIMIT =
    buildStaticConf("spark.sql.connect.ui.retainedStatements")
      .doc("The number of statements kept in the Spark Connect UI history.")
      .version("3.5.0")
      .intConf
      .createWithDefault(200)

  val CONNECT_UI_SESSION_LIMIT = buildStaticConf("spark.sql.connect.ui.retainedSessions")
    .doc("The number of client sessions kept in the Spark Connect UI history.")
    .version("3.5.0")
    .intConf
    .createWithDefault(200)
}
