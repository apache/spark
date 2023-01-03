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

import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.network.util.ByteUnit

private[spark] object Connect {

  val CONNECT_GRPC_BINDING_PORT =
    ConfigBuilder("spark.connect.grpc.binding.port")
      .version("3.4.0")
      .intConf
      .createWithDefault(15002)

  val CONNECT_GRPC_INTERCEPTOR_CLASSES =
    ConfigBuilder("spark.connect.grpc.interceptor.classes")
      .doc(
        "Comma separated list of class names that must " +
          "implement the io.grpc.ServerInterceptor interface.")
      .version("3.4.0")
      .stringConf
      .createOptional

  val CONNECT_GRPC_ARROW_MAX_BATCH_SIZE =
    ConfigBuilder("spark.connect.grpc.arrow.maxBatchSize")
      .doc(
        "When using Apache Arrow, limit the maximum size of one arrow batch that " +
          "can be sent from server side to client side. Currently, we conservatively use 70% " +
          "of it because the size is not accurate but estimated.")
      .version("3.4.0")
      .bytesConf(ByteUnit.MiB)
      .createWithDefaultString("4m")

  val CONNECT_EXTENSIONS_RELATION_CLASSES =
    ConfigBuilder("spark.connect.extensions.relation.classes")
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
    ConfigBuilder("spark.connect.extensions.expression.classes")
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
    ConfigBuilder("spark.connect.extensions.command.classes")
      .doc("""
             |Comma separated list of classes that implement the trait
             |org.apache.spark.sql.connect.plugin.CommandPlugin to support custom
             |Command types in proto.
             |""".stripMargin)
      .version("3.4.0")
      .stringConf
      .toSequence
      .createWithDefault(Nil)
}
