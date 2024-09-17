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
package org.apache.spark.sql.connect.common.config

private[sql] object ConnectCommon {
  val CONNECT_GRPC_BINDING_PORT: Int = 15002
  val CONNECT_GRPC_PORT_MAX_RETRIES: Int = 0
  val CONNECT_GRPC_MAX_MESSAGE_SIZE: Int = 128 * 1024 * 1024
  val CONNECT_GRPC_MARSHALLER_RECURSION_LIMIT: Int = 4096
}
