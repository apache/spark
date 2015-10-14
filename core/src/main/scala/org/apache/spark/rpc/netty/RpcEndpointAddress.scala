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

package org.apache.spark.rpc.netty

import org.apache.spark.SparkException
import org.apache.spark.rpc.RpcAddress

/**
 * An address identifier for an RPC endpoint.
 *
 * @param host host name of the remote process.
 * @param port the port the remote RPC environment binds to.
 * @param name name of the remote endpoint.
 */
private[netty] case class RpcEndpointAddress(host: String, port: Int, name: String) {

  def toRpcAddress: RpcAddress = RpcAddress(host, port)

  override val toString = s"spark://$name@$host:$port"
}

private[netty] object RpcEndpointAddress {

  def apply(sparkUrl: String): RpcEndpointAddress = {
    try {
      val uri = new java.net.URI(sparkUrl)
      val host = uri.getHost
      val port = uri.getPort
      val name = uri.getUserInfo
      if (uri.getScheme != "spark" ||
          host == null ||
          port < 0 ||
          name == null ||
          (uri.getPath != null && !uri.getPath.isEmpty) || // uri.getPath returns "" instead of null
          uri.getFragment != null ||
          uri.getQuery != null) {
        throw new SparkException("Invalid Spark URL: " + sparkUrl)
      }
      RpcEndpointAddress(host, port, name)
    } catch {
      case e: java.net.URISyntaxException =>
        throw new SparkException("Invalid Spark URL: " + sparkUrl, e)
    }
  }
}
