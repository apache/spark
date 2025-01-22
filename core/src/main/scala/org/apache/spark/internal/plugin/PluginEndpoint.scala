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

package org.apache.spark.internal.plugin

import org.apache.spark.api.plugin.DriverPlugin
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.rpc.{IsolatedThreadSafeRpcEndpoint, RpcCallContext, RpcEnv}

case class PluginMessage(pluginName: String, message: AnyRef)

private class PluginEndpoint(
    plugins: Map[String, DriverPlugin],
    override val rpcEnv: RpcEnv)
  extends IsolatedThreadSafeRpcEndpoint with Logging {

  override def receive: PartialFunction[Any, Unit] = {
    case PluginMessage(pluginName, message) =>
      plugins.get(pluginName) match {
        case Some(plugin) =>
          try {
            val reply = plugin.receive(message)
            if (reply != null) {
              logWarning(
                log"Plugin ${MDC(PLUGIN_NAME, pluginName)} " +
                  log"returned reply for one-way message of type " +
                  log"${MDC(CLASS_NAME, message.getClass().getName())}.")
            }
          } catch {
            case e: Exception =>
              logWarning(log"Error in plugin ${MDC(PLUGIN_NAME, pluginName)} " +
                log"when handling message of type " +
                log"${MDC(CLASS_NAME, message.getClass().getName())}.", e)
          }

        case None =>
          throw new IllegalArgumentException(s"Received message for unknown plugin $pluginName.")
      }
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case PluginMessage(pluginName, message) =>
      plugins.get(pluginName) match {
        case Some(plugin) =>
          context.reply(plugin.receive(message))

        case None =>
          throw new IllegalArgumentException(s"Received message for unknown plugin $pluginName.")
      }
  }

}
