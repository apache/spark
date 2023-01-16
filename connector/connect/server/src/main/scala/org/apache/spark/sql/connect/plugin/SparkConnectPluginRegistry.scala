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

package org.apache.spark.sql.connect.plugin

import java.lang.reflect.InvocationTargetException

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.util.Utils

/**
 * This object provides a global list of configured relation and expression plugins for Spark
 * Connect. The plugins are used to handle custom message types.
 */
object SparkConnectPluginRegistry {

  // Contains the list of configured interceptors.
  private lazy val relationPluginChain: Seq[relationPluginBuilder] = Seq(
    // Adding a new plugin at compile time works like the example below:
    // relation[DummyRelationPlugin](classOf[DummyRelationPlugin])
  )

  private lazy val expressionPluginChain: Seq[expressionPluginBuilder] = Seq(
    // Adding a new plugin at compile time works like the example below:
    // expression[DummyExpressionPlugin](classOf[DummyExpressionPlugin])
  )

  private lazy val commandPluginChain: Seq[commandPluginBuilder] = Seq(
    // Adding a new plugin at compile time works like the example below:
    // expression[DummyExpressionPlugin](classOf[DummyExpressionPlugin])
  )

  private var initialized = false
  private var relationRegistryCache: Seq[RelationPlugin] = Seq.empty
  private var expressionRegistryCache: Seq[ExpressionPlugin] = Seq.empty
  private var commandRegistryCache: Seq[CommandPlugin] = Seq.empty

  // Type used to identify the closure responsible to instantiate a ServerInterceptor.
  type relationPluginBuilder = () => RelationPlugin
  type expressionPluginBuilder = () => ExpressionPlugin
  type commandPluginBuilder = () => CommandPlugin

  def relationRegistry: Seq[RelationPlugin] = withInitialize {
    relationRegistryCache
  }
  def expressionRegistry: Seq[ExpressionPlugin] = withInitialize {
    expressionRegistryCache
  }
  def commandRegistry: Seq[CommandPlugin] = withInitialize {
    commandRegistryCache
  }

  private def withInitialize[T](f: => Seq[T]): Seq[T] = {
    synchronized {
      if (!initialized) {
        relationRegistryCache = loadRelationPlugins()
        expressionRegistryCache = loadExpressionPlugins()
        commandRegistryCache = loadCommandPlugins()
        initialized = true
      }
    }
    f
  }

  /**
   * Only visible for testing. Should not be called from any other code path.
   */
  def reset(): Unit = {
    synchronized {
      initialized = false
    }
  }

  /**
   * Only visible for testing
   */
  private[connect] def loadRelationPlugins(): Seq[RelationPlugin] = {
    relationPluginChain.map(x => x()) ++ createConfiguredPlugins[RelationPlugin](
      SparkEnv.get.conf.get(Connect.CONNECT_EXTENSIONS_RELATION_CLASSES))
  }

  /**
   * Only visible for testing
   */
  private[connect] def loadExpressionPlugins(): Seq[ExpressionPlugin] = {
    expressionPluginChain.map(x => x()) ++ createConfiguredPlugins(
      SparkEnv.get.conf.get(Connect.CONNECT_EXTENSIONS_EXPRESSION_CLASSES))
  }

  private[connect] def loadCommandPlugins(): Seq[CommandPlugin] = {
    commandPluginChain.map(x => x()) ++ createConfiguredPlugins(
      SparkEnv.get.conf.get(Connect.CONNECT_EXTENSIONS_COMMAND_CLASSES))
  }

  /**
   * Exposed for testing only.
   */
  def createConfiguredPlugins[T](values: Seq[String]): Seq[T] = {
    // Check all values from the Spark conf.
    if (values.nonEmpty) {
      values
        .map(_.trim)
        .filter(_.nonEmpty)
        .map(Utils.classForName[T](_))
        .map(createInstance(_))
    } else {
      Seq.empty
    }
  }

  /**
   * Creates a new instance of T using the default constructor.
   * @param cls
   * @tparam T
   * @return
   */
  private def createInstance[B, T <: B](cls: Class[T]): B = {
    val ctorOpt = cls.getConstructors.find(_.getParameterCount == 0)
    if (ctorOpt.isEmpty) {
      throw new SparkException(
        errorClass = "CONNECT.PLUGIN_CTOR_MISSING",
        messageParameters = Map("cls" -> cls.getName),
        cause = null)
    }
    try {
      ctorOpt.get.newInstance().asInstanceOf[T]
    } catch {
      case e: InvocationTargetException =>
        throw new SparkException(
          errorClass = "CONNECT.PLUGIN_RUNTIME_ERROR",
          messageParameters = Map("msg" -> e.getTargetException.getMessage),
          cause = e)
      case e: Exception =>
        throw new SparkException(
          errorClass = "CONNECT.PLUGIN_RUNTIME_ERROR",
          messageParameters = Map("msg" -> e.getMessage),
          cause = e)
    }
  }

  /**
   * Creates a callable expression that instantiates the configured Relation plugin.
   *
   * Visible for testing only.
   */
  def relation[T <: RelationPlugin](cls: Class[T]): relationPluginBuilder =
    () => createInstance[RelationPlugin, T](cls)

  /**
   * Creates a callable expression that instantiates the configured Expression plugin.
   *
   * Visible for testing only.
   */
  def expression[T <: ExpressionPlugin](cls: Class[T]): expressionPluginBuilder =
    () => createInstance[ExpressionPlugin, T](cls)

  /**
   * Creates a callable expression that instantiates the configured Command plugin.
   *
   * Visible for testing only.
   */
  def command[T <: CommandPlugin](cls: Class[T]): commandPluginBuilder =
    () => createInstance[CommandPlugin, T](cls)
}
