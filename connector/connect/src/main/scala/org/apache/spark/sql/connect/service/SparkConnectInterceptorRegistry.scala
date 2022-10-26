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

import java.lang.reflect.InvocationTargetException

import io.grpc.ServerInterceptor
import io.grpc.netty.NettyServerBuilder

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.util.Utils

/**
 * This object provides a global list of configured interceptors for GRPC. The interceptors are
 * added to the GRPC server in order of their position in the list. Once the statically compiled
 * interceptors are added, dynamically configured interceptors are added.
 */
object SparkConnectInterceptorRegistry {

  // Contains the list of configured interceptors.
  private lazy val interceptorChain: Seq[InterceptorBuilder] = Seq(
    // Adding a new interceptor at compile time works like the eaxmple below with the dummy
    // interceptor:
    // interceptor[DummyInterceptor](classOf[DummyInterceptor])
  )

  /**
   * Given a NettyServerBuilder instance, will chain all interceptors to it in reverse order.
   * @param sb
   */
  def chainInterceptors(sb: NettyServerBuilder): Unit = {
    interceptorChain.foreach(i => sb.intercept(i()))
    createConfiguredInterceptors().foreach(sb.intercept(_))
  }

  // Type used to identify the closure responsible to instantiate a ServerInterceptor.
  type InterceptorBuilder = () => ServerInterceptor

  /**
   * Exposed for testing only.
   */
  def createConfiguredInterceptors(): Seq[ServerInterceptor] = {
    // Check all values from the Spark conf.
    val classes = SparkEnv.get.conf.get(Connect.CONNECT_GRPC_INTERCEPTOR_CLASSES)
    if (classes.nonEmpty) {
      classes.get
        .split(",")
        .map(_.trim)
        .filter(_.nonEmpty)
        .map(Utils.classForName[ServerInterceptor](_))
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
  private def createInstance[T <: ServerInterceptor](cls: Class[T]): ServerInterceptor = {
    val ctorOpt = cls.getConstructors.find(_.getParameterCount == 0)
    if (ctorOpt.isEmpty) {
      throw new SparkException(
        errorClass = "CONNECT.INTERCEPTOR_CTOR_MISSING",
        messageParameters = Map("cls" -> cls.getName),
        cause = null)
    }
    try {
      ctorOpt.get.newInstance().asInstanceOf[T]
    } catch {
      case e: InvocationTargetException =>
        throw new SparkException(
          errorClass = "CONNECT.INTERCEPTOR_RUNTIME_ERROR",
          messageParameters = Map("msg" -> e.getTargetException.getMessage),
          cause = e)
      case e: Exception =>
        throw new SparkException(
          errorClass = "CONNECT.INTERCEPTOR_RUNTIME_ERROR",
          messageParameters = Map("msg" -> e.getMessage),
          cause = e)
    }
  }

  /**
   * Creates a callable expression that instantiates the configured GPRC interceptor
   * implementation.
   */
  private def interceptor[T <: ServerInterceptor](cls: Class[T]): InterceptorBuilder =
    () => createInstance(cls)
}
