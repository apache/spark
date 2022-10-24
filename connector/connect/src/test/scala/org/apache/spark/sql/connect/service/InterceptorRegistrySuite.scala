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

import io.grpc.{Metadata, ServerCall, ServerCallHandler, ServerInterceptor}
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener
import io.grpc.netty.NettyServerBuilder

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Used for testing only, does not do anything.
 */
class DummyInterceptor extends ServerInterceptor {
  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT]): ServerCall.Listener[ReqT] = {
    val listener = next.startCall(call, headers)
    new SimpleForwardingServerCallListener[ReqT](listener) {
      override def onMessage(message: ReqT): Unit = {
        delegate().onMessage(message)
      }
    }
  }
}

/**
 * Used for testing only.
 */
class TestingInterceptorNoTrivialCtor(id: Int) extends ServerInterceptor {
  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT]): ServerCall.Listener[ReqT] = {
    val listener = next.startCall(call, headers)
    new SimpleForwardingServerCallListener[ReqT](listener) {
      override def onMessage(message: ReqT): Unit = {
        delegate().onMessage(message)
      }
    }
  }
}

/**
 * Used for testing only.
 */
class TestingInterceptorInstantiationError extends ServerInterceptor {
  throw new ArrayIndexOutOfBoundsException("Bad Error")

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT]): ServerCall.Listener[ReqT] = {
    val listener = next.startCall(call, headers)
    new SimpleForwardingServerCallListener[ReqT](listener) {
      override def onMessage(message: ReqT): Unit = {
        delegate().onMessage(message)
      }
    }
  }
}

class InterceptorRegistrySuite extends SharedSparkSession {

  override def beforeEach(): Unit = {
    if (SparkEnv.get.conf.contains(Connect.CONNECT_GRPC_INTERCEPTOR_CLASSES)) {
      SparkEnv.get.conf.remove(Connect.CONNECT_GRPC_INTERCEPTOR_CLASSES)
    }
  }

  def withSparkConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val conf = SparkEnv.get.conf
    pairs.foreach { kv => conf.set(kv._1, kv._2) }
    try f
    finally {
      pairs.foreach { kv => conf.remove(kv._1) }
    }
  }

  test("Check that the empty registry works") {
    val sb = NettyServerBuilder.forPort(9999)
    SparkConnectInterceptorRegistry.chainInterceptors(sb)
  }

  test("Test server builder and configured interceptor") {
    withSparkConf(
      Connect.CONNECT_GRPC_INTERCEPTOR_CLASSES.key ->
        "org.apache.spark.sql.connect.service.DummyInterceptor") {
      val sb = NettyServerBuilder.forPort(9999)
      SparkConnectInterceptorRegistry.chainInterceptors(sb)
    }
  }

  test("Test server build throws when using bad configured interceptor") {
    withSparkConf(
      Connect.CONNECT_GRPC_INTERCEPTOR_CLASSES.key ->
        "org.apache.spark.sql.connect.service.TestingInterceptorNoTrivialCtor") {
      val sb = NettyServerBuilder.forPort(9999)
      assertThrows[SparkException] {
        SparkConnectInterceptorRegistry.chainInterceptors(sb)
      }
    }
  }

  test("Exception handling for interceptor classes") {
    withSparkConf(
      Connect.CONNECT_GRPC_INTERCEPTOR_CLASSES.key ->
        "org.apache.spark.sql.connect.service.TestingInterceptorNoTrivialCtor") {
      assertThrows[SparkException] {
        SparkConnectInterceptorRegistry.createConfiguredInterceptors
      }
    }

    withSparkConf(
      Connect.CONNECT_GRPC_INTERCEPTOR_CLASSES.key ->
        "org.apache.spark.sql.connect.service.TestingInterceptorInstantiationError") {
      assertThrows[SparkException] {
        SparkConnectInterceptorRegistry.createConfiguredInterceptors
      }
    }
  }

  test("No configured interceptors returns empty list") {
    // Not set.
    assert(SparkConnectInterceptorRegistry.createConfiguredInterceptors.isEmpty)
    // Set to empty string
    withSparkConf(Connect.CONNECT_GRPC_INTERCEPTOR_CLASSES.key -> "") {
      assert(SparkConnectInterceptorRegistry.createConfiguredInterceptors.isEmpty)
    }
  }

  test("Configured classes can have multiple entries") {
    withSparkConf(
      Connect.CONNECT_GRPC_INTERCEPTOR_CLASSES.key ->
        (" org.apache.spark.sql.connect.service.DummyInterceptor," +
          "    org.apache.spark.sql.connect.service.DummyInterceptor   ")) {
      assert(SparkConnectInterceptorRegistry.createConfiguredInterceptors.size == 2)
    }
  }

  test("Configured class not found is properly thrown") {
    withSparkConf(Connect.CONNECT_GRPC_INTERCEPTOR_CLASSES.key -> "this.class.does.not.exist") {
      assertThrows[ClassNotFoundException] {
        SparkConnectInterceptorRegistry.createConfiguredInterceptors
      }
    }
  }

}
