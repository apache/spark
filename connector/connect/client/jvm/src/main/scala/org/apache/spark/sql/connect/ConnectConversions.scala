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
package org.apache.spark.sql.connect

import scala.language.implicitConversions

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.connect.proto
import org.apache.spark.sql._
import org.apache.spark.sql.internal.ProtoColumnNode

/**
 * Conversions from sql interfaces to the Connect specific implementation.
 *
 * This class is mainly used by the implementation. It is also meant to be used by extension
 * developers.
 *
 * We provide both a trait and an object. The trait is useful in situations where an extension
 * developer needs to use these conversions in a project covering multiple Spark versions. They
 * can create a shim for these conversions, the Spark 4+ version of the shim implements this
 * trait, and shims for older versions do not.
 */
@DeveloperApi
trait ConnectConversions {
  implicit def castToImpl(session: api.SparkSession): SparkSession =
    session.asInstanceOf[SparkSession]

  implicit def castToImpl[T](ds: api.Dataset[T]): Dataset[T] =
    ds.asInstanceOf[Dataset[T]]

  implicit def castToImpl(rgds: api.RelationalGroupedDataset): RelationalGroupedDataset =
    rgds.asInstanceOf[RelationalGroupedDataset]

  implicit def castToImpl[K, V](
      kvds: api.KeyValueGroupedDataset[K, V]): KeyValueGroupedDataset[K, V] =
    kvds.asInstanceOf[KeyValueGroupedDataset[K, V]]

  /**
   * Create a [[Column]] from a [[proto.Expression]]
   *
   * This method is meant to be used by Connect plugins. We do not guarantee any compatibility
   * between (minor) versions.
   */
  @DeveloperApi
  def column(expr: proto.Expression): Column = {
    Column(ProtoColumnNode(expr))
  }

  /**
   * Create a [[Column]] using a function that manipulates an [[proto.Expression.Builder]].
   *
   * This method is meant to be used by Connect plugins. We do not guarantee any compatibility
   * between (minor) versions.
   */
  @DeveloperApi
  def column(f: proto.Expression.Builder => Unit): Column = {
    val builder = proto.Expression.newBuilder()
    f(builder)
    column(builder.build())
  }

  /**
   * Implicit helper that makes it easy to construct a Column from an Expression or an Expression
   * builder. This allows developers to create a Column in the same way as in earlier versions of
   * Spark (before 4.0).
   */
  @DeveloperApi
  implicit class ColumnConstructorExt(val c: Column.type) {
    def apply(e: proto.Expression): Column = column(e)
  }
}

object ConnectConversions extends ConnectConversions
