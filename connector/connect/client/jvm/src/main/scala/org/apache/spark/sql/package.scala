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

package org.apache.spark

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.connect.ProtoColumnNode

package object sql {
  type DataFrame = Dataset[Row]

  private[sql] def encoderFor[E: Encoder]: AgnosticEncoder[E] = {
    implicitly[Encoder[E]].asInstanceOf[AgnosticEncoder[E]]
  }

  /**
   * Create a [[Column]] from a [[proto.Expression]]
   *
   * This method is meant to be used by Connect plugins. We do not guarantee any compatility
   * between (minor) versions.
   */
  @DeveloperApi
  def column(expr: proto.Expression): Column = {
    Column(ProtoColumnNode(expr))
  }

  /**
   * Creat a [[Column]] using a function that manipulates an [[proto.Expression.Builder]].
   *
   * This method is meant to be used by Connect plugins. We do not guarantee any compatility
   * between (minor) versions.
   */
  @DeveloperApi
  def column(f: proto.Expression.Builder => Unit): Column = {
    val builder = proto.Expression.newBuilder()
    f(builder)
    column(builder.build())
  }
}
