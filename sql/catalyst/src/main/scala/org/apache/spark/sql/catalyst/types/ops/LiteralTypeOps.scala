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

package org.apache.spark.sql.catalyst.types.ops

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.{DataType, TimeType}

// Literal operations over Catalyst's types
trait LiteralTypeOps {
  // Gets a literal with default value of the type
  def getDefaultLiteral: Literal
  // Gets an Java literal as a string. It can be used in codegen
  def getJavaLiteral(v: Any): String
}

object LiteralTypeOps {
  private val supportedDataTypes: Set[DataType] =
    Set(TimeType.MIN_PRECISION to TimeType.MAX_PRECISION map TimeType.apply: _*)

  def supports(dt: DataType): Boolean = supportedDataTypes.contains(dt)
  def apply(dt: DataType): LiteralTypeOps = TypeOps(dt).asInstanceOf[LiteralTypeOps]
}
