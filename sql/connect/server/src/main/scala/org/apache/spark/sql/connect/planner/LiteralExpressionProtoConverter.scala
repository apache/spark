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

package org.apache.spark.sql.connect.planner

import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.connect.common.LiteralValueProtoConverter

object LiteralExpressionProtoConverter {

  /**
   * Transforms the protocol buffers literals into the appropriate Catalyst literal expression.
   *
   * @return
   *   Expression
   */
  def toCatalystExpression(lit: proto.Expression.Literal): expressions.Literal = {
    val dataType = LiteralValueProtoConverter.getDataType(lit)
    val scalaValue = LiteralValueProtoConverter.toScalaValue(lit)
    val convert = CatalystTypeConverters.createToCatalystConverter(dataType)
    expressions.Literal(convert(scalaValue), dataType)
  }
}
