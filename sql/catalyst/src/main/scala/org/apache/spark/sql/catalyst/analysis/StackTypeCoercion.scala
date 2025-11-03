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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Stack}
import org.apache.spark.sql.types.NullType

/**
 * Type coercion helper that matches against [[Stack]] expressions in order to type coerce children
 * that are of [[NullType]] to the expected column type.
 */
object StackTypeCoercion {
  def apply(expression: Expression): Expression = expression match {
    case s @ Stack(children) if s.hasFoldableNumRows =>
      Stack(children.zipWithIndex.map {
        // The first child is the number of rows for stack.
        case (e, 0) => e
        case (Literal(null, NullType), index: Int) =>
          Literal.create(null, s.findDataType(index))
        case (e, _) => e
      })

    case other => other
  }
}
