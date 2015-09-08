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

package org.apache.spark.sql.execution

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.DataType

trait CommonOperatorGenerator extends Logging {

  private def isTesting: Boolean = sys.props.contains("spark.testing")

  protected def codegenEnabled: Boolean

  protected def newProjection(
      expressions: Seq[Expression],
      inputSchema: Seq[Attribute]): Projection = {
    log.debug(
      s"Creating Projection: $expressions, inputSchema: $inputSchema, codegen:$codegenEnabled")
    if (codegenEnabled) {
      try {
        GenerateProjection.generate(expressions, inputSchema)
      } catch {
        case e: Exception =>
          if (isTesting) {
            throw e
          } else {
            log.error("Failed to generate projection, fallback to interpret", e)
            new InterpretedProjection(expressions, inputSchema)
          }
      }
    } else {
      new InterpretedProjection(expressions, inputSchema)
    }
  }

  protected def newMutableProjection(
      expressions: Seq[Expression],
      inputSchema: Seq[Attribute]): () => MutableProjection = {
    log.debug(
      s"Creating MutableProj: $expressions, inputSchema: $inputSchema, codegen:$codegenEnabled")
    if(codegenEnabled) {
      try {
        GenerateMutableProjection.generate(expressions, inputSchema)
      } catch {
        case e: Exception =>
          if (isTesting) {
            throw e
          } else {
            log.error("Failed to generate mutable projection, fallback to interpreted", e)
            () => new InterpretedMutableProjection(expressions, inputSchema)
          }
      }
    } else {
      () => new InterpretedMutableProjection(expressions, inputSchema)
    }
  }

  protected def newPredicate(
      expression: Expression,
      inputSchema: Seq[Attribute]): (InternalRow) => Boolean = {
    if (codegenEnabled) {
      try {
        GeneratePredicate.generate(expression, inputSchema)
      } catch {
        case e: Exception =>
          if (isTesting) {
            throw e
          } else {
            log.error("Failed to generate predicate, fallback to interpreted", e)
            InterpretedPredicate.create(expression, inputSchema)
          }
      }
    } else {
      InterpretedPredicate.create(expression, inputSchema)
    }
  }

  protected def newOrdering(
      order: Seq[SortOrder],
      inputSchema: Seq[Attribute]): Ordering[InternalRow] = {
    if (codegenEnabled) {
      try {
        GenerateOrdering.generate(order, inputSchema)
      } catch {
        case e: Exception =>
          if (isTesting) {
            throw e
          } else {
            log.error("Failed to generate ordering, fallback to interpreted", e)
            new InterpretedOrdering(order, inputSchema)
          }
      }
    } else {
      new InterpretedOrdering(order, inputSchema)
    }
  }

  /**
   * Creates a row ordering for the given schema, in natural ascending order.
   */
  protected def newNaturalAscendingOrdering(dataTypes: Seq[DataType]): Ordering[InternalRow] = {
    val order: Seq[SortOrder] = dataTypes.zipWithIndex.map {
      case (dt, index) => new SortOrder(BoundReference(index, dt, nullable = true), Ascending)
    }
    newOrdering(order, Nil)
  }
}
