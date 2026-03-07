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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.connector.catalog.CustomPredicateDescriptor
import org.apache.spark.sql.types.{BooleanType, DataType}

/**
 * A resolved custom predicate expression declared by a DSv2 table via
 * [[org.apache.spark.sql.connector.catalog.SupportsCustomPredicates]].
 *
 * This expression evaluates to BooleanType and is translatable to a V2 Predicate
 * whose name is the descriptor's canonical name (dot-qualified).
 *
 * Custom predicates cannot be evaluated locally because they are data-source-specific
 * by definition. If the data source rejects the predicate at pushPredicates() time,
 * the post-optimizer rule EnsureCustomPredicatesPushed will detect it and fail with
 * a clear error before execution begins.
 */
case class CustomPredicateExpression(
    descriptor: CustomPredicateDescriptor,
    arguments: Seq[Expression])
  extends Expression with CodegenFallback {

  override def dataType: DataType = BooleanType
  override def nullable: Boolean = true
  override def children: Seq[Expression] = arguments
  override def foldable: Boolean = false
  override lazy val deterministic: Boolean = descriptor.isDeterministic

  override def eval(input: InternalRow): Any = {
    throw new SparkException(
      s"Custom predicate '${descriptor.sqlName()}' cannot be evaluated by Spark. " +
      s"It must be pushed to the data source '${descriptor.canonicalName()}'.")
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression =
    copy(arguments = newChildren)
}
