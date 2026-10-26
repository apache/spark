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

package org.apache.spark.sql.internal.connector

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{Attribute, BasePredicate, BindReferences, Expression, Predicate}

/**
 * The predicate of a storage filter, built on first use from `boundExpression`, since a generated
 * one can be awkward to serialize. The file-source Parquet reader and [[StorageFilterImpl]] share
 * it.
 */
class StorageFilterPredicate(private var boundExpression: Expression) extends Serializable {

  @transient lazy val prepared: BasePredicate = {
    // The expression is released below, so a copy made after a first evaluation would have nothing
    // to build from. That is named here rather than left to an NPE.
    if (boundExpression == null) {
      throw SparkException.internalError("A storage filter was copied after an evaluation " +
        "released the expression of its predicate")
    }
    val created = Predicate.create(boundExpression)
    // A bloom's bytes sit in the expression as a binary literal, while a generated predicate holds
    // the deserialized filter instead, so dropping the expression lets the task reclaim them. An
    // interpreted predicate, the fallback, keeps the expression. Every task deserializes its own
    // copy, and this is the field's only reader.
    boundExpression = null
    created
  }
}

object StorageFilterPredicate {

  /** The predicate of `expr` over rows of `attrs`, in that order. */
  def apply(expr: Expression, attrs: Seq[Attribute]): StorageFilterPredicate =
    new StorageFilterPredicate(BindReferences.bindReference(expr, attrs))
}
