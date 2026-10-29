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
import org.apache.spark.sql.catalyst.expressions.{BasePredicate, Expression, Predicate}

/**
 * The predicate of a storage filter, built on first use from `boundExpression`, since a generated
 * one can be awkward to serialize. The file-source Parquet reader and [[StorageFilterImpl]] share
 * it. `filterName` names the filter in an error.
 */
class StorageFilterPredicate(private var boundExpression: Expression, filterName: String)
  extends Serializable {

  @transient lazy val prepared: BasePredicate = {
    // The expression is released below, so a copy made after a first evaluation would have nothing
    // to build from. That is named here rather than left to an NPE.
    if (boundExpression == null) {
      throw SparkException.internalError(s"$filterName was copied after an evaluation released " +
        "the expression of its predicate. Only a filter that was never evaluated can be copied.")
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
