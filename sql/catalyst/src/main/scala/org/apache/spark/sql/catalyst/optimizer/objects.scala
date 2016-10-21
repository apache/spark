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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.api.java.function.FilterFunction
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

/*
 * This file defines optimization rules related to object manipulation (for the Dataset API).
 */

/**
 * Removes cases where we are unnecessarily going between the object and serialized (InternalRow)
 * representation of data item.  For example back to back map operations.
 */
object EliminateSerialization extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case d @ DeserializeToObject(_, _, s: SerializeFromObject)
      if d.outputObjAttr.dataType == s.inputObjAttr.dataType =>
      // Adds an extra Project here, to preserve the output expr id of `DeserializeToObject`.
      // We will remove it later in RemoveAliasOnlyProject rule.
      val objAttr = Alias(s.inputObjAttr, s.inputObjAttr.name)(exprId = d.outputObjAttr.exprId)
      Project(objAttr :: Nil, s.child)

    case a @ AppendColumns(_, _, _, _, _, s: SerializeFromObject)
      if a.deserializer.dataType == s.inputObjAttr.dataType =>
      AppendColumnsWithObject(a.func, s.serializer, a.serializer, s.child)

    // If there is a `SerializeFromObject` under typed filter and its input object type is same with
    // the typed filter's deserializer, we can convert typed filter to normal filter without
    // deserialization in condition, and push it down through `SerializeFromObject`.
    // e.g. `ds.map(...).filter(...)` can be optimized by this rule to save extra deserialization,
    // but `ds.map(...).as[AnotherType].filter(...)` can not be optimized.
    case f @ TypedFilter(_, _, _, _, s: SerializeFromObject)
      if f.deserializer.dataType == s.inputObjAttr.dataType =>
      s.copy(child = f.withObjectProducerChild(s.child))

    // If there is a `DeserializeToObject` upon typed filter and its output object type is same with
    // the typed filter's deserializer, we can convert typed filter to normal filter without
    // deserialization in condition, and pull it up through `DeserializeToObject`.
    // e.g. `ds.filter(...).map(...)` can be optimized by this rule to save extra deserialization,
    // but `ds.filter(...).as[AnotherType].map(...)` can not be optimized.
    case d @ DeserializeToObject(_, _, f: TypedFilter)
      if d.outputObjAttr.dataType == f.deserializer.dataType =>
      f.withObjectProducerChild(d.copy(child = f.child))
  }
}

/**
 * Combines two adjacent [[TypedFilter]]s, which operate on same type object in condition, into one,
 * mering the filter functions into one conjunctive function.
 */
object CombineTypedFilters extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case t1 @ TypedFilter(_, _, _, _, t2 @ TypedFilter(_, _, _, _, child))
        if t1.deserializer.dataType == t2.deserializer.dataType =>
      TypedFilter(
        combineFilterFunction(t2.func, t1.func),
        t1.argumentClass,
        t1.argumentSchema,
        t1.deserializer,
        child)
  }

  private def combineFilterFunction(func1: AnyRef, func2: AnyRef): Any => Boolean = {
    (func1, func2) match {
      case (f1: FilterFunction[_], f2: FilterFunction[_]) =>
        input => f1.asInstanceOf[FilterFunction[Any]].call(input) &&
          f2.asInstanceOf[FilterFunction[Any]].call(input)
      case (f1: FilterFunction[_], f2) =>
        input => f1.asInstanceOf[FilterFunction[Any]].call(input) &&
          f2.asInstanceOf[Any => Boolean](input)
      case (f1, f2: FilterFunction[_]) =>
        input => f1.asInstanceOf[Any => Boolean].apply(input) &&
          f2.asInstanceOf[FilterFunction[Any]].call(input)
      case (f1, f2) =>
        input => f1.asInstanceOf[Any => Boolean].apply(input) &&
          f2.asInstanceOf[Any => Boolean].apply(input)
    }
  }
}
