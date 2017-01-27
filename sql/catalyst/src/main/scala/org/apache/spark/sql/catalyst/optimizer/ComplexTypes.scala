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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
* push down operations into [[CreateNamedStructLike]].
*/
object SimplifyCreateStructOps extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformExpressionsUp {
      // push down field extraction
      case GetStructField(createNamedStructLike: CreateNamedStructLike, ordinal, _) =>
        createNamedStructLike.valExprs(ordinal)
    }
  }
}

/**
* push down operations into [[CreateArray]].
*/
object SimplifyCreateArrayOps extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformExpressionsUp {
      // push down field selection (array of structs)
      case GetArrayStructFields(CreateArray(elems), field, ordinal, numFields, containsNull) =>
        // instead f selecting the field on the entire array,
        // select it from each member of the array.
        // pushing down the operation this way open other optimizations opportunities
        // (i.e. struct(...,x,...).x)
        CreateArray(elems.map(GetStructField(_, ordinal, Some(field.name))))
      // push down item selection.
      case ga @ GetArrayItem(CreateArray(elems), IntegerLiteral(idx)) =>
        // instead of creating the array and then selecting one row,
        // remove array creation altgether.
        if (idx >= 0 && idx < elems.size) {
          // valid index
          elems(idx)
        } else {
          // out of bounds, mimic the runtime behavior and return null
          Cast(Literal(null), ga.dataType)
        }
    }
  }
}

/**
* push down operations into [[CreateMap]].
*/
object SimplifyCreateMapOps extends Rule[LogicalPlan] {
  object ComparisonResult extends Enumeration {
    val PositiveMatch = Value
    val NegativeMatch = Value
    val UnDetermined = Value
  }

  def compareKeys(k1 : Expression, k2 : Expression) : ComparisonResult.Value = {
    (k1, k2) match {
      case (x, y) if x.semanticEquals(y) => ComparisonResult.PositiveMatch
      // make suret his is null safe, especially when datatypes differ
      // is this even possible?
      case (_ : Literal, _ : Literal) => ComparisonResult.NegativeMatch
      case _ => ComparisonResult.UnDetermined
    }
  }

/**
* classify entries according to their potential to produce a match
* with the requested key in runtime,
* it may somtimes be possible to determine statically if
* a requested key and an entry match or not,
* otherwise the entry is considered undetermined.
* @param mapEntries
* @param requestedKey
* @return list of key-values pairs that may or may not produce a match in runtime
*         and potentially a result known statically to match the requested key.
*         under certain circumstances, this method may identify a positive match but
*         'hide' it as part of the undetermined entries, this has to do with the way we use
*         [[Coalesce]] when constructing the rewriten tree.
*/
  def classifyEntries(mapEntries : Seq[(Expression, Expression)],
                      requestedKey : Expression)
  : (Seq[(Expression, Expression)], Option[Expression]) = {
    // compare all key expressions to the requested key's expression.
    // this comparison classifies the keys as:
    // 1. definitely negative match (i.e. two DIFFERENT literals).
    // 2. definitely a match(i.e. two identical literals, two identical refs).
    // 3. unknown (can't be determined statically).
    val res1 = mapEntries.map{
      case (k, v) => (k, v, compareKeys(k, requestedKey))
    }
    // we first filter away the negatives
    val res2 = res1.filter(_._3 != ComparisonResult.NegativeMatch)
    // now we're left with positives and unknowns (either or both groups can be empty)
    // considering the runtime behavior of [[GetMapValue]],
    // we can eliminate any key following the first known positive (if there is any),
    // this keys are guaranteed not to 'win' the search in key-space.
    val (h, t) = res2.span(_._3 == ComparisonResult.UnDetermined)
    // when one of the potential results is nullable,
    // we cannot separate the positive result from the rest
    // (see apply below for details and the use of Coalesce).
    val hasNullableUndetermined = h.exists(_._2.nullable)
    val (resEntries, resPositive) = if ( hasNullableUndetermined ) {
      // we 'hide' the positive result with the undetermined ones
      (h ++ t.headOption, None)
    } else {
      // positive result can be separated from undetermined results
      (h, t.headOption.map(_._2))
    }
    val resPairs = resEntries.map{
      case (k, v, _) => (k, v)
    }
    (resPairs, resPositive)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformExpressionsUp {
      // attempt to unfold 'constant' key extraction,
      // this enables other optimizations to take place.
      case gmv @ GetMapValue(cm @ CreateMap(elems), key) =>
        val kvs = cm.keys.zip(cm.values)
        val (classifiedEntries, positiveValue) = classifyEntries(kvs, key)

        val newGmv = classifiedEntries match {
          // all keys filtered out, definitely null.
          case Seq() => Literal.create(null, gmv.dataType)
          // no modification, leave the tree as is
          case ces if ces == kvs => gmv
          // some keys trimmed away but there's no way to determine statically what's this
          // expression going to return in runtime, let's construct the trimmed tree.
          case ces =>
            val trimmedKVs = ces.flatMap{
              case (k, v) => Seq(k, v)
            }
            GetMapValue(CreateMap(trimmedKVs), key)
        }
        // this might be further simplified by NullPropagation.
        positiveValue.map( pv => Coalesce(Seq(newGmv, pv)) ).getOrElse(newGmv)
    }
  }
}

