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

import org.apache.spark.sql.catalyst.expressions.{Cast, Coalesce, CreateArray, CreateMap, CreateNamedStructLike, Expression, GetArrayItem, GetArrayStructFields, GetMapValue, GetStructField, IntegerLiteral, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
* push down operations into [[CreateNamedStructLike]].
*/
object SimplifyCreateStructOps extends Rule[LogicalPlan]{
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformExpressionsUp{
      // push down field extraction
      case GetStructField(createNamedStructLike : CreateNamedStructLike, ordinal, _) =>
        createNamedStructLike.valExprs(ordinal)
    }
  }
}

/**
* push down operations into [[CreateArray]].
*/
object SimplifyCreateArrayOps extends Rule[LogicalPlan]{
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformExpressionsUp{
      // push down field selection (array of structs)
      case GetArrayStructFields(CreateArray(elems), field, ordinal, numFields, containsNull) =>
        def getStructField(elem : Expression) = {
          GetStructField(elem, ordinal, Some(field.name))
        }
        CreateArray(elems.map(getStructField))
      // push down item selection.
      case ga @ GetArrayItem(CreateArray(elems), IntegerLiteral(idx)) =>
        if (idx >= 0 && idx < elems.size) {
          elems(idx)
        } else {
          Cast(Literal(null), ga.dataType)
        }
    }
  }
}

/**
* push down operations into [[CreateMap]].
*/
object SimplifyCreateMapOps extends Rule[LogicalPlan]{
  object ComparisonResult extends Enumeration {
    val PositiveMatch = Value
    val NegativeMatch = Value
    val UnDetermined = Value
  }

  def compareKeys(k1 : Expression, k2 : Expression) : ComparisonResult.Value = {
    (k1, k2) match {
      case (x, y) if x.semanticEquals(y) => ComparisonResult.PositiveMatch
      // make surethis is null safe, especially when datatypes differ
      // is this even possible?
      case (_ : Literal, _ : Literal) => ComparisonResult.NegativeMatch
      case _ => ComparisonResult.UnDetermined
    }
  }

  case class ClassifiedEntries(undetermined : Seq[Expression],
                               nullable : Boolean,
                               firstPositive : Option[Expression]) {
    def normalize( k : Expression ) : ClassifiedEntries = this match {
      /**
      * when we have undetermined matches that might bproduce a null value,
      * we can't separate a positive match and use [[Coalesce]] to choose the final result.
      * so we 'hide' the positive match as an undetermined match.
      */
      case ClassifiedEntries( u, true, Some(p)) if u.nonEmpty =>
        ClassifiedEntries(u ++ Seq(k, p), true, None)
      case _ => this
    }
  }

  def classifyEntries(mapEntries : Seq[(Expression, Expression)],
                      requestedKey : Expression) : ClassifiedEntries = {
    val res1 = mapEntries.foldLeft(ClassifiedEntries(Seq.empty, nullable = false, None)) {
      case (prev @ ClassifiedEntries(_, _, Some(_)), _) => prev
      case (ClassifiedEntries(prev, nullable, None), (k, v)) =>
        compareKeys(k, requestedKey) match {
          case ComparisonResult.UnDetermined =>
            val vIsNullable = v.nullable
            val nextNullbale = nullable || vIsNullable
            ClassifiedEntries(prev ++ Seq(k, v), nullable = nextNullbale, None)
          case ComparisonResult.NegativeMatch => ClassifiedEntries(prev, nullable, None)
          case ComparisonResult.PositiveMatch => ClassifiedEntries(prev, nullable, Some(v))
        }
    }
    val res = res1.normalize( requestedKey )
    res
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformExpressionsUp {
      // attempt to unfold 'constant' key extraction,
      // this enables other optimizations to take place.
      case gmv @ GetMapValue(cm @ CreateMap(elems), key) =>
        val kvs = cm.keys.zip(cm.values)
        val classifiedEntries = classifyEntries(kvs, key)
        classifiedEntries match {
          case ClassifiedEntries(Seq(), _, None) => Literal.create(null, gmv.dataType)
          case ClassifiedEntries(`elems`, _, None) => gmv
          case ClassifiedEntries(newElems, _, optPos) =>
            val getFromTrimmedMap = GetMapValue(CreateMap(newElems), key)
            optPos.map(pos => Coalesce(Seq(getFromTrimmedMap, pos)))
              .getOrElse(getFromTrimmedMap)
        }
    }
  }
}

