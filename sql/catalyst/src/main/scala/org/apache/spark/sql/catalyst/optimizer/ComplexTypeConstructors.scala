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

import org.apache.spark.sql.catalyst.expressions.{Cast, CreateArray, CreateMap, CreateNamedStructLike, Expression, GetArrayItem, GetArrayStructFields, GetMapValue, GetStructField, IntegerLiteral, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
* push down operations into [[CreateNamedStructLike]].
*/
object SimplifyCreateStructOps extends Rule[LogicalPlan]{
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformExpressionsUp{
      // push down field extraction
      case GetStructField( createNamedStructLike : CreateNamedStructLike, ordinal, _ ) =>
        val value = createNamedStructLike.valExprs(ordinal)
        value
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
        def getStructField( elem : Expression ) = {
          GetStructField( elem, ordinal, Some(field.name) )
        }
        CreateArray( elems.map(getStructField) )
      // push down item selection.
      case ga @ GetArrayItem( CreateArray(elems), IntegerLiteral( idx ) ) =>
        if ( idx >= 0 && idx < elems.size ) {
          elems(idx)
        } else {
          Cast( Literal( null), ga.dataType )
        }
    }
  }
}

/**
* push down operations into [[CreateMap]].
*/
object SimplifyCreateMapOps extends Rule[LogicalPlan]{
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformExpressionsUp{
      // attempt to unfold 'constant' key extraction,
      // this enables other optimizations to take place.
      case gmv @ GetMapValue(cm @ CreateMap(elems), key @ Literal(v, t)) =>
        if ( cm.keys.contains( key ) ) {
          val idx = cm.keys.indexOf(key)
          cm.values(idx)
        } else {
          Cast( Literal( null ), gmv.dataType)
        }
    }
  }
}

