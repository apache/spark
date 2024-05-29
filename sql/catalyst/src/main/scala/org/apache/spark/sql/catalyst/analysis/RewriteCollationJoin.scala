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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.ArrayImplicits.SparkArrayOps

object RewriteCollationJoin extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case j @ Join(_, _, _, Some(condition), _) =>
      val newCondition = condition transform {
        case e @ Equality(l: AttributeReference, r: AttributeReference) =>
          e.withNewChildren(Seq(processExpression(l, l.dataType), processExpression(r, r.dataType)))
      }
      if (!newCondition.fastEquals(condition)) {
        j.copy(condition = Some(newCondition))
      } else {
        j
      }
  }

  private def processExpression(expr: Expression, dt: DataType): Expression = {
    dt match {
      case st: StringType
        if !CollationFactory.fetchCollation(st.collationId).supportsBinaryEquality =>
          CollationKey(expr)

      case StructType(fields) =>
        processStruct(expr, fields)

      case ArrayType(et, containsNull) =>
        processArray(expr, et, containsNull)

      case _ =>
        expr
    }
  }

  private def processStruct(str: Expression, fields: Array[StructField]): Expression = {
    val struct = CreateNamedStruct(fields.zipWithIndex.flatMap {
      case (f, i) =>
        Seq(Literal(f.name),
          processExpression(GetStructField(str, i, Some(f.name)), f.dataType))
    }.toImmutableArraySeq)
    if (struct.valExprs.forall(_.isInstanceOf[GetStructField])) {
      str
    } else if (str.nullable) {
      If(IsNull(str), Literal(null, struct.dataType), struct)
    } else {
      struct
    }
  }

  private def processArray(arr: Expression, et: DataType, containsNull: Boolean): Expression = {
    val param: NamedExpression = NamedLambdaVariable("a", et, containsNull)
    val funcBody: Expression = processExpression(param, et)
    if (funcBody.fastEquals(param)) {
      arr
    } else {
      ArrayTransform(arr, LambdaFunction(funcBody, Seq(param)))
    }
  }

}
