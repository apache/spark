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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, CollationKey, Equality}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.types.StringType

object RewriteCollationJoin extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case j @ Join(_, _, _, Some(condition), _) =>
      val newCondition = condition transform {
        case e @ Equality(l: AttributeReference, r: AttributeReference) =>
          (l.dataType, r.dataType) match {
            case (st: StringType, _: StringType)
              if !CollationFactory.fetchCollation(st.collationId).supportsBinaryEquality =>
                e.withNewChildren(Seq(CollationKey(l), CollationKey(r)))
            case _ =>
              e
          }
      }
      if (!newCondition.fastEquals(condition)) {
        j.copy(condition = Some(newCondition))
      } else {
        j
      }
  }
}
