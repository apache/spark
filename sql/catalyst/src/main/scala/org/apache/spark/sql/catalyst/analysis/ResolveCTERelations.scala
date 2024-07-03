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

import org.apache.spark.sql.catalyst.plans.logical.{CTERelationRef, LogicalPlan, SubqueryAlias, UnresolvedWithCTERelations}
import org.apache.spark.sql.catalyst.rules.Rule

/*
 Resolve CTE RELATIONS
 */
object ResolveCTERelations extends Rule[LogicalPlan]{

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperatorsDown {
      case u @ UnresolvedWithCTERelations(child, alwaysInline, cteRelations) =>
        val unresolvedRelation = child.asInstanceOf[UnresolvedRelation]
          cteRelations.find(r => plan.conf.resolver(r._1,
            unresolvedRelation.multipartIdentifier.head)).map { case (_, d) =>
            if (alwaysInline) {
              d.child
            } else {
              // Add a `SubqueryAlias` for hint-resolving rules to match relation names.
              SubqueryAlias(unresolvedRelation.multipartIdentifier.head,
                CTERelationRef(d.id, d.resolved, d.output, d.isStreaming))
            }
          }.getOrElse(child)
    }
  }
}
