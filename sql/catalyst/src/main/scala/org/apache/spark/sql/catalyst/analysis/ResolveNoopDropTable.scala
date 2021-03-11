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

import org.apache.spark.sql.catalyst.plans.logical.{DropTable, LogicalPlan, NoopDropTable}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * A rule for handling [[DropTable]] logical plan when the table or temp view is not resolved.
 * If "ifExists" flag is set to true, the plan is resolved to [[NoopDropTable]],
 * which is a no-op command.
 */
object ResolveNoopDropTable extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case DropTable(u: UnresolvedTableOrView, ifExists, _) if ifExists =>
      NoopDropTable(u.multipartIdentifier)
  }
}
