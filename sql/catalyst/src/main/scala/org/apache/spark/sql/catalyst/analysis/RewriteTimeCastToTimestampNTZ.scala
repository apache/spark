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

import org.apache.spark.sql.catalyst.expressions.{Cast, CurrentDate, MakeTimestampNTZ}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{TimestampNTZType, TimeType}

/**
 * Rewrites a cast from [[TimeType]] to [[TimestampNTZType]] into a [[MakeTimestampNTZ]]
 * expression.
 *
 * The conversion from TIME to TIMESTAMP_NTZ requires a date component, which TIME itself does not
 * provide. This rule injects [[CurrentDate]] as the implicit date part, effectively treating the
 * TIME value as a time of day on the current date. This rewrite ensures that all such casts
 * within a query use a consistent date, as required by the [[ComputeCurrentTime]] rule which
 * replaces [[CurrentDate]] with a fixed value during analysis.
 *
 * For example, the following SQL:
 * {{{
 *   SELECT CAST(make_time(12, 30, 0) AS TIMESTAMP_NTZ)
 * }}}
 * will be rewritten to:
 * {{{
 *   SELECT make_timestamp_ntz(current_date, make_time(12, 30, 0))
 * }}}
 *
 * This transformation must happen during resolution, before expression evaluation or
 * optimization.
 */
object RewriteTimeCastToTimestampNTZ extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case c @ Cast(child, TimestampNTZType, _, _) if child.dataType.isInstanceOf[TimeType] =>
      MakeTimestampNTZ(CurrentDate(), child)
  }
}
