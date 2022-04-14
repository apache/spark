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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.ExpressionSet
import org.apache.spark.sql.internal.SQLConf.PROPAGATE_DISTINCT_KEYS_ENABLED

/**
 * A trait to add distinct attributes to [[LogicalPlan]]. For example:
 * {{{
 *   SELECT a, b, SUM(c) FROM Tab1 GROUP BY a, b
 *   // returns a, b
 * }}}
 */
trait LogicalPlanDistinctKeys { self: LogicalPlan =>
  lazy val distinctKeys: Set[ExpressionSet] = {
    if (conf.getConf(PROPAGATE_DISTINCT_KEYS_ENABLED)) {
      val keys = DistinctKeyVisitor.visit(self)
      require(keys.forall(_.nonEmpty))
      keys
    } else {
      Set.empty
    }
  }
}
