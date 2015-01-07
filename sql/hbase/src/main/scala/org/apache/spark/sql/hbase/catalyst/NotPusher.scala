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

package org.apache.spark.sql.hbase.catalyst

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.rules._

/**
 * Pushes NOT through And/Or
 */
object NotPusher extends Rule[Expression] {
  def apply(pred: Expression): Expression = pred transformDown  {
    case Not(And(left, right)) => Or(Not(left), Not(right))
    case Not(Or(left, right)) => And(Not(left), Not(right))
    case not @ Not(exp) =>
      // This pattern has been caught by optimizer but after NOT pushdown
      // more opportunities may present
      exp match {
        case GreaterThan(l, r) => LessThanOrEqual(l, r)
        case GreaterThanOrEqual(l, r) => LessThan(l, r)
        case LessThan(l, r) => GreaterThanOrEqual(l, r)
        case LessThanOrEqual(l, r) => GreaterThan(l, r)
        case Not(e) => e
        case _ => not
      }
  }
}
