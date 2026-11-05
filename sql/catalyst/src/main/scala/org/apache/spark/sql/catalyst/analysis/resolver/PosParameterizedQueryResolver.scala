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

package org.apache.spark.sql.catalyst.analysis.resolver

import org.apache.spark.sql.catalyst.analysis.PosParameterizedQuery
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SupervisingCommand}

/**
 * Resolver class that resolves [[PosParameterizedQuery]] operators.
 */
class PosParameterizedQueryResolver(operatorResolver: Resolver)
    extends ResolvesParameterizedQuery {

  /**
   * Resolves a [[PosParameterizedQuery]] operator. If the child is a [[SupervisingCommand]], the
   * [[PosParameterizedQuery]] is pushed below the command so it gets resolved during the deferred
   * analysis pass. Otherwise, the child is resolved directly.
   */
  def resolve(posParameterizedQuery: PosParameterizedQuery): LogicalPlan = {
    posParameterizedQuery.child match {
      case command: SupervisingCommand =>
        pushParameterizedQueryBelowCommand(posParameterizedQuery, command)
      case _ =>
        operatorResolver.resolve(posParameterizedQuery.child)
    }
  }
}
