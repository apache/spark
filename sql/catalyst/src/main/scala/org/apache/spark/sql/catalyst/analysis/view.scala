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

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, View}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * This file defines view types and analysis rules related to views.
 */

/**
 * This rule removes [[View]] operators from the plan. The operator is respected till the end of
 * analysis stage because we want to see which part of an analyzed logical plan is generated from a
 * view.
 */
object EliminateView extends Rule[LogicalPlan] with CastSupport {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case View(_, _, child) => child
  }
}

/**
 * ViewType is used to specify the expected view type when we want to create or replace a view in
 * [[CreateViewStatement]].
 */
sealed trait ViewType {
  override def toString: String = getClass.getSimpleName.stripSuffix("$")
}

/**
 * LocalTempView means session-scoped local temporary views. Its lifetime is the lifetime of the
 * session that created it, i.e. it will be automatically dropped when the session terminates. It's
 * not tied to any databases, i.e. we can't use `db1.view1` to reference a local temporary view.
 */
object LocalTempView extends ViewType

/**
 * GlobalTempView means cross-session global temporary views. Its lifetime is the lifetime of the
 * Spark application, i.e. it will be automatically dropped when the application terminates. It's
 * tied to a system preserved database `global_temp`, and we must use the qualified name to refer a
 * global temp view, e.g. SELECT * FROM global_temp.view1.
 */
object GlobalTempView extends ViewType

/**
 * PersistedView means cross-session persisted views. Persisted views stay until they are
 * explicitly dropped by user command. It's always tied to a database, default to the current
 * database if not specified.
 *
 * Note that, Existing persisted view with the same name are not visible to the current session
 * while the local temporary view exists, unless the view name is qualified by database.
 */
object PersistedView extends ViewType
