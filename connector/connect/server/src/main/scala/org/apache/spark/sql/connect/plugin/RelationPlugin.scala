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

package org.apache.spark.sql.connect.plugin

import com.google.protobuf

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connect.planner.SparkConnectPlanner

/**
 * Behavior trait for supporting extension mechanisms for the Spark Connect planner.
 *
 * Classes implementing the trait must be trivially constructable and should not rely on internal
 * state. Every registered extension will be passed the Any instance. If the plugin supports
 * handling this type it is responsible of constructing the logical catalyst plan from this object
 * and if necessary traverse it's children.
 */
trait RelationPlugin {
  def transform(relation: protobuf.Any, planner: SparkConnectPlanner): Option[LogicalPlan]
}
