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

/**
 * A trait that exposes 'transform' which will be called by the `TransformAfterAnalysis` rule
 * after all other analysis rules are called. One scenario to use this transformation is to
 * remove any children of a logical plan so that they are not optimized; this is useful for
 * commands that create a view or a cache because they need to work with analyzed plans.
 */
trait TransformationAfterAnalysis {
  def transform: LogicalPlan
}
