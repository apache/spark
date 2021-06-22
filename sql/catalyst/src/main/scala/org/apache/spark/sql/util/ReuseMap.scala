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

package org.apache.spark.sql.util

import scala.collection.mutable.{ArrayBuffer, Map}

import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.types.StructType

/**
 * Map of canonicalized plans that can be used to find reuse possibilities.
 *
 * To avoid costly canonicalization of a plan:
 * - we use its schema first to check if it can be replaced to a reused one at all
 * - we insert it into the map of canonicalized plans only when at least 2 have the same schema
 *
 * @tparam T the type of the node we want to reuse
 * @tparam T2 the type of the canonicalized node
 */
class ReuseMap[T <: T2, T2 <: QueryPlan[T2]] {
  private val map = Map[StructType, ArrayBuffer[T]]()

  /**
   * Find a matching plan with the same canonicalized form in the map or add the new plan to the
   * map otherwise.
   *
   * @param plan the input plan
   * @return the matching plan or the input plan
   */
  private def lookupOrElseAdd(plan: T): T = {
    val sameSchema = map.getOrElseUpdate(plan.schema, ArrayBuffer())
    val samePlan = sameSchema.find(plan.sameResult)
    if (samePlan.isDefined) {
      samePlan.get
    } else {
      sameSchema += plan
      plan
    }
  }

  /**
   * Find a matching plan with the same canonicalized form in the map and apply `f` on it or add
   * the new plan to the map otherwise.
   *
   * @param plan the input plan
   * @param f the function to apply
   * @tparam T2 the type of the reuse node
   * @return the matching plan with `f` applied or the input plan
   */
  def reuseOrElseAdd[T2 >: T](plan: T, f: T => T2): T2 = {
    val found = lookupOrElseAdd(plan)
    if (found eq plan) {
      plan
    } else {
      f(found)
    }
  }
}
