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

package org.apache.spark.sql
package examples

import catalyst.analysis.UnresolvedRelation
import catalyst.plans.Inner
import catalyst.plans.logical._

/* Implicit Conversions */
import catalyst.dsl._

import TestSqlContext._ // For .toRdd execution using locally running sql instance.

object ViewsExample {
  def main(args: Array[String]): Unit = {
    // Create a list of named views that can be substituted into logical plans.
    // In this example the views read from local, in-memory relations with schema
    // (a INT, b STRING) and (c INT, d STRING) respectively.
    //
    // loadData returns a copy of that relation with the specified tuples appended to the Rdd.
    // The .select uses the DSL to add a projection on top of the relation that returns only
    // the column "a".
    val views = Map(
      "view1" -> LocalRelation('a.int, 'b.string).loadData(("a", 1) :: ("b", 2) :: Nil).select('a),
      "view2" -> LocalRelation('c.int, 'd.string).loadData(("c", 1) :: ("d", 2) :: Nil)
    )

    // Construct a plan that has UnresolvedRelations in it using the DSL.
    val unresolvedPlan =
      UnresolvedRelation(None, "view1")
        .join(UnresolvedRelation(None, "view2"), Inner, Some('a === 'c))
        .where('c < 1)
        .select('a, 'c)
    println(s"Unresolved Plan:\n$unresolvedPlan")

    // Replace UnresolvedRelations with logical plans from the views map.
    val withRelations = unresolvedPlan transform {
      case UnresolvedRelation(_, name, _) => views(name)
    }

    println(s"With relations:\n$withRelations ")
    println(s"Analyzed:\n${withRelations.analyze}") // Print with all references resolved.
    println(s"Answer: ${withRelations.toRdd.collect().toSeq}")
  }
}
