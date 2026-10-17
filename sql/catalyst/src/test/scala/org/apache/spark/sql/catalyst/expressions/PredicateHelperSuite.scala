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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.BooleanType

class PredicateHelperSuite extends SparkFunSuite with PredicateHelper {

  test("buildBalancedPredicate builds a balanced (log-depth) tree") {
    val a = AttributeReference("a", BooleanType)()
    val b = AttributeReference("b", BooleanType)()
    val c = AttributeReference("c", BooleanType)()
    val d = AttributeReference("d", BooleanType)()
    assert(buildBalancedPredicate(Seq(a), And) == a)
    assert(buildBalancedPredicate(Seq(a, b), And) == And(a, b))
    assert(buildBalancedPredicate(Seq(a, b, c, d), And) == And(And(a, b), And(c, d)))
    assert(buildBalancedPredicate(Seq(a, b, c, d), Or) == Or(Or(a, b), Or(c, d)))
  }

  test("buildBalancedPredicateOption returns None for empty input, balanced tree otherwise") {
    val a = AttributeReference("a", BooleanType)()
    val b = AttributeReference("b", BooleanType)()
    val c = AttributeReference("c", BooleanType)()
    val d = AttributeReference("d", BooleanType)()
    assert(buildBalancedPredicateOption(Nil, And).isEmpty)
    assert(buildBalancedPredicateOption(Seq(a), And).contains(a))
    assert(buildBalancedPredicateOption(Seq(a, b, c, d), And).contains(And(And(a, b), And(c, d))))
  }
}
