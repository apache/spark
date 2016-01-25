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

package org.apache.spark.sql.catalyst.plans

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._

class ConstraintPropagationSuite extends SparkFunSuite {

  private def resolveColumn(tr: LocalRelation, columnName: String): Expression =
    tr.analyze.resolveQuoted(columnName, caseInsensitiveResolution).get

  test("propagating constraints in filter") {
    val tr = LocalRelation('a.int, 'b.string, 'c.int)
    assert(tr.analyze.constraints.isEmpty)
    assert(tr.select('a.attr).analyze.constraints.isEmpty)
    assert(tr.where('a.attr > 10).analyze.constraints == Set(resolveColumn(tr, "a") > 10))
    assert(tr.where('a.attr > 10).select('c.attr, 'b.attr).analyze.constraints.isEmpty)
    assert(tr.where('a.attr > 10).select('c.attr, 'a.attr).where('c.attr < 100)
      .analyze.constraints == Set(resolveColumn(tr, "a") > 10, resolveColumn(tr, "c") < 100))
  }

  test("propagating constraints in union") {
    val tr1 = LocalRelation('a.int, 'b.string, 'c.int)
    val tr2 = LocalRelation('a.int, 'b.string, 'c.int)
    assert(tr1.analyze.constraints.isEmpty && tr2.analyze.constraints.isEmpty)
    assert(tr1.where('a.attr > 10).unionAll(tr2.where('a.attr > 10))
      .analyze.constraints == Set(resolveColumn(tr1, "a") > 10))
  }
}
