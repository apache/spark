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

package org.apache.spark.sql.catalyst.planning

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.TestRelations
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.DoubleType

class ScanOperationSuite extends SparkFunSuite {
  private val relation = TestRelations.testRelation2
  private val colA = relation.output(0)
  private val colB = relation.output(1)
  private val aliasR = Alias(Rand(1), "r")()
  private val aliasId = Alias(MonotonicallyIncreasingID(), "id")()
  private val colR = AttributeReference("r", DoubleType)(aliasR.exprId, aliasR.qualifier)

  test("Project with a non-deterministic field and a deterministic child Filter") {
    val project1 = Project(Seq(colB, aliasR), Filter(EqualTo(colA, Literal(1)), relation))
    project1 match {
      case ScanOperation(projects, filters, _: LocalRelation) =>
        assert(projects.size === 2)
        assert(projects(0) === colB)
        assert(projects(1) === aliasR)
        assert(filters.size === 1)
      case _ => assert(false)
    }
  }

  test("Project with all deterministic fields but a non-deterministic child Filter") {
    val project2 = Project(Seq(colA, colB), Filter(EqualTo(aliasR, Literal(1)), relation))
    project2 match {
      case ScanOperation(projects, filters, _: LocalRelation) =>
        assert(projects.size === 2)
        assert(projects(0) === colA)
        assert(projects(1) === colB)
        assert(filters.size === 1)
      case _ => assert(false)
    }
  }

  test("Project which has the same non-deterministic expression with its child Project") {
    val project3 = Project(Seq(colA, colR), Project(Seq(colA, aliasR), relation))
    assert(ScanOperation.unapply(project3).isEmpty)
  }

  test("Project which has different non-deterministic expressions with its child Project") {
    val project4 = Project(Seq(colA, aliasId), Project(Seq(colA, aliasR), relation))
    project4 match {
      case ScanOperation(projects, _, _: LocalRelation) =>
        assert(projects.size === 2)
        assert(projects(0) === colA)
        assert(projects(1) === aliasId)
      case _ => assert(false)
    }
  }

  test("Filter with non-deterministic Project") {
    val filter1 = Filter(EqualTo(colA, Literal(1)), Project(Seq(colA, aliasR), relation))
    assert(ScanOperation.unapply(filter1).isEmpty)
  }

  test("Non-deterministic Filter with deterministic Project") {
    val filter3 = Filter(EqualTo(MonotonicallyIncreasingID(), Literal(1)),
      Project(Seq(colA, colB), relation))
    filter3 match {
      case ScanOperation(projects, filters, _: LocalRelation) =>
        assert(projects.size === 2)
        assert(projects(0) === colA)
        assert(projects(1) === colB)
        assert(filters.size === 1)
      case _ => assert(false)
    }
  }


  test("Deterministic filter which has a non-deterministic child Filter") {
    val filter4 = Filter(EqualTo(colA, Literal(1)), Filter(EqualTo(aliasR, Literal(1)), relation))
    assert(ScanOperation.unapply(filter4).isEmpty)
  }
}
