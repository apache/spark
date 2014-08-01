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

import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.types.IntegerType

class AnalysisSuite extends FunSuite with BeforeAndAfter {
  val caseSensitiveCatalog = new SimpleCatalog(true)
  val caseInsensitiveCatalog = new SimpleCatalog(false)
  val caseSensitiveAnalyze =
    new Analyzer(caseSensitiveCatalog, EmptyFunctionRegistry, caseSensitive = true)
  val caseInsensitiveAnalyze =
    new Analyzer(caseInsensitiveCatalog, EmptyFunctionRegistry, caseSensitive = false)

  val testRelation = LocalRelation(AttributeReference("a", IntegerType, nullable = true)())

  before {
    caseSensitiveCatalog.registerTable(None, "TaBlE", testRelation)
    caseInsensitiveCatalog.registerTable(None, "TaBlE", testRelation)
  }

  test("analyze project") {
    assert(
      caseSensitiveAnalyze(Project(Seq(UnresolvedAttribute("a")), testRelation)) ===
        Project(testRelation.output, testRelation))

    assert(
      caseSensitiveAnalyze(
        Project(Seq(UnresolvedAttribute("TbL.a")),
          UnresolvedRelation(None, "TaBlE", Some("TbL")))) ===
        Project(testRelation.output, testRelation))

    val e = intercept[TreeNodeException[_]] {
      caseSensitiveAnalyze(
        Project(Seq(UnresolvedAttribute("tBl.a")),
          UnresolvedRelation(None, "TaBlE", Some("TbL"))))
    }
    assert(e.getMessage().toLowerCase.contains("unresolved"))

    assert(
      caseInsensitiveAnalyze(
        Project(Seq(UnresolvedAttribute("TbL.a")),
          UnresolvedRelation(None, "TaBlE", Some("TbL")))) ===
        Project(testRelation.output, testRelation))

    assert(
      caseInsensitiveAnalyze(
        Project(Seq(UnresolvedAttribute("tBl.a")),
          UnresolvedRelation(None, "TaBlE", Some("TbL")))) ===
        Project(testRelation.output, testRelation))
  }

  test("resolve relations") {
    val e = intercept[RuntimeException] {
      caseSensitiveAnalyze(UnresolvedRelation(None, "tAbLe", None))
    }
    assert(e.getMessage === "Table Not Found: tAbLe")

    assert(
      caseSensitiveAnalyze(UnresolvedRelation(None, "TaBlE", None)) ===
        testRelation)

    assert(
      caseInsensitiveAnalyze(UnresolvedRelation(None, "tAbLe", None)) ===
        testRelation)

    assert(
      caseInsensitiveAnalyze(UnresolvedRelation(None, "TaBlE", None)) ===
        testRelation)
  }

  test("throw errors for unresolved attributes during analysis") {
    val e = intercept[TreeNodeException[_]] {
      caseSensitiveAnalyze(Project(Seq(UnresolvedAttribute("abcd")), testRelation))
    }
    assert(e.getMessage().toLowerCase.contains("unresolved"))
  }
}
