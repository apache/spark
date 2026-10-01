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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{EmptyFunctionRegistry, EmptyTableFunctionRegistry, FakeV2SessionCatalog}
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{Coalesce, CurrentCatalog, CurrentDatabase, Literal}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.DefaultCatalogManager
import org.apache.spark.sql.types.StringType

class ReplaceCurrentLikeSuite extends SparkFunSuite {
  private val catalogManager = new DefaultCatalogManager(
    FakeV2SessionCatalog,
    new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry, EmptyTableFunctionRegistry))
  private val rule = ReplaceCurrentLike(catalogManager)

  test("applyForExpression replaces current catalog/database with literals") {
    assert(rule.applyForExpression(CurrentCatalog()) ==
      Literal.create(catalogManager.currentCatalog.name(), StringType))
    assert(rule.applyForExpression(CurrentDatabase()) ==
      Literal.create(catalogManager.currentNamespace.quoted, StringType))
  }

  test("applyForExpression rewrites current-like nested in a larger expression") {
    val expr = Coalesce(Seq(CurrentCatalog(), Literal("x")))
    val expected = Coalesce(Seq(
      Literal.create(catalogManager.currentCatalog.name(), StringType), Literal("x")))
    assert(rule.applyForExpression(expr) == expected)
  }

  test("applyForExpression leaves expressions without current-like unchanged") {
    assert(rule.applyForExpression(Literal("x")) == Literal("x"))
  }
}
