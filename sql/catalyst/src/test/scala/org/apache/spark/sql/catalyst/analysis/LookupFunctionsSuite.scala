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

import java.net.URI

import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.internal.SQLConf

class LookupFunctionsSuite extends PlanTest {

  test("SPARK-23486: the functionExists for the Persistent function check") {
    val externalCatalog = new CustomInMemoryCatalog
    val conf = new SQLConf()
    val catalog = new SessionCatalog(externalCatalog, FunctionRegistry.builtin, conf)
    val analyzer = {
      catalog.createDatabase(
        CatalogDatabase("default", "", new URI("loc"), Map.empty),
        ignoreIfExists = false)
      new Analyzer(catalog, conf)
    }

    def table(ref: String): LogicalPlan = UnresolvedRelation(TableIdentifier(ref))
    val unresolvedPersistentFunc = UnresolvedFunction("func", Seq.empty, false)
    val unresolvedRegisteredFunc = UnresolvedFunction("max", Seq.empty, false)
    val plan = Project(
      Seq(Alias(unresolvedPersistentFunc, "call1")(), Alias(unresolvedPersistentFunc, "call2")(),
        Alias(unresolvedPersistentFunc, "call3")(), Alias(unresolvedRegisteredFunc, "call4")(),
        Alias(unresolvedRegisteredFunc, "call5")()),
      table("TaBlE"))
    analyzer.LookupFunctions.apply(plan)

    assert(externalCatalog.getFunctionExistsCalledTimes == 1)
    assert(analyzer.LookupFunctions.normalizeFuncName
      (unresolvedPersistentFunc.name).database == Some("default"))
  }

  test("SPARK-23486: the functionExists for the Registered function check") {
    val externalCatalog = new InMemoryCatalog
    val conf = new SQLConf()
    val customerFunctionReg = new CustomerFunctionRegistry
    val catalog = new SessionCatalog(externalCatalog, customerFunctionReg, conf)
    val analyzer = {
      catalog.createDatabase(
        CatalogDatabase("default", "", new URI("loc"), Map.empty),
        ignoreIfExists = false)
      new Analyzer(catalog, conf)
    }

    def table(ref: String): LogicalPlan = UnresolvedRelation(TableIdentifier(ref))
    val unresolvedRegisteredFunc = UnresolvedFunction("max", Seq.empty, false)
    val plan = Project(
      Seq(Alias(unresolvedRegisteredFunc, "call1")(), Alias(unresolvedRegisteredFunc, "call2")()),
      table("TaBlE"))
    analyzer.LookupFunctions.apply(plan)

    assert(customerFunctionReg.getIsRegisteredFunctionCalledTimes == 2)
    assert(analyzer.LookupFunctions.normalizeFuncName
      (unresolvedRegisteredFunc.name).database == Some("default"))
  }
}

class CustomerFunctionRegistry extends SimpleFunctionRegistry {

  private var isRegisteredFunctionCalledTimes: Int = 0;

  override def functionExists(funcN: FunctionIdentifier): Boolean = synchronized {
    isRegisteredFunctionCalledTimes = isRegisteredFunctionCalledTimes + 1
    true
  }

  def getIsRegisteredFunctionCalledTimes: Int = isRegisteredFunctionCalledTimes
}

class CustomInMemoryCatalog extends InMemoryCatalog {

  private var functionExistsCalledTimes: Int = 0

  override def functionExists(db: String, funcName: String): Boolean = synchronized {
    functionExistsCalledTimes = functionExistsCalledTimes + 1
    true
  }

  def getFunctionExistsCalledTimes: Int = functionExistsCalledTimes
}
