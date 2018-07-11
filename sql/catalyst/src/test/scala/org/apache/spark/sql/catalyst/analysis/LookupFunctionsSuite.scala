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

  test("SPARK-23486: LookupFunctions should not check the same function name more than once") {
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
    val unresolvedFunc = UnresolvedFunction("func", Seq.empty, false)
    val plan = Project(
      Seq(Alias(unresolvedFunc, "call1")(), Alias(unresolvedFunc, "call2")(),
        Alias(unresolvedFunc, "call1")()),
      table("TaBlE"))
    analyzer.LookupFunctions.apply(plan)
    assert(externalCatalog.getFunctionExistsCalledTimes == 1)

    assert(analyzer.LookupFunctions.normalizeFuncName
      (unresolvedFunc.name).database == Some("default"))
    assert(catalog.isRegisteredFunction(unresolvedFunc.name) == false)
    assert(catalog.isRegisteredFunction(FunctionIdentifier("max")) == true)

  }
}

class CustomInMemoryCatalog extends InMemoryCatalog {

  private var functionExistsCalledTimes: Int = 0

  override def functionExists(db: String, funcName: String): Boolean = synchronized {
    functionExistsCalledTimes = functionExistsCalledTimes + 1
    true
  }

  def getFunctionExistsCalledTimes: Int = functionExistsCalledTimes

}
