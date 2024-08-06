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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{Alias, ExpressionInfo}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog.{CatalogManager, FunctionCatalog, Identifier}
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.V1Function
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class LookupFunctionsSuite extends PlanTest {

  test("SPARK-19737: detect undefined functions without triggering relation resolution") {
    import org.apache.spark.sql.catalyst.dsl.plans._

    Seq(true, false) foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        val externalCatalog = new CustomInMemoryCatalog
        externalCatalog.createDatabase(
          CatalogDatabase("default", "", new URI("loc1"), Map.empty),
          ignoreIfExists = false)
        externalCatalog.createDatabase(
          CatalogDatabase("db1", "", new URI("loc2"), Map.empty),
          ignoreIfExists = false)
        val catalog = new SessionCatalog(externalCatalog, new SimpleFunctionRegistry)
        val catalogManager = new CatalogManager(new CustomV2SessionCatalog(catalog), catalog)
        catalogManager.setCurrentNamespace(Array("db1"))
        try {
          val analyzer = new Analyzer(catalogManager)

          // The analyzer should report the undefined function
          // rather than the undefined table first.
          val cause = intercept[AnalysisException] {
            analyzer.execute(
              UnresolvedRelation(TableIdentifier("undefined_table")).select(
                UnresolvedFunction("undefined_fn", Nil, isDistinct = false)
              )
            )
          }
          checkError(
            exception = cause,
            errorClass = "UNRESOLVED_ROUTINE",
            parameters = Map(
              "routineName" -> "`undefined_fn`",
              "searchPath" -> "[`system`.`builtin`, `system`.`session`, `spark_catalog`.`db1`]"))
        } finally {
          catalog.reset()
        }
      }
    }
  }

  test("SPARK-23486: the getFunction for the Persistent function check") {
    val externalCatalog = new CustomInMemoryCatalog
    val catalog = new SessionCatalog(externalCatalog, FunctionRegistry.builtin.clone())
    val catalogManager = new CatalogManager(new CustomV2SessionCatalog(catalog), catalog)
    val analyzer = {
      catalog.createDatabase(
        CatalogDatabase("default", "", new URI("loc"), Map.empty),
        ignoreIfExists = false)
      new Analyzer(catalogManager)
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
  }

  test("SPARK-23486: the lookupFunction for the Registered function check") {
    val externalCatalog = new InMemoryCatalog
    val customerFunctionReg = new CustomerFunctionRegistry
    val catalog = new SessionCatalog(externalCatalog, customerFunctionReg)
    val catalogManager = new CatalogManager(new CustomV2SessionCatalog(catalog), catalog)
    val analyzer = {
      catalog.createDatabase(
        CatalogDatabase("default", "", new URI("loc"), Map.empty),
        ignoreIfExists = false)
      new Analyzer(catalogManager)
    }

    def table(ref: String): LogicalPlan = UnresolvedRelation(TableIdentifier(ref))
    val unresolvedRegisteredFunc = UnresolvedFunction("max", Seq.empty, false)
    val plan = Project(
      Seq(Alias(unresolvedRegisteredFunc, "call1")(), Alias(unresolvedRegisteredFunc, "call2")()),
      table("TaBlE"))
    analyzer.LookupFunctions.apply(plan)

    assert(customerFunctionReg.getLookupFunctionCalledTimes == 2)
  }
}

class CustomerFunctionRegistry extends SimpleFunctionRegistry {
  private var lookupFunctionCalledTimes: Int = 0;

  override def lookupFunction(name: FunctionIdentifier): Option[ExpressionInfo] = {
    lookupFunctionCalledTimes += 1
    if (name.funcName == "undefined_fn") return None
    Some(new ExpressionInfo("fake", "name"))
  }

  def getLookupFunctionCalledTimes: Int = lookupFunctionCalledTimes
}

class CustomInMemoryCatalog extends InMemoryCatalog {
  private var functionExistsCalledTimes: Int = 0

  override def functionExists(db: String, funcName: String): Boolean = synchronized {
    functionExistsCalledTimes += 1
    funcName != "undefined_fn"
  }

  def getFunctionExistsCalledTimes: Int = functionExistsCalledTimes
}

class CustomV2SessionCatalog(v1Catalog: SessionCatalog) extends FunctionCatalog {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}
  override def name(): String = CatalogManager.SESSION_CATALOG_NAME
  override def listFunctions(namespace: Array[String]): Array[Identifier] = {
    throw new UnsupportedOperationException()
  }

  override def loadFunction(ident: Identifier): UnboundFunction = {
    V1Function(v1Catalog.lookupPersistentFunction(ident.asFunctionIdentifier))
  }
  override def functionExists(ident: Identifier): Boolean = {
    v1Catalog.isPersistentFunction(ident.asFunctionIdentifier)
  }
}
