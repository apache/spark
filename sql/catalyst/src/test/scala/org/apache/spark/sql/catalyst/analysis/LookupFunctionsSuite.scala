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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{Alias, ExpressionInfo}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogPlugin, DefaultCatalogManager, FunctionCatalog, Identifier}
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
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
        val catalogManager = new DefaultCatalogManager(new CustomV2SessionCatalog(catalog), catalog)
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
            condition = "UNRESOLVED_ROUTINE",
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
    val catalogManager = new DefaultCatalogManager(new CustomV2SessionCatalog(catalog), catalog)
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
    val catalogManager = new DefaultCatalogManager(new CustomV2SessionCatalog(catalog), catalog)
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

  test("resolveFunctionName looks up scalar metadata before table metadata") {
    val scalarRegistry = FunctionRegistry.builtin.clone()
    val tableRegistry = TableFunctionRegistry.builtin.clone()
    val catalog = new SessionCatalog(new InMemoryCatalog, scalarRegistry, tableRegistry)
    val catalogManager = new DefaultCatalogManager(new CustomV2SessionCatalog(catalog), catalog)
    val relationResolution = new RelationResolution(catalogManager, RelationCache.empty) {
      override def expandIdentifier(nameParts: Seq[String]): Seq[String] = {
        fail("Built-in metadata lookup must not expand identifiers")
      }
    }
    var tableLookups = 0
    val functionResolution = new FunctionResolution(catalogManager, relationResolution) {
      override def lookupBuiltinOrTempTableFunction(
          nameParts: Seq[String]): Option[ExpressionInfo] = {
        tableLookups += 1
        super.lookupBuiltinOrTempTableFunction(nameParts)
      }
    }

    val scalarInfo = scalarRegistry.lookupFunction(
      FunctionRegistry.builtinFunctionIdentifier("explode")).get
    val tableInfo = tableRegistry.lookupFunction(
      FunctionRegistry.builtinFunctionIdentifier("range")).get
    case class MetadataCase(
        nameParts: Seq[String],
        info: ExpressionInfo,
        expectedTableLookups: Int)
    val cases = Seq(
      MetadataCase(Seq("ExPlOdE"), scalarInfo, 0),
      MetadataCase(Seq("RaNgE"), tableInfo, 1),
      MetadataCase(Seq("system", "builtin", "explode"), scalarInfo, 1))
    cases.foreach { testCase =>
      val resolved = functionResolution.resolveFunctionName(
        UnresolvedFunctionName(testCase.nameParts, "DESCRIBE FUNCTION"))
        .asInstanceOf[ResolvedNonPersistentFunc]
      assert(resolved.name == testCase.nameParts.head)
      assert(resolved.func.asInstanceOf[V1Function].info eq testCase.info)
      assert(tableLookups == testCase.expectedTableLookups)
    }
  }

  test("resolveFunctionName expands catalog fallbacks and qualifies missing names") {
    val catalog = new SessionCatalog(new InMemoryCatalog, new SimpleFunctionRegistry)
    val function = V1Function.metadataOnly(new ExpressionInfo("fake", "persistent_fn"))
    val catalogError = new IllegalStateException("function catalog failure")
    val loadedIdentifiers = ArrayBuffer.empty[Identifier]
    val v2Catalog = new CustomV2SessionCatalog(catalog) {
      override def name(): String = "loaded_catalog"
      override def loadFunction(ident: Identifier): UnboundFunction = {
        loadedIdentifiers += ident
        ident.name() match {
          case "persistent_fn" => function
          case "broken_fn" => throw catalogError
          case _ => throw new NoSuchFunctionException(ident)
        }
      }
    }
    val catalogManager = new DefaultCatalogManager(v2Catalog, catalog)
    val expandedNames = ArrayBuffer.empty[Seq[String]]
    val relationResolution = new RelationResolution(catalogManager, RelationCache.empty) {
      override def expandIdentifier(nameParts: Seq[String]): Seq[String] = {
        expandedNames += nameParts
        Seq(CatalogManager.SESSION_CATALOG_NAME, "expanded_ns", nameParts.last)
      }
    }
    val functionResolution = new FunctionResolution(catalogManager, relationResolution)
    val persistent = UnresolvedFunctionName(Seq("persistent_fn"), "DESCRIBE FUNCTION")
    val missing = UnresolvedFunctionName(
      Seq("missing_fn"), "DESCRIBE FUNCTION", Some(Seq("stale_name")))
    val broken = UnresolvedFunctionName(Seq("broken_fn"), "DESCRIBE FUNCTION")

    assert(functionResolution.resolveFunctionName(persistent) == ResolvedPersistentFunc(
      v2Catalog, Identifier.of(Array("expanded_ns"), "persistent_fn"), function))
    assert(functionResolution.resolveFunctionName(missing) == missing.copy(
      possibleQualifiedName = Some(Seq("loaded_catalog", "expanded_ns", "missing_fn"))))
    assert(intercept[IllegalStateException] {
      functionResolution.resolveFunctionName(broken)
    } eq catalogError)
    assert(expandedNames.toSeq == Seq(persistent, missing, broken).map(_.multipartIdentifier))
    assert(loadedIdentifiers.toSeq == Seq("persistent_fn", "missing_fn", "broken_fn").map { name =>
      Identifier.of(Array("expanded_ns"), name)
    })
  }

  test("resolveFunctionName preserves the analyzer's catalog and initialized extractor") {
    val catalog = new SessionCatalog(new InMemoryCatalog, new SimpleFunctionRegistry)
    val sessionCatalog = new CustomV2SessionCatalog(catalog) {
      override def loadFunction(ident: Identifier): UnboundFunction = {
        throw new NoSuchFunctionException(ident)
      }
    }
    val catalogManager = new DefaultCatalogManager(sessionCatalog, catalog)
    val function = V1Function.metadataOnly(new ExpressionInfo("fake", "persistent_fn"))
    val overrideCatalog = new CustomV2SessionCatalog(catalog) {
      override def name(): String = "override_catalog"
      override def loadFunction(ident: Identifier): UnboundFunction = {
        if (ident.name() == "persistent_fn") function else throw new NoSuchFunctionException(ident)
      }
    }
    val analyzer = new Analyzer(catalogManager) {
      override def currentCatalog: CatalogPlugin = overrideCatalog
    }
    val initialConf = SQLConf.get.clone()
    initialConf.setConf(StaticSQLConf.GLOBAL_TEMP_DATABASE, "initial_global")
    SQLConf.withExistingConf(initialConf) {
      assert(analyzer.CatalogAndIdentifier.unapply(Seq("initial_global", "probe"))
        .exists(_._1 eq sessionCatalog))
    }
    val laterConf = initialConf.clone()
    laterConf.setConf(StaticSQLConf.GLOBAL_TEMP_DATABASE, "later_global")

    AnalysisContext.withNewAnalysisContext {
      SQLConf.withExistingConf(laterConf) {
        val persistent = UnresolvedFunctionName(Seq("persistent_fn"), "DESCRIBE FUNCTION")
        val missing = UnresolvedFunctionName(Seq("missing_fn"), "DESCRIBE FUNCTION")
        assert(analyzer.ResolveFunctions(persistent) == ResolvedPersistentFunc(
          overrideCatalog, Identifier.of(Array("default"), "persistent_fn"), function))
        assert(analyzer.ResolveFunctions(missing) == missing.copy(
          possibleQualifiedName = Some(Seq("override_catalog", "default", "missing_fn"))))

        val global = UnresolvedFunctionName(
          Seq("initial_global", "missing_fn"), "DESCRIBE FUNCTION")
        assert(analyzer.ResolveFunctions(global) == global.copy(
          possibleQualifiedName = Some(Seq(
            CatalogManager.SESSION_CATALOG_NAME, "initial_global", "missing_fn"))))

        val functionResolution = new FunctionResolution(
          catalogManager, new RelationResolution(catalogManager, RelationCache.empty))
        assert(functionResolution.resolveFunctionName(persistent) == persistent.copy(
          possibleQualifiedName = Some(Seq(
            CatalogManager.SESSION_CATALOG_NAME, "default", "persistent_fn"))))
      }
    }
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
    v1Catalog.loadPersistentScalarFunction(ident.asFunctionIdentifier)
  }
  override def functionExists(ident: Identifier): Boolean = {
    v1Catalog.isPersistentFunction(ident.asFunctionIdentifier)
  }
}
