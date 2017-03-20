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

package org.apache.spark.sql.hive

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.exec.{UDAF, UDF}
import org.apache.hadoop.hive.ql.exec.{FunctionRegistry => HiveFunctionRegistry}
import org.apache.hadoop.hive.ql.udf.generic.{AbstractGenericUDAFResolver, GenericUDF, GenericUDTF}

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.{CatalystConf, FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.catalog.{FunctionResourceLoader, GlobalTempViewManager, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DecimalType, DoubleType}
import org.apache.spark.util.Utils


private[sql] class HiveSessionCatalog(
    externalCatalog: HiveExternalCatalog,
    globalTempViewManager: GlobalTempViewManager,
    private val metastoreCatalog: HiveMetastoreCatalog,
    functionRegistry: FunctionRegistry,
    conf: SQLConf,
    hadoopConf: Configuration,
    parser: ParserInterface)
  extends SessionCatalog(
      externalCatalog,
      globalTempViewManager,
      functionRegistry,
      conf,
      hadoopConf,
      parser) {

  // ----------------------------------------------------------------
  // | Methods and fields for interacting with HiveMetastoreCatalog |
  // ----------------------------------------------------------------

  // These 2 rules must be run before all other DDL post-hoc resolution rules, i.e.
  // `PreprocessTableCreation`, `PreprocessTableInsertion`, `DataSourceAnalysis` and `HiveAnalysis`.
  val ParquetConversions: Rule[LogicalPlan] = metastoreCatalog.ParquetConversions
  val OrcConversions: Rule[LogicalPlan] = metastoreCatalog.OrcConversions

  def hiveDefaultTableFilePath(name: TableIdentifier): String = {
    metastoreCatalog.hiveDefaultTableFilePath(name)
  }

  /**
   * Create a new [[HiveSessionCatalog]] with the provided parameters. `externalCatalog` and
   * `globalTempViewManager` are `inherited`, while `currentDb` and `tempTables` are copied.
   */
  def newSessionCatalogWith(
      newSparkSession: SparkSession,
      conf: SQLConf,
      hadoopConf: Configuration,
      functionRegistry: FunctionRegistry,
      parser: ParserInterface): HiveSessionCatalog = {
    val catalog = HiveSessionCatalog(
      newSparkSession,
      functionRegistry,
      conf,
      hadoopConf,
      parser)

    synchronized {
      catalog.currentDb = currentDb
      // copy over temporary tables
      tempTables.foreach(kv => catalog.tempTables.put(kv._1, kv._2))
    }

    catalog
  }

  /**
   * The parent class [[SessionCatalog]] cannot access the [[SparkSession]] class, so we cannot add
   * a [[SparkSession]] parameter to [[SessionCatalog.newSessionCatalogWith]]. However,
   * [[HiveSessionCatalog]] requires a [[SparkSession]] parameter, so we can a new version of
   * `newSessionCatalogWith` and disable this one.
   *
   * TODO Refactor HiveSessionCatalog to not use [[SparkSession]] directly.
   */
  override def newSessionCatalogWith(
      conf: CatalystConf,
      hadoopConf: Configuration,
      functionRegistry: FunctionRegistry,
      parser: ParserInterface): HiveSessionCatalog = throw new UnsupportedOperationException(
    "to clone HiveSessionCatalog, use the other clone method that also accepts a SparkSession")

  // For testing only
  private[hive] def getCachedDataSourceTable(table: TableIdentifier): LogicalPlan = {
    val key = metastoreCatalog.getQualifiedTableName(table)
    tableRelationCache.getIfPresent(key)
  }

  override def makeFunctionBuilder(funcName: String, className: String): FunctionBuilder = {
    makeFunctionBuilder(funcName, Utils.classForName(className))
  }

  /**
   * Construct a [[FunctionBuilder]] based on the provided class that represents a function.
   */
  private def makeFunctionBuilder(name: String, clazz: Class[_]): FunctionBuilder = {
    // When we instantiate hive UDF wrapper class, we may throw exception if the input
    // expressions don't satisfy the hive UDF, such as type mismatch, input number
    // mismatch, etc. Here we catch the exception and throw AnalysisException instead.
    (children: Seq[Expression]) => {
      try {
        if (classOf[UDF].isAssignableFrom(clazz)) {
          val udf = HiveSimpleUDF(name, new HiveFunctionWrapper(clazz.getName), children)
          udf.dataType // Force it to check input data types.
          udf
        } else if (classOf[GenericUDF].isAssignableFrom(clazz)) {
          val udf = HiveGenericUDF(name, new HiveFunctionWrapper(clazz.getName), children)
          udf.dataType // Force it to check input data types.
          udf
        } else if (classOf[AbstractGenericUDAFResolver].isAssignableFrom(clazz)) {
          val udaf = HiveUDAFFunction(name, new HiveFunctionWrapper(clazz.getName), children)
          udaf.dataType // Force it to check input data types.
          udaf
        } else if (classOf[UDAF].isAssignableFrom(clazz)) {
          val udaf = HiveUDAFFunction(
            name,
            new HiveFunctionWrapper(clazz.getName),
            children,
            isUDAFBridgeRequired = true)
          udaf.dataType  // Force it to check input data types.
          udaf
        } else if (classOf[GenericUDTF].isAssignableFrom(clazz)) {
          val udtf = HiveGenericUDTF(name, new HiveFunctionWrapper(clazz.getName), children)
          udtf.elementSchema // Force it to check input data types.
          udtf
        } else {
          throw new AnalysisException(s"No handler for Hive UDF '${clazz.getCanonicalName}'")
        }
      } catch {
        case ae: AnalysisException =>
          throw ae
        case NonFatal(e) =>
          val analysisException =
            new AnalysisException(s"No handler for Hive UDF '${clazz.getCanonicalName}': $e")
          analysisException.setStackTrace(e.getStackTrace)
          throw analysisException
      }
    }
  }

  override def lookupFunction(name: FunctionIdentifier, children: Seq[Expression]): Expression = {
    try {
      lookupFunction0(name, children)
    } catch {
      case NonFatal(_) =>
        // SPARK-16228 ExternalCatalog may recognize `double`-type only.
        val newChildren = children.map { child =>
          if (child.dataType.isInstanceOf[DecimalType]) Cast(child, DoubleType) else child
        }
        lookupFunction0(name, newChildren)
    }
  }

  private def lookupFunction0(name: FunctionIdentifier, children: Seq[Expression]): Expression = {
    // TODO: Once lookupFunction accepts a FunctionIdentifier, we should refactor this method to
    // if (super.functionExists(name)) {
    //   super.lookupFunction(name, children)
    // } else {
    //   // This function is a Hive builtin function.
    //   ...
    // }
    val database = name.database.map(formatDatabaseName)
    val funcName = name.copy(database = database)
    Try(super.lookupFunction(funcName, children)) match {
      case Success(expr) => expr
      case Failure(error) =>
        if (functionRegistry.functionExists(funcName.unquotedString)) {
          // If the function actually exists in functionRegistry, it means that there is an
          // error when we create the Expression using the given children.
          // We need to throw the original exception.
          throw error
        } else {
          // This function is not in functionRegistry, let's try to load it as a Hive's
          // built-in function.
          // Hive is case insensitive.
          val functionName = funcName.unquotedString.toLowerCase
          if (!hiveFunctions.contains(functionName)) {
            failFunctionLookup(funcName.unquotedString)
          }

          // TODO: Remove this fallback path once we implement the list of fallback functions
          // defined below in hiveFunctions.
          val functionInfo = {
            try {
              Option(HiveFunctionRegistry.getFunctionInfo(functionName)).getOrElse(
                failFunctionLookup(funcName.unquotedString))
            } catch {
              // If HiveFunctionRegistry.getFunctionInfo throws an exception,
              // we are failing to load a Hive builtin function, which means that
              // the given function is not a Hive builtin function.
              case NonFatal(e) => failFunctionLookup(funcName.unquotedString)
            }
          }
          val className = functionInfo.getFunctionClass.getName
          val builder = makeFunctionBuilder(functionName, className)
          // Put this Hive built-in function to our function registry.
          val info = new ExpressionInfo(className, functionName)
          createTempFunction(functionName, info, builder, ignoreIfExists = false)
          // Now, we need to create the Expression.
          functionRegistry.lookupFunction(functionName, children)
        }
    }
  }

  // TODO Removes this method after implementing Spark native "histogram_numeric".
  override def functionExists(name: FunctionIdentifier): Boolean = {
    super.functionExists(name) || hiveFunctions.contains(name.funcName)
  }

  /** List of functions we pass over to Hive. Note that over time this list should go to 0. */
  // We have a list of Hive built-in functions that we do not support. So, we will check
  // Hive's function registry and lazily load needed functions into our own function registry.
  // List of functions we are explicitly not supporting are:
  // compute_stats, context_ngrams, create_union,
  // current_user, ewah_bitmap, ewah_bitmap_and, ewah_bitmap_empty, ewah_bitmap_or, field,
  // in_file, index, matchpath, ngrams, noop, noopstreaming, noopwithmap,
  // noopwithmapstreaming, parse_url_tuple, reflect2, windowingtablefunction.
  // Note: don't forget to update SessionCatalog.isTemporaryFunction
  private val hiveFunctions = Seq(
    "histogram_numeric"
  )
}

private[sql] object HiveSessionCatalog {

  def apply(
      sparkSession: SparkSession,
      functionRegistry: FunctionRegistry,
      conf: SQLConf,
      hadoopConf: Configuration,
      parser: ParserInterface): HiveSessionCatalog = {
    // Catalog for handling data source tables. TODO: This really doesn't belong here since it is
    // essentially a cache for metastore tables. However, it relies on a lot of session-specific
    // things so it would be a lot of work to split its functionality between HiveSessionCatalog
    // and HiveCatalog. We should still do it at some point...
    val metastoreCatalog = new HiveMetastoreCatalog(sparkSession)

    new HiveSessionCatalog(
      sparkSession.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog],
      sparkSession.sharedState.globalTempViewManager,
      metastoreCatalog,
      functionRegistry,
      conf,
      hadoopConf,
      parser)
  }
}
