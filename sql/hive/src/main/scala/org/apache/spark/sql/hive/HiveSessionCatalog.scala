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

import java.util.Locale

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.exec.{UDAF, UDF}
import org.apache.hadoop.hive.ql.exec.{FunctionRegistry => HiveFunctionRegistry}
import org.apache.hadoop.hive.ql.udf.generic.{AbstractGenericUDAFResolver, GenericUDF, GenericUDTF}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.catalog.{CatalogFunction, FunctionResourceLoader, GlobalTempViewManager, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DecimalType, DoubleType}
import org.apache.spark.util.Utils


private[sql] class HiveSessionCatalog(
    externalCatalog: HiveExternalCatalog,
    globalTempViewManager: GlobalTempViewManager,
    val metastoreCatalog: HiveMetastoreCatalog,
    functionRegistry: FunctionRegistry,
    conf: SQLConf,
    hadoopConf: Configuration,
    parser: ParserInterface,
    functionResourceLoader: FunctionResourceLoader)
  extends SessionCatalog(
      externalCatalog,
      globalTempViewManager,
      functionRegistry,
      conf,
      hadoopConf,
      parser,
      functionResourceLoader) {

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
          val functionName = funcName.unquotedString.toLowerCase(Locale.ROOT)
          if (!hiveFunctions.contains(functionName)) {
            failFunctionLookup(funcName)
          }

          // TODO: Remove this fallback path once we implement the list of fallback functions
          // defined below in hiveFunctions.
          val functionInfo = {
            try {
              Option(HiveFunctionRegistry.getFunctionInfo(functionName)).getOrElse(
                failFunctionLookup(funcName))
            } catch {
              // If HiveFunctionRegistry.getFunctionInfo throws an exception,
              // we are failing to load a Hive builtin function, which means that
              // the given function is not a Hive builtin function.
              case NonFatal(e) => failFunctionLookup(funcName)
            }
          }
          val className = functionInfo.getFunctionClass.getName
          val functionIdentifier =
            FunctionIdentifier(functionName.toLowerCase(Locale.ROOT), database)
          val func = CatalogFunction(functionIdentifier, className, Nil)
          // Put this Hive built-in function to our function registry.
          registerFunction(func, ignoreIfExists = false)
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
