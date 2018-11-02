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
import org.apache.spark.sql.catalyst.catalog.{CatalogFunction, ExternalCatalog, FunctionResourceLoader, GlobalTempViewManager, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DecimalType, DoubleType}


private[sql] class HiveSessionCatalog(
    externalCatalogBuilder: () => ExternalCatalog,
    globalTempViewManagerBuilder: () => GlobalTempViewManager,
    val metastoreCatalog: HiveMetastoreCatalog,
    functionRegistry: FunctionRegistry,
    conf: SQLConf,
    hadoopConf: Configuration,
    parser: ParserInterface,
    functionResourceLoader: FunctionResourceLoader)
  extends SessionCatalog(
      externalCatalogBuilder,
      globalTempViewManagerBuilder,
      functionRegistry,
      conf,
      hadoopConf,
      parser,
      functionResourceLoader) {

  /**
   * Constructs a [[Expression]] based on the provided class that represents a function.
   *
   * This performs reflection to decide what type of [[Expression]] to return in the builder.
   */
  override def makeFunctionExpression(
      name: String,
      clazz: Class[_],
      input: Seq[Expression]): Expression = {

    Try(super.makeFunctionExpression(name, clazz, input)).getOrElse {
      var udfExpr: Option[Expression] = None
      try {
        // When we instantiate hive UDF wrapper class, we may throw exception if the input
        // expressions don't satisfy the hive UDF, such as type mismatch, input number
        // mismatch, etc. Here we catch the exception and throw AnalysisException instead.
        if (classOf[UDF].isAssignableFrom(clazz)) {
          udfExpr = Some(HiveSimpleUDF(name, new HiveFunctionWrapper(clazz.getName), input))
          udfExpr.get.dataType // Force it to check input data types.
        } else if (classOf[GenericUDF].isAssignableFrom(clazz)) {
          udfExpr = Some(HiveGenericUDF(name, new HiveFunctionWrapper(clazz.getName), input))
          udfExpr.get.dataType // Force it to check input data types.
        } else if (classOf[AbstractGenericUDAFResolver].isAssignableFrom(clazz)) {
          udfExpr = Some(HiveUDAFFunction(name, new HiveFunctionWrapper(clazz.getName), input))
          udfExpr.get.dataType // Force it to check input data types.
        } else if (classOf[UDAF].isAssignableFrom(clazz)) {
          udfExpr = Some(HiveUDAFFunction(
            name,
            new HiveFunctionWrapper(clazz.getName),
            input,
            isUDAFBridgeRequired = true))
          udfExpr.get.dataType // Force it to check input data types.
        } else if (classOf[GenericUDTF].isAssignableFrom(clazz)) {
          udfExpr = Some(HiveGenericUDTF(name, new HiveFunctionWrapper(clazz.getName), input))
          udfExpr.get.asInstanceOf[HiveGenericUDTF].elementSchema // Force it to check data types.
        }
      } catch {
        case NonFatal(e) =>
          val noHandlerMsg = s"No handler for UDF/UDAF/UDTF '${clazz.getCanonicalName}': $e"
          val errorMsg =
            if (classOf[GenericUDTF].isAssignableFrom(clazz)) {
              s"$noHandlerMsg\nPlease make sure your function overrides " +
                "`public StructObjectInspector initialize(ObjectInspector[] args)`."
            } else {
              noHandlerMsg
            }
          val analysisException = new AnalysisException(errorMsg)
          analysisException.setStackTrace(e.getStackTrace)
          throw analysisException
      }
      udfExpr.getOrElse {
        throw new AnalysisException(s"No handler for UDF/UDAF/UDTF '${clazz.getCanonicalName}'")
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
        if (super.functionExists(name)) {
          // If the function exists (either in functionRegistry or externalCatalog),
          // it means that there is an error when we create the Expression using the given children.
          // We need to throw the original exception.
          throw error
        } else {
          // This function does not exist (neither in functionRegistry or externalCatalog),
          // let's try to load it as a Hive's built-in function.
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
          registerFunction(func, overrideIfExists = false)
          // Now, we need to create the Expression.
          functionRegistry.lookupFunction(functionIdentifier, children)
        }
    }
  }

  // TODO Removes this method after implementing Spark native "histogram_numeric".
  override def functionExists(name: FunctionIdentifier): Boolean = {
    super.functionExists(name) || hiveFunctions.contains(name.funcName)
  }

  override def isPersistentFunction(name: FunctionIdentifier): Boolean = {
    super.isPersistentFunction(name) || hiveFunctions.contains(name.funcName)
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
