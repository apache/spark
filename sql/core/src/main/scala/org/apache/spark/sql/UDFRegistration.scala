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

package org.apache.spark.sql

import java.lang.reflect.ParameterizedType

import org.apache.spark.annotation.Stable
import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.api.java._
import org.apache.spark.sql.catalyst.JavaTypeInference
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.aggregate.{ScalaAggregator, ScalaUDAF}
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.expressions.{SparkUserDefinedFunction, UserDefinedAggregateFunction, UserDefinedAggregator, UserDefinedFunction}
import org.apache.spark.sql.internal.UserDefinedFunctionUtils.toScalaUDF
import org.apache.spark.sql.types.DataType

/**
 * Functions for registering user-defined functions. Use `SparkSession.udf` to access this:
 *
 * {{{
 *   spark.udf
 * }}}
 *
 * @since 1.3.0
 */
@Stable
class UDFRegistration private[sql] (session: SparkSession, functionRegistry: FunctionRegistry)
  extends api.UDFRegistration
  with Logging {
  protected[sql] def registerPython(name: String, udf: UserDefinedPythonFunction): Unit = {
    log.debug(
      s"""
        | Registering new PythonUDF:
        | name: $name
        | command: ${udf.func.command}
        | envVars: ${udf.func.envVars}
        | pythonIncludes: ${udf.func.pythonIncludes}
        | pythonExec: ${udf.func.pythonExec}
        | dataType: ${udf.dataType}
        | pythonEvalType: ${PythonEvalType.toString(udf.pythonEvalType)}
        | udfDeterministic: ${udf.udfDeterministic}
      """.stripMargin)

    functionRegistry.createOrReplaceTempFunction(name, udf.builder, "python_udf")
  }

  /**
   * Registers a user-defined aggregate function (UDAF).
   *
   * @param name the name of the UDAF.
   * @param udaf the UDAF needs to be registered.
   * @return the registered UDAF.
   * @since 1.5.0
   * @deprecated this method and the use of UserDefinedAggregateFunction are deprecated.
   *             Aggregator[IN, BUF, OUT] should now be registered as a UDF via the
   *             functions.udaf(agg) method.
   */
  @deprecated("Aggregator[IN, BUF, OUT] should now be registered as a UDF" +
    " via the functions.udaf(agg) method.", "3.0.0")
  def register(name: String, udaf: UserDefinedAggregateFunction): UserDefinedAggregateFunction = {
    def builder(children: Seq[Expression]) = ScalaUDAF(children, udaf, udafName = Some(name))
    functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
    udaf
  }

  override protected def register(
      name: String,
      udf: UserDefinedFunction,
      source: String,
      validateParameterCount: Boolean): UserDefinedFunction = {
    val named = udf.withName(name)
    val builder: Seq[Expression] => Expression = named match {
      case udaf: UserDefinedAggregator[_, _, _] =>
        ScalaAggregator(udaf, _)
      case udf: SparkUserDefinedFunction if validateParameterCount =>
        val expectedParameterCount = udf.inputEncoders.size
        children => {
          val actualParameterCount = children.length
          if (expectedParameterCount == actualParameterCount) {
            toScalaUDF(udf, children)
          } else {
            throw QueryCompilationErrors.wrongNumArgsError(
              name,
              expectedParameterCount.toString,
              actualParameterCount)
          }
        }
      case udf: SparkUserDefinedFunction =>
        toScalaUDF(udf, _)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder, source)
    named
  }


  /**
   * Register a Java UDAF class using reflection, for use from pyspark
   *
   * @param name      UDAF name
   * @param className fully qualified class name of UDAF
   */
  private[sql] def registerJavaUDAF(name: String, className: String): Unit = {
    try {
      val clazz = session.artifactManager.classloader.loadClass(className)
      if (!classOf[UserDefinedAggregateFunction].isAssignableFrom(clazz)) {
        throw QueryCompilationErrors
          .classDoesNotImplementUserDefinedAggregateFunctionError(className)
      }
      val udaf = clazz.getConstructor().newInstance().asInstanceOf[UserDefinedAggregateFunction]
      register(name, udaf)
    } catch {
      case _: ClassNotFoundException =>
        throw QueryCompilationErrors.cannotLoadClassNotOnClassPathError(className)
      case _: InstantiationException | _: IllegalArgumentException =>
        throw QueryCompilationErrors.classWithoutPublicNonArgumentConstructorError(className)
    }
  }

  // scalastyle:off line.size.limit

  override def registerJava(name: String, className: String, returnDataType: DataType): Unit = {
    try {
      val clazz = session.artifactManager.classloader.loadClass(className)
      val udfInterfaces = clazz.getGenericInterfaces
        .filter(_.isInstanceOf[ParameterizedType])
        .map(_.asInstanceOf[ParameterizedType])
        .filter(e => e.getRawType.isInstanceOf[Class[_]] && e.getRawType.asInstanceOf[Class[_]].getCanonicalName.startsWith("org.apache.spark.sql.api.java.UDF"))
      if (udfInterfaces.length == 0) {
        throw QueryCompilationErrors.udfClassDoesNotImplementAnyUDFInterfaceError(className)
      } else if (udfInterfaces.length > 1) {
        throw QueryCompilationErrors.udfClassImplementMultiUDFInterfacesError(className)
      } else {
        try {
          val udf = clazz.getConstructor().newInstance()
          val udfReturnType = udfInterfaces(0).getActualTypeArguments.last
          var returnType = returnDataType
          if (returnType == null) {
            returnType = JavaTypeInference.inferDataType(udfReturnType)._1
          }

          udfInterfaces(0).getActualTypeArguments.length match {
            case 1 => register(name, udf.asInstanceOf[UDF0[_]], returnType)
            case 2 => register(name, udf.asInstanceOf[UDF1[_, _]], returnType)
            case 3 => register(name, udf.asInstanceOf[UDF2[_, _, _]], returnType)
            case 4 => register(name, udf.asInstanceOf[UDF3[_, _, _, _]], returnType)
            case 5 => register(name, udf.asInstanceOf[UDF4[_, _, _, _, _]], returnType)
            case 6 => register(name, udf.asInstanceOf[UDF5[_, _, _, _, _, _]], returnType)
            case 7 => register(name, udf.asInstanceOf[UDF6[_, _, _, _, _, _, _]], returnType)
            case 8 => register(name, udf.asInstanceOf[UDF7[_, _, _, _, _, _, _, _]], returnType)
            case 9 => register(name, udf.asInstanceOf[UDF8[_, _, _, _, _, _, _, _, _]], returnType)
            case 10 => register(name, udf.asInstanceOf[UDF9[_, _, _, _, _, _, _, _, _, _]], returnType)
            case 11 => register(name, udf.asInstanceOf[UDF10[_, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 12 => register(name, udf.asInstanceOf[UDF11[_, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 13 => register(name, udf.asInstanceOf[UDF12[_, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 14 => register(name, udf.asInstanceOf[UDF13[_, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 15 => register(name, udf.asInstanceOf[UDF14[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 16 => register(name, udf.asInstanceOf[UDF15[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 17 => register(name, udf.asInstanceOf[UDF16[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 18 => register(name, udf.asInstanceOf[UDF17[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 19 => register(name, udf.asInstanceOf[UDF18[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 20 => register(name, udf.asInstanceOf[UDF19[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 21 => register(name, udf.asInstanceOf[UDF20[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 22 => register(name, udf.asInstanceOf[UDF21[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 23 => register(name, udf.asInstanceOf[UDF22[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case n =>
              throw QueryCompilationErrors.udfClassWithTooManyTypeArgumentsError(n)
          }
        } catch {
          case _: InstantiationException | _: IllegalArgumentException =>
            throw QueryCompilationErrors.classWithoutPublicNonArgumentConstructorError(className)
        }
      }
    } catch {
      case _: ClassNotFoundException => throw QueryCompilationErrors.cannotLoadClassNotOnClassPathError(className)
    }
  }
  // scalastyle:on line.size.limit
}
