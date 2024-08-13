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

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.annotation.Stable
import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.api.java._
import org.apache.spark.sql.catalyst.JavaTypeInference
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.aggregate.{ScalaAggregator, ScalaUDAF}
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.expressions.{SparkUserDefinedFunction, UserDefinedAggregateFunction, UserDefinedAggregator, UserDefinedFunction}
import org.apache.spark.sql.expressions.UserDefinedFunctionUtils.toScalaUDF
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.Utils

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
class UDFRegistration private[sql] (functionRegistry: FunctionRegistry) extends Logging {

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
   *
   * @since 1.5.0
   * @deprecated this method and the use of UserDefinedAggregateFunction are deprecated.
   * Aggregator[IN, BUF, OUT] should now be registered as a UDF via the functions.udaf(agg) method.
   */
  @deprecated("Aggregator[IN, BUF, OUT] should now be registered as a UDF" +
    " via the functions.udaf(agg) method.", "3.0.0")
  def register(name: String, udaf: UserDefinedAggregateFunction): UserDefinedAggregateFunction = {
    def builder(children: Seq[Expression]) = ScalaUDAF(children, udaf, udafName = Some(name))
    functionRegistry.createOrReplaceTempFunction(name, builder, "scala_udf")
    udaf
  }

  /**
   * Registers a user-defined function (UDF), for a UDF that's already defined using the Dataset
   * API (i.e. of type UserDefinedFunction). To change a UDF to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`. To change a UDF to nonNullable, call the API
   * `UserDefinedFunction.asNonNullable()`.
   *
   * Example:
   * {{{
   *   val foo = udf(() => Math.random())
   *   spark.udf.register("random", foo.asNondeterministic())
   *
   *   val bar = udf(() => "bar")
   *   spark.udf.register("stringLit", bar.asNonNullable())
   * }}}
   *
   * @param name the name of the UDF.
   * @param udf the UDF needs to be registered.
   * @return the registered UDF.
   *
   * @since 2.2.0
   */
  def register(name: String, udf: UserDefinedFunction): UserDefinedFunction = {
    register(name, udf, "scala_udf", validateParameterCount = false)
  }

  private def registerScalaUDF(
      name: String,
      func: AnyRef,
      returnTypeTag: TypeTag[_],
      inputTypeTags: TypeTag[_]*): UserDefinedFunction = {
    val udf = SparkUserDefinedFunction(func, returnTypeTag, inputTypeTags: _*)
    register(name, udf, "scala_udf", validateParameterCount = true)
  }

  private def registerJavaUDF(
      name: String,
      func: AnyRef,
      returnDataType: DataType,
      cardinality: Int): UserDefinedFunction = {
    val validatedReturnDataType = CharVarcharUtils.failIfHasCharVarchar(returnDataType)
    val udf = SparkUserDefinedFunction(func, validatedReturnDataType, cardinality)
    register(name, udf, "java_udf", validateParameterCount = true)
  }

  private def register(
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

  // scalastyle:off line.size.limit

  /* register 0-22 were generated by this script

    (0 to 22).foreach { x =>
      val types = (1 to x).foldRight("RT")((i, s) => s"A$i, $s")
      val typeSeq = "RT" +: (1 to x).map(i => s"A$i")
      val typeTags = typeSeq.map(t => s"$t: TypeTag").mkString(", ")
      val implicitTypeTags = typeSeq.map(t => s"implicitly[TypeTag[$t]]").mkString(", ")
      println(s"""
        |/**
        | * Registers a deterministic Scala closure of $x arguments as user-defined function (UDF).
        | * @tparam RT return type of UDF.
        | * @since 1.3.0
        | */
        |def register[$typeTags](name: String, func: Function$x[$types]): UserDefinedFunction = {
        |  registerScalaUDF(name, func, $implicitTypeTags)
        |}""".stripMargin)
    }

    (0 to 22).foreach { i =>
      val extTypeArgs = (0 to i).map(_ => "_").mkString(", ")
      val anyTypeArgs = (0 to i).map(_ => "Any").mkString(", ")
      val anyCast = s".asInstanceOf[UDF$i[$anyTypeArgs]]"
      val anyParams = (1 to i).map(_ => "_: Any").mkString(", ")
      val funcCall = if (i == 0) s"() => f$anyCast.call($anyParams)" else s"f$anyCast.call($anyParams)"
      println(s"""
        |/**
        | * Register a deterministic Java UDF$i instance as user-defined function (UDF).
        | * @since 1.3.0
        | */
        |def register(name: String, f: UDF$i[$extTypeArgs], returnType: DataType): Unit = {
        |  val func = $funcCall
        |  registerJavaUDF(name, func, returnType, $i)
        |}""".stripMargin)
    }
    */

  /**
   * Registers a deterministic Scala closure of 0 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag](name: String, func: Function0[RT]): UserDefinedFunction = {
    registerScalaUDF(name, func, implicitly[TypeTag[RT]])
  }

  /**
   * Registers a deterministic Scala closure of 1 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag](name: String, func: Function1[A1, RT]): UserDefinedFunction = {
    registerScalaUDF(name, func, implicitly[TypeTag[RT]], implicitly[TypeTag[A1]])
  }

  /**
   * Registers a deterministic Scala closure of 2 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag](name: String, func: Function2[A1, A2, RT]): UserDefinedFunction = {
    registerScalaUDF(name, func, implicitly[TypeTag[RT]], implicitly[TypeTag[A1]], implicitly[TypeTag[A2]])
  }

  /**
   * Registers a deterministic Scala closure of 3 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag](name: String, func: Function3[A1, A2, A3, RT]): UserDefinedFunction = {
    registerScalaUDF(name, func, implicitly[TypeTag[RT]], implicitly[TypeTag[A1]], implicitly[TypeTag[A2]], implicitly[TypeTag[A3]])
  }

  /**
   * Registers a deterministic Scala closure of 4 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag](name: String, func: Function4[A1, A2, A3, A4, RT]): UserDefinedFunction = {
    registerScalaUDF(name, func, implicitly[TypeTag[RT]], implicitly[TypeTag[A1]], implicitly[TypeTag[A2]], implicitly[TypeTag[A3]], implicitly[TypeTag[A4]])
  }

  /**
   * Registers a deterministic Scala closure of 5 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag](name: String, func: Function5[A1, A2, A3, A4, A5, RT]): UserDefinedFunction = {
    registerScalaUDF(name, func, implicitly[TypeTag[RT]], implicitly[TypeTag[A1]], implicitly[TypeTag[A2]], implicitly[TypeTag[A3]], implicitly[TypeTag[A4]], implicitly[TypeTag[A5]])
  }

  /**
   * Registers a deterministic Scala closure of 6 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag](name: String, func: Function6[A1, A2, A3, A4, A5, A6, RT]): UserDefinedFunction = {
    registerScalaUDF(name, func, implicitly[TypeTag[RT]], implicitly[TypeTag[A1]], implicitly[TypeTag[A2]], implicitly[TypeTag[A3]], implicitly[TypeTag[A4]], implicitly[TypeTag[A5]], implicitly[TypeTag[A6]])
  }

  /**
   * Registers a deterministic Scala closure of 7 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag](name: String, func: Function7[A1, A2, A3, A4, A5, A6, A7, RT]): UserDefinedFunction = {
    registerScalaUDF(name, func, implicitly[TypeTag[RT]], implicitly[TypeTag[A1]], implicitly[TypeTag[A2]], implicitly[TypeTag[A3]], implicitly[TypeTag[A4]], implicitly[TypeTag[A5]], implicitly[TypeTag[A6]], implicitly[TypeTag[A7]])
  }

  /**
   * Registers a deterministic Scala closure of 8 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag](name: String, func: Function8[A1, A2, A3, A4, A5, A6, A7, A8, RT]): UserDefinedFunction = {
    registerScalaUDF(name, func, implicitly[TypeTag[RT]], implicitly[TypeTag[A1]], implicitly[TypeTag[A2]], implicitly[TypeTag[A3]], implicitly[TypeTag[A4]], implicitly[TypeTag[A5]], implicitly[TypeTag[A6]], implicitly[TypeTag[A7]], implicitly[TypeTag[A8]])
  }

  /**
   * Registers a deterministic Scala closure of 9 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag](name: String, func: Function9[A1, A2, A3, A4, A5, A6, A7, A8, A9, RT]): UserDefinedFunction = {
    registerScalaUDF(name, func, implicitly[TypeTag[RT]], implicitly[TypeTag[A1]], implicitly[TypeTag[A2]], implicitly[TypeTag[A3]], implicitly[TypeTag[A4]], implicitly[TypeTag[A5]], implicitly[TypeTag[A6]], implicitly[TypeTag[A7]], implicitly[TypeTag[A8]], implicitly[TypeTag[A9]])
  }

  /**
   * Registers a deterministic Scala closure of 10 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag](name: String, func: Function10[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT]): UserDefinedFunction = {
    registerScalaUDF(name, func, implicitly[TypeTag[RT]], implicitly[TypeTag[A1]], implicitly[TypeTag[A2]], implicitly[TypeTag[A3]], implicitly[TypeTag[A4]], implicitly[TypeTag[A5]], implicitly[TypeTag[A6]], implicitly[TypeTag[A7]], implicitly[TypeTag[A8]], implicitly[TypeTag[A9]], implicitly[TypeTag[A10]])
  }

  /**
   * Registers a deterministic Scala closure of 11 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag](name: String, func: Function11[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, RT]): UserDefinedFunction = {
    registerScalaUDF(name, func, implicitly[TypeTag[RT]], implicitly[TypeTag[A1]], implicitly[TypeTag[A2]], implicitly[TypeTag[A3]], implicitly[TypeTag[A4]], implicitly[TypeTag[A5]], implicitly[TypeTag[A6]], implicitly[TypeTag[A7]], implicitly[TypeTag[A8]], implicitly[TypeTag[A9]], implicitly[TypeTag[A10]], implicitly[TypeTag[A11]])
  }

  /**
   * Registers a deterministic Scala closure of 12 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag](name: String, func: Function12[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, RT]): UserDefinedFunction = {
    registerScalaUDF(name, func, implicitly[TypeTag[RT]], implicitly[TypeTag[A1]], implicitly[TypeTag[A2]], implicitly[TypeTag[A3]], implicitly[TypeTag[A4]], implicitly[TypeTag[A5]], implicitly[TypeTag[A6]], implicitly[TypeTag[A7]], implicitly[TypeTag[A8]], implicitly[TypeTag[A9]], implicitly[TypeTag[A10]], implicitly[TypeTag[A11]], implicitly[TypeTag[A12]])
  }

  /**
   * Registers a deterministic Scala closure of 13 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag](name: String, func: Function13[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, RT]): UserDefinedFunction = {
    registerScalaUDF(name, func, implicitly[TypeTag[RT]], implicitly[TypeTag[A1]], implicitly[TypeTag[A2]], implicitly[TypeTag[A3]], implicitly[TypeTag[A4]], implicitly[TypeTag[A5]], implicitly[TypeTag[A6]], implicitly[TypeTag[A7]], implicitly[TypeTag[A8]], implicitly[TypeTag[A9]], implicitly[TypeTag[A10]], implicitly[TypeTag[A11]], implicitly[TypeTag[A12]], implicitly[TypeTag[A13]])
  }

  /**
   * Registers a deterministic Scala closure of 14 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag](name: String, func: Function14[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, RT]): UserDefinedFunction = {
    registerScalaUDF(name, func, implicitly[TypeTag[RT]], implicitly[TypeTag[A1]], implicitly[TypeTag[A2]], implicitly[TypeTag[A3]], implicitly[TypeTag[A4]], implicitly[TypeTag[A5]], implicitly[TypeTag[A6]], implicitly[TypeTag[A7]], implicitly[TypeTag[A8]], implicitly[TypeTag[A9]], implicitly[TypeTag[A10]], implicitly[TypeTag[A11]], implicitly[TypeTag[A12]], implicitly[TypeTag[A13]], implicitly[TypeTag[A14]])
  }

  /**
   * Registers a deterministic Scala closure of 15 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag](name: String, func: Function15[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, RT]): UserDefinedFunction = {
    registerScalaUDF(name, func, implicitly[TypeTag[RT]], implicitly[TypeTag[A1]], implicitly[TypeTag[A2]], implicitly[TypeTag[A3]], implicitly[TypeTag[A4]], implicitly[TypeTag[A5]], implicitly[TypeTag[A6]], implicitly[TypeTag[A7]], implicitly[TypeTag[A8]], implicitly[TypeTag[A9]], implicitly[TypeTag[A10]], implicitly[TypeTag[A11]], implicitly[TypeTag[A12]], implicitly[TypeTag[A13]], implicitly[TypeTag[A14]], implicitly[TypeTag[A15]])
  }

  /**
   * Registers a deterministic Scala closure of 16 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag](name: String, func: Function16[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, RT]): UserDefinedFunction = {
    registerScalaUDF(name, func, implicitly[TypeTag[RT]], implicitly[TypeTag[A1]], implicitly[TypeTag[A2]], implicitly[TypeTag[A3]], implicitly[TypeTag[A4]], implicitly[TypeTag[A5]], implicitly[TypeTag[A6]], implicitly[TypeTag[A7]], implicitly[TypeTag[A8]], implicitly[TypeTag[A9]], implicitly[TypeTag[A10]], implicitly[TypeTag[A11]], implicitly[TypeTag[A12]], implicitly[TypeTag[A13]], implicitly[TypeTag[A14]], implicitly[TypeTag[A15]], implicitly[TypeTag[A16]])
  }

  /**
   * Registers a deterministic Scala closure of 17 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag](name: String, func: Function17[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, RT]): UserDefinedFunction = {
    registerScalaUDF(name, func, implicitly[TypeTag[RT]], implicitly[TypeTag[A1]], implicitly[TypeTag[A2]], implicitly[TypeTag[A3]], implicitly[TypeTag[A4]], implicitly[TypeTag[A5]], implicitly[TypeTag[A6]], implicitly[TypeTag[A7]], implicitly[TypeTag[A8]], implicitly[TypeTag[A9]], implicitly[TypeTag[A10]], implicitly[TypeTag[A11]], implicitly[TypeTag[A12]], implicitly[TypeTag[A13]], implicitly[TypeTag[A14]], implicitly[TypeTag[A15]], implicitly[TypeTag[A16]], implicitly[TypeTag[A17]])
  }

  /**
   * Registers a deterministic Scala closure of 18 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag](name: String, func: Function18[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, RT]): UserDefinedFunction = {
    registerScalaUDF(name, func, implicitly[TypeTag[RT]], implicitly[TypeTag[A1]], implicitly[TypeTag[A2]], implicitly[TypeTag[A3]], implicitly[TypeTag[A4]], implicitly[TypeTag[A5]], implicitly[TypeTag[A6]], implicitly[TypeTag[A7]], implicitly[TypeTag[A8]], implicitly[TypeTag[A9]], implicitly[TypeTag[A10]], implicitly[TypeTag[A11]], implicitly[TypeTag[A12]], implicitly[TypeTag[A13]], implicitly[TypeTag[A14]], implicitly[TypeTag[A15]], implicitly[TypeTag[A16]], implicitly[TypeTag[A17]], implicitly[TypeTag[A18]])
  }

  /**
   * Registers a deterministic Scala closure of 19 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag, A19: TypeTag](name: String, func: Function19[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, RT]): UserDefinedFunction = {
    registerScalaUDF(name, func, implicitly[TypeTag[RT]], implicitly[TypeTag[A1]], implicitly[TypeTag[A2]], implicitly[TypeTag[A3]], implicitly[TypeTag[A4]], implicitly[TypeTag[A5]], implicitly[TypeTag[A6]], implicitly[TypeTag[A7]], implicitly[TypeTag[A8]], implicitly[TypeTag[A9]], implicitly[TypeTag[A10]], implicitly[TypeTag[A11]], implicitly[TypeTag[A12]], implicitly[TypeTag[A13]], implicitly[TypeTag[A14]], implicitly[TypeTag[A15]], implicitly[TypeTag[A16]], implicitly[TypeTag[A17]], implicitly[TypeTag[A18]], implicitly[TypeTag[A19]])
  }

  /**
   * Registers a deterministic Scala closure of 20 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag, A19: TypeTag, A20: TypeTag](name: String, func: Function20[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, RT]): UserDefinedFunction = {
    registerScalaUDF(name, func, implicitly[TypeTag[RT]], implicitly[TypeTag[A1]], implicitly[TypeTag[A2]], implicitly[TypeTag[A3]], implicitly[TypeTag[A4]], implicitly[TypeTag[A5]], implicitly[TypeTag[A6]], implicitly[TypeTag[A7]], implicitly[TypeTag[A8]], implicitly[TypeTag[A9]], implicitly[TypeTag[A10]], implicitly[TypeTag[A11]], implicitly[TypeTag[A12]], implicitly[TypeTag[A13]], implicitly[TypeTag[A14]], implicitly[TypeTag[A15]], implicitly[TypeTag[A16]], implicitly[TypeTag[A17]], implicitly[TypeTag[A18]], implicitly[TypeTag[A19]], implicitly[TypeTag[A20]])
  }

  /**
   * Registers a deterministic Scala closure of 21 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag, A19: TypeTag, A20: TypeTag, A21: TypeTag](name: String, func: Function21[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21, RT]): UserDefinedFunction = {
    registerScalaUDF(name, func, implicitly[TypeTag[RT]], implicitly[TypeTag[A1]], implicitly[TypeTag[A2]], implicitly[TypeTag[A3]], implicitly[TypeTag[A4]], implicitly[TypeTag[A5]], implicitly[TypeTag[A6]], implicitly[TypeTag[A7]], implicitly[TypeTag[A8]], implicitly[TypeTag[A9]], implicitly[TypeTag[A10]], implicitly[TypeTag[A11]], implicitly[TypeTag[A12]], implicitly[TypeTag[A13]], implicitly[TypeTag[A14]], implicitly[TypeTag[A15]], implicitly[TypeTag[A16]], implicitly[TypeTag[A17]], implicitly[TypeTag[A18]], implicitly[TypeTag[A19]], implicitly[TypeTag[A20]], implicitly[TypeTag[A21]])
  }

  /**
   * Registers a deterministic Scala closure of 22 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag, A19: TypeTag, A20: TypeTag, A21: TypeTag, A22: TypeTag](name: String, func: Function22[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21, A22, RT]): UserDefinedFunction = {
    registerScalaUDF(name, func, implicitly[TypeTag[RT]], implicitly[TypeTag[A1]], implicitly[TypeTag[A2]], implicitly[TypeTag[A3]], implicitly[TypeTag[A4]], implicitly[TypeTag[A5]], implicitly[TypeTag[A6]], implicitly[TypeTag[A7]], implicitly[TypeTag[A8]], implicitly[TypeTag[A9]], implicitly[TypeTag[A10]], implicitly[TypeTag[A11]], implicitly[TypeTag[A12]], implicitly[TypeTag[A13]], implicitly[TypeTag[A14]], implicitly[TypeTag[A15]], implicitly[TypeTag[A16]], implicitly[TypeTag[A17]], implicitly[TypeTag[A18]], implicitly[TypeTag[A19]], implicitly[TypeTag[A20]], implicitly[TypeTag[A21]], implicitly[TypeTag[A22]])
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Register a Java UDF class using reflection, for use from pyspark
   *
   * @param name   udf name
   * @param className   fully qualified class name of udf
   * @param returnDataType  return type of udf. If it is null, spark would try to infer
   *                        via reflection.
   */
  private[sql] def registerJava(name: String, className: String, returnDataType: DataType): Unit = {

    try {
      val clazz = Utils.classForName[AnyRef](className)
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

  /**
   * Register a Java UDAF class using reflection, for use from pyspark
   *
   * @param name     UDAF name
   * @param className    fully qualified class name of UDAF
   */
  private[sql] def registerJavaUDAF(name: String, className: String): Unit = {
    try {
      val clazz = Utils.classForName[AnyRef](className)
      if (!classOf[UserDefinedAggregateFunction].isAssignableFrom(clazz)) {
        throw QueryCompilationErrors.classDoesNotImplementUserDefinedAggregateFunctionError(className)
      }
      val udaf = clazz.getConstructor().newInstance().asInstanceOf[UserDefinedAggregateFunction]
      register(name, udaf)
    } catch {
      case _: ClassNotFoundException => throw QueryCompilationErrors.cannotLoadClassNotOnClassPathError(className)
      case _: InstantiationException | _: IllegalArgumentException =>
        throw QueryCompilationErrors.classWithoutPublicNonArgumentConstructorError(className)
    }
  }

  /**
   * Register a deterministic Java UDF0 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF0[_], returnType: DataType): Unit = {
    val func = () => f.asInstanceOf[UDF0[Any]].call()
    registerJavaUDF(name, func, returnType, 0)
  }

  /**
   * Register a deterministic Java UDF1 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF1[_, _], returnType: DataType): Unit = {
    val func = f.asInstanceOf[UDF1[Any, Any]].call(_: Any)
    registerJavaUDF(name, func, returnType, 1)
  }

  /**
   * Register a deterministic Java UDF2 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF2[_, _, _], returnType: DataType): Unit = {
    val func = f.asInstanceOf[UDF2[Any, Any, Any]].call(_: Any, _: Any)
    registerJavaUDF(name, func, returnType, 2)
  }

  /**
   * Register a deterministic Java UDF3 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF3[_, _, _, _], returnType: DataType): Unit = {
    val func = f.asInstanceOf[UDF3[Any, Any, Any, Any]].call(_: Any, _: Any, _: Any)
    registerJavaUDF(name, func, returnType, 3)
  }

  /**
   * Register a deterministic Java UDF4 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF4[_, _, _, _, _], returnType: DataType): Unit = {
    val func = f.asInstanceOf[UDF4[Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any)
    registerJavaUDF(name, func, returnType, 4)
  }

  /**
   * Register a deterministic Java UDF5 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF5[_, _, _, _, _, _], returnType: DataType): Unit = {
    val func = f.asInstanceOf[UDF5[Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any)
    registerJavaUDF(name, func, returnType, 5)
  }

  /**
   * Register a deterministic Java UDF6 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF6[_, _, _, _, _, _, _], returnType: DataType): Unit = {
    val func = f.asInstanceOf[UDF6[Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    registerJavaUDF(name, func, returnType, 6)
  }

  /**
   * Register a deterministic Java UDF7 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF7[_, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val func = f.asInstanceOf[UDF7[Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    registerJavaUDF(name, func, returnType, 7)
  }

  /**
   * Register a deterministic Java UDF8 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF8[_, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val func = f.asInstanceOf[UDF8[Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    registerJavaUDF(name, func, returnType, 8)
  }

  /**
   * Register a deterministic Java UDF9 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF9[_, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val func = f.asInstanceOf[UDF9[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    registerJavaUDF(name, func, returnType, 9)
  }

  /**
   * Register a deterministic Java UDF10 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF10[_, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val func = f.asInstanceOf[UDF10[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    registerJavaUDF(name, func, returnType, 10)
  }

  /**
   * Register a deterministic Java UDF11 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF11[_, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val func = f.asInstanceOf[UDF11[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    registerJavaUDF(name, func, returnType, 11)
  }

  /**
   * Register a deterministic Java UDF12 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF12[_, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val func = f.asInstanceOf[UDF12[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    registerJavaUDF(name, func, returnType, 12)
  }

  /**
   * Register a deterministic Java UDF13 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF13[_, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val func = f.asInstanceOf[UDF13[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    registerJavaUDF(name, func, returnType, 13)
  }

  /**
   * Register a deterministic Java UDF14 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF14[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val func = f.asInstanceOf[UDF14[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    registerJavaUDF(name, func, returnType, 14)
  }

  /**
   * Register a deterministic Java UDF15 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF15[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val func = f.asInstanceOf[UDF15[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    registerJavaUDF(name, func, returnType, 15)
  }

  /**
   * Register a deterministic Java UDF16 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF16[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val func = f.asInstanceOf[UDF16[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    registerJavaUDF(name, func, returnType, 16)
  }

  /**
   * Register a deterministic Java UDF17 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF17[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val func = f.asInstanceOf[UDF17[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    registerJavaUDF(name, func, returnType, 17)
  }

  /**
   * Register a deterministic Java UDF18 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF18[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val func = f.asInstanceOf[UDF18[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    registerJavaUDF(name, func, returnType, 18)
  }

  /**
   * Register a deterministic Java UDF19 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF19[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val func = f.asInstanceOf[UDF19[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    registerJavaUDF(name, func, returnType, 19)
  }

  /**
   * Register a deterministic Java UDF20 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF20[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val func = f.asInstanceOf[UDF20[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    registerJavaUDF(name, func, returnType, 20)
  }

  /**
   * Register a deterministic Java UDF21 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF21[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val func = f.asInstanceOf[UDF21[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    registerJavaUDF(name, func, returnType, 21)
  }

  /**
   * Register a deterministic Java UDF22 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF22[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val func = f.asInstanceOf[UDF22[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    registerJavaUDF(name, func, returnType, 22)
  }

  // scalastyle:on line.size.limit

}
