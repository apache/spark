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

package org.apache.spark.sql.fuzzing

import java.lang.reflect.InvocationTargetException

import scala.reflect.runtime.{universe => ru}
import scala.util.{Random, Try}

import scalaz._, Scalaz._

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.types._

object ReflectiveFuzzing {

  import DataFrameFuzzingUtils._

  private implicit val m: ru.Mirror = ru.runtimeMirror(this.getClass.getClassLoader)

  /**
   * Method parameter types for which the fuzzer can supply random values. This list is used to
   * filter out methods that we don't know how to call.
   */
  private val whitelistedParameterTypes = Set(
    m.universe.typeOf[DataFrame],
    m.universe.typeOf[Dataset[_]],
    m.universe.typeOf[Seq[Column]],
    m.universe.typeOf[Column],
    m.universe.typeOf[String],
    m.universe.typeOf[Seq[String]]
  )

  /**
   * A list of candidate DataFrame methods that the fuzzer will try to call. Excludes private
   * methods and methods with parameters that we don't know how to supply.
   */
  private val dataFrameTransformations: Seq[ru.MethodSymbol] = {
    val dfType = m.universe.typeOf[Dataset[_]]
    dfType.members
      .filter(_.isPublic)
      .filter(_.isMethod)
      .map(_.asMethod)
      .filter(_.returnType <:< dfType)
      .filterNot(_.isConstructor)
      .filter { m =>
      m.paramss.flatten.forall { p =>
        whitelistedParameterTypes.exists { t => p.typeSignature <:< t }
      }
    }
    .filterNot(_.name.toString == "drop") // since this can lead to a DataFrame with no columns
    .filterNot(_.name.toString == "describe") // since we cannot run all queries on describe output
    .filterNot(_.name.toString == "dropDuplicates")
    .filterNot(_.name.toString == "toDF") // since this is effectively a no-op
    .filterNot(_.name.toString == "toSchemaRDD") // since this is effectively a no-op
    .toSeq
  }

  /**
   * Given a Dataframe and a method, try to choose a set of arguments to call that method with.
   *
   * @param df the data frame to transform
   * @param method the method to call
   * @param typeConstraint an optional type constraint governing the types of the parameters.
   * @return
   */
  def getParamValues(
      df: DataFrame,
      method: ru.MethodSymbol,
      typeConstraint: DataType => Boolean = _ => true): Option[List[Any]] = {
    val params = method.paramss.flatten // We don't use multiple parameter lists
    val maybeValues: List[Option[Any]] = params.map { p =>
      val t = p.typeSignature
      if (t <:< ru.typeOf[Dataset[_]]) {
        randomChoice(
          df ::
            // TODO(josh): restore ability to generate new random DataFrames for use in joins.
            // dataGenerator.randomDataFrame(numCols = Random.nextInt(4) + 1, numRows = 100) ::
          Nil
        ).some
      } else if (t =:= ru.typeOf[Column]) {
        getRandomColumnName(df, typeConstraint).map(df.col)
      } else if (t =:= ru.typeOf[String]) {
        if (p.name == "joinType") {
          randomChoice(JoinType.supportedJoinTypes).some
        } else {
          getRandomColumnName(df, typeConstraint).map(df.col)
        }
      } else if (t <:< ru.typeOf[Seq[Column]]) {
        Seq.fill(Random.nextInt(2) + 1)(
          getRandomColumnName(df, typeConstraint).map(df.col)).flatten.some
      } else if (t <:< ru.typeOf[Seq[String]]) {
        Seq.fill(Random.nextInt(2) + 1)(
          getRandomColumnName(df, typeConstraint).map(df.col)).flatten.some
      } else {
        None
      }
    }
    maybeValues.sequence
  }

  def getTransformation(df: DataFrame): Option[DataFrameTransformation] = {
    val method: ru.MethodSymbol = DataFrameFuzzingUtils.randomChoice(dataFrameTransformations)
    val values: Option[Seq[Any]] = {
      def validateValues(vs: Seq[Any]): Try[Seq[Any]] = {
        Try(CallTransformReflectively(method, vs).apply(df)).map(_ => vs)
      }
      getParamValues(df, method).map { (vs: Seq[Any]) =>
        validateValues(vs).recoverWith {
          case e: AnalysisException if e.getMessage.contains("is not a boolean") =>
            Try(getParamValues(df, method, _ == BooleanType).get).flatMap(validateValues)
          case e: AnalysisException
            if e.getMessage.contains("is not supported for columns of type") =>
            Try(getParamValues(df, method, _.isInstanceOf[AtomicType]).get).flatMap(validateValues)
        }
      }.flatMap(_.toOption)
    }
    values.map(vs => CallTransformReflectively(method, vs))
  }
}

case class CallTransformReflectively(
    method: ru.MethodSymbol,
    args: Seq[Any])(
    implicit runtimeMirror: ru.Mirror) extends DataFrameTransformation {

  override def apply(df: DataFrame): DataFrame = {
    val reflectedMethod: ru.MethodMirror = runtimeMirror.reflect(df).reflectMethod(method)
    try {
      reflectedMethod.apply(args: _*).asInstanceOf[DataFrame]
    } catch {
      case e: InvocationTargetException => throw e.getCause
    }
  }

  override def toString(): String = {
    s"${method.name}(${args.map(_.toString).mkString(", ")})"
  }
}
