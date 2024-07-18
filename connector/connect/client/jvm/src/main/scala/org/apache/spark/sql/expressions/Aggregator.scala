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

package org.apache.spark.sql.expressions

import scala.reflect.runtime.universe._

import org.apache.spark.connect.proto
import org.apache.spark.sql.{encoderFor, Encoder, TypedColumn}
import org.apache.spark.sql.catalyst.ScalaReflection

/**
 * A base class for user-defined aggregations, which can be used in `Dataset` operations to take
 * all of the elements of a group and reduce them to a single value.
 *
 * For example, the following aggregator extracts an `int` from a specific class and adds them up:
 * {{{
 *   case class Data(i: Int)
 *
 *   val customSummer =  new Aggregator[Data, Int, Int] {
 *     def zero: Int = 0
 *     def reduce(b: Int, a: Data): Int = b + a.i
 *     def merge(b1: Int, b2: Int): Int = b1 + b2
 *     def finish(r: Int): Int = r
 *     def bufferEncoder: Encoder[Int] = Encoders.scalaInt
 *     def outputEncoder: Encoder[Int] = Encoders.scalaInt
 *   }
 *
 *   spark.udf.register("customSummer", udaf(customSummer))
 *   val ds: Dataset[Data] = ...
 *   val aggregated = ds.selectExpr("customSummer(i)")
 * }}}
 *
 * Based loosely on Aggregator from Algebird: https://github.com/twitter/algebird
 *
 * @tparam IN
 *   The input type for the aggregation.
 * @tparam BUF
 *   The type of the intermediate value of the reduction.
 * @tparam OUT
 *   The type of the final output result.
 * @since 4.0.0
 */
@SerialVersionUID(2093413866369130093L)
abstract class Aggregator[-IN, BUF, OUT] extends Serializable {

  /**
   * A zero value for this aggregation. Should satisfy the property that any b + zero = b.
   * @since 4.0.0
   */
  def zero: BUF

  /**
   * Combine two values to produce a new value. For performance, the function may modify `b` and
   * return it instead of constructing new object for b.
   * @since 4.0.0
   */
  def reduce(b: BUF, a: IN): BUF

  /**
   * Merge two intermediate values.
   * @since 4.0.0
   */
  def merge(b1: BUF, b2: BUF): BUF

  /**
   * Transform the output of the reduction.
   * @since 4.0.0
   */
  def finish(reduction: BUF): OUT

  /**
   * Specifies the `Encoder` for the intermediate value type.
   * @since 4.0.0
   */
  def bufferEncoder: Encoder[BUF]

  /**
   * Specifies the `Encoder` for the final output value type.
   * @since 4.0.0
   */
  def outputEncoder: Encoder[OUT]

  /**
   * Returns this `Aggregator` as a `TypedColumn` that can be used in `Dataset` operations.
   * @since 4.0.0
   */
  def toColumn: TypedColumn[IN, OUT] = {
    val ttpe = getInputTypeTag[IN]
    val inputEncoder = ScalaReflection.encoderFor(ttpe)
    val udaf =
      ScalaUserDefinedFunction(
        this,
        Seq(inputEncoder),
        encoderFor(outputEncoder),
        aggregate = true)

    val builder = proto.TypedAggregateExpression.newBuilder()
    builder.setScalarScalaUdf(udaf.udf)
    val expr = proto.Expression.newBuilder().setTypedAggregateExpression(builder).build()

    new TypedColumn(expr, encoderFor(outputEncoder))
  }

  private final def getInputTypeTag[T]: TypeTag[T] = {
    val mirror = runtimeMirror(this.getClass.getClassLoader)
    val tpe = mirror.classSymbol(this.getClass).toType
    // Find the most generic (last in the tree) Aggregator class
    val baseAgg =
      tpe.baseClasses
        .findLast(_.asClass.toType <:< typeOf[Aggregator[_, _, _]])
        .getOrElse(throw new IllegalStateException("Could not find the Aggregator base class."))
    val typeArgs = tpe.baseType(baseAgg).typeArgs
    assert(
      typeArgs.length == 3,
      s"Aggregator should have 3 type arguments, " +
        s"but found ${typeArgs.length}: ${typeArgs.mkString}.")
    val inType = typeArgs.head

    import scala.reflect.api._
    def areCompatibleMirrors(one: Mirror[_], another: Mirror[_]): Boolean = {
      def checkAllParents(target: JavaMirror, candidate: JavaMirror): Boolean = {
        var current = candidate.classLoader
        while (current != null) {
          if (current == target.classLoader) {
            return true
          }
          current = current.getParent
        }
        false
      }

      (one, another) match {
        case (a: JavaMirror, b: JavaMirror) =>
          a == b || checkAllParents(a, b) || checkAllParents(b, a)
        case _ => one == another
      }
    }

    TypeTag(
      mirror,
      new TypeCreator {
        def apply[U <: Universe with Singleton](m: Mirror[U]): U#Type =
          if (areCompatibleMirrors(m, mirror)) {
            inType.asInstanceOf[U#Type]
          } else {
            throw new IllegalArgumentException(
              s"Type tag defined in [$mirror] cannot be migrated to another mirror [$m].")
          }
      })
  }
}
