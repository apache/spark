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

package org.apache.spark.sql.catalyst.expressions

import java.lang.reflect.Method

import scala.util.Try

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 * An expression that invokes a method on a class via reflection.
 *
 * @param children the first element should be a literal string for the class name,
 *                 and the second element should be a literal string for the method name,
 *                 and the remaining are input arguments to the Java method.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(class,method[,arg1[,arg2..]]) calls method with reflection",
  extended = "> SELECT _FUNC_('java.util.UUID', 'randomUUID');\nc33fb387-8500-4bfa-81d2-6e0e3e930df2")
// scalastyle:on line.size.limit
case class JavaMethodReflect(children: Seq[Expression])
  extends Expression with CodegenFallback {
  import JavaMethodReflect._

  override def prettyName: String = "reflect"

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.size < 2) {
      TypeCheckFailure("requires at least two arguments")
    } else if (!children.take(2).forall(e => e.dataType == StringType && e.foldable)) {
      // The first two arguments must be string type.
      TypeCheckFailure("first two arguments should be string literals")
    } else if (method == null) {
      TypeCheckFailure("cannot find a method that matches the argument types")
    } else {
      TypeCheckSuccess
    }
  }

  override def deterministic: Boolean = false
  override def nullable: Boolean = true
  override val dataType: DataType = StringType

  override def eval(input: InternalRow): Any = {
    var i = 0
    while (i < argExprs.length) {
      buffer(i) = argExprs(i).eval(input).asInstanceOf[Object]
      // Convert if necessary. Based on the types defined in typeMapping, string is the only
      // type that needs conversion.
      if (buffer(i).isInstanceOf[UTF8String]) {
        buffer(i) = buffer(i).toString
      }
      i += 1
    }
    UTF8String.fromString(String.valueOf(method.invoke(obj, buffer : _*)))
  }

  @transient private lazy val argExprs: Array[Expression] = children.drop(2).toArray

  /** Name of the class -- this has to be called after we verify children has at least two exprs. */
  @transient private lazy val className = children(0).eval(null).asInstanceOf[UTF8String].toString

  /** The reflection method. */
  @transient lazy val method: Method = {
    val methodName = children(1).eval(null).asInstanceOf[UTF8String].toString
    findMethod(className, methodName, argExprs.map(_.dataType)).orNull
  }

  /** If the class has a no-arg ctor, instantiate the object. Otherwise, obj is null. */
  @transient private lazy val obj: Object = instantiate(className).orNull.asInstanceOf[Object]

  /** A temporary buffer used to hold intermediate results returned by children. */
  @transient private lazy val buffer = new Array[Object](argExprs.length)
}

object JavaMethodReflect {
  /** Mapping from Spark's type to acceptable JVM types. */
  val typeMapping = Map[DataType, Seq[Class[_]]](
    BooleanType -> Seq(classOf[java.lang.Boolean], classOf[Boolean]),
    ByteType -> Seq(classOf[java.lang.Byte], classOf[Byte]),
    ShortType -> Seq(classOf[java.lang.Short], classOf[Short]),
    IntegerType -> Seq(classOf[java.lang.Integer], classOf[Int]),
    LongType -> Seq(classOf[java.lang.Long], classOf[Long]),
    FloatType -> Seq(classOf[java.lang.Float], classOf[Float]),
    DoubleType -> Seq(classOf[java.lang.Double], classOf[Double]),
    StringType -> Seq(classOf[String])
  )

  /**
   * Finds a Java method using reflection that matches the given argument types,
   * and whose return type is string.
   *
   * The types sequence must be the valid types defined in [[typeMapping]].
   *
   * This is made public for unit testing.
   */
  def findMethod(className: String, methodName: String, argTypes: Seq[DataType]): Option[Method] = {
    val clazz: Class[_] = Utils.classForName(className)
    clazz.getMethods.find { method =>
      val candidateTypes = method.getParameterTypes
      if (method.getName != methodName) {
        // Name must match
        false
      } else if (candidateTypes.length != argTypes.length) {
        // Argument length must match
        false
      } else {
        // Argument type must match. That is, either the method's argument type matches one of the
        // acceptable types defined in typeMapping, or it is a super type of the acceptable types.
        candidateTypes.zip(argTypes).forall { case (candidateType, argType) =>
          typeMapping(argType).exists { acceptableType =>
            acceptableType == candidateType || candidateType.isAssignableFrom(acceptableType)
          }
        }
      }
    }
  }

  /**
   * Instantiates the class if there is a no-arg constructor.
   * This is made public for unit testing.
   */
  def instantiate(className: String): Option[Any] = {
    val clazz: Class[_] = Utils.classForName(className)
    Try(clazz.getDeclaredConstructor()).toOption.flatMap { ctor =>
      ctor.setAccessible(true)
      Try(ctor.newInstance()).toOption
    }
  }
}
