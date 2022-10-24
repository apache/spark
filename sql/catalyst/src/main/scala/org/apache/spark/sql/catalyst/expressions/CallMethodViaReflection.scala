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

import java.lang.reflect.{Method, Modifier}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, TypeCheckResult}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{DataTypeMismatch, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.Cast.{toSQLExpr, toSQLType}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 * An expression that invokes a method on a class via reflection.
 *
 * For now, only types defined in `Reflect.typeMapping` are supported (basically primitives
 * and string) as input types, and the output is turned automatically to a string.
 *
 * Note that unlike Hive's reflect function, this expression calls only static methods
 * (i.e. does not support calling non-static methods).
 *
 * We should also look into how to consolidate this expression with
 * [[org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke]] in the future.
 *
 * @param children the first element should be a literal string for the class name,
 *                 and the second element should be a literal string for the method name,
 *                 and the remaining are input arguments to the Java method.
 */
@ExpressionDescription(
  usage = "_FUNC_(class, method[, arg1[, arg2 ..]]) - Calls a method with reflection.",
  examples = """
    Examples:
      > SELECT _FUNC_('java.util.UUID', 'randomUUID');
       c33fb387-8500-4bfa-81d2-6e0e3e930df2
      > SELECT _FUNC_('java.util.UUID', 'fromString', 'a5cf6c42-0c85-418f-af6c-3e4e5b1328f2');
       a5cf6c42-0c85-418f-af6c-3e4e5b1328f2
  """,
  since = "2.0.0",
  group = "misc_funcs")
case class CallMethodViaReflection(children: Seq[Expression])
  extends Nondeterministic with CodegenFallback {

  override def prettyName: String = getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("reflect")

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.size < 2) {
      DataTypeMismatch(
        errorSubClass = "WRONG_NUM_PARAMS",
        messageParameters = Map(
          "functionName" -> prettyName,
          "expectedNum" -> "> 1",
          "actualNum" -> children.length.toString))
    } else {
      val unexpectedParameter = children.zipWithIndex.collectFirst {
        case (e, 0) if !(e.dataType == StringType && e.foldable) =>
          DataTypeMismatch(
            errorSubClass = "NON_FOLDABLE_INPUT",
            messageParameters = Map(
              "inputName" -> "class",
              "inputType" -> toSQLType(StringType),
              "inputExpr" -> toSQLExpr(children.head)
            )
          )
        case (e, 1) if !(e.dataType == StringType && e.foldable) =>
          DataTypeMismatch(
            errorSubClass = "NON_FOLDABLE_INPUT",
            messageParameters = Map(
              "inputName" -> "method",
              "inputType" -> toSQLType(StringType),
              "inputExpr" -> toSQLExpr(children(1))
            )
          )
        case (e, idx) if idx > 1 && !CallMethodViaReflection.typeMapping.contains(e.dataType) =>
          DataTypeMismatch(
            errorSubClass = "UNEXPECTED_INPUT_TYPE",
            messageParameters = Map(
              "paramIndex" -> (idx + 1).toString,
              "requiredType" -> toSQLType(
                TypeCollection(BooleanType, ByteType, ShortType,
                  IntegerType, LongType, FloatType, DoubleType, StringType)),
              "inputSql" -> toSQLExpr(e),
              "inputType" -> toSQLType(e.dataType))
          )
      }

      unexpectedParameter match {
        case Some(mismatch) => mismatch
        case _ if !classExists =>
          DataTypeMismatch(
            errorSubClass = "UNEXPECTED_CLASS_TYPE",
            messageParameters = Map("className" -> className)
          )
        case _ if method == null =>
          DataTypeMismatch(
            errorSubClass = "UNEXPECTED_STATIC_METHOD",
            messageParameters = Map("methodName" -> methodName, "className" -> className)
          )
        case _ => TypeCheckSuccess
      }
    }
  }

  override def nullable: Boolean = true
  override val dataType: DataType = StringType
  override protected def initializeInternal(partitionIndex: Int): Unit = {}

  override protected def evalInternal(input: InternalRow): Any = {
    var i = 0
    while (i < argExprs.length) {
      buffer(i) = argExprs(i).eval(input).asInstanceOf[Object]
      // Convert if necessary. Based on the types defined in typeMapping, string is the only
      // type that needs conversion. If we support timestamps, dates, decimals, arrays, or maps
      // in the future, proper conversion needs to happen here too.
      if (buffer(i).isInstanceOf[UTF8String]) {
        buffer(i) = buffer(i).toString
      }
      i += 1
    }
    val ret = method.invoke(null, buffer : _*)
    UTF8String.fromString(String.valueOf(ret))
  }

  @transient private lazy val argExprs: Array[Expression] = children.drop(2).toArray

  /** Name of the class -- this has to be called after we verify children has at least two exprs. */
  @transient private lazy val className = children(0).eval().asInstanceOf[UTF8String].toString

  /** True if the class exists and can be loaded. */
  @transient private lazy val classExists = CallMethodViaReflection.classExists(className)

  /** Name of the method */
  @transient private lazy val methodName = children(1).eval(null).asInstanceOf[UTF8String].toString

  /** The reflection method. */
  @transient lazy val method: Method =
    CallMethodViaReflection.findMethod(className, methodName, argExprs.map(_.dataType)).orNull

  /** A temporary buffer used to hold intermediate results returned by children. */
  @transient private lazy val buffer = new Array[Object](argExprs.length)

  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[Expression]): CallMethodViaReflection = copy(children = newChildren)
}

object CallMethodViaReflection {
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
   * Returns true if the class can be found and loaded.
   */
  private def classExists(className: String): Boolean = {
    try {
      Utils.classForName(className)
      true
    } catch {
      case e: ClassNotFoundException => false
    }
  }

  /**
   * Finds a Java static method using reflection that matches the given argument types,
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
      } else if (!Modifier.isStatic(method.getModifiers)) {
        // Method must be static
        false
      } else if (candidateTypes.length != argTypes.length) {
        // Argument length must match
        false
      } else {
        // Argument type must match. That is, either the method's argument type matches one of the
        // acceptable types defined in typeMapping, or it is a super type of the acceptable types.
        candidateTypes.zip(argTypes).forall { case (candidateType, argType) =>
          typeMapping(argType).exists(candidateType.isAssignableFrom)
        }
      }
    }
  }
}
