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

package org.apache.spark.sql.catalyst.expressions.objects

import java.lang.reflect.{Method, Modifier}

import scala.collection.immutable
import scala.collection.mutable
import scala.collection.mutable.Builder
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.Try

import org.apache.commons.lang3.reflect.MethodUtils

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.serializer._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.encoders.EncoderUtils
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.trees.TernaryLike
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types._
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * Common base class for [[StaticInvoke]], [[Invoke]], and [[NewInstance]].
 */
trait InvokeLike extends Expression with NonSQLExpression with ImplicitCastInputTypes {

  def arguments: Seq[Expression]
  protected def argumentTypes: Seq[AbstractDataType] = inputTypes

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!argumentTypes.isEmpty && argumentTypes.length != arguments.length) {
      TypeCheckResult.DataTypeMismatch(
        errorSubClass = "WRONG_NUM_ARG_TYPES",
        messageParameters = Map(
          "expectedNum" -> arguments.length.toString,
          "actualNum" -> argumentTypes.length.toString))
    } else {
      super.checkInputDataTypes()
    }
  }

  def propagateNull: Boolean

  // InvokeLike is stateful because of the evaluatedArgs Array
  override def stateful: Boolean = true

  override def foldable: Boolean =
    children.forall(_.foldable) && deterministic && trustedSerializable(dataType)
  protected lazy val needNullCheck: Boolean = needNullCheckForIndex.contains(true)
  protected lazy val needNullCheckForIndex: Array[Boolean] =
    arguments.map(a => a.nullable && (propagateNull ||
        EncoderUtils.dataTypeJavaClass(a.dataType).isPrimitive)).toArray
  protected lazy val evaluatedArgs: Array[Object] = new Array[Object](arguments.length)
  private lazy val boxingFn: Any => Any =
    EncoderUtils.typeBoxedJavaMapping
      .get(dataType)
      .map(cls => v => cls.cast(v))
      .getOrElse(identity)

  // Returns true if we can trust all values of the given DataType can be serialized.
  private def trustedSerializable(dt: DataType): Boolean = {
    // Right now we conservatively block all ObjectType (Java objects) regardless of
    // serializability, because the type-level info with java.io.Serializable and
    // java.io.Externalizable marker interfaces are not strong guarantees.
    // This restriction can be relaxed in the future to expose more optimizations.
    !dt.existsRecursively(_.isInstanceOf[ObjectType])
  }

  /**
   * Prepares codes for arguments.
   *
   * - generate codes for argument.
   * - use ctx.splitExpressions() to not exceed 64kb JVM limit while preparing arguments.
   * - avoid some of nullability checking which are not needed because the expression is not
   *   nullable.
   * - when needNullCheck == true, short circuit if we found one of arguments is null because
   *   preparing rest of arguments can be skipped in the case.
   *
   * @param ctx a [[CodegenContext]]
   * @return (code to prepare arguments, argument string, result of argument null check)
   */
  def prepareArguments(ctx: CodegenContext): (String, String, ExprValue) = {

    val resultIsNull = if (needNullCheck) {
      val resultIsNull = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "resultIsNull")
      JavaCode.isNullGlobal(resultIsNull)
    } else {
      FalseLiteral
    }
    val argValues = arguments.map { e =>
      val argValue = ctx.addMutableState(CodeGenerator.javaType(e.dataType), "argValue")
      argValue
    }

    val argCodes = if (needNullCheck) {
      val reset = s"$resultIsNull = false;"
      val argCodes = arguments.zipWithIndex.map { case (e, i) =>
        val expr = e.genCode(ctx)
        val updateResultIsNull = if (needNullCheckForIndex(i)) {
          s"$resultIsNull = ${expr.isNull};"
        } else {
          ""
        }
        s"""
          if (!$resultIsNull) {
            ${expr.code}
            $updateResultIsNull
            ${argValues(i)} = ${expr.value};
          }
        """
      }
      reset +: argCodes
    } else {
      arguments.zipWithIndex.map { case (e, i) =>
        val expr = e.genCode(ctx)
        s"""
          ${expr.code}
          ${argValues(i)} = ${expr.value};
        """
      }
    }
    val argCode = ctx.splitExpressionsWithCurrentInputs(argCodes)

    (argCode, argValues.mkString(", "), resultIsNull)
  }

  /**
   * Evaluate each argument with a given row, invoke a method with a given object and arguments,
   * and cast a return value if the return type can be mapped to a Java Boxed type
   *
   * @param obj the object for the method to be called. If null, perform s static method call
   * @param method the method object to be called
   * @param input the row used for evaluating arguments
   * @return the return object of a method call
   */
  def invoke(obj: Any, method: Method, input: InternalRow): Any = {
    var i = 0
    val len = arguments.length
    var resultNull = false
    while (i < len) {
      val result = arguments(i).eval(input).asInstanceOf[Object]
      evaluatedArgs(i) = result
      resultNull = resultNull || (result == null && needNullCheckForIndex(i))
      i += 1
    }
    if (needNullCheck && resultNull) {
      // return null if one of arguments is null
      null
    } else {
      val ret = try {
        method.invoke(obj, evaluatedArgs: _*)
      } catch {
        // Re-throw the original exception.
        case e: java.lang.reflect.InvocationTargetException if e.getCause != null =>
          throw e.getCause
      }
      boxingFn(ret)
    }
  }

  final def findMethod(cls: Class[_], functionName: String, argClasses: Seq[Class[_]]): Method = {
    val method = MethodUtils.getMatchingAccessibleMethod(cls, functionName, argClasses: _*)
    if (method == null) {
      throw QueryExecutionErrors.methodNotFoundError(cls, functionName, argClasses)
    } else {
      method
    }
  }
}

/**
 * Common trait for [[DecodeUsingSerializer]] and [[EncodeUsingSerializer]]
 */
trait SerializerSupport {
  /**
   * If true, Kryo serialization is used, otherwise the Java one is used
   */
  val kryo: Boolean

  /**
   * The serializer instance to be used for serialization/deserialization in interpreted execution
   */
  lazy val serializerInstance: SerializerInstance = SerializerSupport.newSerializer(kryo)

  /**
   * Adds a immutable state to the generated class containing a reference to the serializer.
   * @return a string containing the name of the variable referencing the serializer
   */
  def addImmutableSerializerIfNeeded(ctx: CodegenContext): String = {
    val (serializerInstance, serializerInstanceClass) = {
      if (kryo) {
        ("kryoSerializer",
          classOf[KryoSerializerInstance].getName)
      } else {
        ("javaSerializer",
          classOf[JavaSerializerInstance].getName)
      }
    }
    val newSerializerMethod = s"${classOf[SerializerSupport].getName}$$.MODULE$$.newSerializer"
    // Code to initialize the serializer
    ctx.addImmutableStateIfNotExists(serializerInstanceClass, serializerInstance, v =>
      s"""
         |$v = ($serializerInstanceClass) $newSerializerMethod($kryo);
       """.stripMargin)
    serializerInstance
  }
}

object SerializerSupport {
  /**
   * It creates a new `SerializerInstance` which is either a `KryoSerializerInstance` (is
   * `useKryo` is set to `true`) or a `JavaSerializerInstance`.
   */
  def newSerializer(useKryo: Boolean): SerializerInstance = {
    // try conf from env, otherwise create a new one
    val conf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf)
    val s = if (useKryo) {
      new KryoSerializer(conf)
    } else {
      new JavaSerializer(conf)
    }
    s.newInstance()
  }
}

/**
 * Invokes a static function, returning the result.  By default, any of the arguments being null
 * will result in returning null instead of calling the function.
 *
 * @param staticObject The target of the static call.  This can either be the object itself
 *                     (methods defined on scala objects), or the class object
 *                     (static methods defined in java).
 * @param dataType The expected return type of the function call
 * @param functionName The name of the method to call.
 * @param arguments An optional list of expressions to pass as arguments to the function.
 * @param inputTypes A list of data types specifying the input types for the method to be invoked.
 *                   If enabled, it must have the same length as [[arguments]]. In case an input
 *                   type differs from the actual argument type, Spark will try to perform
 *                   type coercion and insert cast whenever necessary before invoking the method.
 *                   The above is disabled if this is empty.
 * @param propagateNull When true, and any of the arguments is null, null will be returned instead
 *                      of calling the function. Also note: when this is false but any of the
 *                      arguments is of primitive type and is null, null also will be returned
 *                      without invoking the function.
 * @param returnNullable When false, indicating the invoked method will always return
 *                       non-null value.
 * @param isDeterministic Whether the method invocation is deterministic or not. If false, Spark
 *                        will not apply certain optimizations such as constant folding.
 * @param scalarFunction the [[ScalarFunction]] object if this is calling the magic method of the
 *                       [[ScalarFunction]] otherwise is unset.
 */
case class StaticInvoke(
    staticObject: Class[_],
    dataType: DataType,
    functionName: String,
    arguments: Seq[Expression] = Nil,
    inputTypes: Seq[AbstractDataType] = Nil,
    propagateNull: Boolean = true,
    returnNullable: Boolean = true,
    isDeterministic: Boolean = true,
    scalarFunction: Option[ScalarFunction[_]] = None) extends InvokeLike {

  val objectName = staticObject.getName.stripSuffix("$")
  val cls = if (staticObject.getName == objectName) {
    staticObject
  } else {
    Utils.classForName(objectName)
  }

  override def nullable: Boolean = needNullCheck || returnNullable
  override def children: Seq[Expression] = arguments
  override lazy val deterministic: Boolean = isDeterministic && arguments.forall(_.deterministic)

  lazy val argClasses = EncoderUtils.expressionJavaClasses(arguments)
  @transient lazy val method = findMethod(cls, functionName, argClasses)

  override def eval(input: InternalRow): Any = {
    invoke(null, method, input)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaType = CodeGenerator.javaType(dataType)

    val (argCode, argString, resultIsNull) = prepareArguments(ctx)

    val callFunc = s"$objectName.$functionName($argString)"

    val prepareIsNull = if (nullable) {
      s"boolean ${ev.isNull} = $resultIsNull;"
    } else {
      ev.isNull = FalseLiteral
      ""
    }

    val evaluate = if (returnNullable && !method.getReturnType.isPrimitive) {
      if (CodeGenerator.defaultValue(dataType) == "null") {
        s"""
          ${ev.value} = $callFunc;
          ${ev.isNull} = ${ev.value} == null;
        """
      } else {
        val boxedResult = ctx.freshName("boxedResult")
        s"""
          ${CodeGenerator.boxedType(dataType)} $boxedResult = $callFunc;
          ${ev.isNull} = $boxedResult == null;
          if (!${ev.isNull}) {
            ${ev.value} = $boxedResult;
          }
        """
      }
    } else {
      s"${ev.value} = $callFunc;"
    }

    val code = code"""
      $argCode
      $prepareIsNull
      $javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
      if (!$resultIsNull) {
        $evaluate
      }
     """
    ev.copy(code = code)
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(arguments = newChildren)

  override protected def stringArgs: Iterator[Any] = {
    if (scalarFunction.nonEmpty) {
      super.stringArgs
    } else {
      super.stringArgs.toSeq.dropRight(1).iterator
    }
  }
}

/**
 * Calls the specified function on an object, optionally passing arguments.  If the `targetObject`
 * expression evaluates to null then null will be returned.
 *
 * In some cases, due to erasure, the schema may expect a primitive type when in fact the method
 * is returning java.lang.Object.  In this case, we will generate code that attempts to unbox the
 * value automatically.
 *
 * @param targetObject An expression that will return the object to call the method on.
 * @param functionName The name of the method to call.
 * @param dataType The expected return type of the function.
 * @param arguments An optional list of expressions, whose evaluation will be passed to the
 *                 function.
 * @param methodInputTypes A list of data types specifying the input types for the method to be
 *                         invoked. If enabled, it must have the same length as [[arguments]]. In
 *                         case an input type differs from the actual argument type, Spark will
 *                         try to perform type coercion and insert cast whenever necessary before
 *                         invoking the method. The type coercion is disabled if this is empty.
 * @param propagateNull When true, and any of the arguments is null, null will be returned instead
 *                      of calling the function. Also note: when this is false but any of the
 *                      arguments is of primitive type and is null, null also will be returned
 *                      without invoking the function.
 * @param returnNullable When false, indicating the invoked method will always return
 *                       non-null value.
 * @param isDeterministic Whether the method invocation is deterministic or not. If false, Spark
 *                        will not apply certain optimizations such as constant folding.
 */
case class Invoke(
    targetObject: Expression,
    functionName: String,
    dataType: DataType,
    arguments: Seq[Expression] = Nil,
    methodInputTypes: Seq[AbstractDataType] = Nil,
    propagateNull: Boolean = true,
    returnNullable : Boolean = true,
    isDeterministic: Boolean = true) extends InvokeLike {

  lazy val argClasses = EncoderUtils.expressionJavaClasses(arguments)

  final override val nodePatterns: Seq[TreePattern] = Seq(INVOKE)

  override def nullable: Boolean = targetObject.nullable || needNullCheck || returnNullable
  override def children: Seq[Expression] = targetObject +: arguments
  override lazy val deterministic: Boolean = isDeterministic && arguments.forall(_.deterministic)
  override def inputTypes: Seq[AbstractDataType] =
    if (methodInputTypes.nonEmpty) {
      Seq(targetObject.dataType) ++ methodInputTypes
    } else {
      Nil
    }
  override protected def argumentTypes: Seq[AbstractDataType] = methodInputTypes

  private lazy val encodedFunctionName = ScalaReflection.encodeFieldNameToIdentifier(functionName)

  @transient lazy val method = targetObject.dataType match {
    case ObjectType(cls) =>
      Some(findMethod(cls, encodedFunctionName, argClasses))
    case _ => None
  }

  override def eval(input: InternalRow): Any = {
    val obj = targetObject.eval(input)
    if (obj == null) {
      // return null if obj is null
      null
    } else {
      val invokeMethod = if (method.isDefined) {
        method.get
      } else {
        obj.getClass.getMethod(functionName, argClasses: _*)
      }
      invoke(obj, invokeMethod, input)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaType = CodeGenerator.javaType(dataType)
    val obj = targetObject.genCode(ctx)

    val (argCode, argString, resultIsNull) = prepareArguments(ctx)

    val returnPrimitive = method.isDefined && method.get.getReturnType.isPrimitive
    val needTryCatch = method.isDefined && method.get.getExceptionTypes.nonEmpty

    def getFuncResult(resultVal: String, funcCall: String): String = if (needTryCatch) {
      s"""
        try {
          $resultVal = $funcCall;
        } catch (Exception e) {
          org.apache.spark.unsafe.Platform.throwException(e);
        }
      """
    } else {
      s"$resultVal = $funcCall;"
    }

    val evaluate = if (returnPrimitive) {
      getFuncResult(ev.value, s"${obj.value}.$encodedFunctionName($argString)")
    } else {
      val funcResult = ctx.freshName("funcResult")
      // If the function can return null, we do an extra check to make sure our null bit is still
      // set correctly.
      val assignResult = if (!returnNullable) {
        s"${ev.value} = (${CodeGenerator.boxedType(javaType)}) $funcResult;"
      } else {
        s"""
          if ($funcResult != null) {
            ${ev.value} = (${CodeGenerator.boxedType(javaType)}) $funcResult;
          } else {
            ${ev.isNull} = true;
          }
        """
      }
      s"""
        Object $funcResult = null;
        ${getFuncResult(funcResult, s"${obj.value}.$encodedFunctionName($argString)")}
        $assignResult
      """
    }

    val mainEvalCode =
      code"""
         |$argCode
         |${ev.isNull} = $resultIsNull;
         |if (!${ev.isNull}) {
         |  $evaluate
         |}
         |""".stripMargin

    val evalWithNullCheck = if (targetObject.nullable) {
      code"""
         |if (!${obj.isNull}) {
         |  $mainEvalCode
         |}
         |""".stripMargin
    } else {
      mainEvalCode
    }

    val code = obj.code + code"""
      boolean ${ev.isNull} = true;
      $javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
      $evalWithNullCheck
     """
    ev.copy(code = code)
  }

  override def toString: String = s"$targetObject.$functionName"

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Invoke =
    copy(targetObject = newChildren.head, arguments = newChildren.tail)
}

object NewInstance {
  def apply(
      cls: Class[_],
      arguments: Seq[Expression],
      dataType: DataType,
      propagateNull: Boolean = true): NewInstance =
    new NewInstance(cls, arguments, inputTypes = Nil, propagateNull, dataType, None)
}

/**
 * Constructs a new instance of the given class, using the result of evaluating the specified
 * expressions as arguments.
 *
 * @param cls The class to construct.
 * @param arguments A list of expression to use as arguments to the constructor.
 * @param inputTypes A list of data types specifying the input types for the method to be invoked.
 *                   If enabled, it must have the same length as [[arguments]]. In case an input
 *                   type differs from the actual argument type, Spark will try to perform
 *                   type coercion and insert cast whenever necessary before invoking the method.
 *                   The above is disabled if this is empty.
 * @param propagateNull When true, if any of the arguments is null, then null will be returned
 *                      instead of trying to construct the object. Also note: when this is false
 *                      but any of the arguments is of primitive type and is null, null also will
 *                      be returned without constructing the object.
 * @param dataType The type of object being constructed, as a Spark SQL datatype.  This allows you
 *                 to manually specify the type when the object in question is a valid internal
 *                 representation (i.e. ArrayData) instead of an object.
 * @param outerPointer If the object being constructed is an inner class, the outerPointer for the
 *                     containing class must be specified. This parameter is defined as an optional
 *                     function, which allows us to get the outer pointer lazily,and it's useful if
 *                     the inner class is defined in REPL.
 */
case class NewInstance(
    cls: Class[_],
    arguments: Seq[Expression],
    inputTypes: Seq[AbstractDataType],
    propagateNull: Boolean,
    dataType: DataType,
    outerPointer: Option[() => AnyRef]) extends InvokeLike {
  private val className = cls.getName

  override def nullable: Boolean = needNullCheck

  // Non-foldable to prevent the optimizer from replacing NewInstance with a singleton instance
  // of the specified class.
  override def foldable: Boolean = false
  override def children: Seq[Expression] = arguments

  final override val nodePatterns: Seq[TreePattern] = Seq(NEW_INSTANCE)

  override lazy val resolved: Boolean = {
    // If the class to construct is an inner class, we need to get its outer pointer, or this
    // expression should be regarded as unresolved.
    // Note that static inner classes (e.g., inner classes within Scala objects) don't need
    // outer pointer registration.
    val needOuterPointer =
      outerPointer.isEmpty && cls.isMemberClass && !Modifier.isStatic(cls.getModifiers)
    childrenResolved && !needOuterPointer
  }

  @transient private lazy val constructor: (Seq[AnyRef]) => Any = {
    val paramTypes = EncoderUtils.expressionJavaClasses(arguments)
    val getConstructor = (paramClazz: Seq[Class[_]]) => {
      ScalaReflection.findConstructor(cls, paramClazz).getOrElse {
        throw QueryExecutionErrors.constructorNotFoundError(cls.toString)
      }
    }
    outerPointer.map { p =>
      val outerObj = p()
      val c = getConstructor(outerObj.getClass +: paramTypes)
      (args: Seq[AnyRef]) => {
        c(outerObj +: args)
      }
    }.getOrElse {
      val c = getConstructor(paramTypes)
      (args: Seq[AnyRef]) => {
        c(args)
      }
    }
  }

  override def eval(input: InternalRow): Any = {
    var i = 0
    val len = arguments.length
    var resultNull = false
    while (i < len) {
      val result = arguments(i).eval(input).asInstanceOf[Object]
      evaluatedArgs(i) = result
      resultNull = resultNull || (result == null && needNullCheckForIndex(i))
      i += 1
    }
    if (needNullCheck && resultNull) {
      // return null if one of arguments is null
      null
    } else {
      try {
        constructor(evaluatedArgs.toImmutableArraySeq)
      } catch {
        // Re-throw the original exception.
        case e: java.lang.reflect.InvocationTargetException if e.getCause != null =>
          throw e.getCause
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaType = CodeGenerator.javaType(dataType)

    val (argCode, argString, resultIsNull) = prepareArguments(ctx)

    val outer = outerPointer.map(func => Literal.fromObject(func()).genCode(ctx))

    ev.isNull = resultIsNull

    val constructorCall = cls.getConstructors.length match {
      // If there are no constructors, the `new` method will fail. In
      // this case we can try to call the apply method constructor
      // that might be defined on the companion object.
      case 0 => s"$className$$.MODULE$$.apply($argString)"
      case _ => outer.map { gen =>
        s"${gen.value}.new ${Utils.getSimpleName(cls)}($argString)"
      }.getOrElse {
        s"new $className($argString)"
      }
    }

    val code = code"""
      $argCode
      ${outer.map(_.code).getOrElse("")}
      final $javaType ${ev.value} = ${ev.isNull} ?
        ${CodeGenerator.defaultValue(dataType)} : $constructorCall;
    """
    ev.copy(code = code)
  }

  override def toString: String = s"newInstance($cls)"

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): NewInstance =
    copy(arguments = newChildren)
}

/**
 * Given an expression that returns on object of type `Option[_]`, this expression unwraps the
 * option into the specified Spark SQL datatype.  In the case of `None`, the nullbit is set instead.
 *
 * @param dataType The expected unwrapped option type.
 * @param child An expression that returns an `Option`
 */
case class UnwrapOption(
    dataType: DataType,
    child: Expression) extends UnaryExpression with NonSQLExpression with ExpectsInputTypes {

  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = ObjectType :: Nil

  override def eval(input: InternalRow): Any = {
    val inputObject = child.eval(input)
    if (inputObject == null) {
      null
    } else {
      inputObject.asInstanceOf[Option[_]].orNull
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaType = CodeGenerator.javaType(dataType)
    val inputObject = child.genCode(ctx)

    val code = inputObject.code + code"""
      final boolean ${ev.isNull} = ${inputObject.isNull} || ${inputObject.value}.isEmpty();
      $javaType ${ev.value} = ${ev.isNull} ? ${CodeGenerator.defaultValue(dataType)} :
        (${CodeGenerator.boxedType(javaType)}) ${inputObject.value}.get();
    """
    ev.copy(code = code)
  }

  override protected def withNewChildInternal(newChild: Expression): UnwrapOption =
    copy(child = newChild)
}

/**
 * Converts the result of evaluating `child` into an option, checking both the isNull bit and
 * (in the case of reference types) equality with null.
 *
 * @param child The expression to evaluate and wrap.
 * @param optType The type of this option.
 */
case class WrapOption(child: Expression, optType: DataType)
  extends UnaryExpression with NonSQLExpression with ExpectsInputTypes {

  override def dataType: DataType = ObjectType(classOf[Option[_]])

  override def nullable: Boolean = false

  override def inputTypes: Seq[AbstractDataType] = optType :: Nil

  override def eval(input: InternalRow): Any = Option(child.eval(input))

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val inputObject = child.genCode(ctx)

    val code = inputObject.code + code"""
      scala.Option ${ev.value} =
        ${inputObject.isNull} ?
        scala.Option$$.MODULE$$.apply(null) : new scala.Some(${inputObject.value});
    """
    ev.copy(code = code, isNull = FalseLiteral)
  }

  override protected def withNewChildInternal(newChild: Expression): WrapOption =
    copy(child = newChild)
}

object LambdaVariable {
  private val curId = new java.util.concurrent.atomic.AtomicLong()

  // Returns the codegen-ed `LambdaVariable` and add it to mutable states, so that it can be
  // accessed anywhere in the generated code.
  def prepareLambdaVariable(ctx: CodegenContext, variable: LambdaVariable): ExprCode = {
    val variableCode = variable.genCode(ctx)
    assert(variableCode.code.isEmpty)

    ctx.addMutableState(
      CodeGenerator.javaType(variable.dataType),
      variableCode.value,
      forceInline = true,
      useFreshName = false)

    if (variable.nullable) {
      ctx.addMutableState(
        CodeGenerator.JAVA_BOOLEAN,
        variableCode.isNull,
        forceInline = true,
        useFreshName = false)
    }

    variableCode
  }
}

/**
 * A placeholder for the loop variable used in [[MapObjects]]. This should never be constructed
 * manually, but will instead be passed into the provided lambda function.
 */
// TODO: Merge this and `NamedLambdaVariable`.
case class LambdaVariable(
    name: String,
    dataType: DataType,
    nullable: Boolean,
    id: Long = LambdaVariable.curId.incrementAndGet) extends LeafExpression with NonSQLExpression {

  private val accessor: (InternalRow, Int) => Any = InternalRow.getAccessor(dataType, nullable)

  final override val nodePatterns: Seq[TreePattern] = Seq(LAMBDA_VARIABLE)

  // Interpreted execution of `LambdaVariable` always get the 0-index element from input row.
  override def eval(input: InternalRow): Any = {
    assert(input.numFields == 1,
      "The input row of interpreted LambdaVariable should have only 1 field.")
    accessor(input, 0)
  }

  override def genCode(ctx: CodegenContext): ExprCode = {
    // If `LambdaVariable` IDs are reassigned by the `ReassignLambdaVariableID` rule, the IDs will
    // all be negative.
    val suffix = "lambda_variable_" + math.abs(id)
    val isNull = if (nullable) {
      JavaCode.isNullVariable(s"isNull_${name}_$suffix")
    } else {
      FalseLiteral
    }
    val value = JavaCode.variable(s"value_${name}_$suffix", dataType)
    ExprCode(isNull, value)
  }

  // This won't be called as `genCode` is overrided, just overriding it to make
  // `LambdaVariable` non-abstract.
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ev
}

/**
 * When constructing [[MapObjects]], the element type must be given, which may not be available
 * before analysis. This class acts like a placeholder for [[MapObjects]], and will be replaced by
 * [[MapObjects]] during analysis after the input data is resolved.
 * Note that, ideally we should not serialize and send unresolved expressions to executors, but
 * users may accidentally do this(e.g. mistakenly reference an encoder instance when implementing
 * Aggregator). Here we mark `function` as transient because it may reference scala Type, which is
 * not serializable. Then even users mistakenly reference unresolved expression and serialize it,
 * it's just a performance issue(more network traffic), and will not fail.
 */
case class UnresolvedMapObjects(
    @transient function: Expression => Expression,
    child: Expression,
    customCollectionCls: Option[Class[_]] = None) extends UnaryExpression with Unevaluable {
  override lazy val resolved = false

  override def dataType: DataType = customCollectionCls.map(ObjectType.apply).getOrElse {
    throw QueryExecutionErrors.customCollectionClsNotResolvedError()
  }

  override protected def withNewChildInternal(newChild: Expression): UnresolvedMapObjects =
    copy(child = newChild)
}

object MapObjects {
  /**
   * Construct an instance of MapObjects case class.
   *
   * @param function The function applied on the collection elements.
   * @param inputData An expression that when evaluated returns a collection object.
   * @param elementType The data type of elements in the collection.
   * @param elementNullable When false, indicating elements in the collection are always
   *                        non-null value.
   * @param customCollectionCls Class of the resulting collection (returning ObjectType)
   *                            or None (returning ArrayType)
   */
  def apply(
      function: Expression => Expression,
      inputData: Expression,
      elementType: DataType,
      elementNullable: Boolean = true,
      customCollectionCls: Option[Class[_]] = None): MapObjects = {
    // UnresolvedMapObjects does not serialize its 'function' field.
    // If an array expression or array Encoder is not correctly resolved before
    // serialization, this exception condition may occur.
    require(function != null,
      "MapObjects applied with a null function. " +
      "Likely cause is failure to resolve an array expression or encoder. " +
      "(See UnresolvedMapObjects)")
    val loopVar = LambdaVariable("MapObject", elementType, elementNullable)
    MapObjects(loopVar, function(loopVar), inputData, customCollectionCls)
  }
}

/**
 * Applies the given expression to every element of a collection of items, returning the result
 * as an ArrayType or ObjectType. This is similar to a typical map operation, but where the lambda
 * function is expressed using catalyst expressions.
 *
 * The type of the result is determined as follows:
 * - ArrayType - when customCollectionCls is None
 * - ObjectType(collection) - when customCollectionCls contains a collection class
 *
 * The following collection ObjectTypes are currently supported on input:
 *   Seq, Array, ArrayData, java.util.List
 *
 * @param loopVar the [[LambdaVariable]] expression representing the loop variable that used to
 *                iterate the collection, and used as input for the `lambdaFunction`.
 * @param lambdaFunction A function that take the `loopVar` as input, and used as lambda function
 *                       to handle collection elements.
 * @param inputData An expression that when evaluated returns a collection object.
 * @param customCollectionCls Class of the resulting collection (returning ObjectType)
 *                            or None (returning ArrayType)
 */
case class MapObjects private(
    loopVar: LambdaVariable,
    lambdaFunction: Expression,
    inputData: Expression,
    customCollectionCls: Option[Class[_]]) extends Expression with NonSQLExpression
  with TernaryLike[Expression] {

  override def nullable: Boolean = inputData.nullable

  override def first: Expression = loopVar
  override def second: Expression = lambdaFunction
  override def third: Expression = inputData

  final override val nodePatterns: Seq[TreePattern] = Seq(MAP_OBJECTS)

  // The data with UserDefinedType are actually stored with the data type of its sqlType.
  // When we want to apply MapObjects on it, we have to use it.
  lazy private val inputDataType = inputData.dataType match {
    case u: UserDefinedType[_] => u.sqlType
    case _ => inputData.dataType
  }

  private def executeFuncOnCollection(inputCollection: Iterable[_]): Iterator[_] = {
    val row = new GenericInternalRow(1)
    inputCollection.iterator.map { element =>
      row.update(0, element)
      lambdaFunction.eval(row)
    }
  }

  private lazy val convertToSeq: Any => scala.collection.Seq[_] = inputDataType match {
    case ObjectType(cls) if classOf[scala.collection.Seq[_]].isAssignableFrom(cls) =>
      _.asInstanceOf[scala.collection.Seq[_]].toSeq
    case ObjectType(cls) if cls.isArray =>
      _.asInstanceOf[Array[_]].toImmutableArraySeq
    case ObjectType(cls) if classOf[java.util.List[_]].isAssignableFrom(cls) =>
      _.asInstanceOf[java.util.List[_]].asScala.toSeq
    case ObjectType(cls) if classOf[java.util.Set[_]].isAssignableFrom(cls) =>
      _.asInstanceOf[java.util.Set[_]].asScala.toSeq
    case ObjectType(cls) if cls == classOf[Object] =>
      (inputCollection) => {
        if (inputCollection.getClass.isArray) {
          inputCollection.asInstanceOf[Array[_]].toImmutableArraySeq
        } else {
          inputCollection.asInstanceOf[scala.collection.Seq[_]]
        }
      }
    case ArrayType(et, _) =>
      _.asInstanceOf[ArrayData].toSeq[Any](et)
  }

  private def elementClassTag(): ClassTag[Any] = {
    val clazz = lambdaFunction.dataType match {
      case ObjectType(cls) => cls
      case dt if lambdaFunction.nullable => EncoderUtils.javaBoxedType(dt)
      case dt => EncoderUtils.dataTypeJavaClass(dt)
    }
    ClassTag(clazz).asInstanceOf[ClassTag[Any]]
  }

  private lazy val mapElements: scala.collection.Seq[_] => Any = customCollectionCls match {
    case Some(cls) if classOf[mutable.ArraySeq[_]].isAssignableFrom(cls) =>
      // The implicit tag is a workaround to deal with a small change in the
      // (scala) signature of ArrayBuilder.make between Scala 2.12 and 2.13.
      implicit val tag: ClassTag[Any] = elementClassTag()
      input => {
        val builder = mutable.ArrayBuilder.make[Any]
        builder.sizeHint(input.size)
        executeFuncOnCollection(input).foreach(builder += _)
        mutable.ArraySeq.make(builder.result())
      }
    case Some(cls) if classOf[immutable.ArraySeq[_]].isAssignableFrom(cls) =>
      implicit val tag: ClassTag[Any] = elementClassTag()
      input => {
        val builder = mutable.ArrayBuilder.make[Any]
        builder.sizeHint(input.size)
        executeFuncOnCollection(input).foreach(builder += _)
        immutable.ArraySeq.unsafeWrapArray(builder.result())
      }
    case Some(cls) if classOf[scala.collection.Seq[_]].isAssignableFrom(cls) =>
      // Scala sequence
      executeFuncOnCollection(_).toSeq
    case Some(cls) if classOf[scala.collection.Set[_]].isAssignableFrom(cls) =>
      // Scala set
      executeFuncOnCollection(_).toSet
    case Some(cls) if classOf[java.util.List[_]].isAssignableFrom(cls) =>
      // Java list
      if (cls == classOf[java.util.List[_]] || cls == classOf[java.util.AbstractList[_]] ||
          cls == classOf[java.util.AbstractSequentialList[_]]) {
        // Specifying non concrete implementations of `java.util.List`
        executeFuncOnCollection(_).toSeq.asJava
      } else {
        val constructors = cls.getConstructors()
        val intParamConstructor = constructors.find { constructor =>
          constructor.getParameterCount == 1 && constructor.getParameterTypes()(0) == classOf[Int]
        }
        val noParamConstructor = constructors.find { constructor =>
          constructor.getParameterCount == 0
        }

        val constructor = intParamConstructor.map { intConstructor =>
          (len: Int) => intConstructor.newInstance(len.asInstanceOf[Object])
        }.getOrElse {
          (_: Int) => noParamConstructor.get.newInstance()
        }

        // Specifying concrete implementations of `java.util.List`
        (inputs) => {
          val results = executeFuncOnCollection(inputs)
          val builder = constructor(inputs.length).asInstanceOf[java.util.List[Any]]
          results.foreach(builder.add(_))
          builder
        }
      }
    case Some(cls) if classOf[java.util.Set[_]].isAssignableFrom(cls) =>
      // Java set
      if (cls == classOf[java.util.Set[_]] || cls == classOf[java.util.AbstractSet[_]]) {
        // Specifying non concrete implementations of `java.util.Set`
        executeFuncOnCollection(_).toSet.asJava
      } else {
        val constructors = cls.getConstructors()
        val intParamConstructor = constructors.find { constructor =>
          constructor.getParameterCount == 1 && constructor.getParameterTypes()(0) == classOf[Int]
        }
        val noParamConstructor = constructors.find { constructor =>
          constructor.getParameterCount == 0
        }

        val constructor = intParamConstructor.map { intConstructor =>
          (len: Int) => intConstructor.newInstance(len.asInstanceOf[Object])
        }.getOrElse {
          (_: Int) => noParamConstructor.get.newInstance()
        }

        // Specifying concrete implementations of `java.util.Set`
        (inputs) => {
          val results = executeFuncOnCollection(inputs)
          val builder = constructor(inputs.length).asInstanceOf[java.util.Set[Any]]
          results.foreach(builder.add(_))
          builder
        }
      }
    case None =>
      // array
      x => new GenericArrayData(executeFuncOnCollection(x).toArray)
    case Some(cls) =>
      throw QueryExecutionErrors.classUnsupportedByMapObjectsError(cls)
  }

  override def eval(input: InternalRow): Any = {
    val inputCollection = inputData.eval(input)

    if (inputCollection == null) {
      return null
    }
    mapElements(convertToSeq(inputCollection))
  }

  override def dataType: DataType =
    customCollectionCls.map(ObjectType.apply).getOrElse(
      ArrayType(lambdaFunction.dataType, containsNull = lambdaFunction.nullable))

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val elementJavaType = CodeGenerator.javaType(loopVar.dataType)
    val loopVarCode = LambdaVariable.prepareLambdaVariable(ctx, loopVar)
    val genInputData = inputData.genCode(ctx)
    val genFunction = lambdaFunction.genCode(ctx)
    val dataLength = ctx.freshName("dataLength")
    val convertedArray = ctx.freshName("convertedArray")
    val loopIndex = ctx.freshName("loopIndex")

    val convertedType = CodeGenerator.boxedType(lambdaFunction.dataType)

    // Because of the way Java defines nested arrays, we have to handle the syntax specially.
    // Specifically, we have to insert the [$dataLength] in between the type and any extra nested
    // array declarations (i.e. new String[1][]).
    val arrayConstructor = if (convertedType contains "[]") {
      val rawType = convertedType.takeWhile(_ != '[')
      val arrayPart = convertedType.reverse.takeWhile(c => c == '[' || c == ']').reverse
      s"new $rawType[$dataLength]$arrayPart"
    } else {
      s"new $convertedType[$dataLength]"
    }

    // In RowEncoder, we use `Object` to represent Array or Seq, so we need to determine the type
    // of input collection at runtime for this case.
    val seq = ctx.freshName("seq")
    val array = ctx.freshName("array")
    val determineCollectionType = inputData.dataType match {
      case ObjectType(cls) if cls == classOf[Object] =>
        val seqClass = classOf[scala.collection.Seq[_]].getName
        s"""
          $seqClass $seq = null;
          $elementJavaType[] $array = null;
          if (${genInputData.value}.getClass().isArray()) {
            $array = ($elementJavaType[]) ${genInputData.value};
          } else {
            $seq = ($seqClass) ${genInputData.value};
          }
         """
      case _ => ""
    }

    // `MapObjects` generates a while loop to traverse the elements of the input collection. We
    // need to take care of Seq and List because they may have O(n) complexity for indexed accessing
    // like `list.get(1)`. Here we use Iterator to traverse Seq and List.
    val (getLength, prepareLoop, getLoopVar) = inputDataType match {
      case ObjectType(cls) if classOf[scala.collection.Seq[_]].isAssignableFrom(cls) =>
        val it = ctx.freshName("it")
        (
          s"${genInputData.value}.size()",
          s"scala.collection.Iterator $it = ${genInputData.value}.iterator();",
          s"$it.next()"
        )
      case ObjectType(cls) if cls.isArray =>
        (
          s"${genInputData.value}.length",
          "",
          s"${genInputData.value}[$loopIndex]"
        )
      case ObjectType(cls) if classOf[java.util.List[_]].isAssignableFrom(cls) =>
        val it = ctx.freshName("it")
        (
          s"${genInputData.value}.size()",
          s"java.util.Iterator $it = ${genInputData.value}.iterator();",
          s"$it.next()"
        )
      case ObjectType(cls) if classOf[java.util.Set[_]].isAssignableFrom(cls) =>
        val it = ctx.freshName("it")
        (
          s"${genInputData.value}.size()",
          s"java.util.Iterator $it = ${genInputData.value}.iterator();",
          s"$it.next()"
        )
      case ArrayType(et, _) =>
        (
          s"${genInputData.value}.numElements()",
          "",
          CodeGenerator.getValue(genInputData.value, et, loopIndex)
        )
      case ObjectType(cls) if cls == classOf[Object] =>
        val it = ctx.freshName("it")
        (
          s"$seq == null ? $array.length : $seq.size()",
          s"scala.collection.Iterator $it = $seq == null ? null : $seq.iterator();",
          s"$it == null ? $array[$loopIndex] : $it.next()"
        )
    }

    // Make a copy of the data if it's unsafe-backed
    def makeCopyIfInstanceOf(clazz: Class[_ <: Any], value: String) =
      s"$value instanceof ${clazz.getSimpleName}? ${value}.copy() : $value"
    val genFunctionValue: String = lambdaFunction.dataType match {
      case StructType(_) => makeCopyIfInstanceOf(classOf[UnsafeRow], genFunction.value)
      case ArrayType(_, _) => makeCopyIfInstanceOf(classOf[UnsafeArrayData], genFunction.value)
      case MapType(_, _, _) => makeCopyIfInstanceOf(classOf[UnsafeMapData], genFunction.value)
      case _ => genFunction.value
    }

    val loopNullCheck = if (loopVar.nullable) {
      inputDataType match {
        case _: ArrayType => s"${loopVarCode.isNull} = ${genInputData.value}.isNullAt($loopIndex);"
        case _ => s"${loopVarCode.isNull} = ${loopVarCode.value} == null;"
      }
    } else {
      ""
    }

    val (initCollection, addElement, getResult): (String, String => String, String) =
      customCollectionCls match {
        case Some(cls) if classOf[mutable.ArraySeq[_]].isAssignableFrom(cls) =>
          val tag = ctx.addReferenceObj("tag", elementClassTag())
          val builderClassName = classOf[mutable.ArrayBuilder[_]].getName
          val getBuilder = s"$builderClassName$$.MODULE$$.make($tag)"
          val builder = ctx.freshName("collectionBuilder")
          (
            s"""
                 ${classOf[Builder[_, _]].getName} $builder = $getBuilder;
                 $builder.sizeHint($dataLength);
               """,
            (genValue: String) => s"$builder.$$plus$$eq($genValue);",
            s"(${cls.getName}) ${classOf[mutable.ArraySeq[_]].getName}$$." +
              s"MODULE$$.make($builder.result());"
          )
        case Some(cls) if classOf[immutable.ArraySeq[_]].isAssignableFrom(cls) =>
          val tag = ctx.addReferenceObj("tag", elementClassTag())
          val builderClassName = classOf[mutable.ArrayBuilder[_]].getName
          val getBuilder = s"$builderClassName$$.MODULE$$.make($tag)"
          val builder = ctx.freshName("collectionBuilder")
          (
            s"""
               ${classOf[Builder[_, _]].getName} $builder = $getBuilder;
               $builder.sizeHint($dataLength);
             """,
            (genValue: String) => s"$builder.$$plus$$eq($genValue);",
            s"(${cls.getName}) ${classOf[immutable.ArraySeq[_]].getName}$$." +
              s"MODULE$$.unsafeWrapArray($builder.result());"
          )
        case Some(cls) if classOf[scala.collection.Seq[_]].isAssignableFrom(cls) ||
          classOf[scala.collection.Set[_]].isAssignableFrom(cls) =>
          // Scala sequence or set
          val getBuilder = s"${cls.getName}$$.MODULE$$.newBuilder()"
          val builder = ctx.freshName("collectionBuilder")
          (
            s"""
               ${classOf[Builder[_, _]].getName} $builder = $getBuilder;
               $builder.sizeHint($dataLength);
             """,
            (genValue: String) => s"$builder.$$plus$$eq($genValue);",
            s"(${cls.getName}) $builder.result();"
          )
        case Some(cls) if classOf[java.util.List[_]].isAssignableFrom(cls) =>
          // Java list
          val builder = ctx.freshName("collectionBuilder")
          (
            if (cls == classOf[java.util.List[_]] || cls == classOf[java.util.AbstractList[_]] ||
              cls == classOf[java.util.AbstractSequentialList[_]]) {
              s"${cls.getName} $builder = new java.util.ArrayList($dataLength);"
            } else {
              val param = Try(cls.getConstructor(Integer.TYPE)).map(_ => dataLength).getOrElse("")
              s"${cls.getName} $builder = new ${cls.getName}($param);"
            },
            (genValue: String) => s"$builder.add($genValue);",
            s"$builder;"
          )
        case Some(cls) if classOf[java.util.Set[_]].isAssignableFrom(cls) =>
          // Java set
          val builder = ctx.freshName("collectionBuilder")
          (
            if (cls == classOf[java.util.Set[_]] || cls == classOf[java.util.AbstractSet[_]]) {
              s"${cls.getName} $builder = new java.util.HashSet($dataLength);"
            } else {
              val param = Try(cls.getConstructor(Integer.TYPE)).map(_ => dataLength).getOrElse("")
              s"${cls.getName} $builder = new ${cls.getName}($param);"
            },
            (genValue: String) => s"$builder.add($genValue);",
            s"$builder;"
          )
        case _ =>
          // array
          (
            s"""
               $convertedType[] $convertedArray = null;
               $convertedArray = $arrayConstructor;
             """,
            (genValue: String) => s"$convertedArray[$loopIndex] = $genValue;",
            s"new ${classOf[GenericArrayData].getName}($convertedArray);"
          )
      }

    val code = genInputData.code + code"""
      ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};

      if (!${genInputData.isNull}) {
        $determineCollectionType
        int $dataLength = $getLength;
        $initCollection

        int $loopIndex = 0;
        $prepareLoop
        while ($loopIndex < $dataLength) {
          ${loopVarCode.value} = ($elementJavaType) ($getLoopVar);
          $loopNullCheck

          ${genFunction.code}
          if (${genFunction.isNull}) {
            ${addElement("null")}
          } else {
            ${addElement(genFunctionValue)}
          }

          $loopIndex += 1;
        }

        ${ev.value} = $getResult
      }
    """
    ev.copy(code = code, isNull = genInputData.isNull)
  }

  override protected def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression, newThird: Expression): Expression =
    copy(
      loopVar = newFirst.asInstanceOf[LambdaVariable],
      lambdaFunction = newSecond,
      inputData = newThird)
}

/**
 * Similar to [[UnresolvedMapObjects]], this is a placeholder of [[CatalystToExternalMap]].
 *
 * @param child An expression that when evaluated returns a map object.
 * @param keyFunction The function applied on the key collection elements.
 * @param valueFunction The function applied on the value collection elements.
 * @param collClass The type of the resulting collection.
 */
case class UnresolvedCatalystToExternalMap(
    child: Expression,
    @transient keyFunction: Expression => Expression,
    @transient valueFunction: Expression => Expression,
    collClass: Class[_]) extends UnaryExpression with Unevaluable {

  override lazy val resolved = false

  override def dataType: DataType = ObjectType(collClass)

  override protected def withNewChildInternal(
    newChild: Expression): UnresolvedCatalystToExternalMap = copy(child = newChild)
}

object CatalystToExternalMap {
  def apply(u: UnresolvedCatalystToExternalMap): CatalystToExternalMap = {
    val mapType = u.child.dataType.asInstanceOf[MapType]
    val keyLoopVar = LambdaVariable(
      "CatalystToExternalMap_key", mapType.keyType, nullable = false)
    val valueLoopVar = LambdaVariable(
      "CatalystToExternalMap_value", mapType.valueType, mapType.valueContainsNull)
    CatalystToExternalMap(
      keyLoopVar, u.keyFunction(keyLoopVar),
      valueLoopVar, u.valueFunction(valueLoopVar),
      u.child, u.collClass)
  }
}

/**
 * Expression used to convert a Catalyst Map to an external Scala Map.
 * The collection is constructed using the associated builder, obtained by calling `newBuilder`
 * on the collection's companion object.
 *
 * @param keyLoopVar the [[LambdaVariable]] expression representing the loop variable that is used
 *                   when iterating over the key collection, and which is used as input for the
 *                   `keyLambdaFunction`.
 * @param keyLambdaFunction A function that takes the `keyLoopVar` as input, and is used as
 *                          a lambda function to handle collection elements.
 * @param valueLoopVar the [[LambdaVariable]] expression representing the loop variable that is used
 *                     when iterating over the value collection, and which is used as input for the
 *                     `valueLambdaFunction`.
 * @param valueLambdaFunction A function that takes the `valueLoopVar` as input, and is used as
 *                            a lambda function to handle collection elements.
 * @param inputData An expression that when evaluated returns a map object.
 * @param collClass The type of the resulting collection.
 */
case class CatalystToExternalMap private(
    keyLoopVar: LambdaVariable,
    keyLambdaFunction: Expression,
    valueLoopVar: LambdaVariable,
    valueLambdaFunction: Expression,
    inputData: Expression,
    collClass: Class[_]) extends Expression with NonSQLExpression {

  override def nullable: Boolean = inputData.nullable

  override def children: Seq[Expression] = Seq(
    keyLoopVar, keyLambdaFunction, valueLoopVar, valueLambdaFunction, inputData)

  private lazy val inputMapType = inputData.dataType.asInstanceOf[MapType]

  private lazy val (newMapBuilderMethod, moduleField) = {
    val clazz = Utils.classForName(collClass.getCanonicalName + "$")
    (clazz.getMethod("newBuilder"), clazz.getField("MODULE$").get(null))
  }

  private def newMapBuilder(): Builder[AnyRef, AnyRef] = {
    newMapBuilderMethod.invoke(moduleField).asInstanceOf[Builder[AnyRef, AnyRef]]
  }

  override def eval(input: InternalRow): Any = {
    val result = inputData.eval(input).asInstanceOf[MapData]
    if (result != null) {
      val builder = newMapBuilder()
      builder.sizeHint(result.numElements())
      val keyArray = result.keyArray()
      val valueArray = result.valueArray()
      val row = new GenericInternalRow(1)
      var i = 0
      while (i < result.numElements()) {
        row.update(0, keyArray.get(i, inputMapType.keyType))
        val key = keyLambdaFunction.eval(row)
        row.update(0, valueArray.get(i, inputMapType.valueType))
        val value = valueLambdaFunction.eval(row)
        builder += Tuple2(key, value)
        i += 1
      }
      builder.result()
    } else {
      null
    }
  }

  override def dataType: DataType = ObjectType(collClass)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val keyCode = LambdaVariable.prepareLambdaVariable(ctx, keyLoopVar)
    val valueCode = LambdaVariable.prepareLambdaVariable(ctx, valueLoopVar)
    val keyElementJavaType = CodeGenerator.javaType(keyLoopVar.dataType)
    val genKeyFunction = keyLambdaFunction.genCode(ctx)
    val valueElementJavaType = CodeGenerator.javaType(valueLoopVar.dataType)
    val genValueFunction = valueLambdaFunction.genCode(ctx)
    val genInputData = inputData.genCode(ctx)
    val dataLength = ctx.freshName("dataLength")
    val loopIndex = ctx.freshName("loopIndex")
    val tupleLoopValue = ctx.freshName("tupleLoopValue")
    val builderValue = ctx.freshName("builderValue")

    val keyArray = ctx.freshName("keyArray")
    val valueArray = ctx.freshName("valueArray")
    val getKeyLoopVar = CodeGenerator.getValue(keyArray, keyLoopVar.dataType, loopIndex)
    val getValueLoopVar = CodeGenerator.getValue(valueArray, valueLoopVar.dataType, loopIndex)

    // Make a copy of the data if it's unsafe-backed
    def makeCopyIfInstanceOf(clazz: Class[_ <: Any], value: String) =
      s"$value instanceof ${clazz.getSimpleName}? $value.copy() : $value"
    def genFunctionValue(lambdaFunction: Expression, genFunction: ExprCode) =
      lambdaFunction.dataType match {
        case StructType(_) => makeCopyIfInstanceOf(classOf[UnsafeRow], genFunction.value)
        case ArrayType(_, _) => makeCopyIfInstanceOf(classOf[UnsafeArrayData], genFunction.value)
        case MapType(_, _, _) => makeCopyIfInstanceOf(classOf[UnsafeMapData], genFunction.value)
        case _ => genFunction.value
      }
    val genKeyFunctionValue = genFunctionValue(keyLambdaFunction, genKeyFunction)
    val genValueFunctionValue = genFunctionValue(valueLambdaFunction, genValueFunction)

    val valueLoopNullCheck = if (valueLoopVar.nullable) {
      s"${valueCode.isNull} = $valueArray.isNullAt($loopIndex);"
    } else {
      ""
    }

    val builderClass = classOf[Builder[_, _]].getName
    val constructBuilder = s"""
      $builderClass $builderValue = ${collClass.getName}$$.MODULE$$.newBuilder();
      $builderValue.sizeHint($dataLength);
    """

    val tupleClass = classOf[(_, _)].getName
    val appendToBuilder = s"""
      $tupleClass $tupleLoopValue;

      if (${genValueFunction.isNull}) {
        $tupleLoopValue = new $tupleClass($genKeyFunctionValue, null);
      } else {
        $tupleLoopValue = new $tupleClass($genKeyFunctionValue, $genValueFunctionValue);
      }

      $builderValue.$$plus$$eq($tupleLoopValue);
     """
    val getBuilderResult = s"${ev.value} = (${collClass.getName}) $builderValue.result();"

    val code = genInputData.code + code"""
      ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};

      if (!${genInputData.isNull}) {
        int $dataLength = ${genInputData.value}.numElements();
        $constructBuilder
        ArrayData $keyArray = ${genInputData.value}.keyArray();
        ArrayData $valueArray = ${genInputData.value}.valueArray();

        int $loopIndex = 0;
        while ($loopIndex < $dataLength) {
          ${keyCode.value} = ($keyElementJavaType) ($getKeyLoopVar);
          ${valueCode.value} = ($valueElementJavaType) ($getValueLoopVar);
          $valueLoopNullCheck

          ${genKeyFunction.code}
          ${genValueFunction.code}

          $appendToBuilder

          $loopIndex += 1;
        }

        $getBuilderResult
      }
    """
    ev.copy(code = code, isNull = genInputData.isNull)
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): CatalystToExternalMap =
    copy(
      keyLoopVar = newChildren(0).asInstanceOf[LambdaVariable],
      keyLambdaFunction = newChildren(1),
      valueLoopVar = newChildren(2).asInstanceOf[LambdaVariable],
      valueLambdaFunction = newChildren(3),
      inputData = newChildren(4))
}

object ExternalMapToCatalyst {
  def apply(
      inputMap: Expression,
      keyType: DataType,
      keyConverter: Expression => Expression,
      keyNullable: Boolean,
      valueType: DataType,
      valueConverter: Expression => Expression,
      valueNullable: Boolean): ExternalMapToCatalyst = {
    val keyLoopVar = LambdaVariable("ExternalMapToCatalyst_key", keyType, keyNullable)
    val valueLoopVar = LambdaVariable("ExternalMapToCatalyst_value", valueType, valueNullable)
    ExternalMapToCatalyst(
      keyLoopVar,
      keyConverter(keyLoopVar),
      valueLoopVar,
      valueConverter(valueLoopVar),
      inputMap)
  }
}

/**
 * Converts a Scala/Java map object into catalyst format, by applying the key/value converter when
 * iterate the map.
 *
 * @param keyLoopVar the [[LambdaVariable]] expression representing the loop variable that is used
 *                   when iterating over the key collection, and which is used as input for the
 *                   `keyConverter`.
 * @param keyConverter A function that take the `key` as input, and converts it to catalyst format.
 * @param valueLoopVar the [[LambdaVariable]] expression representing the loop variable that is used
 *                     when iterating over the value collection, and which is used as input for the
 *                     `valueConverter`.
 * @param valueConverter A function that take the `value` as input, and converts it to catalyst
 *                       format.
 * @param inputData An expression that when evaluated returns the input map object.
 */
case class ExternalMapToCatalyst private(
    keyLoopVar: LambdaVariable,
    keyConverter: Expression,
    valueLoopVar: LambdaVariable,
    valueConverter: Expression,
    inputData: Expression)
  extends Expression with NonSQLExpression {

  override def foldable: Boolean = false

  override def nullable: Boolean = inputData.nullable

  // ExternalMapToCatalyst is stateful because of the rowBuffer in mapCatalystConverter
  override def stateful: Boolean = true

  override def children: Seq[Expression] = Seq(
    keyLoopVar, keyConverter, valueLoopVar, valueConverter, inputData)

  override def dataType: MapType = MapType(
    keyConverter.dataType, valueConverter.dataType, valueContainsNull = valueConverter.nullable)

  private lazy val mapCatalystConverter: Any => (Array[Any], Array[Any]) = {
    val rowBuffer = new GenericInternalRow(Array[Any](1))
    def rowWrapper(data: Any): InternalRow = {
      rowBuffer.update(0, data)
      rowBuffer
    }

    inputData.dataType match {
      case ObjectType(cls) if classOf[java.util.Map[_, _]].isAssignableFrom(cls) =>
        (input: Any) => {
          val data = input.asInstanceOf[java.util.Map[Any, Any]]
          val keys = new Array[Any](data.size)
          val values = new Array[Any](data.size)
          val iter = data.entrySet().iterator()
          var i = 0
          while (iter.hasNext) {
            val entry = iter.next()
            val (key, value) = (entry.getKey, entry.getValue)
            keys(i) = if (key != null) {
              keyConverter.eval(rowWrapper(key))
            } else {
              throw QueryExecutionErrors.nullAsMapKeyNotAllowedError()
            }
            values(i) = if (value != null) {
              valueConverter.eval(rowWrapper(value))
            } else {
              null
            }
            i += 1
          }
          (keys, values)
        }

      case ObjectType(cls) if classOf[scala.collection.Map[_, _]].isAssignableFrom(cls) =>
        (input: Any) => {
          val data = input.asInstanceOf[scala.collection.Map[Any, Any]]
          val keys = new Array[Any](data.size)
          val values = new Array[Any](data.size)
          var i = 0
          for ((key, value) <- data) {
            keys(i) = if (key != null) {
              keyConverter.eval(rowWrapper(key))
            } else {
              throw QueryExecutionErrors.nullAsMapKeyNotAllowedError()
            }
            values(i) = if (value != null) {
              valueConverter.eval(rowWrapper(value))
            } else {
              null
            }
            i += 1
          }
          (keys, values)
        }
    }
  }

  override def eval(input: InternalRow): Any = {
    val result = inputData.eval(input)
    if (result != null) {
      val (keys, values) = mapCatalystConverter(result)
      new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values))
    } else {
      null
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val inputMap = inputData.genCode(ctx)
    val genKeyConverter = keyConverter.genCode(ctx)
    val genValueConverter = valueConverter.genCode(ctx)
    val length = ctx.freshName("length")
    val index = ctx.freshName("index")
    val convertedKeys = ctx.freshName("convertedKeys")
    val convertedValues = ctx.freshName("convertedValues")
    val entry = ctx.freshName("entry")
    val entries = ctx.freshName("entries")

    val keyJavaType = CodeGenerator.javaType(keyLoopVar.dataType)
    val valueJavaType = CodeGenerator.javaType(valueLoopVar.dataType)
    val keyCode = LambdaVariable.prepareLambdaVariable(ctx, keyLoopVar)
    val valueCode = LambdaVariable.prepareLambdaVariable(ctx, valueLoopVar)

    val (defineEntries, defineKeyValue) = inputData.dataType match {
      case ObjectType(cls) if classOf[java.util.Map[_, _]].isAssignableFrom(cls) =>
        val javaIteratorCls = classOf[java.util.Iterator[_]].getName
        val javaMapEntryCls = classOf[java.util.Map.Entry[_, _]].getName

        val defineEntries =
          s"final $javaIteratorCls $entries = ${inputMap.value}.entrySet().iterator();"

        val defineKeyValue =
          s"""
            final $javaMapEntryCls $entry = ($javaMapEntryCls) $entries.next();
            ${keyCode.value} = (${CodeGenerator.boxedType(keyJavaType)}) $entry.getKey();
            ${valueCode.value} = (${CodeGenerator.boxedType(valueJavaType)}) $entry.getValue();
          """

        defineEntries -> defineKeyValue

      case ObjectType(cls) if classOf[scala.collection.Map[_, _]].isAssignableFrom(cls) =>
        val scalaIteratorCls = classOf[Iterator[_]].getName
        val scalaMapEntryCls = classOf[Tuple2[_, _]].getName

        val defineEntries = s"final $scalaIteratorCls $entries = ${inputMap.value}.iterator();"

        val defineKeyValue =
          s"""
            final $scalaMapEntryCls $entry = ($scalaMapEntryCls) $entries.next();
            ${keyCode.value} = (${CodeGenerator.boxedType(keyJavaType)}) $entry._1();
            ${valueCode.value} = (${CodeGenerator.boxedType(valueJavaType)}) $entry._2();
          """

        defineEntries -> defineKeyValue
    }

    val keyNullCheck = if (keyLoopVar.nullable) {
      s"${keyCode.isNull} = ${keyCode.value} == null;"
    } else {
      ""
    }

    val valueNullCheck = if (valueLoopVar.nullable) {
      s"${valueCode.isNull} = ${valueCode.value} == null;"
    } else {
      ""
    }

    val arrayCls = classOf[GenericArrayData].getName
    val mapCls = classOf[ArrayBasedMapData].getName
    val convertedKeyType = CodeGenerator.boxedType(keyConverter.dataType)
    val convertedValueType = CodeGenerator.boxedType(valueConverter.dataType)
    val code = inputMap.code +
      code"""
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        if (!${inputMap.isNull}) {
          final int $length = ${inputMap.value}.size();
          final Object[] $convertedKeys = new Object[$length];
          final Object[] $convertedValues = new Object[$length];
          int $index = 0;
          $defineEntries
          while($entries.hasNext()) {
            $defineKeyValue
            $keyNullCheck
            $valueNullCheck

            ${genKeyConverter.code}
            if (${genKeyConverter.isNull}) {
              throw QueryExecutionErrors.nullAsMapKeyNotAllowedError();
            } else {
              $convertedKeys[$index] = ($convertedKeyType) ${genKeyConverter.value};
            }

            ${genValueConverter.code}
            if (${genValueConverter.isNull}) {
              $convertedValues[$index] = null;
            } else {
              $convertedValues[$index] = ($convertedValueType) ${genValueConverter.value};
            }

            $index++;
          }

          ${ev.value} = new $mapCls(new $arrayCls($convertedKeys), new $arrayCls($convertedValues));
        }
      """
    ev.copy(code = code, isNull = inputMap.isNull)
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): ExternalMapToCatalyst =
    copy(
      keyLoopVar = newChildren(0).asInstanceOf[LambdaVariable],
      keyConverter = newChildren(1),
      valueLoopVar = newChildren(2).asInstanceOf[LambdaVariable],
      valueConverter = newChildren(3),
      inputData = newChildren(4))
}

/**
 * Constructs a new external row, using the result of evaluating the specified expressions
 * as content.
 *
 * @param children A list of expression to use as content of the external row.
 */
case class CreateExternalRow(children: Seq[Expression], schema: StructType)
  extends Expression with NonSQLExpression {

  override def dataType: DataType = ObjectType(classOf[Row])

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val values = children.map(_.eval(input)).toArray
    new GenericRowWithSchema(values, schema)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val rowClass = classOf[GenericRowWithSchema].getName
    val values = ctx.freshName("values")

    val childrenCodes = children.zipWithIndex.map { case (e, i) =>
      val eval = e.genCode(ctx)
      s"""
         |${eval.code}
         |if (${eval.isNull}) {
         |  $values[$i] = null;
         |} else {
         |  $values[$i] = ${eval.value};
         |}
       """.stripMargin
    }

    val childrenCode = ctx.splitExpressionsWithCurrentInputs(
      expressions = childrenCodes,
      funcName = "createExternalRow",
      extraArguments = "Object[]" -> values :: Nil)
    val schemaField = ctx.addReferenceObj("schema", schema)

    val code =
      code"""
         |Object[] $values = new Object[${children.size}];
         |$childrenCode
         |final ${classOf[Row].getName} ${ev.value} = new $rowClass($values, $schemaField);
       """.stripMargin
    ev.copy(code = code, isNull = FalseLiteral)
  }

  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[Expression]): CreateExternalRow = copy(children = newChildren)
}

/**
 * Serializes an input object using a generic serializer (Kryo or Java).
 *
 * @param kryo if true, use Kryo. Otherwise, use Java.
 */
case class EncodeUsingSerializer(child: Expression, kryo: Boolean)
  extends UnaryExpression with NonSQLExpression with SerializerSupport {

  override def nullSafeEval(input: Any): Any = {
    serializerInstance.serialize(input).array()
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val serializer = addImmutableSerializerIfNeeded(ctx)
    // Code to serialize.
    val input = child.genCode(ctx)
    val javaType = CodeGenerator.javaType(dataType)
    val serialize = s"$serializer.serialize(${input.value}, null).array()"

    val code = input.code + code"""
      final $javaType ${ev.value} =
        ${input.isNull} ? ${CodeGenerator.defaultValue(dataType)} : $serialize;
     """
    ev.copy(code = code, isNull = input.isNull)
  }

  override def dataType: DataType = BinaryType

  override protected def withNewChildInternal(newChild: Expression): EncodeUsingSerializer =
    copy(child = newChild)
}

/**
 * Serializes an input object using a generic serializer (Kryo or Java).  Note that the ClassTag
 * is not an implicit parameter because TreeNode cannot copy implicit parameters.
 *
 * @param kryo if true, use Kryo. Otherwise, use Java.
 */
case class DecodeUsingSerializer[T](child: Expression, tag: ClassTag[T], kryo: Boolean)
  extends UnaryExpression with NonSQLExpression with SerializerSupport {

  override def nullSafeEval(input: Any): Any = {
    val inputBytes = java.nio.ByteBuffer.wrap(input.asInstanceOf[Array[Byte]])
    serializerInstance.deserialize(inputBytes)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val serializer = addImmutableSerializerIfNeeded(ctx)
    // Code to deserialize.
    val input = child.genCode(ctx)
    val javaType = CodeGenerator.javaType(dataType)
    val deserialize =
      s"($javaType) $serializer.deserialize(java.nio.ByteBuffer.wrap(${input.value}), null)"

    val code = input.code + code"""
      final $javaType ${ev.value} =
         ${input.isNull} ? ${CodeGenerator.defaultValue(dataType)} : $deserialize;
     """
    ev.copy(code = code, isNull = input.isNull)
  }

  override def dataType: DataType = ObjectType(tag.runtimeClass)

  override protected def withNewChildInternal(newChild: Expression): DecodeUsingSerializer[T] =
    copy(child = newChild)
}

/**
 * Initialize a Java Bean instance by setting its field values via setters.
 */
case class InitializeJavaBean(beanInstance: Expression, setters: Map[String, Expression])
  extends Expression with NonSQLExpression {

  override def nullable: Boolean = beanInstance.nullable
  override def children: Seq[Expression] = beanInstance +: setters.values.toSeq
  override def dataType: DataType = beanInstance.dataType

  private lazy val resolvedSetters = {
    assert(beanInstance.dataType.isInstanceOf[ObjectType])

    val ObjectType(beanClass) = beanInstance.dataType
    setters.map {
      case (name, expr) =>
        // Looking for known type mapping.
        // But also looking for general `Object`-type parameter for generic methods.
        val paramTypes = EncoderUtils.expressionJavaClasses(Seq(expr)) :+
          classOf[Object]
        val methods = paramTypes.flatMap { fieldClass =>
          try {
            Some(beanClass.getDeclaredMethod(name, fieldClass))
          } catch {
            case e: NoSuchMethodException => None
          }
        }
        if (methods.isEmpty) {
          throw QueryExecutionErrors.methodNotDeclaredError(name)
        }
        methods.head -> expr
    }
  }

  override def eval(input: InternalRow): Any = {
    val instance = beanInstance.eval(input)
    if (instance != null) {
      val bean = instance.asInstanceOf[Object]
      resolvedSetters.foreach {
        case (setter, expr) =>
          val paramVal = expr.eval(input)
          // We don't call setter if input value is null.
          if (paramVal != null) {
            setter.invoke(bean, paramVal.asInstanceOf[AnyRef])
          }
      }
    }
    instance
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val instanceGen = beanInstance.genCode(ctx)

    val javaBeanInstance = ctx.freshName("javaBean")
    val beanInstanceJavaType = CodeGenerator.javaType(beanInstance.dataType)

    val initialize = setters.map {
      case (setterMethod, fieldValue) =>
        val fieldGen = fieldValue.genCode(ctx)
        s"""
           |${fieldGen.code}
           |if (!${fieldGen.isNull}) {
           |  $javaBeanInstance.$setterMethod(${fieldGen.value});
           |}
         """.stripMargin
    }
    val initializeCode = ctx.splitExpressionsWithCurrentInputs(
      expressions = initialize.toSeq,
      funcName = "initializeJavaBean",
      extraArguments = beanInstanceJavaType -> javaBeanInstance :: Nil)

    val code = instanceGen.code +
      code"""
         |$beanInstanceJavaType $javaBeanInstance = ${instanceGen.value};
         |if (!${instanceGen.isNull}) {
         |  $initializeCode
         |}
       """.stripMargin
    ev.copy(code = code, isNull = instanceGen.isNull, value = instanceGen.value)
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): InitializeJavaBean =
    super.legacyWithNewChildren(newChildren).asInstanceOf[InitializeJavaBean]
}

/**
 * Asserts that input values of a non-nullable child expression are not null.
 *
 * Note that there are cases where `child.nullable == true`, while we still need to add this
 * assertion.  Consider a nullable column `s` whose data type is a struct containing a non-nullable
 * `Int` field named `i`.  Expression `s.i` is nullable because `s` can be null.  However, for all
 * non-null `s`, `s.i` can't be null.
 */
case class AssertNotNull(child: Expression, walkedTypePath: Seq[String] = Nil)
  extends UnaryExpression with NonSQLExpression {

  override def dataType: DataType = child.dataType
  override def foldable: Boolean = false
  override def nullable: Boolean = false

  final override val nodePatterns: Seq[TreePattern] = Seq(NULL_CHECK)

  override def flatArguments: Iterator[Any] = Iterator(child)

  private val errMsg = walkedTypePath.mkString("\n", "\n", "\n")

  override def eval(input: InternalRow): Any = {
    val result = child.eval(input)
    if (result == null) {
      throw QueryExecutionErrors.notNullAssertViolation(errMsg)
    }
    result
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childGen = child.genCode(ctx)

    // Use unnamed reference that doesn't create a local field here to reduce the number of fields
    // because errMsgField is used only when the value is null.
    val errMsgField = ctx.addReferenceObj("errMsg", errMsg)

    val code = childGen.code + code"""
      if (${childGen.isNull}) {
        throw QueryExecutionErrors.notNullAssertViolation($errMsgField);
      }
     """
    ev.copy(code = code, isNull = FalseLiteral, value = childGen.value)
  }

  override protected def withNewChildInternal(newChild: Expression): AssertNotNull =
    copy(child = newChild)
}

/**
 * Returns the value of field at index `index` from the external row `child`.
 * This class can be viewed as [[GetStructField]] for [[Row]]s instead of [[InternalRow]]s.
 *
 * Note that the input row and the field we try to get are both guaranteed to be not null, if they
 * are null, a runtime exception will be thrown.
 */
case class GetExternalRowField(
    child: Expression,
    index: Int,
    fieldName: String) extends UnaryExpression with NonSQLExpression {

  override def nullable: Boolean = false

  override def dataType: DataType = ObjectType(classOf[Object])

  private val errMsg = QueryExecutionErrors.fieldCannotBeNullMsg(index, fieldName)

  override def eval(input: InternalRow): Any = {
    val inputRow = child.eval(input).asInstanceOf[Row]
    if (inputRow == null) {
      throw QueryExecutionErrors.inputExternalRowCannotBeNullError()
    }
    if (inputRow.isNullAt(index)) {
      throw QueryExecutionErrors.fieldCannotBeNullError(index, fieldName)
    }
    inputRow.get(index)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // Use unnamed reference that doesn't create a local field here to reduce the number of fields
    // because errMsgField is used only when the field is null.
    val errMsgField = ctx.addReferenceObj("errMsg", errMsg)
    val row = child.genCode(ctx)
    val code = code"""
      ${row.code}

      if (${row.isNull}) {
        throw QueryExecutionErrors.inputExternalRowCannotBeNullError();
      }

      if (${row.value}.isNullAt($index)) {
        throw new RuntimeException($errMsgField);
      }

      final Object ${ev.value} = ${row.value}.get($index);
     """
    ev.copy(code = code, isNull = FalseLiteral)
  }

  override protected def withNewChildInternal(newChild: Expression): GetExternalRowField =
    copy(child = newChild)
}

/**
 * Validates the actual data type of input expression at runtime.  If it doesn't match the
 * expectation, throw an exception.
 */
case class ValidateExternalType(child: Expression, expected: DataType, externalDataType: DataType)
  extends UnaryExpression with NonSQLExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(ObjectType(classOf[Object]))

  override def nullable: Boolean = child.nullable

  override val dataType: DataType = externalDataType

  private lazy val errMsg = s" is not a valid external type for schema of ${expected.simpleString}"

  private lazy val checkType: (Any) => Boolean = expected match {
    case _: DecimalType =>
      (value: Any) => {
        value.isInstanceOf[java.math.BigDecimal] || value.isInstanceOf[scala.math.BigDecimal] ||
          value.isInstanceOf[Decimal]
      }
    case _: ArrayType =>
      (value: Any) => {
        value.getClass.isArray ||
          value.isInstanceOf[scala.collection.Seq[_]] ||
          value.isInstanceOf[Set[_]] ||
          value.isInstanceOf[java.util.List[_]]
      }
    case _: DateType =>
      (value: Any) => {
        value.isInstanceOf[java.sql.Date] || value.isInstanceOf[java.time.LocalDate]
      }
    case _: TimestampType =>
      (value: Any) => {
        value.isInstanceOf[java.sql.Timestamp] || value.isInstanceOf[java.time.Instant]
      }
    case _ =>
      val dataTypeClazz = EncoderUtils.javaBoxedType(dataType)
      (value: Any) => {
        dataTypeClazz.isInstance(value)
      }
  }

  override def nullSafeEval(input: Any): Any = {
    if (checkType(input)) {
      input
    } else {
      throw new RuntimeException(s"${input.getClass.getName}$errMsg")
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // Use unnamed reference that doesn't create a local field here to reduce the number of fields
    // because errMsgField is used only when the type doesn't match.
    val errMsgField = ctx.addReferenceObj("errMsg", errMsg)
    val input = child.genCode(ctx)
    val obj = input.value
    def genCheckTypes(classes: Seq[Class[_]]): String = {
      classes.map(cls => s"$obj instanceof ${cls.getName}").mkString(" || ")
    }
    val typeCheck = expected match {
      case _: DecimalType =>
        genCheckTypes(Seq(
          classOf[java.math.BigDecimal],
          classOf[scala.math.BigDecimal],
          classOf[Decimal]))
      case _: ArrayType =>
        val check = genCheckTypes(Seq(
          classOf[scala.collection.Seq[_]],
          classOf[Set[_]],
          classOf[java.util.List[_]]))
        s"$obj.getClass().isArray() || $check"
      case _: DateType =>
        genCheckTypes(Seq(classOf[java.sql.Date], classOf[java.time.LocalDate]))
      case _: TimestampType =>
        genCheckTypes(Seq(classOf[java.sql.Timestamp], classOf[java.time.Instant]))
      case _ =>
        s"$obj instanceof ${CodeGenerator.boxedType(dataType)}"
    }

    val code = code"""
      ${input.code}
      ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
      if (!${input.isNull}) {
        if ($typeCheck) {
          ${ev.value} = (${CodeGenerator.boxedType(dataType)}) $obj;
        } else {
          throw new RuntimeException($obj.getClass().getName() + $errMsgField);
        }
      }

    """
    ev.copy(code = code, isNull = input.isNull)
  }

  override protected def withNewChildInternal(newChild: Expression): ValidateExternalType =
    copy(child = newChild)
}
