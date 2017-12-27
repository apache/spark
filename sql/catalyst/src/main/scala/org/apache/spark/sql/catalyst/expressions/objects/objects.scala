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

import java.lang.reflect.Modifier

import scala.collection.mutable.Builder
import scala.language.existentials
import scala.reflect.ClassTag
import scala.util.Try

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.serializer._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.ScalaReflection.universe.TermName
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData}
import org.apache.spark.sql.types._

/**
 * Common base class for [[StaticInvoke]], [[Invoke]], and [[NewInstance]].
 */
trait InvokeLike extends Expression with NonSQLExpression {

  def arguments: Seq[Expression]

  def propagateNull: Boolean

  protected lazy val needNullCheck: Boolean = propagateNull && arguments.exists(_.nullable)

  /**
   * Prepares codes for arguments.
   *
   * - generate codes for argument.
   * - use ctx.splitExpressions() to not exceed 64kb JVM limit while preparing arguments.
   * - avoid some of nullabilty checking which are not needed because the expression is not
   *   nullable.
   * - when needNullCheck == true, short circuit if we found one of arguments is null because
   *   preparing rest of arguments can be skipped in the case.
   *
   * @param ctx a [[CodegenContext]]
   * @return (code to prepare arguments, argument string, result of argument null check)
   */
  def prepareArguments(ctx: CodegenContext): (String, String, String) = {

    val resultIsNull = if (needNullCheck) {
      val resultIsNull = ctx.addMutableState(ctx.JAVA_BOOLEAN, "resultIsNull")
      resultIsNull
    } else {
      "false"
    }
    val argValues = arguments.map { e =>
      val argValue = ctx.addMutableState(ctx.javaType(e.dataType), "argValue")
      argValue
    }

    val argCodes = if (needNullCheck) {
      val reset = s"$resultIsNull = false;"
      val argCodes = arguments.zipWithIndex.map { case (e, i) =>
        val expr = e.genCode(ctx)
        val updateResultIsNull = if (e.nullable) {
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
 * @param propagateNull When true, and any of the arguments is null, null will be returned instead
 *                      of calling the function.
 * @param returnNullable When false, indicating the invoked method will always return
 *                       non-null value.
 */
case class StaticInvoke(
    staticObject: Class[_],
    dataType: DataType,
    functionName: String,
    arguments: Seq[Expression] = Nil,
    propagateNull: Boolean = true,
    returnNullable: Boolean = true) extends InvokeLike {

  val objectName = staticObject.getName.stripSuffix("$")

  override def nullable: Boolean = needNullCheck || returnNullable
  override def children: Seq[Expression] = arguments

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaType = ctx.javaType(dataType)

    val (argCode, argString, resultIsNull) = prepareArguments(ctx)

    val callFunc = s"$objectName.$functionName($argString)"

    val prepareIsNull = if (nullable) {
      s"boolean ${ev.isNull} = $resultIsNull;"
    } else {
      ev.isNull = "false"
      ""
    }

    val evaluate = if (returnNullable) {
      if (ctx.defaultValue(dataType) == "null") {
        s"""
          ${ev.value} = $callFunc;
          ${ev.isNull} = ${ev.value} == null;
        """
      } else {
        val boxedResult = ctx.freshName("boxedResult")
        s"""
          ${ctx.boxedType(dataType)} $boxedResult = $callFunc;
          ${ev.isNull} = $boxedResult == null;
          if (!${ev.isNull}) {
            ${ev.value} = $boxedResult;
          }
        """
      }
    } else {
      s"${ev.value} = $callFunc;"
    }

    val code = s"""
      $argCode
      $prepareIsNull
      $javaType ${ev.value} = ${ctx.defaultValue(dataType)};
      if (!$resultIsNull) {
        $evaluate
      }
     """
    ev.copy(code = code)
  }
}

/**
 * Invokes a call to reference to a static field.
 *
 * @param staticObject The target of the static call.  This can either be the object itself
 *                     (methods defined on scala objects), or the class object
 *                     (static methods defined in java).
 * @param dataType The expected return type of the function call.
 * @param fieldName The field to reference.
 */
case class StaticField(
  staticObject: Class[_],
  dataType: DataType,
  fieldName: String) extends Expression with NonSQLExpression {

  val objectName = staticObject.getName.stripSuffix("$")

  override def nullable: Boolean = false
  override def children: Seq[Expression] = Nil

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaType = ctx.javaType(dataType)

    val code = s"""
      final $javaType ${ev.value} = $objectName.$fieldName;
    """

    ev.copy(code = code, isNull = "false")
  }
}

/**
 * Wraps an expression in a try-catch block, which can be used if the body expression may throw a
 * exception.
 *
 * @param body The expression body to wrap in a try-catch block.
 * @param dataType The return type of the try block.
 * @param returnNullable When false, indicating the invoked method will always return
 *                       non-null value.
 */
case class WrapException(
    body: Expression,
    dataType: DataType,
    returnNullable: Boolean = true) extends Expression with NonSQLExpression {

  override def nullable: Boolean = returnNullable
  override def children: Seq[Expression] = Seq(body)

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaType = ctx.javaType(dataType)
    val returnName = ctx.freshName("returnName")

    val bodyExpr = body.genCode(ctx)

    val code =
      s"""
         |final $javaType $returnName;
         |try {
         |  ${bodyExpr.code}
         |  $returnName = ${bodyExpr.value};
         |} catch (Exception e) {
         |  org.apache.spark.unsafe.Platform.throwException(e);
         |}
       """.stripMargin

    ev.copy(code = code, isNull = bodyExpr.isNull, value = returnName)
  }
}

/**
 * Determines if the given value is an instanceof a given class
 *
 * @param value the value to check
 * @param checkedType the class to check the value against
 */
case class InstanceOf(
    value: Expression,
    checkedType: Class[_]) extends Expression with NonSQLExpression {

  override def nullable: Boolean = false
  override def children: Seq[Expression] = value :: Nil
  override def dataType: DataType = BooleanType

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {

    val obj = value.genCode(ctx)

    val code =
      s"""
         ${obj.code}
         final boolean ${ev.value} = ${obj.value} instanceof ${checkedType.getName};
       """

    ev.copy(code = code, isNull = "false")
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
 * @param arguments An optional list of expressions, whos evaluation will be passed to the function.
 * @param propagateNull When true, and any of the arguments is null, null will be returned instead
 *                      of calling the function.
 * @param returnNullable When false, indicating the invoked method will always return
 *                       non-null value.
 */
case class Invoke(
    targetObject: Expression,
    functionName: String,
    dataType: DataType,
    arguments: Seq[Expression] = Nil,
    propagateNull: Boolean = true,
    returnNullable : Boolean = true) extends InvokeLike {

  override def nullable: Boolean = targetObject.nullable || needNullCheck || returnNullable
  override def children: Seq[Expression] = targetObject +: arguments

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  private lazy val encodedFunctionName = TermName(functionName).encodedName.toString

  @transient lazy val method = targetObject.dataType match {
    case ObjectType(cls) =>
      val m = cls.getMethods.find(_.getName == encodedFunctionName)
      if (m.isEmpty) {
        sys.error(s"Couldn't find $encodedFunctionName on $cls")
      } else {
        m
      }
    case _ => None
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaType = ctx.javaType(dataType)
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
        s"${ev.value} = (${ctx.boxedType(javaType)}) $funcResult;"
      } else {
        s"""
          if ($funcResult != null) {
            ${ev.value} = (${ctx.boxedType(javaType)}) $funcResult;
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

    val code = s"""
      ${obj.code}
      boolean ${ev.isNull} = true;
      $javaType ${ev.value} = ${ctx.defaultValue(dataType)};
      if (!${obj.isNull}) {
        $argCode
        ${ev.isNull} = $resultIsNull;
        if (!${ev.isNull}) {
          $evaluate
        }
      }
     """
    ev.copy(code = code)
  }

  override def toString: String = s"$targetObject.$functionName"
}

object NewInstance {
  def apply(
      cls: Class[_],
      arguments: Seq[Expression],
      dataType: DataType,
      propagateNull: Boolean = true): NewInstance =
    new NewInstance(cls, arguments, propagateNull, dataType, None)
}

/**
 * Constructs a new instance of the given class, using the result of evaluating the specified
 * expressions as arguments.
 *
 * @param cls The class to construct.
 * @param arguments A list of expression to use as arguments to the constructor.
 * @param propagateNull When true, if any of the arguments is null, then null will be returned
 *                      instead of trying to construct the object.
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
    propagateNull: Boolean,
    dataType: DataType,
    outerPointer: Option[() => AnyRef]) extends InvokeLike {
  private val className = cls.getName

  override def nullable: Boolean = needNullCheck

  override def children: Seq[Expression] = arguments

  override lazy val resolved: Boolean = {
    // If the class to construct is an inner class, we need to get its outer pointer, or this
    // expression should be regarded as unresolved.
    // Note that static inner classes (e.g., inner classes within Scala objects) don't need
    // outer pointer registration.
    val needOuterPointer =
      outerPointer.isEmpty && cls.isMemberClass && !Modifier.isStatic(cls.getModifiers)
    childrenResolved && !needOuterPointer
  }

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaType = ctx.javaType(dataType)

    val (argCode, argString, resultIsNull) = prepareArguments(ctx)

    val outer = outerPointer.map(func => Literal.fromObject(func()).genCode(ctx))

    ev.isNull = resultIsNull

    val constructorCall = outer.map { gen =>
      s"${gen.value}.new ${cls.getSimpleName}($argString)"
    }.getOrElse {
      s"new $className($argString)"
    }

    val code = s"""
      $argCode
      ${outer.map(_.code).getOrElse("")}
      final $javaType ${ev.value} = ${ev.isNull} ? ${ctx.defaultValue(javaType)} : $constructorCall;
    """
    ev.copy(code = code)
  }

  override def toString: String = s"newInstance($cls)"
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

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaType = ctx.javaType(dataType)
    val inputObject = child.genCode(ctx)

    val code = s"""
      ${inputObject.code}

      final boolean ${ev.isNull} = ${inputObject.isNull} || ${inputObject.value}.isEmpty();
      $javaType ${ev.value} = ${ev.isNull} ?
        ${ctx.defaultValue(javaType)} : (${ctx.boxedType(javaType)}) ${inputObject.value}.get();
    """
    ev.copy(code = code)
  }
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

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val inputObject = child.genCode(ctx)

    val code = s"""
      ${inputObject.code}

      scala.Option ${ev.value} =
        ${inputObject.isNull} ?
        scala.Option$$.MODULE$$.apply(null) : new scala.Some(${inputObject.value});
    """
    ev.copy(code = code, isNull = "false")
  }
}

/**
 * A placeholder for the loop variable used in [[MapObjects]].  This should never be constructed
 * manually, but will instead be passed into the provided lambda function.
 */
case class LambdaVariable(
    value: String,
    isNull: String,
    dataType: DataType,
    nullable: Boolean = true) extends LeafExpression
  with Unevaluable with NonSQLExpression {

  override def genCode(ctx: CodegenContext): ExprCode = {
    ExprCode(code = "", value = value, isNull = if (nullable) isNull else "false")
  }
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
    throw new UnsupportedOperationException("not resolved")
  }
}

object MapObjects {
  private val curId = new java.util.concurrent.atomic.AtomicInteger()

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
    val id = curId.getAndIncrement()
    val loopValue = s"MapObjects_loopValue$id"
    val loopIsNull = if (elementNullable) {
      s"MapObjects_loopIsNull$id"
    } else {
      "false"
    }
    val loopVar = LambdaVariable(loopValue, loopIsNull, elementType, elementNullable)
    MapObjects(
      loopValue, loopIsNull, elementType, function(loopVar), inputData, customCollectionCls)
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
 * @param loopValue the name of the loop variable that used when iterate the collection, and used
 *                  as input for the `lambdaFunction`
 * @param loopIsNull the nullity of the loop variable that used when iterate the collection, and
 *                   used as input for the `lambdaFunction`
 * @param loopVarDataType the data type of the loop variable that used when iterate the collection,
 *                        and used as input for the `lambdaFunction`
 * @param lambdaFunction A function that take the `loopVar` as input, and used as lambda function
 *                       to handle collection elements.
 * @param inputData An expression that when evaluated returns a collection object.
 * @param customCollectionCls Class of the resulting collection (returning ObjectType)
 *                            or None (returning ArrayType)
 */
case class MapObjects private(
    loopValue: String,
    loopIsNull: String,
    loopVarDataType: DataType,
    lambdaFunction: Expression,
    inputData: Expression,
    customCollectionCls: Option[Class[_]]) extends Expression with NonSQLExpression {

  override def nullable: Boolean = inputData.nullable

  override def children: Seq[Expression] = lambdaFunction :: inputData :: Nil

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported")

  override def dataType: DataType =
    customCollectionCls.map(ObjectType.apply).getOrElse(
      ArrayType(lambdaFunction.dataType, containsNull = lambdaFunction.nullable))

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val elementJavaType = ctx.javaType(loopVarDataType)
    ctx.addMutableState(elementJavaType, loopValue, forceInline = true, useFreshName = false)
    val genInputData = inputData.genCode(ctx)
    val genFunction = lambdaFunction.genCode(ctx)
    val dataLength = ctx.freshName("dataLength")
    val convertedArray = ctx.freshName("convertedArray")
    val loopIndex = ctx.freshName("loopIndex")

    val convertedType = ctx.boxedType(lambdaFunction.dataType)

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
        val seqClass = classOf[Seq[_]].getName
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

    // The data with PythonUserDefinedType are actually stored with the data type of its sqlType.
    // When we want to apply MapObjects on it, we have to use it.
    val inputDataType = inputData.dataType match {
      case p: PythonUserDefinedType => p.sqlType
      case _ => inputData.dataType
    }

    // `MapObjects` generates a while loop to traverse the elements of the input collection. We
    // need to take care of Seq and List because they may have O(n) complexity for indexed accessing
    // like `list.get(1)`. Here we use Iterator to traverse Seq and List.
    val (getLength, prepareLoop, getLoopVar) = inputDataType match {
      case ObjectType(cls) if classOf[Seq[_]].isAssignableFrom(cls) =>
        val it = ctx.freshName("it")
        (
          s"${genInputData.value}.size()",
          s"scala.collection.Iterator $it = ${genInputData.value}.toIterator();",
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
      case ArrayType(et, _) =>
        (
          s"${genInputData.value}.numElements()",
          "",
          ctx.getValue(genInputData.value, et, loopIndex)
        )
      case ObjectType(cls) if cls == classOf[Object] =>
        val it = ctx.freshName("it")
        (
          s"$seq == null ? $array.length : $seq.size()",
          s"scala.collection.Iterator $it = $seq == null ? null : $seq.toIterator();",
          s"$it == null ? $array[$loopIndex] : $it.next()"
        )
    }

    // Make a copy of the data if it's unsafe-backed
    def makeCopyIfInstanceOf(clazz: Class[_ <: Any], value: String) =
      s"$value instanceof ${clazz.getSimpleName}? ${value}.copy() : $value"
    val genFunctionValue = lambdaFunction.dataType match {
      case StructType(_) => makeCopyIfInstanceOf(classOf[UnsafeRow], genFunction.value)
      case ArrayType(_, _) => makeCopyIfInstanceOf(classOf[UnsafeArrayData], genFunction.value)
      case MapType(_, _, _) => makeCopyIfInstanceOf(classOf[UnsafeMapData], genFunction.value)
      case _ => genFunction.value
    }

    val loopNullCheck = if (loopIsNull != "false") {
      ctx.addMutableState(ctx.JAVA_BOOLEAN, loopIsNull, forceInline = true, useFreshName = false)
      inputDataType match {
        case _: ArrayType => s"$loopIsNull = ${genInputData.value}.isNullAt($loopIndex);"
        case _ => s"$loopIsNull = $loopValue == null;"
      }
    } else {
      ""
    }

    val (initCollection, addElement, getResult): (String, String => String, String) =
      customCollectionCls match {
        case Some(cls) if classOf[Seq[_]].isAssignableFrom(cls) ||
          classOf[scala.collection.Set[_]].isAssignableFrom(cls) =>
          // Scala sequence or set
          val getBuilder = s"${cls.getName}$$.MODULE$$.newBuilder()"
          val builder = ctx.freshName("collectionBuilder")
          (
            s"""
               ${classOf[Builder[_, _]].getName} $builder = $getBuilder;
               $builder.sizeHint($dataLength);
             """,
            genValue => s"$builder.$$plus$$eq($genValue);",
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
            genValue => s"$builder.add($genValue);",
            s"$builder;"
          )
        case None =>
          // array
          (
            s"""
               $convertedType[] $convertedArray = null;
               $convertedArray = $arrayConstructor;
             """,
            genValue => s"$convertedArray[$loopIndex] = $genValue;",
            s"new ${classOf[GenericArrayData].getName}($convertedArray);"
          )
      }

    val code = s"""
      ${genInputData.code}
      ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};

      if (!${genInputData.isNull}) {
        $determineCollectionType
        int $dataLength = $getLength;
        $initCollection

        int $loopIndex = 0;
        $prepareLoop
        while ($loopIndex < $dataLength) {
          $loopValue = ($elementJavaType) ($getLoopVar);
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
}

object CatalystToExternalMap {
  private val curId = new java.util.concurrent.atomic.AtomicInteger()

  /**
   * Construct an instance of CatalystToExternalMap case class.
   *
   * @param keyFunction The function applied on the key collection elements.
   * @param valueFunction The function applied on the value collection elements.
   * @param inputData An expression that when evaluated returns a map object.
   * @param collClass The type of the resulting collection.
   */
  def apply(
      keyFunction: Expression => Expression,
      valueFunction: Expression => Expression,
      inputData: Expression,
      collClass: Class[_]): CatalystToExternalMap = {
    val id = curId.getAndIncrement()
    val keyLoopValue = s"CatalystToExternalMap_keyLoopValue$id"
    val mapType = inputData.dataType.asInstanceOf[MapType]
    val keyLoopVar = LambdaVariable(keyLoopValue, "", mapType.keyType, nullable = false)
    val valueLoopValue = s"CatalystToExternalMap_valueLoopValue$id"
    val valueLoopIsNull = if (mapType.valueContainsNull) {
      s"CatalystToExternalMap_valueLoopIsNull$id"
    } else {
      "false"
    }
    val valueLoopVar = LambdaVariable(valueLoopValue, valueLoopIsNull, mapType.valueType)
    CatalystToExternalMap(
      keyLoopValue, keyFunction(keyLoopVar),
      valueLoopValue, valueLoopIsNull, valueFunction(valueLoopVar),
      inputData, collClass)
  }
}

/**
 * Expression used to convert a Catalyst Map to an external Scala Map.
 * The collection is constructed using the associated builder, obtained by calling `newBuilder`
 * on the collection's companion object.
 *
 * @param keyLoopValue the name of the loop variable that is used when iterating over the key
 *                     collection, and which is used as input for the `keyLambdaFunction`
 * @param keyLambdaFunction A function that takes the `keyLoopVar` as input, and is used as
 *                          a lambda function to handle collection elements.
 * @param valueLoopValue the name of the loop variable that is used when iterating over the value
 *                       collection, and which is used as input for the `valueLambdaFunction`
 * @param valueLoopIsNull the nullability of the loop variable that is used when iterating over
 *                        the value collection, and which is used as input for the
 *                        `valueLambdaFunction`
 * @param valueLambdaFunction A function that takes the `valueLoopVar` as input, and is used as
 *                            a lambda function to handle collection elements.
 * @param inputData An expression that when evaluated returns a map object.
 * @param collClass The type of the resulting collection.
 */
case class CatalystToExternalMap private(
    keyLoopValue: String,
    keyLambdaFunction: Expression,
    valueLoopValue: String,
    valueLoopIsNull: String,
    valueLambdaFunction: Expression,
    inputData: Expression,
    collClass: Class[_]) extends Expression with NonSQLExpression {

  override def nullable: Boolean = inputData.nullable

  override def children: Seq[Expression] =
    keyLambdaFunction :: valueLambdaFunction :: inputData :: Nil

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported")

  override def dataType: DataType = ObjectType(collClass)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // The data with PythonUserDefinedType are actually stored with the data type of its sqlType.
    // When we want to apply MapObjects on it, we have to use it.
    def inputDataType(dataType: DataType) = dataType match {
      case p: PythonUserDefinedType => p.sqlType
      case _ => dataType
    }

    val mapType = inputDataType(inputData.dataType).asInstanceOf[MapType]
    val keyElementJavaType = ctx.javaType(mapType.keyType)
    ctx.addMutableState(keyElementJavaType, keyLoopValue, forceInline = true, useFreshName = false)
    val genKeyFunction = keyLambdaFunction.genCode(ctx)
    val valueElementJavaType = ctx.javaType(mapType.valueType)
    ctx.addMutableState(valueElementJavaType, valueLoopValue, forceInline = true,
      useFreshName = false)
    val genValueFunction = valueLambdaFunction.genCode(ctx)
    val genInputData = inputData.genCode(ctx)
    val dataLength = ctx.freshName("dataLength")
    val loopIndex = ctx.freshName("loopIndex")
    val tupleLoopValue = ctx.freshName("tupleLoopValue")
    val builderValue = ctx.freshName("builderValue")

    val getLength = s"${genInputData.value}.numElements()"

    val keyArray = ctx.freshName("keyArray")
    val valueArray = ctx.freshName("valueArray")
    val getKeyArray =
      s"${classOf[ArrayData].getName} $keyArray = ${genInputData.value}.keyArray();"
    val getKeyLoopVar = ctx.getValue(keyArray, inputDataType(mapType.keyType), loopIndex)
    val getValueArray =
      s"${classOf[ArrayData].getName} $valueArray = ${genInputData.value}.valueArray();"
    val getValueLoopVar = ctx.getValue(valueArray, inputDataType(mapType.valueType), loopIndex)

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

    val valueLoopNullCheck = if (valueLoopIsNull != "false") {
      ctx.addMutableState(ctx.JAVA_BOOLEAN, valueLoopIsNull, forceInline = true,
        useFreshName = false)
      s"$valueLoopIsNull = $valueArray.isNullAt($loopIndex);"
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

    val code = s"""
      ${genInputData.code}
      ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};

      if (!${genInputData.isNull}) {
        int $dataLength = $getLength;
        $constructBuilder
        $getKeyArray
        $getValueArray

        int $loopIndex = 0;
        while ($loopIndex < $dataLength) {
          $keyLoopValue = ($keyElementJavaType) ($getKeyLoopVar);
          $valueLoopValue = ($valueElementJavaType) ($getValueLoopVar);
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
}

object ExternalMapToCatalyst {
  private val curId = new java.util.concurrent.atomic.AtomicInteger()

  def apply(
      inputMap: Expression,
      keyType: DataType,
      keyConverter: Expression => Expression,
      keyNullable: Boolean,
      valueType: DataType,
      valueConverter: Expression => Expression,
      valueNullable: Boolean): ExternalMapToCatalyst = {
    val id = curId.getAndIncrement()
    val keyName = "ExternalMapToCatalyst_key" + id
    val keyIsNull = if (keyNullable) {
      "ExternalMapToCatalyst_key_isNull" + id
    } else {
      "false"
    }
    val valueName = "ExternalMapToCatalyst_value" + id
    val valueIsNull = if (valueNullable) {
      "ExternalMapToCatalyst_value_isNull" + id
    } else {
      "false"
    }

    ExternalMapToCatalyst(
      keyName,
      keyIsNull,
      keyType,
      keyConverter(LambdaVariable(keyName, keyIsNull, keyType, keyNullable)),
      valueName,
      valueIsNull,
      valueType,
      valueConverter(LambdaVariable(valueName, valueIsNull, valueType, valueNullable)),
      inputMap
    )
  }
}

/**
 * Converts a Scala/Java map object into catalyst format, by applying the key/value converter when
 * iterate the map.
 *
 * @param key the name of the map key variable that used when iterate the map, and used as input for
 *            the `keyConverter`
 * @param keyIsNull the nullability of the map key variable that used when iterate the map, and
 *                  used as input for the `keyConverter`
 * @param keyType the data type of the map key variable that used when iterate the map, and used as
 *                input for the `keyConverter`
 * @param keyConverter A function that take the `key` as input, and converts it to catalyst format.
 * @param value the name of the map value variable that used when iterate the map, and used as input
 *              for the `valueConverter`
 * @param valueIsNull the nullability of the map value variable that used when iterate the map, and
 *                    used as input for the `valueConverter`
 * @param valueType the data type of the map value variable that used when iterate the map, and
 *                  used as input for the `valueConverter`
 * @param valueConverter A function that take the `value` as input, and converts it to catalyst
 *                       format.
 * @param child An expression that when evaluated returns the input map object.
 */
case class ExternalMapToCatalyst(
    key: String,
    keyIsNull: String,
    keyType: DataType,
    keyConverter: Expression,
    value: String,
    valueIsNull: String,
    valueType: DataType,
    valueConverter: Expression,
    child: Expression)
  extends UnaryExpression with NonSQLExpression {

  override def foldable: Boolean = false

  override def dataType: MapType = MapType(
    keyConverter.dataType, valueConverter.dataType, valueContainsNull = valueConverter.nullable)

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported")

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val inputMap = child.genCode(ctx)
    val genKeyConverter = keyConverter.genCode(ctx)
    val genValueConverter = valueConverter.genCode(ctx)
    val length = ctx.freshName("length")
    val index = ctx.freshName("index")
    val convertedKeys = ctx.freshName("convertedKeys")
    val convertedValues = ctx.freshName("convertedValues")
    val entry = ctx.freshName("entry")
    val entries = ctx.freshName("entries")

    val keyElementJavaType = ctx.javaType(keyType)
    val valueElementJavaType = ctx.javaType(valueType)
    ctx.addMutableState(keyElementJavaType, key, forceInline = true, useFreshName = false)
    ctx.addMutableState(valueElementJavaType, value, forceInline = true, useFreshName = false)

    val (defineEntries, defineKeyValue) = child.dataType match {
      case ObjectType(cls) if classOf[java.util.Map[_, _]].isAssignableFrom(cls) =>
        val javaIteratorCls = classOf[java.util.Iterator[_]].getName
        val javaMapEntryCls = classOf[java.util.Map.Entry[_, _]].getName

        val defineEntries =
          s"final $javaIteratorCls $entries = ${inputMap.value}.entrySet().iterator();"

        val defineKeyValue =
          s"""
            final $javaMapEntryCls $entry = ($javaMapEntryCls) $entries.next();
            $key = (${ctx.boxedType(keyType)}) $entry.getKey();
            $value = (${ctx.boxedType(valueType)}) $entry.getValue();
          """

        defineEntries -> defineKeyValue

      case ObjectType(cls) if classOf[scala.collection.Map[_, _]].isAssignableFrom(cls) =>
        val scalaIteratorCls = classOf[Iterator[_]].getName
        val scalaMapEntryCls = classOf[Tuple2[_, _]].getName

        val defineEntries = s"final $scalaIteratorCls $entries = ${inputMap.value}.iterator();"

        val defineKeyValue =
          s"""
            final $scalaMapEntryCls $entry = ($scalaMapEntryCls) $entries.next();
            $key = (${ctx.boxedType(keyType)}) $entry._1();
            $value = (${ctx.boxedType(valueType)}) $entry._2();
          """

        defineEntries -> defineKeyValue
    }

    val keyNullCheck = if (keyIsNull != "false") {
      ctx.addMutableState(ctx.JAVA_BOOLEAN, keyIsNull, forceInline = true, useFreshName = false)
      s"$keyIsNull = $key == null;"
    } else {
      ""
    }

    val valueNullCheck = if (valueIsNull != "false") {
      ctx.addMutableState(ctx.JAVA_BOOLEAN, valueIsNull, forceInline = true, useFreshName = false)
      s"$valueIsNull = $value == null;"
    } else {
      ""
    }

    val arrayCls = classOf[GenericArrayData].getName
    val mapCls = classOf[ArrayBasedMapData].getName
    val convertedKeyType = ctx.boxedType(keyConverter.dataType)
    val convertedValueType = ctx.boxedType(valueConverter.dataType)
    val code =
      s"""
        ${inputMap.code}
        ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
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
              throw new RuntimeException("Cannot use null as map key!");
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

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported")

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
      s"""
         |Object[] $values = new Object[${children.size}];
         |$childrenCode
         |final ${classOf[Row].getName} ${ev.value} = new $rowClass($values, $schemaField);
       """.stripMargin
    ev.copy(code = code, isNull = "false")
  }
}

/**
 * Serializes an input object using a generic serializer (Kryo or Java).
 *
 * @param kryo if true, use Kryo. Otherwise, use Java.
 */
case class EncodeUsingSerializer(child: Expression, kryo: Boolean)
  extends UnaryExpression with NonSQLExpression {

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported")

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // Code to initialize the serializer.
    val (serializer, serializerClass, serializerInstanceClass) = {
      if (kryo) {
        ("kryoSerializer",
          classOf[KryoSerializer].getName,
          classOf[KryoSerializerInstance].getName)
      } else {
        ("javaSerializer",
          classOf[JavaSerializer].getName,
          classOf[JavaSerializerInstance].getName)
      }
    }
    // try conf from env, otherwise create a new one
    val env = s"${classOf[SparkEnv].getName}.get()"
    val sparkConf = s"new ${classOf[SparkConf].getName}()"
    ctx.addImmutableStateIfNotExists(serializerInstanceClass, serializer, v =>
      s"""
         |if ($env == null) {
         |  $v = ($serializerInstanceClass) new $serializerClass($sparkConf).newInstance();
         |} else {
         |  $v = ($serializerInstanceClass) new $serializerClass($env.conf()).newInstance();
         |}
       """.stripMargin)

    // Code to serialize.
    val input = child.genCode(ctx)
    val javaType = ctx.javaType(dataType)
    val serialize = s"$serializer.serialize(${input.value}, null).array()"

    val code = s"""
      ${input.code}
      final $javaType ${ev.value} = ${input.isNull} ? ${ctx.defaultValue(javaType)} : $serialize;
     """
    ev.copy(code = code, isNull = input.isNull)
  }

  override def dataType: DataType = BinaryType
}

/**
 * Serializes an input object using a generic serializer (Kryo or Java).  Note that the ClassTag
 * is not an implicit parameter because TreeNode cannot copy implicit parameters.
 *
 * @param kryo if true, use Kryo. Otherwise, use Java.
 */
case class DecodeUsingSerializer[T](child: Expression, tag: ClassTag[T], kryo: Boolean)
  extends UnaryExpression with NonSQLExpression {

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // Code to initialize the serializer.
    val (serializer, serializerClass, serializerInstanceClass) = {
      if (kryo) {
        ("kryoSerializer",
          classOf[KryoSerializer].getName,
          classOf[KryoSerializerInstance].getName)
      } else {
        ("javaSerializer",
          classOf[JavaSerializer].getName,
          classOf[JavaSerializerInstance].getName)
      }
    }
    // try conf from env, otherwise create a new one
    val env = s"${classOf[SparkEnv].getName}.get()"
    val sparkConf = s"new ${classOf[SparkConf].getName}()"
    ctx.addImmutableStateIfNotExists(serializerInstanceClass, serializer, v =>
      s"""
         |if ($env == null) {
         |  $v = ($serializerInstanceClass) new $serializerClass($sparkConf).newInstance();
         |} else {
         |  $v = ($serializerInstanceClass) new $serializerClass($env.conf()).newInstance();
         |}
       """.stripMargin)

    // Code to deserialize.
    val input = child.genCode(ctx)
    val javaType = ctx.javaType(dataType)
    val deserialize =
      s"($javaType) $serializer.deserialize(java.nio.ByteBuffer.wrap(${input.value}), null)"

    val code = s"""
      ${input.code}
      final $javaType ${ev.value} = ${input.isNull} ? ${ctx.defaultValue(javaType)} : $deserialize;
     """
    ev.copy(code = code, isNull = input.isNull)
  }

  override def dataType: DataType = ObjectType(tag.runtimeClass)
}

/**
 * Initialize an object by invoking the given sequence of method names and method arguments.
 *
 * @param objectInstance An expression evaluating to a new instance of the object to initialize
 * @param setters A sequence of method names and their sequence of argument expressions to apply in
 *                series to the object instance
 */
case class InitializeObject(
  objectInstance: Expression,
  setters: Seq[(String, Seq[Expression])])
  extends Expression with NonSQLExpression {

  override def nullable: Boolean = objectInstance.nullable
  override def children: Seq[Expression] = objectInstance +: setters.flatMap(_._2)
  override def dataType: DataType = objectInstance.dataType

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val instanceGen = objectInstance.genCode(ctx)

    val objectInstanceName = ctx.freshName("objectInstance")
    val objectInstanceType = ctx.javaType(objectInstance.dataType)

    lazy val initialize = setters.map {
      case (setterMethod, args) =>
        val argGen = args.map {
          case (arg) =>
            arg.genCode(ctx)
        }

        s"""
           |${argGen.map(arg => arg.code).mkString("\n")}
           |$objectInstanceName.$setterMethod(${argGen.map(arg => arg.value).mkString(", ")});
         """.stripMargin
    }

    val initializeCode = ctx.splitExpressionsWithCurrentInputs(
      expressions = initialize,
      funcName = "initializeObject",
      extraArguments = objectInstanceType -> objectInstanceName :: Nil)

    val code =
      s"""
         |${instanceGen.code}
         |$objectInstanceType $objectInstanceName = ${instanceGen.value};
         |if (!${instanceGen.isNull}) {
         |  $initializeCode
         |}
       """.stripMargin

    ev.copy(code = code, isNull = instanceGen.isNull, value = instanceGen.value)
  }

}

/**
 * Casts the result of an expression to another type.
 *
 * @param value The value to cast
 * @param resultType The type to which the value should be cast
 */
case class ObjectCast(value: Expression, resultType: DataType)
  extends Expression with NonSQLExpression {

  override def nullable: Boolean = value.nullable
  override def dataType: DataType = resultType
  override def children: Seq[Expression] = value :: Nil

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {

    val javaType = ctx.javaType(resultType)
    val obj = value.genCode(ctx)

    val code =
      s"""
         ${obj.code}
         final $javaType ${ev.value} = ($javaType) ${obj.value};
       """

    ev.copy(code = code, isNull = obj.isNull)
  }
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

  override def flatArguments: Iterator[Any] = Iterator(child)

  private val errMsg = "Null value appeared in non-nullable field:" +
    walkedTypePath.mkString("\n", "\n", "\n") +
    "If the schema is inferred from a Scala tuple/case class, or a Java bean, " +
    "please try to use scala.Option[_] or other nullable types " +
    "(e.g. java.lang.Integer instead of int/scala.Int)."

  override def eval(input: InternalRow): Any = {
    val result = child.eval(input)
    if (result == null) {
      throw new NullPointerException(errMsg)
    }
    result
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childGen = child.genCode(ctx)

    // Use unnamed reference that doesn't create a local field here to reduce the number of fields
    // because errMsgField is used only when the value is null.
    val errMsgField = ctx.addReferenceObj("errMsg", errMsg)

    val code = s"""
      ${childGen.code}

      if (${childGen.isNull}) {
        throw new NullPointerException($errMsgField);
      }
     """
    ev.copy(code = code, isNull = "false", value = childGen.value)
  }
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

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported")

  private val errMsg = s"The ${index}th field '$fieldName' of input row cannot be null."

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // Use unnamed reference that doesn't create a local field here to reduce the number of fields
    // because errMsgField is used only when the field is null.
    val errMsgField = ctx.addReferenceObj("errMsg", errMsg)
    val row = child.genCode(ctx)
    val code = s"""
      ${row.code}

      if (${row.isNull}) {
        throw new RuntimeException("The input external row cannot be null.");
      }

      if (${row.value}.isNullAt($index)) {
        throw new RuntimeException($errMsgField);
      }

      final Object ${ev.value} = ${row.value}.get($index);
     """
    ev.copy(code = code, isNull = "false")
  }
}

/**
 * Validates the actual data type of input expression at runtime.  If it doesn't match the
 * expectation, throw an exception.
 */
case class ValidateExternalType(child: Expression, expected: DataType)
  extends UnaryExpression with NonSQLExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(ObjectType(classOf[Object]))

  override def nullable: Boolean = child.nullable

  override def dataType: DataType = RowEncoder.externalDataTypeForInput(expected)

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported")

  private val errMsg = s" is not a valid external type for schema of ${expected.simpleString}"

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // Use unnamed reference that doesn't create a local field here to reduce the number of fields
    // because errMsgField is used only when the type doesn't match.
    val errMsgField = ctx.addReferenceObj("errMsg", errMsg)
    val input = child.genCode(ctx)
    val obj = input.value

    val typeCheck = expected match {
      case _: DecimalType =>
        Seq(classOf[java.math.BigDecimal], classOf[scala.math.BigDecimal], classOf[Decimal])
          .map(cls => s"$obj instanceof ${cls.getName}").mkString(" || ")
      case _: ArrayType =>
        s"$obj instanceof ${classOf[Seq[_]].getName} || $obj.getClass().isArray()"
      case _ =>
        s"$obj instanceof ${ctx.boxedType(dataType)}"
    }

    val code = s"""
      ${input.code}
      ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
      if (!${input.isNull}) {
        if ($typeCheck) {
          ${ev.value} = (${ctx.boxedType(dataType)}) $obj;
        } else {
          throw new RuntimeException($obj.getClass().getName() + $errMsgField);
        }
      }

    """
    ev.copy(code = code, isNull = input.isNull)
  }
}
