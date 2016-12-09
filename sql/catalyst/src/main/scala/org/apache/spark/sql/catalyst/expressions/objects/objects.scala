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

import scala.language.existentials
import scala.reflect.ClassTag

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.serializer._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
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
      val resultIsNull = ctx.freshName("resultIsNull")
      ctx.addMutableState("boolean", resultIsNull, "")
      resultIsNull
    } else {
      "false"
    }
    val argValues = arguments.map { e =>
      val argValue = ctx.freshName("argValue")
      ctx.addMutableState(ctx.javaType(e.dataType), argValue, "")
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
    val argCode = ctx.splitExpressions(ctx.INPUT_ROW, argCodes)

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
 */
case class StaticInvoke(
    staticObject: Class[_],
    dataType: DataType,
    functionName: String,
    arguments: Seq[Expression] = Nil,
    propagateNull: Boolean = true) extends InvokeLike {

  val objectName = staticObject.getName.stripSuffix("$")

  override def nullable: Boolean = true
  override def children: Seq[Expression] = arguments

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaType = ctx.javaType(dataType)

    val (argCode, argString, resultIsNull) = prepareArguments(ctx)

    val callFunc = s"$objectName.$functionName($argString)"

    // If the function can return null, we do an extra check to make sure our null bit is still set
    // correctly.
    val postNullCheck = if (ctx.defaultValue(dataType) == "null") {
      s"${ev.isNull} = ${ev.value} == null;"
    } else {
      ""
    }

    val code = s"""
      $argCode
      boolean ${ev.isNull} = $resultIsNull;
      final $javaType ${ev.value} = $resultIsNull ? ${ctx.defaultValue(dataType)} : $callFunc;
      $postNullCheck
     """
    ev.copy(code = code)
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

  @transient lazy val method = targetObject.dataType match {
    case ObjectType(cls) =>
      val m = cls.getMethods.find(_.getName == functionName)
      if (m.isEmpty) {
        sys.error(s"Couldn't find $functionName on $cls")
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
      getFuncResult(ev.value, s"${obj.value}.$functionName($argString)")
    } else {
      val funcResult = ctx.freshName("funcResult")
      s"""
        Object $funcResult = null;
        ${getFuncResult(funcResult, s"${obj.value}.$functionName($argString)")}
        if ($funcResult == null) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = (${ctx.boxedType(javaType)}) $funcResult;
        }
      """
    }

    // If the function can return null, we do an extra check to make sure our null bit is still set
    // correctly.
    val postNullCheck = if (ctx.defaultValue(dataType) == "null") {
      s"${ev.isNull} = ${ev.value} == null;"
    } else {
      ""
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
        $postNullCheck
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
 * A place holder for the loop variable used in [[MapObjects]].  This should never be constructed
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

object MapObjects {
  private val curId = new java.util.concurrent.atomic.AtomicInteger()

  /**
   * Construct an instance of MapObjects case class.
   *
   * @param function The function applied on the collection elements.
   * @param inputData An expression that when evaluated returns a collection object.
   * @param elementType The data type of elements in the collection.
   */
  def apply(
      function: Expression => Expression,
      inputData: Expression,
      elementType: DataType): MapObjects = {
    val loopValue = "MapObjects_loopValue" + curId.getAndIncrement()
    val loopIsNull = "MapObjects_loopIsNull" + curId.getAndIncrement()
    val loopVar = LambdaVariable(loopValue, loopIsNull, elementType)
    MapObjects(loopValue, loopIsNull, elementType, function(loopVar), inputData)
  }
}

/**
 * Applies the given expression to every element of a collection of items, returning the result
 * as an ArrayType.  This is similar to a typical map operation, but where the lambda function
 * is expressed using catalyst expressions.
 *
 * The following collection ObjectTypes are currently supported:
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
 */
case class MapObjects private(
    loopValue: String,
    loopIsNull: String,
    loopVarDataType: DataType,
    lambdaFunction: Expression,
    inputData: Expression) extends Expression with NonSQLExpression {

  override def nullable: Boolean = inputData.nullable

  override def children: Seq[Expression] = lambdaFunction :: inputData :: Nil

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported")

  override def dataType: DataType =
    ArrayType(lambdaFunction.dataType, containsNull = lambdaFunction.nullable)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val elementJavaType = ctx.javaType(loopVarDataType)
    ctx.addMutableState("boolean", loopIsNull, "")
    ctx.addMutableState(elementJavaType, loopValue, "")
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

    val (getLength, getLoopVar) = inputDataType match {
      case ObjectType(cls) if classOf[Seq[_]].isAssignableFrom(cls) =>
        s"${genInputData.value}.size()" -> s"${genInputData.value}.apply($loopIndex)"
      case ObjectType(cls) if cls.isArray =>
        s"${genInputData.value}.length" -> s"${genInputData.value}[$loopIndex]"
      case ObjectType(cls) if classOf[java.util.List[_]].isAssignableFrom(cls) =>
        s"${genInputData.value}.size()" -> s"${genInputData.value}.get($loopIndex)"
      case ArrayType(et, _) =>
        s"${genInputData.value}.numElements()" -> ctx.getValue(genInputData.value, et, loopIndex)
      case ObjectType(cls) if cls == classOf[Object] =>
        s"$seq == null ? $array.length : $seq.size()" ->
          s"$seq == null ? $array[$loopIndex] : $seq.apply($loopIndex)"
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

    val loopNullCheck = inputDataType match {
      case _: ArrayType => s"$loopIsNull = ${genInputData.value}.isNullAt($loopIndex);"
      // The element of primitive array will never be null.
      case ObjectType(cls) if cls.isArray && cls.getComponentType.isPrimitive =>
        s"$loopIsNull = false"
      case _ => s"$loopIsNull = $loopValue == null;"
    }

    val code = s"""
      ${genInputData.code}
      ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};

      if (!${genInputData.isNull}) {
        $determineCollectionType
        $convertedType[] $convertedArray = null;
        int $dataLength = $getLength;
        $convertedArray = $arrayConstructor;

        int $loopIndex = 0;
        while ($loopIndex < $dataLength) {
          $loopValue = ($elementJavaType) ($getLoopVar);
          $loopNullCheck

          ${genFunction.code}
          if (${genFunction.isNull}) {
            $convertedArray[$loopIndex] = null;
          } else {
            $convertedArray[$loopIndex] = $genFunctionValue;
          }

          $loopIndex += 1;
        }

        ${ev.value} = new ${classOf[GenericArrayData].getName}($convertedArray);
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
      valueType: DataType,
      valueConverter: Expression => Expression,
      valueNullable: Boolean): ExternalMapToCatalyst = {
    val id = curId.getAndIncrement()
    val keyName = "ExternalMapToCatalyst_key" + id
    val valueName = "ExternalMapToCatalyst_value" + id
    val valueIsNull = "ExternalMapToCatalyst_value_isNull" + id

    ExternalMapToCatalyst(
      keyName,
      keyType,
      keyConverter(LambdaVariable(keyName, "false", keyType, false)),
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
case class ExternalMapToCatalyst private(
    key: String,
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

    val (defineEntries, defineKeyValue) = child.dataType match {
      case ObjectType(cls) if classOf[java.util.Map[_, _]].isAssignableFrom(cls) =>
        val javaIteratorCls = classOf[java.util.Iterator[_]].getName
        val javaMapEntryCls = classOf[java.util.Map.Entry[_, _]].getName

        val defineEntries =
          s"final $javaIteratorCls $entries = ${inputMap.value}.entrySet().iterator();"

        val defineKeyValue =
          s"""
            final $javaMapEntryCls $entry = ($javaMapEntryCls) $entries.next();
            ${ctx.javaType(keyType)} $key = (${ctx.boxedType(keyType)}) $entry.getKey();
            ${ctx.javaType(valueType)} $value = (${ctx.boxedType(valueType)}) $entry.getValue();
          """

        defineEntries -> defineKeyValue

      case ObjectType(cls) if classOf[scala.collection.Map[_, _]].isAssignableFrom(cls) =>
        val scalaIteratorCls = classOf[Iterator[_]].getName
        val scalaMapEntryCls = classOf[Tuple2[_, _]].getName

        val defineEntries = s"final $scalaIteratorCls $entries = ${inputMap.value}.iterator();"

        val defineKeyValue =
          s"""
            final $scalaMapEntryCls $entry = ($scalaMapEntryCls) $entries.next();
            ${ctx.javaType(keyType)} $key = (${ctx.boxedType(keyType)}) $entry._1();
            ${ctx.javaType(valueType)} $value = (${ctx.boxedType(valueType)}) $entry._2();
          """

        defineEntries -> defineKeyValue
    }

    val valueNullCheck = if (ctx.isPrimitiveType(valueType)) {
      s"boolean $valueIsNull = false;"
    } else {
      s"boolean $valueIsNull = $value == null;"
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
    ctx.addMutableState("Object[]", values, "")

    val childrenCodes = children.zipWithIndex.map { case (e, i) =>
      val eval = e.genCode(ctx)
      eval.code + s"""
          if (${eval.isNull}) {
            $values[$i] = null;
          } else {
            $values[$i] = ${eval.value};
          }
         """
    }

    val childrenCode = ctx.splitExpressions(ctx.INPUT_ROW, childrenCodes)
    val schemaField = ctx.addReferenceObj("schema", schema)

    val code = s"""
      $values = new Object[${children.size}];
      $childrenCode
      final ${classOf[Row].getName} ${ev.value} = new $rowClass($values, $schemaField);
      """
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
    val serializer = ctx.freshName("serializer")
    val (serializerClass, serializerInstanceClass) = {
      if (kryo) {
        (classOf[KryoSerializer].getName, classOf[KryoSerializerInstance].getName)
      } else {
        (classOf[JavaSerializer].getName, classOf[JavaSerializerInstance].getName)
      }
    }
    // try conf from env, otherwise create a new one
    val env = s"${classOf[SparkEnv].getName}.get()"
    val sparkConf = s"new ${classOf[SparkConf].getName}()"
    val serializerInit = s"""
      if ($env == null) {
        $serializer = ($serializerInstanceClass) new $serializerClass($sparkConf).newInstance();
       } else {
         $serializer = ($serializerInstanceClass) new $serializerClass($env.conf()).newInstance();
       }
     """
    ctx.addMutableState(serializerInstanceClass, serializer, serializerInit)

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
    val serializer = ctx.freshName("serializer")
    val (serializerClass, serializerInstanceClass) = {
      if (kryo) {
        (classOf[KryoSerializer].getName, classOf[KryoSerializerInstance].getName)
      } else {
        (classOf[JavaSerializer].getName, classOf[JavaSerializerInstance].getName)
      }
    }
    // try conf from env, otherwise create a new one
    val env = s"${classOf[SparkEnv].getName}.get()"
    val sparkConf = s"new ${classOf[SparkConf].getName}()"
    val serializerInit = s"""
      if ($env == null) {
        $serializer = ($serializerInstanceClass) new $serializerClass($sparkConf).newInstance();
       } else {
         $serializer = ($serializerInstanceClass) new $serializerClass($env.conf()).newInstance();
       }
     """
    ctx.addMutableState(serializerInstanceClass, serializer, serializerInit)

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
 * Initialize a Java Bean instance by setting its field values via setters.
 */
case class InitializeJavaBean(beanInstance: Expression, setters: Map[String, Expression])
  extends Expression with NonSQLExpression {

  override def nullable: Boolean = beanInstance.nullable
  override def children: Seq[Expression] = beanInstance +: setters.values.toSeq
  override def dataType: DataType = beanInstance.dataType

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val instanceGen = beanInstance.genCode(ctx)

    val javaBeanInstance = ctx.freshName("javaBean")
    val beanInstanceJavaType = ctx.javaType(beanInstance.dataType)
    ctx.addMutableState(beanInstanceJavaType, javaBeanInstance, "")

    val initialize = setters.map {
      case (setterMethod, fieldValue) =>
        val fieldGen = fieldValue.genCode(ctx)
        s"""
           ${fieldGen.code}
           ${javaBeanInstance}.$setterMethod(${fieldGen.value});
         """
    }
    val initializeCode = ctx.splitExpressions(ctx.INPUT_ROW, initialize.toSeq)

    val code = s"""
      ${instanceGen.code}
      this.${javaBeanInstance} = ${instanceGen.value};
      if (!${instanceGen.isNull}) {
        $initializeCode
      }
     """
    ev.copy(code = code, isNull = instanceGen.isNull, value = instanceGen.value)
  }
}

/**
 * Asserts that input values of a non-nullable child expression are not null.
 *
 * Note that there are cases where `child.nullable == true`, while we still needs to add this
 * assertion.  Consider a nullable column `s` whose data type is a struct containing a non-nullable
 * `Int` field named `i`.  Expression `s.i` is nullable because `s` can be null.  However, for all
 * non-null `s`, `s.i` can't be null.
 */
case class AssertNotNull(child: Expression, walkedTypePath: Seq[String])
  extends UnaryExpression with NonSQLExpression {

  override def dataType: DataType = child.dataType
  override def foldable: Boolean = false
  override def nullable: Boolean = false

  private val errMsg = "Null value appeared in non-nullable field:" +
    walkedTypePath.mkString("\n", "\n", "\n") +
    "If the schema is inferred from a Scala tuple/case class, or a Java bean, " +
    "please try to use scala.Option[_] or other nullable types " +
    "(e.g. java.lang.Integer instead of int/scala.Int)."

  override def eval(input: InternalRow): Any = {
    val result = child.eval(input)
    if (result == null) {
      throw new RuntimeException(errMsg);
    }
    result
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childGen = child.genCode(ctx)

    // Use unnamed reference that doesn't create a local field here to reduce the number of fields
    // because errMsgField is used only when the value is null.
    val errMsgField = ctx.addReferenceObj(errMsg)

    val code = s"""
      ${childGen.code}

      if (${childGen.isNull}) {
        throw new RuntimeException($errMsgField);
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
    val errMsgField = ctx.addReferenceObj(errMsg)
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
    val errMsgField = ctx.addReferenceObj(errMsg)
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
