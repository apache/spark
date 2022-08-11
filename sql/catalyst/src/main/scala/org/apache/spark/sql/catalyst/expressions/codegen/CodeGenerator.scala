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

package org.apache.spark.sql.catalyst.expressions.codegen

import java.io.ByteArrayInputStream

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.google.common.util.concurrent.{ExecutionError, UncheckedExecutionException}
import org.codehaus.commons.compiler.{CompileException, InternalCompilerException}
import org.codehaus.janino.ClassBodyEvaluator
import org.codehaus.janino.util.ClassFile

import org.apache.spark.{TaskContext, TaskKilledException}
import org.apache.spark.executor.InputMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.CodegenMetrics
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData, SQLOrderingUtil}
import org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_MILLIS
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types._
import org.apache.spark.util.{LongAccumulator, ParentClassLoader, Utils}

/**
 * Java source for evaluating an [[Expression]] given a [[InternalRow]] of input.
 *
 * @param code The sequence of statements required to evaluate the expression.
 *             It should be empty string, if `isNull` and `value` are already existed, or no code
 *             needed to evaluate them (literals).
 * @param isNull A term that holds a boolean value representing whether the expression evaluated
 *                 to null.
 * @param value A term for a (possibly primitive) value of the result of the evaluation. Not
 *              valid if `isNull` is set to `true`.
 */
case class ExprCode(var code: Block, var isNull: ExprValue, var value: ExprValue)

object ExprCode {
  def apply(isNull: ExprValue, value: ExprValue): ExprCode = {
    ExprCode(code = EmptyBlock, isNull, value)
  }

  def forNullValue(dataType: DataType): ExprCode = {
    ExprCode(code = EmptyBlock, isNull = TrueLiteral, JavaCode.defaultLiteral(dataType))
  }

  def forNonNullValue(value: ExprValue): ExprCode = {
    ExprCode(code = EmptyBlock, isNull = FalseLiteral, value = value)
  }
}

/**
 * State used for subexpression elimination.
 *
 * @param eval The source code for evaluating the subexpression.
 * @param children The sequence of subexpressions as the children expressions. Before
 *                 evaluating this subexpression, we should evaluate all children
 *                 subexpressions first. This is used if we want to selectively evaluate
 *                 particular subexpressions, instead of all at once. In the case, we need
 *                 to make sure we evaluate all children subexpressions too.
 */
case class SubExprEliminationState(eval: ExprCode, children: Seq[SubExprEliminationState])

object SubExprEliminationState {
  def apply(eval: ExprCode): SubExprEliminationState = {
    new SubExprEliminationState(eval, Seq.empty)
  }

  def apply(
      eval: ExprCode,
      children: Seq[SubExprEliminationState]): SubExprEliminationState = {
    new SubExprEliminationState(eval, children.reverse)
  }
}

/**
 * Codes and common subexpressions mapping used for subexpression elimination.
 *
 * @param states Foreach expression that is participating in subexpression elimination,
 *               the state to use.
 * @param exprCodesNeedEvaluate Some expression codes that need to be evaluated before
 *                              calling common subexpressions.
 */
case class SubExprCodes(
    states: Map[ExpressionEquals, SubExprEliminationState],
    exprCodesNeedEvaluate: Seq[ExprCode])

/**
 * The main information about a new added function.
 *
 * @param functionName String representing the name of the function
 * @param innerClassName Optional value which is empty if the function is added to
 *                       the outer class, otherwise it contains the name of the
 *                       inner class in which the function has been added.
 * @param innerClassInstance Optional value which is empty if the function is added to
 *                           the outer class, otherwise it contains the name of the
 *                           instance of the inner class in the outer class.
 */
private[codegen] case class NewFunctionSpec(
    functionName: String,
    innerClassName: Option[String],
    innerClassInstance: Option[String])

/**
 * A context for codegen, tracking a list of objects that could be passed into generated Java
 * function.
 */
class CodegenContext extends Logging {

  import CodeGenerator._

  /**
   * Holding a list of objects that could be used passed into generated class.
   */
  val references: mutable.ArrayBuffer[Any] = new mutable.ArrayBuffer[Any]()

  /**
   * Add an object to `references`.
   *
   * Returns the code to access it.
   *
   * This does not to store the object into field but refer it from the references field at the
   * time of use because number of fields in class is limited so we should reduce it.
   */
  def addReferenceObj(objName: String, obj: Any, className: String = null): String = {
    val idx = references.length
    references += obj
    val clsName = Option(className).getOrElse(CodeGenerator.typeName(obj.getClass))
    s"(($clsName) references[$idx] /* $objName */)"
  }

  /**
   * Holding the variable name of the input row of the current operator, will be used by
   * `BoundReference` to generate code.
   *
   * Note that if `currentVars` is not null, `BoundReference` prefers `currentVars` over `INPUT_ROW`
   * to generate code. If you want to make sure the generated code use `INPUT_ROW`, you need to set
   * `currentVars` to null, or set `currentVars(i)` to null for certain columns, before calling
   * `Expression.genCode`.
   */
  var INPUT_ROW = "i"

  /**
   * Holding a list of generated columns as input of current operator, will be used by
   * BoundReference to generate code.
   */
  var currentVars: Seq[ExprCode] = null

  /**
   * Holding expressions' inlined mutable states like `MonotonicallyIncreasingID.count` as a
   * 2-tuple: java type, variable name.
   * As an example, ("int", "count") will produce code:
   * {{{
   *   private int count;
   * }}}
   * as a member variable
   *
   * They will be kept as member variables in generated classes like `SpecificProjection`.
   *
   * Exposed for tests only.
   */
  private[catalyst] val inlinedMutableStates: mutable.ArrayBuffer[(String, String)] =
    mutable.ArrayBuffer.empty[(String, String)]

  /**
   * The mapping between mutable state types and corresponding compacted arrays.
   * The keys are java type string. The values are [[MutableStateArrays]] which encapsulates
   * the compacted arrays for the mutable states with the same java type.
   *
   * Exposed for tests only.
   */
  private[catalyst] val arrayCompactedMutableStates: mutable.Map[String, MutableStateArrays] =
    mutable.Map.empty[String, MutableStateArrays]

  // An array holds the code that will initialize each state
  // Exposed for tests only.
  private[catalyst] val mutableStateInitCode: mutable.ArrayBuffer[String] =
    mutable.ArrayBuffer.empty[String]

  // Tracks the names of all the mutable states.
  private val mutableStateNames: mutable.HashSet[String] = mutable.HashSet.empty

  /**
   * This class holds a set of names of mutableStateArrays that is used for compacting mutable
   * states for a certain type, and holds the next available slot of the current compacted array.
   */
  class MutableStateArrays {
    val arrayNames = mutable.ListBuffer.empty[String]
    createNewArray()

    private[this] var currentIndex = 0

    private def createNewArray() = {
      val newArrayName = freshName("mutableStateArray")
      mutableStateNames += newArrayName
      arrayNames.append(newArrayName)
    }

    def getCurrentIndex: Int = currentIndex

    /**
     * Returns the reference of next available slot in current compacted array. The size of each
     * compacted array is controlled by the constant `MUTABLESTATEARRAY_SIZE_LIMIT`.
     * Once reaching the threshold, new compacted array is created.
     */
    def getNextSlot(): String = {
      if (currentIndex < MUTABLESTATEARRAY_SIZE_LIMIT) {
        val res = s"${arrayNames.last}[$currentIndex]"
        currentIndex += 1
        res
      } else {
        createNewArray()
        currentIndex = 1
        s"${arrayNames.last}[0]"
      }
    }

  }

  /**
   * A map containing the mutable states which have been defined so far using
   * `addImmutableStateIfNotExists`. Each entry contains the name of the mutable state as key and
   * its Java type and init code as value.
   */
  private val immutableStates: mutable.Map[String, (String, String)] =
    mutable.Map.empty[String, (String, String)]

  /**
   * Add a mutable state as a field to the generated class. c.f. the comments above.
   *
   * @param javaType Java type of the field. Note that short names can be used for some types,
   *                 e.g. InternalRow, UnsafeRow, UnsafeArrayData, etc. Other types will have to
   *                 specify the fully-qualified Java type name. See the code in doCompile() for
   *                 the list of default imports available.
   *                 Also, generic type arguments are accepted but ignored.
   * @param variableName Name of the field.
   * @param initFunc Function includes statement(s) to put into the init() method to initialize
   *                 this field. The argument is the name of the mutable state variable.
   *                 If left blank, the field will be default-initialized.
   * @param forceInline whether the declaration and initialization code may be inlined rather than
   *                    compacted. Please set `true` into forceInline for one of the followings:
   *                    1. use the original name of the status
   *                    2. expect to non-frequently generate the status
   *                       (e.g. not much sort operators in one stage)
   * @param useFreshName If this is false and the mutable state ends up inlining in the outer
   *                     class, the name is not changed
   * @return the name of the mutable state variable, which is the original name or fresh name if
   *         the variable is inlined to the outer class, or an array access if the variable is to
   *         be stored in an array of variables of the same type.
   *         A variable will be inlined into the outer class when one of the following conditions
   *         are satisfied:
   *         1. forceInline is true
   *         2. its type is primitive type and the total number of the inlined mutable variables
   *            is less than `OUTER_CLASS_VARIABLES_THRESHOLD`
   *         3. its type is multi-dimensional array
   *         When a variable is compacted into an array, the max size of the array for compaction
   *         is given by `MUTABLESTATEARRAY_SIZE_LIMIT`.
   */
  def addMutableState(
      javaType: String,
      variableName: String,
      initFunc: String => String = _ => "",
      forceInline: Boolean = false,
      useFreshName: Boolean = true): String = {

    // want to put a primitive type variable at outerClass for performance
    val canInlinePrimitive = isPrimitiveType(javaType) &&
      (inlinedMutableStates.length < OUTER_CLASS_VARIABLES_THRESHOLD)
    if (forceInline || canInlinePrimitive || javaType.contains("[][]")) {
      val varName = if (useFreshName) freshName(variableName) else variableName
      val initCode = initFunc(varName)
      inlinedMutableStates += ((javaType, varName))
      mutableStateInitCode += initCode
      mutableStateNames += varName
      varName
    } else {
      val arrays = arrayCompactedMutableStates.getOrElseUpdate(javaType, new MutableStateArrays)
      val element = arrays.getNextSlot()

      val initCode = initFunc(element)
      mutableStateInitCode += initCode
      element
    }
  }

  /**
   * Add an immutable state as a field to the generated class only if it does not exist yet a field
   * with that name. This helps reducing the number of the generated class' fields, since the same
   * variable can be reused by many functions.
   *
   * Even though the added variables are not declared as final, they should never be reassigned in
   * the generated code to prevent errors and unexpected behaviors.
   *
   * Internally, this method calls `addMutableState`.
   *
   * @param javaType Java type of the field.
   * @param variableName Name of the field.
   * @param initFunc Function includes statement(s) to put into the init() method to initialize
   *                 this field. The argument is the name of the mutable state variable.
   */
  def addImmutableStateIfNotExists(
      javaType: String,
      variableName: String,
      initFunc: String => String = _ => ""): Unit = {
    val existingImmutableState = immutableStates.get(variableName)
    if (existingImmutableState.isEmpty) {
      addMutableState(javaType, variableName, initFunc, useFreshName = false, forceInline = true)
      immutableStates(variableName) = (javaType, initFunc(variableName))
    } else {
      val (prevJavaType, prevInitCode) = existingImmutableState.get
      assert(prevJavaType == javaType, s"$variableName has already been defined with type " +
        s"$prevJavaType and now it is tried to define again with type $javaType.")
      assert(prevInitCode == initFunc(variableName), s"$variableName has already been defined " +
        s"with different initialization statements.")
    }
  }

  /**
   * Add buffer variable which stores data coming from an [[InternalRow]]. This methods guarantees
   * that the variable is safely stored, which is important for (potentially) byte array backed
   * data types like: UTF8String, ArrayData, MapData & InternalRow.
   */
  def addBufferedState(dataType: DataType, variableName: String, initCode: String): ExprCode = {
    val value = addMutableState(javaType(dataType), variableName)
    val code = UserDefinedType.sqlType(dataType) match {
      case StringType => code"$value = $initCode.clone();"
      case _: StructType | _: ArrayType | _: MapType => code"$value = $initCode.copy();"
      case _ => code"$value = $initCode;"
    }
    ExprCode(code, FalseLiteral, JavaCode.global(value, dataType))
  }

  def declareMutableStates(): String = {
    // It's possible that we add same mutable state twice, e.g. the `mergeExpressions` in
    // `TypedAggregateExpression`, we should call `distinct` here to remove the duplicated ones.
    val inlinedStates = inlinedMutableStates.distinct.map { case (javaType, variableName) =>
      s"private $javaType $variableName;"
    }

    val arrayStates = arrayCompactedMutableStates.flatMap { case (javaType, mutableStateArrays) =>
      val numArrays = mutableStateArrays.arrayNames.size
      mutableStateArrays.arrayNames.zipWithIndex.map { case (arrayName, index) =>
        val length = if (index + 1 == numArrays) {
          mutableStateArrays.getCurrentIndex
        } else {
          MUTABLESTATEARRAY_SIZE_LIMIT
        }
        if (javaType.contains("[]")) {
          // initializer had an one-dimensional array variable
          val baseType = javaType.substring(0, javaType.length - 2)
          s"private $javaType[] $arrayName = new $baseType[$length][];"
        } else {
          // initializer had a scalar variable
          s"private $javaType[] $arrayName = new $javaType[$length];"
        }
      }
    }

    (inlinedStates ++ arrayStates).mkString("\n")
  }

  def initMutableStates(): String = {
    // It's possible that we add same mutable state twice, e.g. the `mergeExpressions` in
    // `TypedAggregateExpression`, we should call `distinct` here to remove the duplicated ones.
    val initCodes = mutableStateInitCode.distinct.map(_ + "\n")

    // The generated initialization code may exceed 64kb function size limit in JVM if there are too
    // many mutable states, so split it into multiple functions.
    splitExpressions(expressions = initCodes.toSeq, funcName = "init", arguments = Nil)
  }

  /**
   * Code statements to initialize states that depend on the partition index.
   * An integer `partitionIndex` will be made available within the scope.
   */
  val partitionInitializationStatements: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty

  def addPartitionInitializationStatement(statement: String): Unit = {
    partitionInitializationStatements += statement
  }

  def initPartition(): String = {
    partitionInitializationStatements.mkString("\n")
  }

  /**
   * Holds expressions that are equivalent. Used to perform subexpression elimination
   * during codegen.
   *
   * For expressions that appear more than once, generate additional code to prevent
   * recomputing the value.
   *
   * For example, consider two expression generated from this SQL statement:
   *  SELECT (col1 + col2), (col1 + col2) / col3.
   *
   *  equivalentExpressions will match the tree containing `col1 + col2` and it will only
   *  be evaluated once.
   */
  private val equivalentExpressions: EquivalentExpressions = new EquivalentExpressions

  // Foreach expression that is participating in subexpression elimination, the state to use.
  // Visible for testing.
  private[expressions] var subExprEliminationExprs =
    Map.empty[ExpressionEquals, SubExprEliminationState]

  // The collection of sub-expression result resetting methods that need to be called on each row.
  private val subexprFunctions = mutable.ArrayBuffer.empty[String]

  val outerClassName = "OuterClass"

  /**
   * Holds the class and instance names to be generated, where `OuterClass` is a placeholder
   * standing for whichever class is generated as the outermost class and which will contain any
   * inner sub-classes. All other classes and instance names in this list will represent private,
   * inner sub-classes.
   */
  private val classes: mutable.ListBuffer[(String, String)] =
    mutable.ListBuffer[(String, String)](outerClassName -> null)

  // A map holding the current size in bytes of each class to be generated.
  private val classSize: mutable.Map[String, Int] =
    mutable.Map[String, Int](outerClassName -> 0)

  // Nested maps holding function names and their code belonging to each class.
  private val classFunctions: mutable.Map[String, mutable.Map[String, String]] =
    mutable.Map(outerClassName -> mutable.Map.empty[String, String])

  // Verbatim extra code to be added to the OuterClass.
  private val extraClasses: mutable.ListBuffer[String] = mutable.ListBuffer[String]()

  // Returns the size of the most recently added class.
  private def currClassSize(): Int = classSize(classes.head._1)

  // Returns the class name and instance name for the most recently added class.
  private def currClass(): (String, String) = classes.head

  // Adds a new class. Requires the class' name, and its instance name.
  private def addClass(className: String, classInstance: String): Unit = {
    classes.prepend(className -> classInstance)
    classSize += className -> 0
    classFunctions += className -> mutable.Map.empty[String, String]
  }

  /**
   * Adds a function to the generated class. If the code for the `OuterClass` grows too large, the
   * function will be inlined into a new private, inner class, and a class-qualified name for the
   * function will be returned. Otherwise, the function will be inlined to the `OuterClass` the
   * simple `funcName` will be returned.
   *
   * @param funcName the class-unqualified name of the function
   * @param funcCode the body of the function
   * @param inlineToOuterClass whether the given code must be inlined to the `OuterClass`. This
   *                           can be necessary when a function is declared outside of the context
   *                           it is eventually referenced and a returned qualified function name
   *                           cannot otherwise be accessed.
   * @return the name of the function, qualified by class if it will be inlined to a private,
   *         inner class
   */
  def addNewFunction(
      funcName: String,
      funcCode: String,
      inlineToOuterClass: Boolean = false): String = {
    val newFunction = addNewFunctionInternal(funcName, funcCode, inlineToOuterClass)
    newFunction match {
      case NewFunctionSpec(functionName, None, None) => functionName
      case NewFunctionSpec(functionName, Some(_), Some(innerClassInstance)) =>
        innerClassInstance + "." + functionName
      case _ =>
        throw QueryExecutionErrors.addNewFunctionMismatchedWithFunctionError(funcName)
    }
  }

  private[this] def addNewFunctionInternal(
      funcName: String,
      funcCode: String,
      inlineToOuterClass: Boolean): NewFunctionSpec = {
    val (className, classInstance) = if (inlineToOuterClass) {
      outerClassName -> ""
    } else if (currClassSize > GENERATED_CLASS_SIZE_THRESHOLD) {
      val className = freshName("NestedClass")
      val classInstance = freshName("nestedClassInstance")

      addClass(className, classInstance)

      className -> classInstance
    } else {
      currClass()
    }

    addNewFunctionToClass(funcName, funcCode, className)

    if (className == outerClassName) {
      NewFunctionSpec(funcName, None, None)
    } else {
      NewFunctionSpec(funcName, Some(className), Some(classInstance))
    }
  }

  private[this] def addNewFunctionToClass(
      funcName: String,
      funcCode: String,
      className: String) = {
    classSize(className) += funcCode.length
    classFunctions(className) += funcName -> funcCode
  }

  /**
   * Declares all function code. If the added functions are too many, split them into nested
   * sub-classes to avoid hitting Java compiler constant pool limitation.
   */
  def declareAddedFunctions(): String = {
    val inlinedFunctions = classFunctions(outerClassName).values

    // Nested, private sub-classes have no mutable state (though they do reference the outer class'
    // mutable state), so we declare and initialize them inline to the OuterClass.
    val initNestedClasses = classes.filter(_._1 != outerClassName).map {
      case (className, classInstance) =>
        s"private $className $classInstance = new $className();"
    }

    val declareNestedClasses = classFunctions.filterKeys(_ != outerClassName).map {
      case (className, functions) =>
        s"""
           |private class $className {
           |  ${functions.values.mkString("\n")}
           |}
           """.stripMargin
    }

    (inlinedFunctions ++ initNestedClasses ++ declareNestedClasses).mkString("\n")
  }

  /**
   * Emits extra inner classes added with addExtraCode
   */
  def emitExtraCode(): String = {
    extraClasses.mkString("\n")
  }

  /**
   * Add extra source code to the outermost generated class.
   * @param code verbatim source code of the inner class to be added.
   */
  def addInnerClass(code: String): Unit = {
    extraClasses.append(code)
  }

  /**
   * The map from a variable name to it's next ID.
   */
  private val freshNameIds = new mutable.HashMap[String, Int]
  freshNameIds += INPUT_ROW -> 1

  /**
   * A prefix used to generate fresh name.
   */
  var freshNamePrefix = ""

  /**
   * The map from a place holder to a corresponding comment
   */
  private val placeHolderToComments = new mutable.HashMap[String, String]

  /**
   * Returns a term name that is unique within this instance of a `CodegenContext`.
   */
  def freshName(name: String): String = synchronized {
    val fullName = if (freshNamePrefix == "") {
      name
    } else {
      s"${freshNamePrefix}_$name"
    }
    val id = freshNameIds.getOrElse(fullName, 0)
    freshNameIds(fullName) = id + 1
    s"${fullName}_$id"
  }

  /**
   * Creates an `ExprValue` representing a local java variable of required data type.
   */
  def freshVariable(name: String, dt: DataType): VariableValue =
    JavaCode.variable(freshName(name), dt)

  /**
   * Creates an `ExprValue` representing a local java variable of required Java class.
   */
  def freshVariable(name: String, javaClass: Class[_]): VariableValue =
    JavaCode.variable(freshName(name), javaClass)

  /**
   * Generates code for equal expression in Java.
   */
  def genEqual(dataType: DataType, c1: String, c2: String): String = dataType match {
    case BinaryType => s"java.util.Arrays.equals($c1, $c2)"
    case FloatType =>
      s"((java.lang.Float.isNaN($c1) && java.lang.Float.isNaN($c2)) || $c1 == $c2)"
    case DoubleType =>
      s"((java.lang.Double.isNaN($c1) && java.lang.Double.isNaN($c2)) || $c1 == $c2)"
    case dt: DataType if isPrimitiveType(dt) => s"$c1 == $c2"
    case dt: DataType if dt.isInstanceOf[AtomicType] => s"$c1.equals($c2)"
    case array: ArrayType => genComp(array, c1, c2) + " == 0"
    case struct: StructType => genComp(struct, c1, c2) + " == 0"
    case udt: UserDefinedType[_] => genEqual(udt.sqlType, c1, c2)
    case NullType => "false"
    case _ =>
      throw QueryExecutionErrors.cannotGenerateCodeForIncomparableTypeError(
        "equality", dataType)
  }

  /**
   * Generates code for comparing two expressions.
   *
   * @param dataType data type of the expressions
   * @param c1 name of the variable of expression 1's output
   * @param c2 name of the variable of expression 2's output
   */
  def genComp(dataType: DataType, c1: String, c2: String): String = dataType match {
    // java boolean doesn't support > or < operator
    case BooleanType => s"($c1 == $c2 ? 0 : ($c1 ? 1 : -1))"
    case DoubleType =>
      val clsName = SQLOrderingUtil.getClass.getName.stripSuffix("$")
      s"$clsName.compareDoubles($c1, $c2)"
    case FloatType =>
      val clsName = SQLOrderingUtil.getClass.getName.stripSuffix("$")
      s"$clsName.compareFloats($c1, $c2)"
    // use c1 - c2 may overflow
    case dt: DataType if isPrimitiveType(dt) => s"($c1 > $c2 ? 1 : $c1 < $c2 ? -1 : 0)"
    case BinaryType => s"org.apache.spark.unsafe.types.ByteArray.compareBinary($c1, $c2)"
    case NullType => "0"
    case array: ArrayType =>
      val elementType = array.elementType
      val elementA = freshName("elementA")
      val isNullA = freshName("isNullA")
      val elementB = freshName("elementB")
      val isNullB = freshName("isNullB")
      val compareFunc = freshName("compareArray")
      val minLength = freshName("minLength")
      val jt = javaType(elementType)
      val funcCode: String =
        s"""
          public int $compareFunc(ArrayData a, ArrayData b) {
            // when comparing unsafe arrays, try equals first as it compares the binary directly
            // which is very fast.
            if (a instanceof UnsafeArrayData && b instanceof UnsafeArrayData && a.equals(b)) {
              return 0;
            }
            int lengthA = a.numElements();
            int lengthB = b.numElements();
            int $minLength = (lengthA > lengthB) ? lengthB : lengthA;
            for (int i = 0; i < $minLength; i++) {
              boolean $isNullA = a.isNullAt(i);
              boolean $isNullB = b.isNullAt(i);
              if ($isNullA && $isNullB) {
                // Nothing
              } else if ($isNullA) {
                return -1;
              } else if ($isNullB) {
                return 1;
              } else {
                $jt $elementA = ${getValue("a", elementType, "i")};
                $jt $elementB = ${getValue("b", elementType, "i")};
                int comp = ${genComp(elementType, elementA, elementB)};
                if (comp != 0) {
                  return comp;
                }
              }
            }

            if (lengthA < lengthB) {
              return -1;
            } else if (lengthA > lengthB) {
              return 1;
            }
            return 0;
          }
        """
      s"${addNewFunction(compareFunc, funcCode)}($c1, $c2)"
    case schema: StructType =>
      val comparisons = GenerateOrdering.genComparisons(this, schema)
      val compareFunc = freshName("compareStruct")
      val funcCode: String =
        s"""
          public int $compareFunc(InternalRow a, InternalRow b) {
            // when comparing unsafe rows, try equals first as it compares the binary directly
            // which is very fast.
            if (a instanceof UnsafeRow && b instanceof UnsafeRow && a.equals(b)) {
              return 0;
            }
            $comparisons
            return 0;
          }
        """
      s"${addNewFunction(compareFunc, funcCode)}($c1, $c2)"
    case other if other.isInstanceOf[AtomicType] => s"$c1.compare($c2)"
    case udt: UserDefinedType[_] => genComp(udt.sqlType, c1, c2)
    case _ =>
      throw QueryExecutionErrors.cannotGenerateCodeForIncomparableTypeError("compare", dataType)
  }

  /**
   * Generates code for greater of two expressions.
   *
   * @param dataType data type of the expressions
   * @param c1 name of the variable of expression 1's output
   * @param c2 name of the variable of expression 2's output
   */
  def genGreater(dataType: DataType, c1: String, c2: String): String = javaType(dataType) match {
    case JAVA_BYTE | JAVA_SHORT | JAVA_INT | JAVA_LONG => s"$c1 > $c2"
    case _ => s"(${genComp(dataType, c1, c2)}) > 0"
  }

  /**
   * Generates code for updating `partialResult` if `item` is smaller than it.
   *
   * @param dataType data type of the expressions
   * @param partialResult `ExprCode` representing the partial result which has to be updated
   * @param item `ExprCode` representing the new expression to evaluate for the result
   */
  def reassignIfSmaller(dataType: DataType, partialResult: ExprCode, item: ExprCode): String = {
    s"""
       |if (!${item.isNull} && (${partialResult.isNull} ||
       |  ${genGreater(dataType, partialResult.value, item.value)})) {
       |  ${partialResult.isNull} = false;
       |  ${partialResult.value} = ${item.value};
       |}
      """.stripMargin
  }

  /**
   * Generates code for updating `partialResult` if `item` is greater than it.
   *
   * @param dataType data type of the expressions
   * @param partialResult `ExprCode` representing the partial result which has to be updated
   * @param item `ExprCode` representing the new expression to evaluate for the result
   */
  def reassignIfGreater(dataType: DataType, partialResult: ExprCode, item: ExprCode): String = {
    s"""
       |if (!${item.isNull} && (${partialResult.isNull} ||
       |  ${genGreater(dataType, item.value, partialResult.value)})) {
       |  ${partialResult.isNull} = false;
       |  ${partialResult.value} = ${item.value};
       |}
      """.stripMargin
  }

  /**
   * Generates code to do null safe execution, i.e. only execute the code when the input is not
   * null by adding null check if necessary.
   *
   * @param nullable used to decide whether we should add null check or not.
   * @param isNull the code to check if the input is null.
   * @param execute the code that should only be executed when the input is not null.
   */
  def nullSafeExec(nullable: Boolean, isNull: String)(execute: String): String = {
    if (nullable) {
      s"""
        if (!$isNull) {
          $execute
        }
      """
    } else {
      "\n" + execute
    }
  }

  /**
   * Generates code to do null safe execution when accessing properties of complex
   * ArrayData elements.
   *
   * @param nullElements used to decide whether the ArrayData might contain null or not.
   * @param isNull a variable indicating whether the result will be evaluated to null or not.
   * @param arrayData a variable name representing the ArrayData.
   * @param execute the code that should be executed only if the ArrayData doesn't contain
   *                any null.
   */
  def nullArrayElementsSaveExec(
      nullElements: Boolean,
      isNull: String,
      arrayData: String)(
      execute: String): String = {
    val i = freshName("idx")
    if (nullElements) {
      s"""
         |for (int $i = 0; !$isNull && $i < $arrayData.numElements(); $i++) {
         |  $isNull |= $arrayData.isNullAt($i);
         |}
         |if (!$isNull) {
         |  $execute
         |}
       """.stripMargin
    } else {
      execute
    }
  }

  /**
   * Splits the generated code of expressions into multiple functions, because function has
   * 64kb code size limit in JVM. If the class to which the function would be inlined would grow
   * beyond 1000kb, we declare a private, inner sub-class, and the function is inlined to it
   * instead, because classes have a constant pool limit of 65,536 named values.
   *
   * Note that different from `splitExpressions`, we will extract the current inputs of this
   * context and pass them to the generated functions. The input is `INPUT_ROW` for normal codegen
   * path, and `currentVars` for whole stage codegen path. Whole stage codegen path is not
   * supported yet.
   *
   * @param expressions the codes to evaluate expressions.
   * @param funcName the split function name base.
   * @param extraArguments the list of (type, name) of the arguments of the split function,
   *                       except for the current inputs like `ctx.INPUT_ROW`.
   * @param returnType the return type of the split function.
   * @param makeSplitFunction makes split function body, e.g. add preparation or cleanup.
   * @param foldFunctions folds the split function calls.
   */
  def splitExpressionsWithCurrentInputs(
      expressions: Seq[String],
      funcName: String = "apply",
      extraArguments: Seq[(String, String)] = Nil,
      returnType: String = "void",
      makeSplitFunction: String => String = identity,
      foldFunctions: Seq[String] => String = _.mkString("", ";\n", ";")): String = {
    // TODO: support whole stage codegen
    if (INPUT_ROW == null || currentVars != null) {
      expressions.mkString("\n")
    } else {
      splitExpressions(
        expressions,
        funcName,
        ("InternalRow", INPUT_ROW) +: extraArguments,
        returnType,
        makeSplitFunction,
        foldFunctions)
    }
  }

  /**
   * Splits the generated code of expressions into multiple functions, because function has
   * 64kb code size limit in JVM. If the class to which the function would be inlined would grow
   * beyond 1000kb, we declare a private, inner sub-class, and the function is inlined to it
   * instead, because classes have a constant pool limit of 65,536 named values.
   *
   * @param expressions the codes to evaluate expressions.
   * @param funcName the split function name base.
   * @param arguments the list of (type, name) of the arguments of the split function.
   * @param returnType the return type of the split function.
   * @param makeSplitFunction makes split function body, e.g. add preparation or cleanup.
   * @param foldFunctions folds the split function calls.
   */
  def splitExpressions(
      expressions: Seq[String],
      funcName: String,
      arguments: Seq[(String, String)],
      returnType: String = "void",
      makeSplitFunction: String => String = identity,
      foldFunctions: Seq[String] => String = _.mkString("", ";\n", ";")): String = {
    val blocks = buildCodeBlocks(expressions)

    if (blocks.length == 1) {
      // inline execution if only one block
      blocks.head
    } else {
      if (Utils.isTesting) {
        // Passing global variables to the split method is dangerous, as any mutating to it is
        // ignored and may lead to unexpected behavior.
        arguments.foreach { case (_, name) =>
          assert(!mutableStateNames.contains(name),
            s"split function argument $name cannot be a global variable.")
        }
      }

      val func = freshName(funcName)
      val argString = arguments.map { case (t, name) => s"$t $name" }.mkString(", ")
      val functions = blocks.zipWithIndex.map { case (body, i) =>
        val name = s"${func}_$i"
        val code = s"""
           |private $returnType $name($argString) {
           |  ${makeSplitFunction(body)}
           |}
         """.stripMargin
        addNewFunctionInternal(name, code, inlineToOuterClass = false)
      }

      val (outerClassFunctions, innerClassFunctions) = functions.partition(_.innerClassName.isEmpty)

      val argsString = arguments.map(_._2).mkString(", ")
      val outerClassFunctionCalls = outerClassFunctions.map(f => s"${f.functionName}($argsString)")

      val innerClassFunctionCalls = generateInnerClassesFunctionCalls(
        innerClassFunctions,
        func,
        arguments,
        returnType,
        makeSplitFunction,
        foldFunctions)

      foldFunctions(outerClassFunctionCalls ++ innerClassFunctionCalls)
    }
  }

  /**
   * Splits the generated code of expressions into multiple sequences of String
   * based on a threshold of length of a String
   *
   * @param expressions the codes to evaluate expressions.
   */
  private def buildCodeBlocks(expressions: Seq[String]): Seq[String] = {
    val blocks = new ArrayBuffer[String]()
    val blockBuilder = new StringBuilder()
    var length = 0
    val splitThreshold = SQLConf.get.methodSplitThreshold
    for (code <- expressions) {
      // We can't know how many bytecode will be generated, so use the length of source code
      // as metric. A method should not go beyond 8K, otherwise it will not be JITted, should
      // also not be too small, or it will have many function calls (for wide table), see the
      // results in BenchmarkWideTable.
      if (length > splitThreshold) {
        blocks += blockBuilder.toString()
        blockBuilder.clear()
        length = 0
      }
      blockBuilder.append(code)
      length += CodeFormatter.stripExtraNewLinesAndComments(code).length
    }
    blocks += blockBuilder.toString()
    blocks.toSeq
  }

  /**
   * Here we handle all the methods which have been added to the inner classes and
   * not to the outer class.
   * Since they can be many, their direct invocation in the outer class adds many entries
   * to the outer class' constant pool. This can cause the constant pool to past JVM limit.
   * Moreover, this can cause also the outer class method where all the invocations are
   * performed to grow beyond the 64k limit.
   * To avoid these problems, we group them and we call only the grouping methods in the
   * outer class.
   *
   * @param functions a [[Seq]] of [[NewFunctionSpec]] defined in the inner classes
   * @param funcName the split function name base.
   * @param arguments the list of (type, name) of the arguments of the split function.
   * @param returnType the return type of the split function.
   * @param makeSplitFunction makes split function body, e.g. add preparation or cleanup.
   * @param foldFunctions folds the split function calls.
   * @return an [[Iterable]] containing the methods' invocations
   */
  private def generateInnerClassesFunctionCalls(
      functions: Seq[NewFunctionSpec],
      funcName: String,
      arguments: Seq[(String, String)],
      returnType: String,
      makeSplitFunction: String => String,
      foldFunctions: Seq[String] => String): Iterable[String] = {
    val innerClassToFunctions = mutable.LinkedHashMap.empty[(String, String), Seq[String]]
    functions.foreach(f => {
      val key = (f.innerClassName.get, f.innerClassInstance.get)
      val value = f.functionName +: innerClassToFunctions.getOrElse(key, Seq.empty[String])
      innerClassToFunctions.put(key, value)
    })

    val argDefinitionString = arguments.map { case (t, name) => s"$t $name" }.mkString(", ")
    val argInvocationString = arguments.map(_._2).mkString(", ")

    innerClassToFunctions.flatMap {
      case ((innerClassName, innerClassInstance), innerClassFunctions) =>
        // for performance reasons, the functions are prepended, instead of appended,
        // thus here they are in reversed order
        val orderedFunctions = innerClassFunctions.reverse
        if (orderedFunctions.size > MERGE_SPLIT_METHODS_THRESHOLD) {
          // Adding a new function to each inner class which contains the invocation of all the
          // ones which have been added to that inner class. For example,
          //   private class NestedClass {
          //     private void apply_862(InternalRow i) { ... }
          //     private void apply_863(InternalRow i) { ... }
          //       ...
          //     private void apply(InternalRow i) {
          //       apply_862(i);
          //       apply_863(i);
          //       ...
          //     }
          //   }
          val body = foldFunctions(orderedFunctions.map(name => s"$name($argInvocationString)"))
          val code = s"""
              |private $returnType $funcName($argDefinitionString) {
              |  ${makeSplitFunction(body)}
              |}
            """.stripMargin
          addNewFunctionToClass(funcName, code, innerClassName)
          Seq(s"$innerClassInstance.$funcName($argInvocationString)")
        } else {
          orderedFunctions.map(f => s"$innerClassInstance.$f($argInvocationString)")
        }
    }
  }

  /**
   * Returns the code for subexpression elimination after splitting it if necessary.
   */
  def subexprFunctionsCode: String = {
    // Whole-stage codegen's subexpression elimination is handled in another code path
    assert(currentVars == null || subexprFunctions.isEmpty)
    splitExpressions(subexprFunctions.toSeq, "subexprFunc_split", Seq("InternalRow" -> INPUT_ROW))
  }

  /**
   * Perform a function which generates a sequence of ExprCodes with a given mapping between
   * expressions and common expressions, instead of using the mapping in current context.
   */
  def withSubExprEliminationExprs(
      newSubExprEliminationExprs: Map[ExpressionEquals, SubExprEliminationState])(
      f: => Seq[ExprCode]): Seq[ExprCode] = {
    val oldsubExprEliminationExprs = subExprEliminationExprs
    subExprEliminationExprs = newSubExprEliminationExprs

    val genCodes = f

    // Restore previous subExprEliminationExprs
    subExprEliminationExprs = oldsubExprEliminationExprs
    genCodes
  }

  /**
   * Evaluates a sequence of `SubExprEliminationState` which represent subexpressions. After
   * evaluating a subexpression, this method will clean up the code block to avoid duplicate
   * evaluation.
   */
  def evaluateSubExprEliminationState(subExprStates: Iterable[SubExprEliminationState]): String = {
    val code = new StringBuilder()

    subExprStates.foreach { state =>
      val currentCode = evaluateSubExprEliminationState(state.children) + "\n" + state.eval.code
      code.append(currentCode + "\n")
      state.eval.code = EmptyBlock
    }

    code.toString()
  }

  /**
   * Checks and sets up the state and codegen for subexpression elimination in whole-stage codegen.
   *
   * This finds the common subexpressions, generates the code snippets that evaluate those
   * expressions and populates the mapping of common subexpressions to the generated code snippets.
   *
   * The generated code snippet for subexpression is wrapped in `SubExprEliminationState`, which
   * contains an `ExprCode` and the children `SubExprEliminationState` if any. The `ExprCode`
   * includes java source code, result variable name and is-null variable name of the subexpression.
   *
   * Besides, this also returns a sequences of `ExprCode` which are expression codes that need to
   * be evaluated (as their input parameters) before evaluating subexpressions.
   *
   * To evaluate the returned subexpressions, please call `evaluateSubExprEliminationState` with
   * the `SubExprEliminationState`s to be evaluated. During generating the code, it will cleanup
   * the states to avoid duplicate evaluation.
   *
   * The details of subexpression generation:
   *   1. Gets subexpression set. See `EquivalentExpressions`.
   *   2. Generate code of subexpressions as a whole block of code (non-split case)
   *   3. Check if the total length of the above block is larger than the split-threshold. If so,
   *      try to split it in step 4, otherwise returning the non-split code block.
   *   4. Check if parameter lengths of all subexpressions satisfy the JVM limitation, if so,
   *      try to split, otherwise returning the non-split code block.
   *   5. For each subexpression, generating a function and put the code into it. To evaluate the
   *      subexpression, just call the function.
   *
   * The explanation of subexpression codegen:
   *   1. Wrapping in `withSubExprEliminationExprs` call with current subexpression map. Each
   *      subexpression may depends on other subexpressions (children). So when generating code
   *      for subexpressions, we iterate over each subexpression and put the mapping between
   *      (subexpression -> `SubExprEliminationState`) into the map. So in next subexpression
   *      evaluation, we can look for generated subexpressions and do replacement.
   */
  def subexpressionEliminationForWholeStageCodegen(expressions: Seq[Expression]): SubExprCodes = {
    // Create a clear EquivalentExpressions and SubExprEliminationState mapping
    val equivalentExpressions: EquivalentExpressions = new EquivalentExpressions
    val localSubExprEliminationExprsForNonSplit =
      mutable.HashMap.empty[ExpressionEquals, SubExprEliminationState]

    // Add each expression tree and compute the common subexpressions.
    expressions.foreach(equivalentExpressions.addExprTree(_))

    // Get all the expressions that appear at least twice and set up the state for subexpression
    // elimination.
    val commonExprs = equivalentExpressions.getCommonSubexpressions

    val nonSplitCode = {
      val allStates = mutable.ArrayBuffer.empty[SubExprEliminationState]
      commonExprs.map { expr =>
        withSubExprEliminationExprs(localSubExprEliminationExprsForNonSplit.toMap) {
          val eval = expr.genCode(this)
          // Collects other subexpressions from the children.
          val childrenSubExprs = mutable.ArrayBuffer.empty[SubExprEliminationState]
          expr.foreach { e =>
            subExprEliminationExprs.get(ExpressionEquals(e)) match {
              case Some(state) => childrenSubExprs += state
              case _ =>
            }
          }
          val state = SubExprEliminationState(eval, childrenSubExprs.toSeq)
          localSubExprEliminationExprsForNonSplit.put(ExpressionEquals(expr), state)
          allStates += state
          Seq(eval)
        }
      }
      allStates.toSeq
    }

    // For some operators, they do not require all its child's outputs to be evaluated in advance.
    // Instead it only early evaluates part of outputs, for example, `ProjectExec` only early
    // evaluate the outputs used more than twice. So we need to extract these variables used by
    // subexpressions and evaluate them before subexpressions.
    val (inputVarsForAllFuncs, exprCodesNeedEvaluate) = commonExprs.map { expr =>
      val (inputVars, exprCodes) = getLocalInputVariableValues(this, expr)
      (inputVars.toSeq, exprCodes.toSeq)
    }.unzip

    val needSplit = nonSplitCode.map(_.eval.code.length).sum > SQLConf.get.methodSplitThreshold
    val (subExprsMap, exprCodes) = if (needSplit) {
      if (inputVarsForAllFuncs.map(calculateParamLengthFromExprValues).forall(isValidParamLength)) {
        val localSubExprEliminationExprs =
          mutable.HashMap.empty[ExpressionEquals, SubExprEliminationState]

        commonExprs.zipWithIndex.foreach { case (expr, i) =>
          val eval = withSubExprEliminationExprs(localSubExprEliminationExprs.toMap) {
            Seq(expr.genCode(this))
          }.head

          val value = addMutableState(javaType(expr.dataType), "subExprValue")

          val isNullLiteral = eval.isNull match {
            case TrueLiteral | FalseLiteral => true
            case _ => false
          }
          val (isNull, isNullEvalCode) = if (!isNullLiteral) {
            val v = addMutableState(JAVA_BOOLEAN, "subExprIsNull")
            (JavaCode.isNullGlobal(v), s"$v = ${eval.isNull};")
          } else {
            (eval.isNull, "")
          }

          // Generate the code for this expression tree and wrap it in a function.
          val fnName = freshName("subExpr")
          val inputVars = inputVarsForAllFuncs(i)
          val argList =
            inputVars.map(v => s"${CodeGenerator.typeName(v.javaType)} ${v.variableName}")
          val fn =
            s"""
               |private void $fnName(${argList.mkString(", ")}) {
               |  ${eval.code}
               |  $isNullEvalCode
               |  $value = ${eval.value};
               |}
               """.stripMargin

          // Collects other subexpressions from the children.
          val childrenSubExprs = mutable.ArrayBuffer.empty[SubExprEliminationState]
          expr.foreach { e =>
            localSubExprEliminationExprs.get(ExpressionEquals(e)) match {
              case Some(state) => childrenSubExprs += state
              case _ =>
            }
          }

          val inputVariables = inputVars.map(_.variableName).mkString(", ")
          val code = code"${addNewFunction(fnName, fn)}($inputVariables);"
          val state = SubExprEliminationState(
            ExprCode(code, isNull, JavaCode.global(value, expr.dataType)),
            childrenSubExprs.toSeq)
          localSubExprEliminationExprs.put(ExpressionEquals(expr), state)
        }
        (localSubExprEliminationExprs, exprCodesNeedEvaluate)
      } else {
        val errMsg = "Failed to split subexpression code into small functions because " +
          "the parameter length of at least one split function went over the JVM limit: " +
          MAX_JVM_METHOD_PARAMS_LENGTH
        if (Utils.isTesting) {
          throw new IllegalStateException(errMsg)
        } else {
          logInfo(errMsg)
          (localSubExprEliminationExprsForNonSplit, Seq.empty)
        }
      }
    } else {
      (localSubExprEliminationExprsForNonSplit, Seq.empty)
    }
    SubExprCodes(subExprsMap.toMap, exprCodes.flatten)
  }

  /**
   * Checks and sets up the state and codegen for subexpression elimination. This finds the
   * common subexpressions, generates the functions that evaluate those expressions and populates
   * the mapping of common subexpressions to the generated functions.
   */
  private def subexpressionElimination(expressions: Seq[Expression]): Unit = {
    // Add each expression tree and compute the common subexpressions.
    expressions.foreach(equivalentExpressions.addExprTree(_))

    // Get all the expressions that appear at least twice and set up the state for subexpression
    // elimination.
    val commonExprs = equivalentExpressions.getCommonSubexpressions
    commonExprs.foreach { expr =>
      val fnName = freshName("subExpr")
      val isNull = addMutableState(JAVA_BOOLEAN, "subExprIsNull")
      val value = addMutableState(javaType(expr.dataType), "subExprValue")

      // Generate the code for this expression tree and wrap it in a function.
      val eval = expr.genCode(this)
      val fn =
        s"""
           |private void $fnName(InternalRow $INPUT_ROW) {
           |  ${eval.code}
           |  $isNull = ${eval.isNull};
           |  $value = ${eval.value};
           |}
           """.stripMargin

      // Add a state and a mapping of the common subexpressions that are associate with this
      // state. Adding this expression to subExprEliminationExprMap means it will call `fn`
      // when it is code generated. This decision should be a cost based one.
      //
      // The cost of doing subexpression elimination is:
      //   1. Extra function call, although this is probably *good* as the JIT can decide to
      //      inline or not.
      // The benefit doing subexpression elimination is:
      //   1. Running the expression logic. Even for a simple expression, it is likely more than 3
      //      above.
      //   2. Less code.
      // Currently, we will do this for all non-leaf only expression trees (i.e. expr trees with
      // at least two nodes) as the cost of doing it is expected to be low.

      val subExprCode = s"${addNewFunction(fnName, fn)}($INPUT_ROW);"
      subexprFunctions += subExprCode
      val state = SubExprEliminationState(
        ExprCode(code"$subExprCode",
          JavaCode.isNullGlobal(isNull),
          JavaCode.global(value, expr.dataType)))
      subExprEliminationExprs += ExpressionEquals(expr) -> state
    }
  }

  /**
   * Generates code for expressions. If doSubexpressionElimination is true, subexpression
   * elimination will be performed. Subexpression elimination assumes that the code for each
   * expression will be combined in the `expressions` order.
   */
  def generateExpressions(
      expressions: Seq[Expression],
      doSubexpressionElimination: Boolean = false): Seq[ExprCode] = {
    if (doSubexpressionElimination) subexpressionElimination(expressions)
    expressions.map(e => e.genCode(this))
  }

  /**
   * get a map of the pair of a place holder and a corresponding comment
   */
  def getPlaceHolderToComments(): collection.Map[String, String] = placeHolderToComments

  /**
   * Register a comment and return the corresponding place holder
   *
   * @param placeholderId an optionally specified identifier for the comment's placeholder.
   *                      The caller should make sure this identifier is unique within the
   *                      compilation unit. If this argument is not specified, a fresh identifier
   *                      will be automatically created and used as the placeholder.
   * @param force whether to force registering the comments
   */
   def registerComment(
       text: => String,
       placeholderId: String = "",
       force: Boolean = false): Block = {
    if (force || SQLConf.get.codegenComments) {
      val name = if (placeholderId != "") {
        assert(!placeHolderToComments.contains(placeholderId))
        placeholderId
      } else {
        freshName("c")
      }
      val comment = if (text.contains("\n") || text.contains("\r")) {
        text.split("(\r\n)|\r|\n").mkString("/**\n * ", "\n * ", "\n */")
      } else {
        s"// $text"
      }
      placeHolderToComments += (name -> comment)
      code"/*$name*/"
    } else {
      EmptyBlock
    }
  }
}

/**
 * A wrapper for generated class, defines a `generate` method so that we can pass extra objects
 * into generated class.
 */
abstract class GeneratedClass {
  def generate(references: Array[Any]): Any
}

/**
 * A wrapper for the source code to be compiled by [[CodeGenerator]].
 */
class CodeAndComment(val body: String, val comment: collection.Map[String, String])
  extends Serializable {
  override def equals(that: Any): Boolean = that match {
    case t: CodeAndComment if t.body == body => true
    case _ => false
  }

  override def hashCode(): Int = body.hashCode
}

/**
 * A base class for generators of byte code to perform expression evaluation.  Includes a set of
 * helpers for referring to Catalyst types and building trees that perform evaluation of individual
 * expressions.
 */
abstract class CodeGenerator[InType <: AnyRef, OutType <: AnyRef] extends Logging {

  protected val genericMutableRowType: String = classOf[GenericInternalRow].getName

  /**
   * Generates a class for a given input expression.  Called when there is not cached code
   * already available.
   */
  protected def create(in: InType): OutType

  /**
   * Canonicalizes an input expression. Used to avoid double caching expressions that differ only
   * cosmetically.
   */
  protected def canonicalize(in: InType): InType

  /** Binds an input expression to a given input schema */
  protected def bind(in: InType, inputSchema: Seq[Attribute]): InType

  /** Generates the requested evaluator binding the given expression(s) to the inputSchema. */
  def generate(expressions: InType, inputSchema: Seq[Attribute]): OutType =
    generate(bind(expressions, inputSchema))

  /** Generates the requested evaluator given already bound expression(s). */
  def generate(expressions: InType): OutType = create(canonicalize(expressions))

  /**
   * Create a new codegen context for expression evaluator, used to store those
   * expressions that don't support codegen
   */
  def newCodeGenContext(): CodegenContext = {
    new CodegenContext
  }
}

/**
 * Java bytecode statistics of a compiled class by Janino.
 */
case class ByteCodeStats(maxMethodCodeSize: Int, maxConstPoolSize: Int, numInnerClasses: Int)

object ByteCodeStats {
  val UNAVAILABLE = ByteCodeStats(-1, -1, -1)
}

object CodeGenerator extends Logging {

  // This is the default value of HugeMethodLimit in the OpenJDK HotSpot JVM,
  // beyond which methods will be rejected from JIT compilation
  final val DEFAULT_JVM_HUGE_METHOD_LIMIT = 8000

  // The max valid length of method parameters in JVM.
  final val MAX_JVM_METHOD_PARAMS_LENGTH = 255

  // The max number of constant pool entries in JVM.
  final val MAX_JVM_CONSTANT_POOL_SIZE = 65535

  // This is the threshold over which the methods in an inner class are grouped in a single
  // method which is going to be called by the outer class instead of the many small ones
  final val MERGE_SPLIT_METHODS_THRESHOLD = 3

  // The number of named constants that can exist in the class is limited by the Constant Pool
  // limit, 65,536. We cannot know how many constants will be inserted for a class, so we use a
  // threshold of 1000k bytes to determine when a function should be inlined to a private, inner
  // class.
  final val GENERATED_CLASS_SIZE_THRESHOLD = 1000000

  // This is the threshold for the number of global variables, whose types are primitive type or
  // complex type (e.g. more than one-dimensional array), that will be placed at the outer class
  final val OUTER_CLASS_VARIABLES_THRESHOLD = 10000

  // This is the maximum number of array elements to keep global variables in one Java array
  // 32767 is the maximum integer value that does not require a constant pool entry in a Java
  // bytecode instruction
  final val MUTABLESTATEARRAY_SIZE_LIMIT = 32768

  // The Java source code generated by whole-stage codegen on the Driver side is sent to each
  // Executor for compilation and data processing. This is very effective in processing large
  // amounts of data in a distributed environment. However, in the test environment,
  // because the amount of data is not large or not executed in parallel, the compilation time
  // of these Java source code will become a major part of the entire test runtime. When
  // running test cases, we summarize the total compilation time and output it to the execution
  // log for easy analysis and view.
  private val _compileTime = new LongAccumulator

  // Returns the total compile time of Java source code in nanoseconds.
  // Visible for testing
  def compileTime: Long = _compileTime.sum

  // Reset compile time.
  // Visible for testing
  def resetCompileTime(): Unit = _compileTime.reset()

  /**
   * Compile the Java source code into a Java class, using Janino.
   *
   * @return a pair of a generated class and the bytecode statistics of generated functions.
   */
  def compile(code: CodeAndComment): (GeneratedClass, ByteCodeStats) = try {
    cache.get(code)
  } catch {
    // Cache.get() may wrap the original exception. See the following URL
    // http://google.github.io/guava/releases/14.0/api/docs/com/google/common/cache/
    //   Cache.html#get(K,%20java.util.concurrent.Callable)
    case e @ (_: UncheckedExecutionException | _: ExecutionError) =>
      throw e.getCause
  }

  /**
   * Compile the Java source code into a Java class, using Janino.
   */
  private[this] def doCompile(code: CodeAndComment): (GeneratedClass, ByteCodeStats) = {
    val evaluator = new ClassBodyEvaluator()

    // A special classloader used to wrap the actual parent classloader of
    // [[org.codehaus.janino.ClassBodyEvaluator]] (see CodeGenerator.doCompile). This classloader
    // does not throw a ClassNotFoundException with a cause set (i.e. exception.getCause returns
    // a null). This classloader is needed because janino will throw the exception directly if
    // the parent classloader throws a ClassNotFoundException with cause set instead of trying to
    // find other possible classes (see org.codehaus.janinoClassLoaderIClassLoader's
    // findIClass method). Please also see https://issues.apache.org/jira/browse/SPARK-15622 and
    // https://issues.apache.org/jira/browse/SPARK-11636.
    val parentClassLoader = new ParentClassLoader(Utils.getContextOrSparkClassLoader)
    evaluator.setParentClassLoader(parentClassLoader)
    // Cannot be under package codegen, or fail with java.lang.InstantiationException
    evaluator.setClassName("org.apache.spark.sql.catalyst.expressions.GeneratedClass")
    evaluator.setDefaultImports(
      classOf[Platform].getName,
      classOf[InternalRow].getName,
      classOf[UnsafeRow].getName,
      classOf[UTF8String].getName,
      classOf[Decimal].getName,
      classOf[CalendarInterval].getName,
      classOf[ArrayData].getName,
      classOf[UnsafeArrayData].getName,
      classOf[MapData].getName,
      classOf[UnsafeMapData].getName,
      classOf[Expression].getName,
      classOf[TaskContext].getName,
      classOf[TaskKilledException].getName,
      classOf[InputMetrics].getName,
      QueryExecutionErrors.getClass.getName.stripSuffix("$")
    )
    evaluator.setExtendedClass(classOf[GeneratedClass])

    logDebug({
      // Only add extra debugging info to byte code when we are going to print the source code.
      evaluator.setDebuggingInformation(true, true, false)
      s"\n${CodeFormatter.format(code)}"
    })

    val codeStats = try {
      evaluator.cook("generated.java", code.body)
      updateAndGetCompilationStats(evaluator)
    } catch {
      case e: InternalCompilerException =>
        val msg = QueryExecutionErrors.failedToCompileMsg(e)
        logError(msg, e)
        logGeneratedCode(code)
        throw QueryExecutionErrors.internalCompilerError(e)
      case e: CompileException =>
        val msg = QueryExecutionErrors.failedToCompileMsg(e)
        logError(msg, e)
        logGeneratedCode(code)
        throw QueryExecutionErrors.compilerError(e)
    }

    (evaluator.getClazz().getConstructor().newInstance().asInstanceOf[GeneratedClass], codeStats)
  }

  private def logGeneratedCode(code: CodeAndComment): Unit = {
    val maxLines = SQLConf.get.loggingMaxLinesForCodegen
    if (Utils.isTesting) {
      logError(s"\n${CodeFormatter.format(code, maxLines)}")
    } else {
      logInfo(s"\n${CodeFormatter.format(code, maxLines)}")
    }
  }

  /**
   * Returns the bytecode statistics (max method bytecode size, max constant pool size, and
   * # of inner classes) of generated classes by inspecting Janino classes.
   * Also, this method updates the metrics information.
   */
  private def updateAndGetCompilationStats(evaluator: ClassBodyEvaluator): ByteCodeStats = {
    // First retrieve the generated classes.
    val classes = evaluator.getBytecodes.asScala

    // Then walk the classes to get at the method bytecode.
    val codeAttr = Utils.classForName("org.codehaus.janino.util.ClassFile$CodeAttribute")
    val codeAttrField = codeAttr.getDeclaredField("code")
    codeAttrField.setAccessible(true)
    val codeStats = classes.map { case (_, classBytes) =>
      val classCodeSize = classBytes.length
      CodegenMetrics.METRIC_GENERATED_CLASS_BYTECODE_SIZE.update(classCodeSize)
      try {
        val cf = new ClassFile(new ByteArrayInputStream(classBytes))
        val constPoolSize = cf.getConstantPoolSize
        val methodCodeSizes = cf.methodInfos.asScala.flatMap { method =>
          method.getAttributes().filter(_.getClass eq codeAttr).map { a =>
            val byteCodeSize = codeAttrField.get(a).asInstanceOf[Array[Byte]].length
            CodegenMetrics.METRIC_GENERATED_METHOD_BYTECODE_SIZE.update(byteCodeSize)

            if (byteCodeSize > DEFAULT_JVM_HUGE_METHOD_LIMIT) {
              logInfo("Generated method too long to be JIT compiled: " +
                s"${cf.getThisClassName}.${method.getName} is $byteCodeSize bytes")
            }

            byteCodeSize
          }
        }
        (methodCodeSizes.max, constPoolSize)
      } catch {
        case NonFatal(e) =>
          logWarning("Error calculating stats of compiled class.", e)
          (-1, -1)
      }
    }

    val (maxMethodSizes, constPoolSize) = codeStats.unzip
    ByteCodeStats(
      maxMethodCodeSize = maxMethodSizes.max,
      maxConstPoolSize = constPoolSize.max,
      // Minus 2 for `GeneratedClass` and an outer-most generated class
      numInnerClasses = classes.size - 2)
  }

  /**
   * A cache of generated classes.
   *
   * From the Guava Docs: A Cache is similar to ConcurrentMap, but not quite the same. The most
   * fundamental difference is that a ConcurrentMap persists all elements that are added to it until
   * they are explicitly removed. A Cache on the other hand is generally configured to evict entries
   * automatically, in order to constrain its memory footprint.  Note that this cache does not use
   * weak keys/values and thus does not respond to memory pressure.
   */
  private val cache = CacheBuilder.newBuilder()
    .maximumSize(SQLConf.get.codegenCacheMaxEntries)
    .build(
      new CacheLoader[CodeAndComment, (GeneratedClass, ByteCodeStats)]() {
        override def load(code: CodeAndComment): (GeneratedClass, ByteCodeStats) = {
          val startTime = System.nanoTime()
          val result = doCompile(code)
          val endTime = System.nanoTime()
          val duration = endTime - startTime
          val timeMs: Double = duration.toDouble / NANOS_PER_MILLIS
          CodegenMetrics.METRIC_SOURCE_CODE_SIZE.update(code.body.length)
          CodegenMetrics.METRIC_COMPILATION_TIME.update(timeMs.toLong)
          logInfo(s"Code generated in $timeMs ms")
          _compileTime.add(duration)
          result
        }
      })

  /**
   * Name of Java primitive data type
   */
  final val JAVA_BOOLEAN = "boolean"
  final val JAVA_BYTE = "byte"
  final val JAVA_SHORT = "short"
  final val JAVA_INT = "int"
  final val JAVA_LONG = "long"
  final val JAVA_FLOAT = "float"
  final val JAVA_DOUBLE = "double"

  /**
   * List of java primitive data types
   */
  val primitiveTypes =
    Seq(JAVA_BOOLEAN, JAVA_BYTE, JAVA_SHORT, JAVA_INT, JAVA_LONG, JAVA_FLOAT, JAVA_DOUBLE)

  /**
   * Returns true if a Java type is Java primitive primitive type
   */
  def isPrimitiveType(jt: String): Boolean = primitiveTypes.contains(jt)

  def isPrimitiveType(dt: DataType): Boolean = isPrimitiveType(javaType(dt))

  /**
   * Returns the specialized code to access a value from `inputRow` at `ordinal`.
   */
  @tailrec
  def getValue(input: String, dataType: DataType, ordinal: String): String = {
    val jt = javaType(dataType)
    dataType match {
      case _ if isPrimitiveType(jt) => s"$input.get${primitiveTypeName(jt)}($ordinal)"
      case t: DecimalType => s"$input.getDecimal($ordinal, ${t.precision}, ${t.scale})"
      case StringType => s"$input.getUTF8String($ordinal)"
      case BinaryType => s"$input.getBinary($ordinal)"
      case CalendarIntervalType => s"$input.getInterval($ordinal)"
      case t: StructType => s"$input.getStruct($ordinal, ${t.size})"
      case _: ArrayType => s"$input.getArray($ordinal)"
      case _: MapType => s"$input.getMap($ordinal)"
      case NullType => "null"
      case udt: UserDefinedType[_] => getValue(input, udt.sqlType, ordinal)
      case _ => s"($jt)$input.get($ordinal, null)"
    }
  }

  /**
   * Generates code creating a [[UnsafeArrayData]] or
   * [[org.apache.spark.sql.catalyst.util.GenericArrayData]] based on given parameters.
   *
   * @param arrayName name of the array to create
   * @param elementType data type of the elements in source array
   * @param numElements code representing the number of elements the array should contain
   * @param additionalErrorMessage string to include in the error message
   *
   * @return code representing the allocation of [[ArrayData]]
   */
  def createArrayData(
      arrayName: String,
      elementType: DataType,
      numElements: String,
      additionalErrorMessage: String): String = {
    val elementSize = if (CodeGenerator.isPrimitiveType(elementType)) {
      elementType.defaultSize
    } else {
      -1
    }
    s"""
       |ArrayData $arrayName = ArrayData.allocateArrayData(
       |  $elementSize, $numElements, "$additionalErrorMessage");
     """.stripMargin
  }

  /**
   * Generates assignment code for an [[ArrayData]]
   *
   * @param dstArray name of the array to be assigned
   * @param elementType data type of the elements in destination and source arrays
   * @param srcArray name of the array to be read
   * @param needNullCheck value which shows whether a nullcheck is required for the returning
   *                      assignment
   * @param dstArrayIndex an index variable to access each element of destination array
   * @param srcArrayIndex an index variable to access each element of source array
   *
   * @return code representing an assignment to each element of the [[ArrayData]], which requires
   *         a pair of destination and source loop index variables
   */
  def createArrayAssignment(
      dstArray: String,
      elementType: DataType,
      srcArray: String,
      dstArrayIndex: String,
      srcArrayIndex: String,
      needNullCheck: Boolean): String = {
    CodeGenerator.setArrayElement(dstArray, elementType, dstArrayIndex,
      CodeGenerator.getValue(srcArray, elementType, srcArrayIndex),
      if (needNullCheck) Some(s"$srcArray.isNullAt($srcArrayIndex)") else None)
  }

  /**
   * Returns the code to update a column in Row for a given DataType.
   */
  @tailrec
  def setColumn(row: String, dataType: DataType, ordinal: Int, value: String): String = {
    val jt = javaType(dataType)
    dataType match {
      case _ if isPrimitiveType(jt) => s"$row.set${primitiveTypeName(jt)}($ordinal, $value)"
      case CalendarIntervalType => s"$row.setInterval($ordinal, $value)"
      case t: DecimalType => s"$row.setDecimal($ordinal, $value, ${t.precision})"
      case udt: UserDefinedType[_] => setColumn(row, udt.sqlType, ordinal, value)
      // The UTF8String, InternalRow, ArrayData and MapData may came from UnsafeRow, we should copy
      // it to avoid keeping a "pointer" to a memory region which may get updated afterwards.
      case StringType | _: StructType | _: ArrayType | _: MapType =>
        s"$row.update($ordinal, $value.copy())"
      case _ => s"$row.update($ordinal, $value)"
    }
  }

  /**
   * Update a column in MutableRow from ExprCode.
   *
   * @param isVectorized True if the underlying row is of type `ColumnarBatch.Row`, false otherwise
   */
  def updateColumn(
      row: String,
      dataType: DataType,
      ordinal: Int,
      ev: ExprCode,
      nullable: Boolean,
      isVectorized: Boolean = false): String = {
    if (nullable) {
      // Can't call setNullAt on DecimalType/CalendarIntervalType, because we need to keep the
      // offset
      if (!isVectorized && (dataType.isInstanceOf[DecimalType] ||
        dataType.isInstanceOf[CalendarIntervalType])) {
        s"""
           |if (!${ev.isNull}) {
           |  ${setColumn(row, dataType, ordinal, ev.value)};
           |} else {
           |  ${setColumn(row, dataType, ordinal, "null")};
           |}
         """.stripMargin
      } else {
        s"""
           |if (!${ev.isNull}) {
           |  ${setColumn(row, dataType, ordinal, ev.value)};
           |} else {
           |  $row.setNullAt($ordinal);
           |}
         """.stripMargin
      }
    } else {
      s"""${setColumn(row, dataType, ordinal, ev.value)};"""
    }
  }

  /**
   * Returns the specialized code to set a given value in a column vector for a given `DataType`.
   */
  def setValue(vector: String, rowId: String, dataType: DataType, value: String): String = {
    val jt = javaType(dataType)
    dataType match {
      case _ if isPrimitiveType(jt) =>
        s"$vector.put${primitiveTypeName(jt)}($rowId, $value);"
      case t: DecimalType => s"$vector.putDecimal($rowId, $value, ${t.precision});"
      case CalendarIntervalType => s"$vector.putInterval($rowId, $value);"
      case t: StringType => s"$vector.putByteArray($rowId, $value.getBytes());"
      case _ =>
        throw new IllegalArgumentException(s"cannot generate code for unsupported type: $dataType")
    }
  }

  /**
   * Generates code of setter for an [[ArrayData]].
   */
  def setArrayElement(
      array: String,
      elementType: DataType,
      i: String,
      value: String,
      isNull: Option[String] = None): String = {
    val isPrimitiveType = CodeGenerator.isPrimitiveType(elementType)
    val setFunc = if (isPrimitiveType) {
      s"set${CodeGenerator.primitiveTypeName(elementType)}"
    } else {
      "update"
    }
    if (isNull.isDefined && isPrimitiveType) {
      s"""
         |if (${isNull.get}) {
         |  $array.setNullAt($i);
         |} else {
         |  $array.$setFunc($i, $value);
         |}
       """.stripMargin
    } else {
      s"$array.$setFunc($i, $value);"
    }
  }

  /**
   * Returns the specialized code to set a given value in a column vector for a given `DataType`
   * that could potentially be nullable.
   */
  def updateColumn(
      vector: String,
      rowId: String,
      dataType: DataType,
      ev: ExprCode,
      nullable: Boolean): String = {
    if (nullable) {
      s"""
         |if (!${ev.isNull}) {
         |  ${setValue(vector, rowId, dataType, ev.value)}
         |} else {
         |  $vector.putNull($rowId);
         |}
       """.stripMargin
    } else {
      s"""${setValue(vector, rowId, dataType, ev.value)};"""
    }
  }

  /**
   * Returns the specialized code to access a value from a column vector for a given `DataType`.
   */
  def getValueFromVector(vector: String, dataType: DataType, rowId: String): String = {
    val sqlDataType = dataType match {
      case udt: UserDefinedType[_] => udt.sqlType
      case _ => dataType
    }

    if (sqlDataType.isInstanceOf[StructType]) {
      // `ColumnVector.getStruct` is different from `InternalRow.getStruct`, it only takes an
      // `ordinal` parameter.
      s"$vector.getStruct($rowId)"
    } else {
      getValue(vector, sqlDataType, rowId)
    }
  }

  /**
   * This methods returns two values in a Tuple.
   *
   * First value: Extracts all the input variables from references and subexpression
   * elimination states for a given `expr`. This result will be used to split the
   * generated code of expressions into multiple functions.
   *
   * Second value: Returns the set of `ExprCodes`s which are necessary codes before
   * evaluating subexpressions.
   */
  def getLocalInputVariableValues(
      ctx: CodegenContext,
      expr: Expression,
      subExprs: Map[ExpressionEquals, SubExprEliminationState] = Map.empty)
      : (Set[VariableValue], Set[ExprCode]) = {
    val argSet = mutable.Set[VariableValue]()
    val exprCodesNeedEvaluate = mutable.Set[ExprCode]()

    if (ctx.INPUT_ROW != null) {
      argSet += JavaCode.variable(ctx.INPUT_ROW, classOf[InternalRow])
    }

    // Collects local variables from a given `expr` tree
    val collectLocalVariable = (ev: ExprValue) => ev match {
      case vv: VariableValue => argSet += vv
      case _ =>
    }

    val stack = mutable.Stack[Expression](expr)
    while (stack.nonEmpty) {
      stack.pop() match {
        case ref: BoundReference if ctx.currentVars != null &&
            ctx.currentVars(ref.ordinal) != null =>
          val exprCode = ctx.currentVars(ref.ordinal)
          // If the referred variable is not evaluated yet.
          if (exprCode.code != EmptyBlock) {
            exprCodesNeedEvaluate += exprCode.copy()
            exprCode.code = EmptyBlock
          }
          collectLocalVariable(exprCode.value)
          collectLocalVariable(exprCode.isNull)

        case e =>
          subExprs.get(ExpressionEquals(e)) match {
            case Some(state) =>
              collectLocalVariable(state.eval.value)
              collectLocalVariable(state.eval.isNull)
            case None =>
              stack.pushAll(e.children)
          }
      }
    }

    (argSet.toSet, exprCodesNeedEvaluate.toSet)
  }

  /**
   * Returns the name used in accessor and setter for a Java primitive type.
   */
  def primitiveTypeName(jt: String): String = jt match {
    case JAVA_INT => "Int"
    case _ => boxedType(jt)
  }

  def primitiveTypeName(dt: DataType): String = primitiveTypeName(javaType(dt))

  /**
   * Returns the Java type for a DataType.
   */
  def javaType(dt: DataType): String = dt match {
    case BooleanType => JAVA_BOOLEAN
    case ByteType => JAVA_BYTE
    case ShortType => JAVA_SHORT
    case IntegerType | DateType | _: YearMonthIntervalType => JAVA_INT
    case LongType | TimestampType | TimestampNTZType | _: DayTimeIntervalType => JAVA_LONG
    case FloatType => JAVA_FLOAT
    case DoubleType => JAVA_DOUBLE
    case _: DecimalType => "Decimal"
    case BinaryType => "byte[]"
    case StringType => "UTF8String"
    case CalendarIntervalType => "CalendarInterval"
    case _: StructType => "InternalRow"
    case _: ArrayType => "ArrayData"
    case _: MapType => "MapData"
    case udt: UserDefinedType[_] => javaType(udt.sqlType)
    case ObjectType(cls) if cls.isArray => s"${javaType(ObjectType(cls.getComponentType))}[]"
    case ObjectType(cls) => cls.getName
    case _ => "Object"
  }

  @tailrec
  def javaClass(dt: DataType): Class[_] = dt match {
    case BooleanType => java.lang.Boolean.TYPE
    case ByteType => java.lang.Byte.TYPE
    case ShortType => java.lang.Short.TYPE
    case IntegerType | DateType | _: YearMonthIntervalType => java.lang.Integer.TYPE
    case LongType | TimestampType | TimestampNTZType | _: DayTimeIntervalType =>
      java.lang.Long.TYPE
    case FloatType => java.lang.Float.TYPE
    case DoubleType => java.lang.Double.TYPE
    case _: DecimalType => classOf[Decimal]
    case BinaryType => classOf[Array[Byte]]
    case StringType => classOf[UTF8String]
    case CalendarIntervalType => classOf[CalendarInterval]
    case _: StructType => classOf[InternalRow]
    case _: ArrayType => classOf[ArrayData]
    case _: MapType => classOf[MapData]
    case udt: UserDefinedType[_] => javaClass(udt.sqlType)
    case ObjectType(cls) => cls
    case _ => classOf[Object]
  }

  /**
   * Returns the boxed type in Java.
   */
  def boxedType(jt: String): String = jt match {
    case JAVA_BOOLEAN => "Boolean"
    case JAVA_BYTE => "Byte"
    case JAVA_SHORT => "Short"
    case JAVA_INT => "Integer"
    case JAVA_LONG => "Long"
    case JAVA_FLOAT => "Float"
    case JAVA_DOUBLE => "Double"
    case other => other
  }

  def boxedType(dt: DataType): String = boxedType(javaType(dt))

  def typeName(clazz: Class[_]): String = {
    if (clazz.isArray) {
      typeName(clazz.getComponentType) + "[]"
    } else {
      clazz.getName
    }
  }

  /**
   * Returns the representation of default value for a given Java Type.
   * @param jt the string name of the Java type
   * @param typedNull if true, for null literals, return a typed (with a cast) version
   */
  def defaultValue(jt: String, typedNull: Boolean): String = jt match {
    case JAVA_BOOLEAN => "false"
    case JAVA_BYTE => "(byte)-1"
    case JAVA_SHORT => "(short)-1"
    case JAVA_INT => "-1"
    case JAVA_LONG => "-1L"
    case JAVA_FLOAT => "-1.0f"
    case JAVA_DOUBLE => "-1.0"
    case _ => if (typedNull) s"(($jt)null)" else "null"
  }

  def defaultValue(dt: DataType, typedNull: Boolean = false): String =
    defaultValue(javaType(dt), typedNull)

  /**
   * Returns the length of parameters for a Java method descriptor. `this` contributes one unit
   * and a parameter of type long or double contributes two units. Besides, for nullable parameter,
   * we also need to pass a boolean parameter for the null status.
   */
  def calculateParamLength(params: Seq[Expression]): Int = {
    def paramLengthForExpr(input: Expression): Int = {
      val javaParamLength = javaType(input.dataType) match {
        case JAVA_LONG | JAVA_DOUBLE => 2
        case _ => 1
      }
      // For a nullable expression, we need to pass in an extra boolean parameter.
      (if (input.nullable) 1 else 0) + javaParamLength
    }
    // Initial value is 1 for `this`.
    1 + params.map(paramLengthForExpr).sum
  }

  def calculateParamLengthFromExprValues(params: Seq[ExprValue]): Int = {
    def paramLengthForExpr(input: ExprValue): Int = input.javaType match {
      case java.lang.Long.TYPE | java.lang.Double.TYPE => 2
      case _ => 1
    }
    // Initial value is 1 for `this`.
    1 + params.map(paramLengthForExpr).sum
  }

  /**
   * In Java, a method descriptor is valid only if it represents method parameters with a total
   * length less than a pre-defined constant.
   */
  def isValidParamLength(paramLength: Int): Boolean = {
    // This config is only for testing
    SQLConf.get.getConfString("spark.sql.CodeGenerator.validParamLength", null) match {
      case null | "" => paramLength <= MAX_JVM_METHOD_PARAMS_LENGTH
      case validLength => paramLength <= validLength.toInt
    }
  }
}
