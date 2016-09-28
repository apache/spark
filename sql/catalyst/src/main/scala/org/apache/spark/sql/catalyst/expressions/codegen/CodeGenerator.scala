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
import java.util.{Map => JavaMap}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.codehaus.janino.{ByteArrayClassLoader, ClassBodyEvaluator, SimpleCompiler}
import org.codehaus.janino.util.ClassFile
import scala.language.existentials

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.CodegenMetrics
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types._
import org.apache.spark.util.{ParentClassLoader, Utils}

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
case class ExprCode(var code: String, var isNull: String, var value: String)

/**
 * State used for subexpression elimination.
 *
 * @param isNull A term that holds a boolean value representing whether the expression evaluated
 *               to null.
 * @param value A term for a value of a common sub-expression. Not valid if `isNull`
 *              is set to `true`.
 */
case class SubExprEliminationState(isNull: String, value: String)

/**
 * Codes and common subexpressions mapping used for subexpression elimination.
 *
 * @param codes Strings representing the codes that evaluate common subexpressions.
 * @param states Foreach expression that is participating in subexpression elimination,
 *               the state to use.
 */
case class SubExprCodes(codes: Seq[String], states: Map[Expression, SubExprEliminationState])

/**
 * A context for codegen, tracking a list of objects that could be passed into generated Java
 * function.
 */
class CodegenContext {

  /**
   * Holding a list of objects that could be used passed into generated class.
   */
  val references: mutable.ArrayBuffer[Any] = new mutable.ArrayBuffer[Any]()

  /**
   * Add an object to `references`, create a class member to access it.
   *
   * Returns the name of class member.
   */
  def addReferenceObj(name: String, obj: Any, className: String = null): String = {
    val term = freshName(name)
    val idx = references.length
    references += obj
    val clsName = Option(className).getOrElse(obj.getClass.getName)
    addMutableState(clsName, term, s"this.$term = ($clsName) references[$idx];")
    term
  }

  /**
   * Holding a list of generated columns as input of current operator, will be used by
   * BoundReference to generate code.
   */
  var currentVars: Seq[ExprCode] = null

  /**
   * Whether should we copy the result rows or not.
   *
   * If any operator inside WholeStageCodegen generate multiple rows from a single row (for
   * example, Join), this should be true.
   *
   * If an operator starts a new pipeline, this should be reset to false before calling `consume()`.
   */
  var copyResult: Boolean = false

  /**
   * Holding expressions' mutable states like `MonotonicallyIncreasingID.count` as a
   * 3-tuple: java type, variable name, code to init it.
   * As an example, ("int", "count", "count = 0;") will produce code:
   * {{{
   *   private int count;
   * }}}
   * as a member variable, and add
   * {{{
   *   count = 0;
   * }}}
   * to the constructor.
   *
   * They will be kept as member variables in generated classes like `SpecificProjection`.
   */
  val mutableStates: mutable.ArrayBuffer[(String, String, String)] =
    mutable.ArrayBuffer.empty[(String, String, String)]

  def addMutableState(javaType: String, variableName: String, initCode: String): Unit = {
    mutableStates += ((javaType, variableName, initCode))
  }

  /**
   * Add buffer variable which stores data coming from an [[InternalRow]]. This methods guarantees
   * that the variable is safely stored, which is important for (potentially) byte array backed
   * data types like: UTF8String, ArrayData, MapData & InternalRow.
   */
  def addBufferedState(dataType: DataType, variableName: String, initCode: String): ExprCode = {
    val value = freshName(variableName)
    addMutableState(javaType(dataType), value, "")
    val code = dataType match {
      case StringType => s"$value = $initCode.clone();"
      case _: StructType | _: ArrayType | _: MapType => s"$value = $initCode.copy();"
      case _ => s"$value = $initCode;"
    }
    ExprCode(code, "false", value)
  }

  def declareMutableStates(): String = {
    // It's possible that we add same mutable state twice, e.g. the `mergeExpressions` in
    // `TypedAggregateExpression`, we should call `distinct` here to remove the duplicated ones.
    mutableStates.distinct.map { case (javaType, variableName, _) =>
      s"private $javaType $variableName;"
    }.mkString("\n")
  }

  def initMutableStates(): String = {
    // It's possible that we add same mutable state twice, e.g. the `mergeExpressions` in
    // `TypedAggregateExpression`, we should call `distinct` here to remove the duplicated ones.
    val initCodes = mutableStates.distinct.map(_._3 + "\n")
    splitExpressions(initCodes, "init", Nil)
  }

  /**
   * Holding all the functions those will be added into generated class.
   */
  val addedFunctions: mutable.Map[String, String] =
    mutable.Map.empty[String, String]

  def addNewFunction(funcName: String, funcCode: String): Unit = {
    addedFunctions += ((funcName, funcCode))
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
  val equivalentExpressions: EquivalentExpressions = new EquivalentExpressions

  // Foreach expression that is participating in subexpression elimination, the state to use.
  val subExprEliminationExprs = mutable.HashMap.empty[Expression, SubExprEliminationState]

  // The collection of sub-expression result resetting methods that need to be called on each row.
  val subexprFunctions = mutable.ArrayBuffer.empty[String]

  def declareAddedFunctions(): String = {
    addedFunctions.map { case (funcName, funcCode) => funcCode }.mkString("\n")
  }

  final val JAVA_BOOLEAN = "boolean"
  final val JAVA_BYTE = "byte"
  final val JAVA_SHORT = "short"
  final val JAVA_INT = "int"
  final val JAVA_LONG = "long"
  final val JAVA_FLOAT = "float"
  final val JAVA_DOUBLE = "double"

  /** The variable name of the input row in generated code. */
  final var INPUT_ROW = "i"

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
    if (freshNameIds.contains(fullName)) {
      val id = freshNameIds(fullName)
      freshNameIds(fullName) = id + 1
      s"$fullName$id"
    } else {
      freshNameIds += fullName -> 1
      fullName
    }
  }

  /**
   * Returns the specialized code to access a value from `inputRow` at `ordinal`.
   */
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
   * Returns the code to update a column in Row for a given DataType.
   */
  def setColumn(row: String, dataType: DataType, ordinal: Int, value: String): String = {
    val jt = javaType(dataType)
    dataType match {
      case _ if isPrimitiveType(jt) => s"$row.set${primitiveTypeName(jt)}($ordinal, $value)"
      case t: DecimalType => s"$row.setDecimal($ordinal, $value, ${t.precision})"
      // The UTF8String may came from UnsafeRow, otherwise clone is cheap (re-use the bytes)
      case StringType => s"$row.update($ordinal, $value.clone())"
      case udt: UserDefinedType[_] => setColumn(row, udt.sqlType, ordinal, value)
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
      // Can't call setNullAt on DecimalType, because we need to keep the offset
      if (!isVectorized && dataType.isInstanceOf[DecimalType]) {
        s"""
           if (!${ev.isNull}) {
             ${setColumn(row, dataType, ordinal, ev.value)};
           } else {
             ${setColumn(row, dataType, ordinal, "null")};
           }
         """
      } else {
        s"""
           if (!${ev.isNull}) {
             ${setColumn(row, dataType, ordinal, ev.value)};
           } else {
             $row.setNullAt($ordinal);
           }
         """
      }
    } else {
      s"""${setColumn(row, dataType, ordinal, ev.value)};"""
    }
  }

  /**
   * Returns the specialized code to set a given value in a column vector for a given `DataType`.
   */
  def setValue(batch: String, row: String, dataType: DataType, ordinal: Int,
      value: String): String = {
    val jt = javaType(dataType)
    dataType match {
      case _ if isPrimitiveType(jt) =>
        s"$batch.column($ordinal).put${primitiveTypeName(jt)}($row, $value);"
      case t: DecimalType => s"$batch.column($ordinal).putDecimal($row, $value, ${t.precision});"
      case t: StringType => s"$batch.column($ordinal).putByteArray($row, $value.getBytes());"
      case _ =>
        throw new IllegalArgumentException(s"cannot generate code for unsupported type: $dataType")
    }
  }

  /**
   * Returns the specialized code to set a given value in a column vector for a given `DataType`
   * that could potentially be nullable.
   */
  def updateColumn(
      batch: String,
      row: String,
      dataType: DataType,
      ordinal: Int,
      ev: ExprCode,
      nullable: Boolean): String = {
    if (nullable) {
      s"""
         if (!${ev.isNull}) {
           ${setValue(batch, row, dataType, ordinal, ev.value)}
         } else {
           $batch.column($ordinal).putNull($row);
         }
       """
    } else {
      s"""${setValue(batch, row, dataType, ordinal, ev.value)};"""
    }
  }

  /**
   * Returns the specialized code to access a value from a column vector for a given `DataType`.
   */
  def getValue(batch: String, row: String, dataType: DataType, ordinal: Int): String = {
    val jt = javaType(dataType)
    dataType match {
      case _ if isPrimitiveType(jt) =>
        s"$batch.column($ordinal).get${primitiveTypeName(jt)}($row)"
      case t: DecimalType =>
        s"$batch.column($ordinal).getDecimal($row, ${t.precision}, ${t.scale})"
      case StringType =>
        s"$batch.column($ordinal).getUTF8String($row)"
      case _ =>
        throw new IllegalArgumentException(s"cannot generate code for unsupported type: $dataType")
    }
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
    case IntegerType | DateType => JAVA_INT
    case LongType | TimestampType => JAVA_LONG
    case FloatType => JAVA_FLOAT
    case DoubleType => JAVA_DOUBLE
    case dt: DecimalType => "Decimal"
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

  /**
   * Returns the representation of default value for a given Java Type.
   */
  def defaultValue(jt: String): String = jt match {
    case JAVA_BOOLEAN => "false"
    case JAVA_BYTE => "(byte)-1"
    case JAVA_SHORT => "(short)-1"
    case JAVA_INT => "-1"
    case JAVA_LONG => "-1L"
    case JAVA_FLOAT => "-1.0f"
    case JAVA_DOUBLE => "-1.0"
    case _ => "null"
  }

  def defaultValue(dt: DataType): String = defaultValue(javaType(dt))

  /**
   * Generates code for equal expression in Java.
   */
  def genEqual(dataType: DataType, c1: String, c2: String): String = dataType match {
    case BinaryType => s"java.util.Arrays.equals($c1, $c2)"
    case FloatType => s"(java.lang.Float.isNaN($c1) && java.lang.Float.isNaN($c2)) || $c1 == $c2"
    case DoubleType => s"(java.lang.Double.isNaN($c1) && java.lang.Double.isNaN($c2)) || $c1 == $c2"
    case dt: DataType if isPrimitiveType(dt) => s"$c1 == $c2"
    case udt: UserDefinedType[_] => genEqual(udt.sqlType, c1, c2)
    case other => s"$c1.equals($c2)"
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
    case DoubleType => s"org.apache.spark.util.Utils.nanSafeCompareDoubles($c1, $c2)"
    case FloatType => s"org.apache.spark.util.Utils.nanSafeCompareFloats($c1, $c2)"
    // use c1 - c2 may overflow
    case dt: DataType if isPrimitiveType(dt) => s"($c1 > $c2 ? 1 : $c1 < $c2 ? -1 : 0)"
    case BinaryType => s"org.apache.spark.sql.catalyst.util.TypeUtils.compareBinary($c1, $c2)"
    case NullType => "0"
    case array: ArrayType =>
      val elementType = array.elementType
      val elementA = freshName("elementA")
      val isNullA = freshName("isNullA")
      val elementB = freshName("elementB")
      val isNullB = freshName("isNullB")
      val compareFunc = freshName("compareArray")
      val minLength = freshName("minLength")
      val funcCode: String =
        s"""
          public int $compareFunc(ArrayData a, ArrayData b) {
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
                ${javaType(elementType)} $elementA = ${getValue("a", elementType, "i")};
                ${javaType(elementType)} $elementB = ${getValue("b", elementType, "i")};
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
      addNewFunction(compareFunc, funcCode)
      s"this.$compareFunc($c1, $c2)"
    case schema: StructType =>
      INPUT_ROW = "i"
      val comparisons = GenerateOrdering.genComparisons(this, schema)
      val compareFunc = freshName("compareStruct")
      val funcCode: String =
        s"""
          public int $compareFunc(InternalRow a, InternalRow b) {
            InternalRow i = null;
            $comparisons
            return 0;
          }
        """
      addNewFunction(compareFunc, funcCode)
      s"this.$compareFunc($c1, $c2)"
    case other if other.isInstanceOf[AtomicType] => s"$c1.compare($c2)"
    case udt: UserDefinedType[_] => genComp(udt.sqlType, c1, c2)
    case _ =>
      throw new IllegalArgumentException("cannot generate compare code for un-comparable type")
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
   * List of java data types that have special accessors and setters in [[InternalRow]].
   */
  val primitiveTypes =
    Seq(JAVA_BOOLEAN, JAVA_BYTE, JAVA_SHORT, JAVA_INT, JAVA_LONG, JAVA_FLOAT, JAVA_DOUBLE)

  /**
   * Returns true if the Java type has a special accessor and setter in [[InternalRow]].
   */
  def isPrimitiveType(jt: String): Boolean = primitiveTypes.contains(jt)

  def isPrimitiveType(dt: DataType): Boolean = isPrimitiveType(javaType(dt))

  /**
   * Splits the generated code of expressions into multiple functions, because function has
   * 64kb code size limit in JVM
   *
   * @param row the variable name of row that is used by expressions
   * @param expressions the codes to evaluate expressions.
   */
  def splitExpressions(row: String, expressions: Seq[String]): String = {
    if (row == null || currentVars != null) {
      // Cannot split these expressions because they are not created from a row object.
      return expressions.mkString("\n")
    }
    splitExpressions(expressions, "apply", ("InternalRow", row) :: Nil)
  }

  private def splitExpressions(
      expressions: Seq[String], funcName: String, arguments: Seq[(String, String)]): String = {
    val blocks = new ArrayBuffer[String]()
    val blockBuilder = new StringBuilder()
    for (code <- expressions) {
      // We can't know how many bytecode will be generated, so use the length of source code
      // as metric. A method should not go beyond 8K, otherwise it will not be JITted, should
      // also not be too small, or it will have many function calls (for wide table), see the
      // results in BenchmarkWideTable.
      if (blockBuilder.length > 1024) {
        blocks += blockBuilder.toString()
        blockBuilder.clear()
      }
      blockBuilder.append(code)
    }
    blocks += blockBuilder.toString()

    if (blocks.length == 1) {
      // inline execution if only one block
      blocks.head
    } else {
      val func = freshName(funcName)
      val functions = blocks.zipWithIndex.map { case (body, i) =>
        val name = s"${func}_$i"
        val code = s"""
           |private void $name(${arguments.map { case (t, name) => s"$t $name" }.mkString(", ")}) {
           |  $body
           |}
         """.stripMargin
        addNewFunction(name, code)
        name
      }

      functions.map(name => s"$name(${arguments.map(_._2).mkString(", ")});").mkString("\n")
    }
  }

  /**
   * Perform a function which generates a sequence of ExprCodes with a given mapping between
   * expressions and common expressions, instead of using the mapping in current context.
   */
  def withSubExprEliminationExprs(
      newSubExprEliminationExprs: Map[Expression, SubExprEliminationState])(
      f: => Seq[ExprCode]): Seq[ExprCode] = {
    val oldsubExprEliminationExprs = subExprEliminationExprs
    subExprEliminationExprs.clear
    newSubExprEliminationExprs.foreach(subExprEliminationExprs += _)

    val genCodes = f

    // Restore previous subExprEliminationExprs
    subExprEliminationExprs.clear
    oldsubExprEliminationExprs.foreach(subExprEliminationExprs += _)
    genCodes
  }

  /**
   * Checks and sets up the state and codegen for subexpression elimination. This finds the
   * common subexpressions, generates the code snippets that evaluate those expressions and
   * populates the mapping of common subexpressions to the generated code snippets. The generated
   * code snippets will be returned and should be inserted into generated codes before these
   * common subexpressions actually are used first time.
   */
  def subexpressionEliminationForWholeStageCodegen(expressions: Seq[Expression]): SubExprCodes = {
    // Create a clear EquivalentExpressions and SubExprEliminationState mapping
    val equivalentExpressions: EquivalentExpressions = new EquivalentExpressions
    val subExprEliminationExprs = mutable.HashMap.empty[Expression, SubExprEliminationState]

    // Add each expression tree and compute the common subexpressions.
    expressions.foreach(equivalentExpressions.addExprTree(_, true, false))

    // Get all the expressions that appear at least twice and set up the state for subexpression
    // elimination.
    val commonExprs = equivalentExpressions.getAllEquivalentExprs.filter(_.size > 1)
    val codes = commonExprs.map { e =>
      val expr = e.head
      // Generate the code for this expression tree.
      val code = expr.genCode(this)
      val state = SubExprEliminationState(code.isNull, code.value)
      e.foreach(subExprEliminationExprs.put(_, state))
      code.code.trim
    }
    SubExprCodes(codes, subExprEliminationExprs.toMap)
  }

  /**
   * Checks and sets up the state and codegen for subexpression elimination. This finds the
   * common subexpressions, generates the functions that evaluate those expressions and populates
   * the mapping of common subexpressions to the generated functions.
   */
  private def subexpressionElimination(expressions: Seq[Expression]) = {
    // Add each expression tree and compute the common subexpressions.
    expressions.foreach(equivalentExpressions.addExprTree(_))

    // Get all the expressions that appear at least twice and set up the state for subexpression
    // elimination.
    val commonExprs = equivalentExpressions.getAllEquivalentExprs.filter(_.size > 1)
    commonExprs.foreach { e =>
      val expr = e.head
      val fnName = freshName("evalExpr")
      val isNull = s"${fnName}IsNull"
      val value = s"${fnName}Value"

      // Generate the code for this expression tree and wrap it in a function.
      val code = expr.genCode(this)
      val fn =
        s"""
           |private void $fnName(InternalRow $INPUT_ROW) {
           |  ${code.code.trim}
           |  $isNull = ${code.isNull};
           |  $value = ${code.value};
           |}
           """.stripMargin

      addNewFunction(fnName, fn)

      // Add a state and a mapping of the common subexpressions that are associate with this
      // state. Adding this expression to subExprEliminationExprMap means it will call `fn`
      // when it is code generated. This decision should be a cost based one.
      //
      // The cost of doing subexpression elimination is:
      //   1. Extra function call, although this is probably *good* as the JIT can decide to
      //      inline or not.
      //   2. Extra branch to check isLoaded. This branch is likely to be predicted correctly
      //      very often. The reason it is not loaded is because of a prior branch.
      //   3. Extra store into isLoaded.
      // The benefit doing subexpression elimination is:
      //   1. Running the expression logic. Even for a simple expression, it is likely more than 3
      //      above.
      //   2. Less code.
      // Currently, we will do this for all non-leaf only expression trees (i.e. expr trees with
      // at least two nodes) as the cost of doing it is expected to be low.
      addMutableState("boolean", isNull, s"$isNull = false;")
      addMutableState(javaType(expr.dataType), value,
        s"$value = ${defaultValue(expr.dataType)};")

      subexprFunctions += s"$fnName($INPUT_ROW);"
      val state = SubExprEliminationState(isNull, value)
      e.foreach(subExprEliminationExprs.put(_, state))
    }
  }

  /**
   * Generates code for expressions. If doSubexpressionElimination is true, subexpression
   * elimination will be performed. Subexpression elimination assumes that the code will for each
   * expression will be combined in the `expressions` order.
   */
  def generateExpressions(expressions: Seq[Expression],
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
   */
  def registerComment(text: => String): String = {
    // By default, disable comments in generated code because computing the comments themselves can
    // be extremely expensive in certain cases, such as deeply-nested expressions which operate over
    // inputs with wide schemas. For more details on the performance issues that motivated this
    // flat, see SPARK-15680.
    if (SparkEnv.get != null && SparkEnv.get.conf.getBoolean("spark.sql.codegen.comments", false)) {
      val name = freshName("c")
      val comment = if (text.contains("\n") || text.contains("\r")) {
        text.split("(\r\n)|\r|\n").mkString("/**\n * ", "\n * ", "\n */")
      } else {
        s"// $text"
      }
      placeHolderToComments += (name -> comment)
      s"/*$name*/"
    } else {
      ""
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

  protected val genericMutableRowType: String = classOf[GenericMutableRow].getName

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

object CodeGenerator extends Logging {
  /**
   * Compile the Java source code into a Java class, using Janino.
   */
  def compile(code: CodeAndComment): GeneratedClass = {
    cache.get(code)
  }

  /**
   * Compile the Java source code into a Java class, using Janino.
   */
  private[this] def doCompile(code: CodeAndComment): GeneratedClass = {
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
    evaluator.setDefaultImports(Array(
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
      classOf[MutableRow].getName,
      classOf[Expression].getName
    ))
    evaluator.setExtendedClass(classOf[GeneratedClass])

    lazy val formatted = CodeFormatter.format(code)

    logDebug({
      // Only add extra debugging info to byte code when we are going to print the source code.
      evaluator.setDebuggingInformation(true, true, false)
      s"\n$formatted"
    })

    try {
      evaluator.cook("generated.java", code.body)
      recordCompilationStats(evaluator)
    } catch {
      case e: Exception =>
        val msg = s"failed to compile: $e\n$formatted"
        logError(msg, e)
        throw new Exception(msg, e)
    }
    evaluator.getClazz().newInstance().asInstanceOf[GeneratedClass]
  }

  /**
   * Records the generated class and method bytecode sizes by inspecting janino private fields.
   */
  private def recordCompilationStats(evaluator: ClassBodyEvaluator): Unit = {
    // First retrieve the generated classes.
    val classes = {
      val resultField = classOf[SimpleCompiler].getDeclaredField("result")
      resultField.setAccessible(true)
      val loader = resultField.get(evaluator).asInstanceOf[ByteArrayClassLoader]
      val classesField = loader.getClass.getDeclaredField("classes")
      classesField.setAccessible(true)
      classesField.get(loader).asInstanceOf[JavaMap[String, Array[Byte]]].asScala
    }

    // Then walk the classes to get at the method bytecode.
    val codeAttr = Utils.classForName("org.codehaus.janino.util.ClassFile$CodeAttribute")
    val codeAttrField = codeAttr.getDeclaredField("code")
    codeAttrField.setAccessible(true)
    classes.foreach { case (_, classBytes) =>
      CodegenMetrics.METRIC_GENERATED_CLASS_BYTECODE_SIZE.update(classBytes.length)
      try {
        val cf = new ClassFile(new ByteArrayInputStream(classBytes))
        cf.methodInfos.asScala.foreach { method =>
          method.getAttributes().foreach { a =>
            if (a.getClass.getName == codeAttr.getName) {
              CodegenMetrics.METRIC_GENERATED_METHOD_BYTECODE_SIZE.update(
                codeAttrField.get(a).asInstanceOf[Array[Byte]].length)
            }
          }
        }
      } catch {
        case NonFatal(e) =>
          logWarning("Error calculating stats of compiled class.", e)
      }
    }
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
    .maximumSize(100)
    .build(
      new CacheLoader[CodeAndComment, GeneratedClass]() {
        override def load(code: CodeAndComment): GeneratedClass = {
          val startTime = System.nanoTime()
          val result = doCompile(code)
          val endTime = System.nanoTime()
          def timeMs: Double = (endTime - startTime).toDouble / 1000000
          CodegenMetrics.METRIC_SOURCE_CODE_SIZE.update(code.body.length)
          CodegenMetrics.METRIC_COMPILATION_TIME.update(timeMs.toLong)
          logInfo(s"Code generated in $timeMs ms")
          result
        }
      })
}
