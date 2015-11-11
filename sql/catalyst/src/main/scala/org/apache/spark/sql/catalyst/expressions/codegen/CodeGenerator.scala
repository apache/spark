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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.existentials

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.codehaus.janino.ClassBodyEvaluator

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.{MapData, ArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types._


// These classes are here to avoid issues with serialization and integration with quasiquotes.
class IntegerHashSet extends org.apache.spark.util.collection.OpenHashSet[Int]
class LongHashSet extends org.apache.spark.util.collection.OpenHashSet[Long]

/**
 * Java source for evaluating an [[Expression]] given a [[InternalRow]] of input.
 *
 * @param code The sequence of statements required to evaluate the expression.
 * @param isNull A term that holds a boolean value representing whether the expression evaluated
 *                 to null.
 * @param value A term for a (possibly primitive) value of the result of the evaluation. Not
 *              valid if `isNull` is set to `true`.
 */
case class GeneratedExpressionCode(var code: String, var isNull: String, var value: String)

/**
 * A context for codegen, which is used to bookkeeping the expressions those are not supported
 * by codegen, then they are evaluated directly. The unsupported expression is appended at the
 * end of `references`, the position of it is kept in the code, used to access and evaluate it.
 */
class CodeGenContext {

  /**
   * Holding all the expressions those do not support codegen, will be evaluated directly.
   */
  val references: mutable.ArrayBuffer[Expression] = new mutable.ArrayBuffer[Expression]()

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
   * For example, consider two exprsesion generated from this SQL statement:
   *  SELECT (col1 + col2), (col1 + col2) / col3.
   *
   *  equivalentExpressions will match the tree containing `col1 + col2` and it will only
   *  be evaluated once.
   */
  val equivalentExpressions: EquivalentExpressions = new EquivalentExpressions

  // State used for subexpression elimination.
  case class SubExprEliminationState(
    val isLoaded: String, code: GeneratedExpressionCode, val fnName: String)

  // Foreach expression that is participating in subexpression elimination, the state to use.
  val subExprEliminationExprs: mutable.HashMap[Expression, SubExprEliminationState] =
    mutable.HashMap[Expression, SubExprEliminationState]()

  // The collection of isLoaded variables that need to be reset on each row.
  val subExprIsLoadedVariables: mutable.ArrayBuffer[String] =
    mutable.ArrayBuffer.empty[String]

  final val JAVA_BOOLEAN = "boolean"
  final val JAVA_BYTE = "byte"
  final val JAVA_SHORT = "short"
  final val JAVA_INT = "int"
  final val JAVA_LONG = "long"
  final val JAVA_FLOAT = "float"
  final val JAVA_DOUBLE = "double"

  /** The variable name of the input row in generated code. */
  final val INPUT_ROW = "i"

  private val curId = new java.util.concurrent.atomic.AtomicInteger()

  /**
   * Returns a term name that is unique within this instance of a `CodeGenerator`.
   *
   * (Since we aren't in a macro context we do not seem to have access to the built in `freshName`
   * function.)
   */
  def freshName(prefix: String): String = {
    s"$prefix${curId.getAndIncrement}"
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
    case dt: OpenHashSetUDT if dt.elementType == IntegerType => classOf[IntegerHashSet].getName
    case dt: OpenHashSetUDT if dt.elementType == LongType => classOf[LongHashSet].getName
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
    case schema: StructType =>
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
    val blocks = new ArrayBuffer[String]()
    val blockBuilder = new StringBuilder()
    for (code <- expressions) {
      // We can't know how many byte code will be generated, so use the number of bytes as limit
      if (blockBuilder.length > 64 * 1000) {
        blocks.append(blockBuilder.toString())
        blockBuilder.clear()
      }
      blockBuilder.append(code)
    }
    blocks.append(blockBuilder.toString())

    if (blocks.length == 1) {
      // inline execution if only one block
      blocks.head
    } else {
      val apply = freshName("apply")
      val functions = blocks.zipWithIndex.map { case (body, i) =>
        val name = s"${apply}_$i"
        val code = s"""
           |private void $name(InternalRow $row) {
           |  $body
           |}
         """.stripMargin
        addNewFunction(name, code)
        name
      }

      functions.map(name => s"$name($row);").mkString("\n")
    }
  }

  /**
   * Checks and sets up the state and codegen for subexpression elimination. This finds the
   * common subexpresses, generates the functions that evaluate those expressions and populates
   * the mapping of common subexpressions to the generated functions.
   */
  private def subexpressionElimination(expressions: Seq[Expression]) = {
    // Add each expression tree and compute the common subexpressions.
    expressions.foreach(equivalentExpressions.addExprTree(_))

    // Get all the exprs that appear at least twice and set up the state for subexpression
    // elimination.
    val commonExprs = equivalentExpressions.getAllEquivalentExprs.filter(_.size > 1)
    commonExprs.foreach(e => {
      val expr = e.head
      val isLoaded = freshName("isLoaded")
      val isNull = freshName("isNull")
      val primitive = freshName("primitive")
      val fnName = freshName("evalExpr")

      // Generate the code for this expression tree and wrap it in a function.
      val code = expr.gen(this)
      val fn =
        s"""
           |private void $fnName(InternalRow ${INPUT_ROW}) {
           |  if (!$isLoaded) {
           |    ${code.code.trim}
           |    $isLoaded = true;
           |    $isNull = ${code.isNull};
           |    $primitive = ${code.value};
           |  }
           |}
           """.stripMargin
      code.code = fn
      code.isNull = isNull
      code.value = primitive

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

      // Maintain the loaded value and isNull as member variables. This is necessary if the codegen
      // function is split across multiple functions.
      // TODO: maintaining this as a local variable probably allows the compiler to do better
      // optimizations.
      addMutableState("boolean", isLoaded, s"$isLoaded = false;")
      addMutableState("boolean", isNull, s"$isNull = false;")
      addMutableState(javaType(expr.dataType), primitive,
        s"$primitive = ${defaultValue(expr.dataType)};")
      subExprIsLoadedVariables += isLoaded

      val state = SubExprEliminationState(isLoaded, code, fnName)
      e.foreach(subExprEliminationExprs.put(_, state))
    })
  }

  /**
   * Generates code for expressions. If doSubexpressionElimination is true, subexpression
   * elimination will be performed. Subexpression elimination assumes that the code will for each
   * expression will be combined in the `expressions` order.
   */
  def generateExpressions(expressions: Seq[Expression],
      doSubexpressionElimination: Boolean = false): Seq[GeneratedExpressionCode] = {
    if (doSubexpressionElimination) subexpressionElimination(expressions)
    expressions.map(e => e.gen(this))
  }
}

/**
 * A wrapper for generated class, defines a `generate` method so that we can pass extra objects
 * into generated class.
 */
abstract class GeneratedClass {
  def generate(expressions: Array[Expression]): Any
}

/**
 * A base class for generators of byte code to perform expression evaluation.  Includes a set of
 * helpers for referring to Catalyst types and building trees that perform evaluation of individual
 * expressions.
 */
abstract class CodeGenerator[InType <: AnyRef, OutType <: AnyRef] extends Logging {

  protected val exprType: String = classOf[Expression].getName
  protected val mutableRowType: String = classOf[MutableRow].getName
  protected val genericMutableRowType: String = classOf[GenericMutableRow].getName

  protected def declareMutableStates(ctx: CodeGenContext): String = {
    ctx.mutableStates.map { case (javaType, variableName, _) =>
      s"private $javaType $variableName;"
    }.mkString("\n")
  }

  protected def initMutableStates(ctx: CodeGenContext): String = {
    ctx.mutableStates.map(_._3).mkString("\n")
  }

  protected def declareAddedFunctions(ctx: CodeGenContext): String = {
    ctx.addedFunctions.map { case (funcName, funcCode) => funcCode }.mkString("\n").trim
  }

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

  /**
   * Compile the Java source code into a Java class, using Janino.
   */
  protected def compile(code: String): GeneratedClass = {
    cache.get(code)
  }

  /**
   * Compile the Java source code into a Java class, using Janino.
   */
  private[this] def doCompile(code: String): GeneratedClass = {
    val evaluator = new ClassBodyEvaluator()
    evaluator.setParentClassLoader(getClass.getClassLoader)
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
      classOf[MutableRow].getName
    ))
    evaluator.setExtendedClass(classOf[GeneratedClass])

    def formatted = CodeFormatter.format(code)

    logDebug({
      // Only add extra debugging info to byte code when we are going to print the source code.
      evaluator.setDebuggingInformation(true, true, false)
      formatted
    })

    try {
      evaluator.cook("generated.java", code)
    } catch {
      case e: Exception =>
        val msg = s"failed to compile: $e\n$formatted"
        logError(msg, e)
        throw new Exception(msg, e)
    }
    evaluator.getClazz().newInstance().asInstanceOf[GeneratedClass]
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
      new CacheLoader[String, GeneratedClass]() {
        override def load(code: String): GeneratedClass = {
          val startTime = System.nanoTime()
          val result = doCompile(code)
          val endTime = System.nanoTime()
          def timeMs: Double = (endTime - startTime).toDouble / 1000000
          logInfo(s"Code generated in $timeMs ms")
          result
        }
      })

  /** Generates the requested evaluator binding the given expression(s) to the inputSchema. */
  def generate(expressions: InType, inputSchema: Seq[Attribute]): OutType =
    generate(bind(expressions, inputSchema))

  /** Generates the requested evaluator given already bound expression(s). */
  def generate(expressions: InType): OutType = create(canonicalize(expressions))

  /**
   * Create a new codegen context for expression evaluator, used to store those
   * expressions that don't support codegen
   */
  def newCodeGenContext(): CodeGenContext = {
    new CodeGenContext
  }
}
