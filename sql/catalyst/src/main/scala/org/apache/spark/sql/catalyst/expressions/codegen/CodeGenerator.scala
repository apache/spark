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
import scala.language.existentials

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.codehaus.janino.ClassBodyEvaluator

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

// These classes are here to avoid issues with serialization and integration with quasiquotes.
class IntegerHashSet extends org.apache.spark.util.collection.OpenHashSet[Int]
class LongHashSet extends org.apache.spark.util.collection.OpenHashSet[Long]

/**
 * Java source for evaluating an [[Expression]] given a [[Row]] of input.
 *
 * @param code The sequence of statements required to evaluate the expression.
 * @param nullTerm A term that holds a boolean value representing whether the expression evaluated
 *                 to null.
 * @param primitiveTerm A term for a possible primitive value of the result of the evaluation. Not
 *                      valid if `nullTerm` is set to `true`.
 * @param objectTerm A possibly boxed version of the result of evaluating this expression.
 */
case class GeneratedExpressionCode(var code: Code,
                                   nullTerm: Term,
                                   primitiveTerm: Term,
                                   objectTerm: Term)

/**
 * A context for codegen, which is used to bookkeeping the expressions those are not supported
 * by codegen, then they are evaluated directly. The unsupported expression is appended at the
 * end of `references`, the position of it is kept in the code, used to access and evaluate it.
 */
class CodeGenContext {

  /**
   * Holding all the expressions those do not support codegen, will be evaluated directly.
   */
  val references: Seq[Expression] = new mutable.ArrayBuffer[Expression]()

  val stringType = classOf[UTF8String].getName
  val decimalType = classOf[Decimal].getName

  private val curId = new java.util.concurrent.atomic.AtomicInteger()

  /**
   * Returns a term name that is unique within this instance of a `CodeGenerator`.
   *
   * (Since we aren't in a macro context we do not seem to have access to the built in `freshName`
   * function.)
   */
  def freshName(prefix: String): Term = {
    s"$prefix${curId.getAndIncrement}"
  }

  def getColumn(dataType: DataType, ordinal: Int): Code = {
    dataType match {
      case StringType => s"($stringType)i.apply($ordinal)"
      case dt: DataType if isNativeType(dt) => s"i.${accessorForType(dt)}($ordinal)"
      case _ => s"(${boxedType(dataType)})i.apply($ordinal)"
    }
  }

  def setColumn(destinationRow: Term, dataType: DataType, ordinal: Int, value: Term): Code = {
    dataType match {
      case StringType => s"$destinationRow.update($ordinal, $value)"
      case dt: DataType if isNativeType(dt) =>
        s"$destinationRow.${mutatorForType(dt)}($ordinal, $value)"
      case _ => s"$destinationRow.update($ordinal, $value)"
    }
  }

  def accessorForType(dt: DataType): Term = dt match {
    case IntegerType => "getInt"
    case other => s"get${boxedType(dt)}"
  }

  def mutatorForType(dt: DataType): Term = dt match {
    case IntegerType => "setInt"
    case other => s"set${boxedType(dt)}"
  }

  def hashSetForType(dt: DataType): Term = dt match {
    case IntegerType => classOf[IntegerHashSet].getName
    case LongType => classOf[LongHashSet].getName
    case unsupportedType =>
      sys.error(s"Code generation not support for hashset of type $unsupportedType")
  }

  /**
   * Return the primitive type for a DataType
   */
  def primitiveType(dt: DataType): Term = dt match {
    case IntegerType => "int"
    case LongType => "long"
    case ShortType => "short"
    case ByteType => "byte"
    case DoubleType => "double"
    case FloatType => "float"
    case BooleanType => "boolean"
    case dt: DecimalType => decimalType
    case BinaryType => "byte[]"
    case StringType => stringType
    case DateType => "int"
    case TimestampType => "java.sql.Timestamp"
    case _ => "Object"
  }

  /**
   * Return the representation of default value for given DataType
   */
  def defaultValue(dt: DataType): Term = dt match {
    case BooleanType => "false"
    case FloatType => "-1.0f"
    case ShortType => "-1"
    case LongType => "-1"
    case ByteType => "-1"
    case DoubleType => "-1.0"
    case IntegerType => "-1"
    case DateType => "-1"
    case dt: DecimalType => "null"
    case StringType => "null"
    case _ => "null"
  }

  /**
   * Return the boxed type in Java
   */
  def boxedType(dt: DataType): Term = dt match {
    case IntegerType => "Integer"
    case LongType => "Long"
    case ShortType => "Short"
    case ByteType => "Byte"
    case DoubleType => "Double"
    case FloatType => "Float"
    case BooleanType => "Boolean"
    case dt: DecimalType => decimalType
    case BinaryType => "byte[]"
    case StringType => stringType
    case DateType => "Integer"
    case TimestampType => "java.sql.Timestamp"
    case _ => "Object"
  }

  /**
   * Returns a function to generate equal expression in Java
   */
  def equalFunc(dataType: DataType): ((Term, Term) => Code) = dataType match {
    case BinaryType => { case (eval1, eval2) => s"java.util.Arrays.equals($eval1, $eval2)" }
    case dt if isNativeType(dt) => { case (eval1, eval2) => s"$eval1 == $eval2" }
    case other => { case (eval1, eval2) => s"$eval1.equals($eval2)" }
  }

  /**
   * List of data types that have special accessors and setters in [[Row]].
   */
  val nativeTypes =
    Seq(IntegerType, BooleanType, LongType, DoubleType, FloatType, ShortType, ByteType)

  /**
   * Returns true if the data type has a special accessor and setter in [[Row]].
   */
  def isNativeType(dt: DataType): Boolean = nativeTypes.contains(dt)
}

/**
 * A base class for generators of byte code to perform expression evaluation.  Includes a set of
 * helpers for referring to Catalyst types and building trees that perform evaluation of individual
 * expressions.
 */
abstract class CodeGenerator[InType <: AnyRef, OutType <: AnyRef] extends Logging {

  protected val exprType = classOf[Expression].getName
  protected val mutableRowType = classOf[MutableRow].getName
  protected val genericMutableRowType = classOf[GenericMutableRow].getName

  /**
   * Can be flipped on manually in the console to add (expensive) expression evaluation trace code.
   */
  var debugLogging = false

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
   *
   * It will track the time used to compile
   */
  protected def compile(code: String): Class[_] = {
    val startTime = System.nanoTime()
    val clazz = new ClassBodyEvaluator(code).getClazz()
    val endTime = System.nanoTime()
    def timeMs: Double = (endTime - startTime).toDouble / 1000000
    logDebug(s"Code (${code.size} bytes) compiled in $timeMs ms")
    clazz
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
  protected val cache = CacheBuilder.newBuilder()
    .maximumSize(1000)
    .build(
      new CacheLoader[InType, OutType]() {
        override def load(in: InType): OutType = {
          val startTime = System.nanoTime()
          val result = create(in)
          val endTime = System.nanoTime()
          def timeMs: Double = (endTime - startTime).toDouble / 1000000
          logInfo(s"Code generated expression $in in $timeMs ms")
          result
        }
      })

  /** Generates the requested evaluator binding the given expression(s) to the inputSchema. */
  def generate(expressions: InType, inputSchema: Seq[Attribute]): OutType =
    generate(bind(expressions, inputSchema))

  /** Generates the requested evaluator given already bound expression(s). */
  def generate(expressions: InType): OutType = cache.get(canonicalize(expressions))

  /**
   * Create a new codegen context for expression evaluator, used to store those
   * expressions that don't support codegen
   */
  def newCodeGenContext(): CodeGenContext = {
    new CodeGenContext
  }
}
