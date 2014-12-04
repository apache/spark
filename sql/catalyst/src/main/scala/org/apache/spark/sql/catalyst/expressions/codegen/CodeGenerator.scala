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

import com.google.common.cache.{CacheLoader, CacheBuilder}
import org.apache.spark.sql.catalyst.types.decimal.Decimal

import scala.language.existentials

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types._

// These classes are here to avoid issues with serialization and integration with quasiquotes.
class IntegerHashSet extends org.apache.spark.util.collection.OpenHashSet[Int]
class LongHashSet extends org.apache.spark.util.collection.OpenHashSet[Long]

/**
 * A base class for generators of byte code to perform expression evaluation.  Includes a set of
 * helpers for referring to Catalyst types and building trees that perform evaluation of individual
 * expressions.
 */
abstract class CodeGenerator[InType <: AnyRef, OutType <: AnyRef] extends Logging {
  import scala.reflect.runtime.{universe => ru}
  import scala.reflect.runtime.universe._

  import scala.tools.reflect.ToolBox

  protected val toolBox = runtimeMirror(getClass.getClassLoader).mkToolBox()

  protected val rowType = typeOf[Row]
  protected val mutableRowType = typeOf[MutableRow]
  protected val genericRowType = typeOf[GenericRow]
  protected val genericMutableRowType = typeOf[GenericMutableRow]

  protected val projectionType = typeOf[Projection]
  protected val mutableProjectionType = typeOf[MutableProjection]

  private val curId = new java.util.concurrent.atomic.AtomicInteger()
  private val javaSeparator = "$"

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
        override def load(in: InType): OutType = globalLock.synchronized {
          val startTime = System.nanoTime()
          val result = create(in)
          val endTime = System.nanoTime()
          def timeMs = (endTime - startTime).toDouble / 1000000
          logInfo(s"Code generated expression $in in $timeMs ms")
          result
        }
      })

  /** Generates the requested evaluator binding the given expression(s) to the inputSchema. */
  def apply(expressions: InType, inputSchema: Seq[Attribute]): OutType =
    apply(bind(expressions, inputSchema))

  /** Generates the requested evaluator given already bound expression(s). */
  def apply(expressions: InType): OutType = cache.get(canonicalize(expressions))

  /**
   * Returns a term name that is unique within this instance of a `CodeGenerator`.
   *
   * (Since we aren't in a macro context we do not seem to have access to the built in `freshName`
   * function.)
   */
  protected def freshName(prefix: String): TermName = {
    newTermName(s"$prefix$javaSeparator${curId.getAndIncrement}")
  }

  /**
   * Scala ASTs for evaluating an [[Expression]] given a [[Row]] of input.
   *
   * @param code The sequence of statements required to evaluate the expression.
   * @param nullTerm A term that holds a boolean value representing whether the expression evaluated
   *                 to null.
   * @param primitiveTerm A term for a possible primitive value of the result of the evaluation. Not
   *                      valid if `nullTerm` is set to `false`.
   * @param objectTerm A possibly boxed version of the result of evaluating this expression.
   */
  protected case class EvaluatedExpression(
      code: Seq[Tree],
      nullTerm: TermName,
      primitiveTerm: TermName,
      objectTerm: TermName)

  /**
   * Given an expression tree returns an [[EvaluatedExpression]], which contains Scala trees that
   * can be used to determine the result of evaluating the expression on an input row.
   */
  def expressionEvaluator(e: Expression): EvaluatedExpression = {
    val primitiveTerm = freshName("primitiveTerm")
    val nullTerm = freshName("nullTerm")
    val objectTerm = freshName("objectTerm")

    implicit class Evaluate1(e: Expression) {
      def castOrNull(f: TermName => Tree, dataType: DataType): Seq[Tree] = {
        val eval = expressionEvaluator(e)
        eval.code ++
        q"""
          val $nullTerm = ${eval.nullTerm}
          val $primitiveTerm =
            if($nullTerm)
              ${defaultPrimitive(dataType)}
            else
              ${f(eval.primitiveTerm)}
        """.children
      }
    }

    implicit class Evaluate2(expressions: (Expression, Expression)) {

      /**
       * Short hand for generating binary evaluation code, which depends on two sub-evaluations of
       * the same type.  If either of the sub-expressions is null, the result of this computation
       * is assumed to be null.
       *
       * @param f a function from two primitive term names to a tree that evaluates them.
       */
      def evaluate(f: (TermName, TermName) => Tree): Seq[Tree] =
        evaluateAs(expressions._1.dataType)(f)

      def evaluateAs(resultType: DataType)(f: (TermName, TermName) => Tree): Seq[Tree] = {
        // TODO: Right now some timestamp tests fail if we enforce this...
        if (expressions._1.dataType != expressions._2.dataType) {
          log.warn(s"${expressions._1.dataType} != ${expressions._2.dataType}")
        }

        val eval1 = expressionEvaluator(expressions._1)
        val eval2 = expressionEvaluator(expressions._2)
        val resultCode = f(eval1.primitiveTerm, eval2.primitiveTerm)

        eval1.code ++ eval2.code ++
        q"""
          val $nullTerm = ${eval1.nullTerm} || ${eval2.nullTerm}
          val $primitiveTerm: ${termForType(resultType)} =
            if($nullTerm) {
              ${defaultPrimitive(resultType)}
            } else {
              $resultCode.asInstanceOf[${termForType(resultType)}]
            }
        """.children : Seq[Tree]
      }
    }

    val inputTuple = newTermName(s"i")

    // TODO: Skip generation of null handling code when expression are not nullable.
    val primitiveEvaluation: PartialFunction[Expression, Seq[Tree]] = {
      case b @ BoundReference(ordinal, dataType, nullable) =>
        val nullValue = q"$inputTuple.isNullAt($ordinal)"
        q"""
          val $nullTerm: Boolean = $nullValue
          val $primitiveTerm: ${termForType(dataType)} =
            if($nullTerm)
              ${defaultPrimitive(dataType)}
            else
              ${getColumn(inputTuple, dataType, ordinal)}
         """.children

      case expressions.Literal(null, dataType) =>
        q"""
          val $nullTerm = true
          val $primitiveTerm: ${termForType(dataType)} = null.asInstanceOf[${termForType(dataType)}]
         """.children

      case expressions.Literal(value: Boolean, dataType) =>
        q"""
          val $nullTerm = ${value == null}
          val $primitiveTerm: ${termForType(dataType)} = $value
         """.children

      case expressions.Literal(value: String, dataType) =>
        q"""
          val $nullTerm = ${value == null}
          val $primitiveTerm: ${termForType(dataType)} = $value
         """.children

      case expressions.Literal(value: Int, dataType) =>
        q"""
          val $nullTerm = ${value == null}
          val $primitiveTerm: ${termForType(dataType)} = $value
         """.children

      case expressions.Literal(value: Long, dataType) =>
        q"""
          val $nullTerm = ${value == null}
          val $primitiveTerm: ${termForType(dataType)} = $value
         """.children

      case Cast(e @ BinaryType(), StringType) =>
        val eval = expressionEvaluator(e)
        eval.code ++
        q"""
          val $nullTerm = ${eval.nullTerm}
          val $primitiveTerm =
            if($nullTerm)
              ${defaultPrimitive(StringType)}
            else
              new String(${eval.primitiveTerm}.asInstanceOf[Array[Byte]])
        """.children

      case Cast(child @ NumericType(), IntegerType) =>
        child.castOrNull(c => q"$c.toInt", IntegerType)

      case Cast(child @ NumericType(), LongType) =>
        child.castOrNull(c => q"$c.toLong", LongType)

      case Cast(child @ NumericType(), DoubleType) =>
        child.castOrNull(c => q"$c.toDouble", DoubleType)

      case Cast(child @ NumericType(), FloatType) =>
        child.castOrNull(c => q"$c.toFloat", IntegerType)

      // Special handling required for timestamps in hive test cases since the toString function
      // does not match the expected output.
      case Cast(e, StringType) if e.dataType != TimestampType =>
        val eval = expressionEvaluator(e)
        eval.code ++
        q"""
          val $nullTerm = ${eval.nullTerm}
          val $primitiveTerm =
            if($nullTerm)
              ${defaultPrimitive(StringType)}
            else
              ${eval.primitiveTerm}.toString
        """.children

      case EqualTo(e1, e2) =>
        (e1, e2).evaluateAs (BooleanType) { case (eval1, eval2) => q"$eval1 == $eval2" }

      /* TODO: Fix null semantics.
      case In(e1, list) if !list.exists(!_.isInstanceOf[expressions.Literal]) =>
        val eval = expressionEvaluator(e1)

        val checks = list.map {
          case expressions.Literal(v: String, dataType) =>
            q"if(${eval.primitiveTerm} == $v) return true"
          case expressions.Literal(v: Int, dataType) =>
            q"if(${eval.primitiveTerm} == $v) return true"
        }

        val funcName = newTermName(s"isIn${curId.getAndIncrement()}")

        q"""
            def $funcName: Boolean = {
              ..${eval.code}
              if(${eval.nullTerm}) return false
              ..$checks
              return false
            }
            val $nullTerm = false
            val $primitiveTerm = $funcName
        """.children
      */

      case GreaterThan(e1 @ NumericType(), e2 @ NumericType()) =>
        (e1, e2).evaluateAs (BooleanType) { case (eval1, eval2) => q"$eval1 > $eval2" }
      case GreaterThanOrEqual(e1 @ NumericType(), e2 @ NumericType()) =>
        (e1, e2).evaluateAs (BooleanType) { case (eval1, eval2) => q"$eval1 >= $eval2" }
      case LessThan(e1 @ NumericType(), e2 @ NumericType()) =>
        (e1, e2).evaluateAs (BooleanType) { case (eval1, eval2) => q"$eval1 < $eval2" }
      case LessThanOrEqual(e1 @ NumericType(), e2 @ NumericType()) =>
        (e1, e2).evaluateAs (BooleanType) { case (eval1, eval2) => q"$eval1 <= $eval2" }

      case And(e1, e2) =>
        val eval1 = expressionEvaluator(e1)
        val eval2 = expressionEvaluator(e2)

        q"""
          ..${eval1.code}
          var $nullTerm = false
          var $primitiveTerm: ${termForType(BooleanType)} = false

          if (!${eval1.nullTerm} && ${eval1.primitiveTerm} == false) {
          } else {
            ..${eval2.code}
            if (!${eval2.nullTerm} && ${eval2.primitiveTerm} == false) {
            } else if (!${eval1.nullTerm} && !${eval2.nullTerm}) {
              $primitiveTerm = true
            } else {
              $nullTerm = true
            }
          }
         """.children

      case Or(e1, e2) =>
        val eval1 = expressionEvaluator(e1)
        val eval2 = expressionEvaluator(e2)

        q"""
          ..${eval1.code}
          var $nullTerm = false
          var $primitiveTerm: ${termForType(BooleanType)} = false

          if (!${eval1.nullTerm} && ${eval1.primitiveTerm}) {
            $primitiveTerm = true
          } else {
            ..${eval2.code}
            if (!${eval2.nullTerm} && ${eval2.primitiveTerm}) {
              $primitiveTerm = true
            } else if (!${eval1.nullTerm} && !${eval2.nullTerm}) {
              $primitiveTerm = false
            } else {
              $nullTerm = true
            }
          }
         """.children

      case Not(child) =>
        // Uh, bad function name...
        child.castOrNull(c => q"!$c", BooleanType)

      case Add(e1, e2) =>      (e1, e2) evaluate { case (eval1, eval2) => q"$eval1 + $eval2" }
      case Subtract(e1, e2) => (e1, e2) evaluate { case (eval1, eval2) => q"$eval1 - $eval2" }
      case Multiply(e1, e2) => (e1, e2) evaluate { case (eval1, eval2) => q"$eval1 * $eval2" }
      case Divide(e1, e2) =>
        val eval1 = expressionEvaluator(e1)
        val eval2 = expressionEvaluator(e2)

        eval1.code ++ eval2.code ++
        q"""
          var $nullTerm = false
          var $primitiveTerm: ${termForType(e1.dataType)} = 0

          if (${eval1.nullTerm} || ${eval2.nullTerm} ) {
            $nullTerm = true
          } else if (${eval2.primitiveTerm} == 0)
            $nullTerm = true
          else {
            $primitiveTerm = ${eval1.primitiveTerm} / ${eval2.primitiveTerm}
          }
         """.children

      case IsNotNull(e) =>
        val eval = expressionEvaluator(e)
        q"""
          ..${eval.code}
          var $nullTerm = false
          var $primitiveTerm: ${termForType(BooleanType)} = !${eval.nullTerm}
        """.children

      case IsNull(e) =>
        val eval = expressionEvaluator(e)
        q"""
          ..${eval.code}
          var $nullTerm = false
          var $primitiveTerm: ${termForType(BooleanType)} = ${eval.nullTerm}
        """.children

      case c @ Coalesce(children) =>
        q"""
          var $nullTerm = true
          var $primitiveTerm: ${termForType(c.dataType)} = ${defaultPrimitive(c.dataType)}
        """.children ++
        children.map { c =>
          val eval = expressionEvaluator(c)
          q"""
            if($nullTerm) {
              ..${eval.code}
              if(!${eval.nullTerm}) {
                $nullTerm = false
                $primitiveTerm = ${eval.primitiveTerm}
              }
            }
          """
        }

      case i @ expressions.If(condition, trueValue, falseValue) =>
        val condEval = expressionEvaluator(condition)
        val trueEval = expressionEvaluator(trueValue)
        val falseEval = expressionEvaluator(falseValue)

        q"""
          var $nullTerm = false
          var $primitiveTerm: ${termForType(i.dataType)} = ${defaultPrimitive(i.dataType)}
          ..${condEval.code}
          if(!${condEval.nullTerm} && ${condEval.primitiveTerm}) {
            ..${trueEval.code}
            $nullTerm = ${trueEval.nullTerm}
            $primitiveTerm = ${trueEval.primitiveTerm}
          } else {
            ..${falseEval.code}
            $nullTerm = ${falseEval.nullTerm}
            $primitiveTerm = ${falseEval.primitiveTerm}
          }
        """.children

      case NewSet(elementType) =>
        q"""
          val $nullTerm = false
          val $primitiveTerm = new ${hashSetForType(elementType)}()
        """.children

      case AddItemToSet(item, set) =>
        val itemEval = expressionEvaluator(item)
        val setEval = expressionEvaluator(set)

        val ArrayType(elementType, _) = set.dataType

        itemEval.code ++ setEval.code ++
        q"""
           if (!${itemEval.nullTerm}) {
             ${setEval.primitiveTerm}
               .asInstanceOf[${hashSetForType(elementType)}]
               .add(${itemEval.primitiveTerm})
           }

           val $nullTerm = false
           val $primitiveTerm = ${setEval.primitiveTerm}
         """.children

      case CombineSets(left, right) =>
        val leftEval = expressionEvaluator(left)
        val rightEval = expressionEvaluator(right)

        val ArrayType(elementType, _) = left.dataType

        leftEval.code ++ rightEval.code ++
        q"""
          val $nullTerm = false
          var $primitiveTerm: ${hashSetForType(elementType)} = null

          {
            val leftSet = ${leftEval.primitiveTerm}.asInstanceOf[${hashSetForType(elementType)}]
            val rightSet = ${rightEval.primitiveTerm}.asInstanceOf[${hashSetForType(elementType)}]
            val iterator = rightSet.iterator
            while (iterator.hasNext) {
              leftSet.add(iterator.next())
            }
            $primitiveTerm = leftSet
          }
        """.children

      case MaxOf(e1, e2) =>
        val eval1 = expressionEvaluator(e1)
        val eval2 = expressionEvaluator(e2)

        eval1.code ++ eval2.code ++
        q"""
          var $nullTerm = false
          var $primitiveTerm: ${termForType(e1.dataType)} = ${defaultPrimitive(e1.dataType)}

          if (${eval1.nullTerm}) {
            $nullTerm = ${eval2.nullTerm}
            $primitiveTerm = ${eval2.primitiveTerm}
          } else if (${eval2.nullTerm}) {
            $nullTerm = ${eval1.nullTerm}
            $primitiveTerm = ${eval1.primitiveTerm}
          } else {
            if (${eval1.primitiveTerm} > ${eval2.primitiveTerm}) {
              $primitiveTerm = ${eval1.primitiveTerm}
            } else {
              $primitiveTerm = ${eval2.primitiveTerm}
            }
          }
        """.children

      case UnscaledValue(child) =>
        val childEval = expressionEvaluator(child)

        childEval.code ++
        q"""
         var $nullTerm = ${childEval.nullTerm}
         var $primitiveTerm: Long = if (!$nullTerm) {
           ${childEval.primitiveTerm}.toUnscaledLong
         } else {
           ${defaultPrimitive(LongType)}
         }
         """.children

      case MakeDecimal(child, precision, scale) =>
        val childEval = expressionEvaluator(child)

        childEval.code ++
        q"""
         var $nullTerm = ${childEval.nullTerm}
         var $primitiveTerm: org.apache.spark.sql.catalyst.types.decimal.Decimal =
           ${defaultPrimitive(DecimalType())}

         if (!$nullTerm) {
           $primitiveTerm = new org.apache.spark.sql.catalyst.types.decimal.Decimal()
           $primitiveTerm = $primitiveTerm.setOrNull(${childEval.primitiveTerm}, $precision, $scale)
           $nullTerm = $primitiveTerm == null
         }
         """.children
    }

    // If there was no match in the partial function above, we fall back on calling the interpreted
    // expression evaluator.
    val code: Seq[Tree] =
      primitiveEvaluation.lift.apply(e).getOrElse {
        log.debug(s"No rules to generate $e")
        val tree = reify { e }
        q"""
          val $objectTerm = $tree.eval(i)
          val $nullTerm = $objectTerm == null
          val $primitiveTerm = $objectTerm.asInstanceOf[${termForType(e.dataType)}]
         """.children
      }

    // Only inject debugging code if debugging is turned on.
    val debugCode =
      if (debugLogging) {
        val localLogger = log
        val localLoggerTree = reify { localLogger }
        q"""
          $localLoggerTree.debug(${e.toString} + ": " +  (if($nullTerm) "null" else $primitiveTerm))
        """ :: Nil
      } else {
        Nil
      }

    EvaluatedExpression(code ++ debugCode, nullTerm, primitiveTerm, objectTerm)
  }

  protected def getColumn(inputRow: TermName, dataType: DataType, ordinal: Int) = {
    dataType match {
      case dt @ NativeType() => q"$inputRow.${accessorForType(dt)}($ordinal)"
      case _ => q"$inputRow.apply($ordinal).asInstanceOf[${termForType(dataType)}]"
    }
  }

  protected def setColumn(
      destinationRow: TermName,
      dataType: DataType,
      ordinal: Int,
      value: TermName) = {
    dataType match {
      case dt @ NativeType() => q"$destinationRow.${mutatorForType(dt)}($ordinal, $value)"
      case _ => q"$destinationRow.update($ordinal, $value)"
    }
  }

  protected def accessorForType(dt: DataType) = newTermName(s"get${primitiveForType(dt)}")
  protected def mutatorForType(dt: DataType) = newTermName(s"set${primitiveForType(dt)}")

  protected def hashSetForType(dt: DataType) = dt match {
    case IntegerType => typeOf[IntegerHashSet]
    case LongType => typeOf[LongHashSet]
    case unsupportedType =>
      sys.error(s"Code generation not support for hashset of type $unsupportedType")
  }

  protected def primitiveForType(dt: DataType) = dt match {
    case IntegerType => "Int"
    case LongType => "Long"
    case ShortType => "Short"
    case ByteType => "Byte"
    case DoubleType => "Double"
    case FloatType => "Float"
    case BooleanType => "Boolean"
    case StringType => "String"
  }

  protected def defaultPrimitive(dt: DataType) = dt match {
    case BooleanType => ru.Literal(Constant(false))
    case FloatType => ru.Literal(Constant(-1.0.toFloat))
    case StringType => ru.Literal(Constant("<uninit>"))
    case ShortType => ru.Literal(Constant(-1.toShort))
    case LongType => ru.Literal(Constant(1L))
    case ByteType => ru.Literal(Constant(-1.toByte))
    case DoubleType => ru.Literal(Constant(-1.toDouble))
    case DecimalType() => q"org.apache.spark.sql.catalyst.types.decimal.Decimal(-1)"
    case IntegerType => ru.Literal(Constant(-1))
    case _ => ru.Literal(Constant(null))
  }

  protected def termForType(dt: DataType) = dt match {
    case n: NativeType => n.tag
    case _ => typeTag[Any]
  }
}
