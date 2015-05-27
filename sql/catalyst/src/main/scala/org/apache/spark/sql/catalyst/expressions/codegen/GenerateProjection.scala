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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._


/**
 * Generates bytecode that produces a new [[Row]] object based on a fixed set of input
 * [[Expression Expressions]] and a given input [[Row]].  The returned [[Row]] object is custom
 * generated based on the output types of the [[Expression]] to avoid boxing of primitive values.
 */
object GenerateProjection extends CodeGenerator[Seq[Expression], Projection] {
  import scala.reflect.runtime.universe._
  import scala.reflect.runtime.{universe => ru}

  protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer.execute)

  protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    in.map(BindReferences.bindReference(_, inputSchema))

  // Make Mutablility optional...
  protected def create(expressions: Seq[Expression]): Projection = {
    val tupleLength = ru.Literal(Constant(expressions.length))
    val lengthDef = s"final val length = $tupleLength\n"

    /* TODO: Configurable...
    val nullFunctions =
      s"""
        private final val nullSet = new org.apache.spark.util.collection.BitSet(length)
        final def setNullAt(i: Int) = nullSet.set(i)
        final def isNullAt(i: Int) = nullSet.get(i)
      """
     */

    val nullFunctions =
      s"""
        private[this] var nullBits = new Array[Boolean](${expressions.size})
        override def setNullAt(i: Int) = { nullBits(i) = true }
        override def isNullAt(i: Int) = nullBits(i)
      """
    val ctx = newCodeGenContext()
    val tupleElements = expressions.zipWithIndex.map {
      case (e, i) =>
        val elementName = newTermName(s"c$i")
        val evaluatedExpression = expressionEvaluator(e, ctx)
        val iLit = ru.Literal(Constant(i))

        s"""
        var ${newTermName(s"c$i")}: ${termForType(e.dataType)} = _
        {
          ${evaluatedExpression.code}
          if(${evaluatedExpression.nullTerm})
            setNullAt($iLit)
          else {
            nullBits($iLit) = false
            $elementName = ${evaluatedExpression.primitiveTerm}
          }
        }
        """ : String
    }.mkString("\n")

    val accessorFailure = s"""scala.sys.error("Invalid ordinal:" + i)\n"""
    val applyFunction = {
      val cases = (0 until expressions.size).map { i =>
        val ordinal = ru.Literal(Constant(i))
        val elementName = newTermName(s"c$i")
        val iLit = ru.Literal(Constant(i))

        s"if(i == $ordinal) { if(isNullAt($i)) return null else return $elementName }"
      }.mkString("\n")
      s"""
      override def apply(i: Int): Any = {
        $cases
        $accessorFailure
      }
      """
    }

    val updateFunction = {
      val cases = expressions.zipWithIndex.map {case (e, i) =>
        val ordinal = ru.Literal(Constant(i))
        val elementName = newTermName(s"c$i")
        val iLit = ru.Literal(Constant(i))

        s"""
          if(i == $ordinal) {
            if(value == null) {
              setNullAt(i)
            } else {
              nullBits(i) = false
              $elementName = value.asInstanceOf[${termForType(e.dataType)}]
            }
            return
          }"""
      }.mkString("\n")
      s"""override def update(i: Int, value: Any): Unit = {
         $cases
         $accessorFailure
      }"""
    }

    val specificAccessorFunctions = nativeTypes.map { dataType =>
      val ifStatements = expressions.zipWithIndex.map {
        // getString() is not used by expressions
        case (e, i) if e.dataType == dataType && dataType != StringType =>
          val elementName = newTermName(s"c$i")
          // TODO: The string of ifs gets pretty inefficient as the row grows in size.
          // TODO: Optional null checks?
          s"if(i == $i) return $elementName"
        case _ => ""
      }.mkString("\n")
      dataType match {
        // Row() need this interface to compile
        case StringType =>
          s"""
          override def getString(i: Int): String = {
            $accessorFailure
          }"""
        case other =>
          s"""
          override def ${accessorForType(dataType)}(i: Int): ${termForType(dataType)} = {
            $ifStatements
            $accessorFailure
          }"""
      }
    }.mkString("\n")

    val specificMutatorFunctions = nativeTypes.map { dataType =>
      val ifStatements = expressions.zipWithIndex.map {
        // setString() is not used by expressions
        case (e, i) if e.dataType == dataType && dataType != StringType =>
          val elementName = newTermName(s"c$i")
          // TODO: The string of ifs gets pretty inefficient as the row grows in size.
          // TODO: Optional null checks?
          s"if(i == $i) { nullBits($i) = false; $elementName = value; return }"
        case _ => ""
      }.mkString("\n")
      dataType match {
        case StringType =>
          // MutableRow() need this interface to compile
          s"""
          override def setString(i: Int, value: String) {
            $accessorFailure
          }"""
        case other =>
          s"""
          override def ${mutatorForType(dataType)}(i: Int, value: ${termForType(dataType)}) {
            $ifStatements
            $accessorFailure
          }"""
      }
    }.mkString("\n")

    val hashValues = expressions.zipWithIndex.map { case (e,i) =>
      val elementName = newTermName(s"c$i")
      val nonNull = e.dataType match {
        case BooleanType => s"if ($elementName) 0 else 1"
        case ByteType | ShortType | IntegerType => s"$elementName.toInt"
        case LongType => s"($elementName ^ ($elementName >>> 32)).toInt"
        case FloatType => s"java.lang.Float.floatToIntBits($elementName)"
        case DoubleType =>
          s"{ val b = java.lang.Double.doubleToLongBits($elementName); (b ^ (b >>>32)).toInt }"
        case _ => s"$elementName.hashCode"
      }
      s"if (isNullAt($i)) 0 else $nonNull"
    }

    val hashUpdates: String = hashValues.map(v =>
      s"""
      result = 37 * result + ($v)
      """
    ).mkString("\n")

    val columnChecks = (0 until expressions.size).map { i =>
      val elementName = newTermName(s"c$i")
      s"if (this.$elementName != specificType.$elementName) return false"
    }.mkString("\n")

    val allColumns = (0 until expressions.size).map { i =>
      val iLit = ru.Literal(Constant(i))
      s"if(isNullAt($iLit)) null else ${newTermName(s"c$i")}"
    }.mkString(", ")

    val code = s"""
    (expressions: Seq[$exprType]) => {
      final class SpecificRow(i: $rowType) extends $mutableRowType {

        $lengthDef

        $nullFunctions

        $applyFunction
        $specificAccessorFunctions

        $updateFunction
        $specificMutatorFunctions

        $tupleElements

        override def hashCode(): Int = {
          var result: Int = 37
          $hashUpdates
          result
        }

        override def equals(other: Any): Boolean = other match {
          case specificType: SpecificRow =>
            $columnChecks
            return true
          case other => super.equals(other)
        }

        override def copy() = new $genericRowType(Array[Any]($allColumns))

        override def toSeq: Seq[Any] = Seq($allColumns)
      }

      new $projectionType { def apply(r: $rowType) = new SpecificRow(r) }
    }
    """

    logWarning(
      s"MutableRow, initExprs: ${expressions.mkString(",")} code:\n${code}")
    toolBox.eval(toolBox.parse(code)).asInstanceOf[(Seq[Expression]) => Projection](ctx.borrowed)
  }
}
