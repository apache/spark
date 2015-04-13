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
  import scala.reflect.runtime.{universe => ru}
  import scala.reflect.runtime.universe._

  protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer(_))

  protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    in.map(BindReferences.bindReference(_, inputSchema))

  // Make Mutablility optional...
  protected def create(expressions: Seq[Expression]): Projection = {
    val tupleLength = ru.Literal(Constant(expressions.length))
    val lengthDef = q"final val length = $tupleLength"

    /* TODO: Configurable...
    val nullFunctions =
      q"""
        private final val nullSet = new org.apache.spark.util.collection.BitSet(length)
        final def setNullAt(i: Int) = nullSet.set(i)
        final def isNullAt(i: Int) = nullSet.get(i)
      """
     */

    val nullFunctions =
      q"""
        private[this] var nullBits = new Array[Boolean](${expressions.size})
        override def setNullAt(i: Int) = { nullBits(i) = true }
        override def isNullAt(i: Int) = nullBits(i)
      """.children

    val tupleElements = expressions.zipWithIndex.flatMap {
      case (e, i) =>
        val elementName = newTermName(s"c$i")
        val evaluatedExpression = expressionEvaluator(e)
        val iLit = ru.Literal(Constant(i))

        q"""
        var ${newTermName(s"c$i")}: ${termForType(e.dataType)} = _
        {
          ..${evaluatedExpression.code}
          if(${evaluatedExpression.nullTerm})
            setNullAt($iLit)
          else {
            nullBits($iLit) = false
            $elementName = ${evaluatedExpression.primitiveTerm}
          }
        }
        """.children : Seq[Tree]
    }

    val accessorFailure = q"""scala.sys.error("Invalid ordinal:" + i)"""
    val applyFunction = {
      val cases = (0 until expressions.size).map { i =>
        val ordinal = ru.Literal(Constant(i))
        val elementName = newTermName(s"c$i")
        val iLit = ru.Literal(Constant(i))

        q"if(i == $ordinal) { if(isNullAt($i)) return null else return $elementName }"
      }
      q"override def apply(i: Int): Any = { ..$cases; $accessorFailure }"
    }

    val updateFunction = {
      val cases = expressions.zipWithIndex.map {case (e, i) =>
        val ordinal = ru.Literal(Constant(i))
        val elementName = newTermName(s"c$i")
        val iLit = ru.Literal(Constant(i))

        q"""
          if(i == $ordinal) {
            if(value == null) {
              setNullAt(i)
            } else {
              nullBits(i) = false
              $elementName = value.asInstanceOf[${termForType(e.dataType)}]
            }
            return
          }"""
      }
      q"override def update(i: Int, value: Any): Unit = { ..$cases; $accessorFailure }"
    }

    val specificAccessorFunctions = NativeType.all.map { dataType =>
      val ifStatements = expressions.zipWithIndex.flatMap {
        case (e, i) if e.dataType == dataType =>
          val elementName = newTermName(s"c$i")
          // TODO: The string of ifs gets pretty inefficient as the row grows in size.
          // TODO: Optional null checks?
          q"if(i == $i) return $elementName" :: Nil
        case _ => Nil
      }

      q"""
      override def ${accessorForType(dataType)}(i: Int):${termForType(dataType)} = {
        ..$ifStatements;
        $accessorFailure
      }"""
    }

    val specificMutatorFunctions = NativeType.all.map { dataType =>
      val ifStatements = expressions.zipWithIndex.flatMap {
        case (e, i) if e.dataType == dataType =>
          val elementName = newTermName(s"c$i")
          // TODO: The string of ifs gets pretty inefficient as the row grows in size.
          // TODO: Optional null checks?
          q"if(i == $i) { nullBits($i) = false; $elementName = value; return }" :: Nil
        case _ => Nil
      }

      q"""
      override def ${mutatorForType(dataType)}(i: Int, value: ${termForType(dataType)}): Unit = {
        ..$ifStatements;
        $accessorFailure
      }"""
    }

    val hashValues = expressions.zipWithIndex.map { case (e,i) =>
      val elementName = newTermName(s"c$i")
      val nonNull = e.dataType match {
        case BooleanType => q"if ($elementName) 0 else 1"
        case ByteType | ShortType | IntegerType => q"$elementName.toInt"
        case LongType => q"($elementName ^ ($elementName >>> 32)).toInt"
        case FloatType => q"java.lang.Float.floatToIntBits($elementName)"
        case DoubleType =>
          q"{ val b = java.lang.Double.doubleToLongBits($elementName); (b ^ (b >>>32)).toInt }"
        case _ => q"$elementName.hashCode"
      }
      q"if (isNullAt($i)) 0 else $nonNull"
    }

    val hashUpdates: Seq[Tree] = hashValues.map(v => q"""result = 37 * result + $v""": Tree)

    val hashCodeFunction =
      q"""
        override def hashCode(): Int = {
          var result: Int = 37
          ..$hashUpdates
          result
        }
      """

    val columnChecks = (0 until expressions.size).map { i =>
      val elementName = newTermName(s"c$i")
      q"if (this.$elementName != specificType.$elementName) return false"
    }

    val equalsFunction =
      q"""
        override def equals(other: Any): Boolean = other match {
          case specificType: SpecificRow =>
            ..$columnChecks
            return true
          case other => super.equals(other)
        }
      """

    val allColumns = (0 until expressions.size).map { i =>
      val iLit = ru.Literal(Constant(i))
      q"if(isNullAt($iLit)) { null } else { ${newTermName(s"c$i")} }"
    }

    val copyFunction =
      q"override def copy() = new $genericRowType(Array[Any](..$allColumns))"

    val toSeqFunction =
      q"override def toSeq: Seq[Any] = Seq(..$allColumns)"

    val classBody =
      nullFunctions ++ (
        lengthDef +:
        applyFunction +:
        updateFunction +:
        equalsFunction +:
        hashCodeFunction +:
        copyFunction +:
        toSeqFunction +:
        (tupleElements ++ specificAccessorFunctions ++ specificMutatorFunctions))

    val code = q"""
      final class SpecificRow(i: $rowType) extends $mutableRowType {
        ..$classBody
      }

      new $projectionType { def apply(r: $rowType) = new SpecificRow(r) }
    """

    log.debug(
      s"MutableRow, initExprs: ${expressions.mkString(",")} code:\n${toolBox.typeCheck(code)}")
    toolBox.eval(code).asInstanceOf[Projection]
  }
}
