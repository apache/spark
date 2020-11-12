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

import java.lang.{Boolean => JBool}

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.types.{BooleanType, DataType}

/**
 * Trait representing an opaque fragments of java code.
 */
trait JavaCode {
  def code: String
  override def toString: String = code
}

/**
 * Utility functions for creating [[JavaCode]] fragments.
 */
object JavaCode {
  /**
   * Create a java literal.
   */
  def literal(v: String, dataType: DataType): LiteralValue = dataType match {
    case BooleanType if v == "true" => TrueLiteral
    case BooleanType if v == "false" => FalseLiteral
    case _ => new LiteralValue(v, CodeGenerator.javaClass(dataType))
  }

  /**
   * Create a default literal. This is null for reference types, false for boolean types and
   * -1 for other primitive types.
   */
  def defaultLiteral(dataType: DataType): LiteralValue = {
    new LiteralValue(
      CodeGenerator.defaultValue(dataType, typedNull = true),
      CodeGenerator.javaClass(dataType))
  }

  /**
   * Create a local java variable.
   */
  def variable(name: String, dataType: DataType): VariableValue = {
    variable(name, CodeGenerator.javaClass(dataType))
  }

  /**
   * Create a local java variable.
   */
  def variable(name: String, javaClass: Class[_]): VariableValue = {
    VariableValue(name, javaClass)
  }

  /**
   * Create a local isNull variable.
   */
  def isNullVariable(name: String): VariableValue = variable(name, BooleanType)

  /**
   * Create a global java variable.
   */
  def global(name: String, dataType: DataType): GlobalValue = {
    global(name, CodeGenerator.javaClass(dataType))
  }

  /**
   * Create a global java variable.
   */
  def global(name: String, javaClass: Class[_]): GlobalValue = {
    GlobalValue(name, javaClass)
  }

  /**
   * Create a global isNull variable.
   */
  def isNullGlobal(name: String): GlobalValue = global(name, BooleanType)

  /**
   * Create an expression fragment.
   */
  def expression(code: String, dataType: DataType): SimpleExprValue = {
    expression(code, CodeGenerator.javaClass(dataType))
  }

  /**
   * Create an expression fragment.
   */
  def expression(code: String, javaClass: Class[_]): SimpleExprValue = {
    SimpleExprValue(code, javaClass)
  }

  /**
   * Create a isNull expression fragment.
   */
  def isNullExpression(code: String): SimpleExprValue = {
    expression(code, BooleanType)
  }

  /**
   * Create an `Inline` for Java Class name.
   */
  def javaType(javaClass: Class[_]): Inline = Inline(javaClass.getName)

  /**
   * Create an `Inline` for Java Type name.
   */
  def javaType(dataType: DataType): Inline = Inline(CodeGenerator.javaType(dataType))

  /**
   * Create an `Inline` for boxed Java Type name.
   */
  def boxedType(dataType: DataType): Inline = Inline(CodeGenerator.boxedType(dataType))
}

/**
 * A trait representing a block of java code.
 */
trait Block extends TreeNode[Block] with JavaCode {
  import Block._

  // Returns java code string for this code block.
  override def toString: String = _marginChar match {
    case Some(c) => code.stripMargin(c).trim
    case _ => code.trim
  }

  // We could remove comments, extra whitespaces and newlines when calculating length as it is used
  // only for codegen method splitting, but SPARK-30564 showed that this is a performance critical
  // function so we decided not to do so.
  def length: Int = toString.length

  def isEmpty: Boolean = toString.isEmpty

  def nonEmpty: Boolean = !isEmpty

  // The leading prefix that should be stripped from each line.
  // By default we strip blanks or control characters followed by '|' from the line.
  var _marginChar: Option[Char] = Some('|')

  def stripMargin(c: Char): this.type = {
    _marginChar = Some(c)
    this
  }

  def stripMargin: this.type = {
    _marginChar = Some('|')
    this
  }

  /**
   * Apply a map function to each java expression codes present in this java code, and return a new
   * java code based on the mapped java expression codes.
   */
  def transformExprValues(f: PartialFunction[ExprValue, ExprValue]): this.type = {
    var changed = false

    @inline def transform(e: ExprValue): ExprValue = {
      val newE = f lift e
      if (!newE.isDefined || newE.get.equals(e)) {
        e
      } else {
        changed = true
        newE.get
      }
    }

    def doTransform(arg: Any): AnyRef = arg match {
      case e: ExprValue => transform(e)
      case Some(value) => Some(doTransform(value))
      case seq: Iterable[_] => seq.map(doTransform)
      case other: AnyRef => other
    }

    val newArgs = mapProductIterator(doTransform)
    if (changed) makeCopy(newArgs).asInstanceOf[this.type] else this
  }

  // Concatenates this block with other block.
  def + (other: Block): Block = other match {
    case EmptyBlock => this
    case _ => code"$this\n$other"
  }

  override def verboseString(maxFields: Int): String = toString
  override def simpleStringWithNodeId(): String = {
    throw new UnsupportedOperationException(s"$nodeName does not implement simpleStringWithNodeId")
  }
}

object Block {

  val CODE_BLOCK_BUFFER_LENGTH: Int = 512

  /**
   * A custom string interpolator which inlines a string into code block.
   */
  implicit class InlineHelper(val sc: StringContext) extends AnyVal {
    def inline(args: Any*): Inline = {
      val inlineString = sc.raw(args: _*)
      Inline(inlineString)
    }
  }

  implicit def blocksToBlock(blocks: Seq[Block]): Block = blocks.reduceLeft(_ + _)

  implicit class BlockHelper(val sc: StringContext) extends AnyVal {
    /**
     * A string interpolator that retains references to the `JavaCode` inputs, and behaves like
     * the Scala builtin StringContext.s() interpolator otherwise, i.e. it will treat escapes in
     * the code parts, and will not treat escapes in the input arguments.
     */
    def code(args: Any*): Block = {
      sc.checkLengths(args)
      if (sc.parts.length == 0) {
        EmptyBlock
      } else {
        args.foreach {
          case _: ExprValue | _: Inline | _: Block =>
          case _: Boolean | _: Int | _: Long | _: Float | _: Double | _: String =>
          case other => throw new IllegalArgumentException(
            s"Can not interpolate ${other.getClass.getName} into code block.")
        }

        val (codeParts, blockInputs) = foldLiteralArgs(sc.parts, args)
        CodeBlock(codeParts, blockInputs)
      }
    }
  }

  // Folds eagerly the literal args into the code parts.
  private def foldLiteralArgs(parts: Seq[String], args: Seq[Any]): (Seq[String], Seq[JavaCode]) = {
    val codeParts = ArrayBuffer.empty[String]
    val blockInputs = ArrayBuffer.empty[JavaCode]

    val strings = parts.iterator
    val inputs = args.iterator
    val buf = new StringBuilder(Block.CODE_BLOCK_BUFFER_LENGTH)

    buf.append(StringContext.treatEscapes(strings.next))
    while (strings.hasNext) {
      val input = inputs.next
      input match {
        case _: ExprValue | _: CodeBlock =>
          codeParts += buf.toString
          buf.clear
          blockInputs += input.asInstanceOf[JavaCode]
        case EmptyBlock =>
        case _ =>
          buf.append(input)
      }
      buf.append(StringContext.treatEscapes(strings.next))
    }
    codeParts += buf.toString

    (codeParts.toSeq, blockInputs.toSeq)
  }
}

/**
 * A block of java code. Including a sequence of code parts and some inputs to this block.
 * The actual java code is generated by embedding the inputs into the code parts. Here we keep
 * inputs of `JavaCode` instead of simply folding them as a string of code, because we need to
 * track expressions (`ExprValue`) in this code block. We need to be able to manipulate the
 * expressions later without changing the behavior of this code block in some applications, e.g.,
 * method splitting.
 */
case class CodeBlock(codeParts: Seq[String], blockInputs: Seq[JavaCode]) extends Block {
  override def children: Seq[Block] =
    blockInputs.filter(_.isInstanceOf[Block]).asInstanceOf[Seq[Block]]

  override lazy val code: String = {
    val strings = codeParts.iterator
    val inputs = blockInputs.iterator
    val buf = new StringBuilder(Block.CODE_BLOCK_BUFFER_LENGTH)
    buf.append(strings.next)
    while (strings.hasNext) {
      buf.append(inputs.next)
      buf.append(strings.next)
    }
    buf.toString
  }
}

case object EmptyBlock extends Block with Serializable {
  override val code: String = ""
  override def children: Seq[Block] = Seq.empty
}

/**
 * A piece of java code snippet inlines all types of input arguments into a string without
 * tracking any reference of `JavaCode` instances.
 */
case class Inline(codeString: String) extends JavaCode {
  override val code: String = codeString
}

/**
 * A typed java fragment that must be a valid java expression.
 */
trait ExprValue extends JavaCode {
  def javaType: Class[_]
  def isPrimitive: Boolean = javaType.isPrimitive
}

object ExprValue {
  implicit def exprValueToString(exprValue: ExprValue): String = exprValue.code
}

/**
 * A java expression fragment.
 */
case class SimpleExprValue(expr: String, javaType: Class[_]) extends ExprValue {
  override def code: String = s"($expr)"
}

/**
 * A local variable java expression.
 */
case class VariableValue(variableName: String, javaType: Class[_]) extends ExprValue {
  override def code: String = variableName
}

/**
 * A global variable java expression.
 */
case class GlobalValue(value: String, javaType: Class[_]) extends ExprValue {
  override def code: String = value
}

/**
 * A literal java expression.
 */
class LiteralValue(val value: String, val javaType: Class[_]) extends ExprValue with Serializable {
  override def code: String = value

  override def equals(arg: Any): Boolean = arg match {
    case l: LiteralValue => l.javaType == javaType && l.value == value
    case _ => false
  }

  override def hashCode(): Int = value.hashCode() * 31 + javaType.hashCode()
}

case object TrueLiteral extends LiteralValue("true", JBool.TYPE)
case object FalseLiteral extends LiteralValue("false", JBool.TYPE)
