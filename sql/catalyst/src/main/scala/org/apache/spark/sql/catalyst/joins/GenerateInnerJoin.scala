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

package org.apache.spark.sql.catalyst.joins

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.expressions._

case class IntJoin(
    streamedKey: Expression,
    outputSchema: Seq[Attribute],
    table: UniqueIntKeyHashedRelation)

case class JoinSequence(
    inputSchema: Seq[Attribute],
    outputProjection: Seq[Attribute],
    joins: Seq[IntJoin]) {
  override def equals(other: Any) = other match {
    case JoinSequence(oi, op, oj) =>
      inputSchema.map(_.name) == oi.map(_.name) &&
      outputProjection.map(_.name) == op.map(_.name)
    case _ => false
  }

  override def hashCode(): Int =
    inputSchema.map(_.name).hashCode() *  37 +
    outputProjection.map(_.name).hashCode()

  override def toString =
    s"JoinSequence ${inputSchema.map(_.name).mkString("[", ",", "]")} ${outputProjection.map(_.name).mkString("[", ",", "]")}"
}

object GenerateInnerJoin extends CodeGenerator[JoinSequence, Iterator[Row] => Iterator[Row]] {
  import scala.reflect.runtime.{universe => ru}
  import scala.reflect.runtime.universe._


  /**
   * Canonicalizes an input expression. Used to avoid double caching expressions that differ only
   * cosmetically.
   */
  override protected def canonicalize(in: JoinSequence): JoinSequence = in
  //  in.copy(
  //    inputSchema = in.inputSchema.map(f => UnresolvedAttribute(f.name)),
  //    outputProjection = in.outputProjection.map(f => UnresolvedAttribute(f.name)))

  /** Binds an input expression to a given input schema */
  override protected def bind(in: JoinSequence, inputSchema: Seq[Attribute]): JoinSequence = in

  /**
   * Generates a class for a given input expression.  Called when there is not cached code
   * already available.
   */
  override protected def create(in: JoinSequence): (Iterator[Row]) => Iterator[Row] = {
    val lookups = in.joins.zipWithIndex.flatMap { case (join, idx) =>
      val lookup = BindReferences.bindReference(join.streamedKey, in.inputSchema)
      val table = reify { join.table }

      val matchedRow = newTermName(s"matchedRow$idx")

      val lookupEval = expressionEvaluator(lookup)
      lookupEval.code ++
      q"""
        val $matchedRow = $table.get(${lookupEval.primitiveTerm})
        if ($matchedRow eq null) return false
       """.children ++
      copyAvailableAttributes(matchedRow, join.outputSchema, in.outputProjection)
    }

    val streamCopy = copyAvailableAttributes("i", in.inputSchema, in.outputProjection)
    val outputSchema = reify { in.outputProjection.map(_.dataType) }

    val code =
    q"""
      (iter: Iterator[$rowType]) => new Iterator[$rowType] {
        private[this] val inputIter = iter
        private[this] val outputRow = new $specificMutableRowType($outputSchema)
        private[this] var isReady = false

        override def hasNext = isReady || findNext()

        def next() = {
          assert(hasNext)
          isReady = false
          outputRow
        }

        private def findNext(): Boolean = {
          while(inputIter.hasNext && !searchInput()) { }
          isReady
        }

        private def searchInput(): Boolean = {
          val i = inputIter.next()
          ..$lookups

          ..$streamCopy
          // If we made it this far the row if fully populated.
          isReady = true
          return true
        }
      }
    """

    //println(s"code for $in:\n$code")
    val startTime = System.currentTimeMillis
    val result = toolBox.eval(code).asInstanceOf[(Iterator[Row]) => Iterator[Row]]
    println(s"Generated in ${System.currentTimeMillis - startTime}ms $in")
    result
  }

  protected def copyAvailableAttributes(
      source: TermName,
      inputSchema: Seq[Attribute],
      outputSchema: Seq[Attribute]) = {
    val inputSet = AttributeSet(inputSchema)
    val inputOrdinals = AttributeMap(inputSchema.zipWithIndex)
    val attrsToCopy = outputSchema.zipWithIndex.filter { case (a, _) => inputSet.contains(a) }

    attrsToCopy.map { case (a, i) =>
      val getValue = q"$source.${accessorForType(a.dataType)}(${inputOrdinals(a)})"
      val copyCommand = q"outputRow.${mutatorForType(a.dataType)}($i, $getValue)"

      if (a.nullable) {
        q"""
          if ($source.isNullAt(${inputOrdinals(a)})) {
            outputRow.setNullAt($i)
          } else {
            $copyCommand
          }
        """
      } else {
        copyCommand
      }
    }
  }

}