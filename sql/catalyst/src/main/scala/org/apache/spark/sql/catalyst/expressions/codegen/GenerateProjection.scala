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

import org.apache.spark.sql.BaseMutableRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

/**
 * Java can not access Projection (in package object)
 */
abstract class BaseProject extends Projection {}

/**
 * Generates bytecode that produces a new [[Row]] object based on a fixed set of input
 * [[Expression Expressions]] and a given input [[Row]].  The returned [[Row]] object is custom
 * generated based on the output types of the [[Expression]] to avoid boxing of primitive values.
 */
object GenerateProjection extends CodeGenerator[Seq[Expression], Projection] {
  import scala.reflect.runtime.universe._

  protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer.execute)

  protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    in.map(BindReferences.bindReference(_, inputSchema))

  // Make Mutablility optional...
  protected def create(expressions: Seq[Expression]): Projection = {
    val ctx = newCodeGenContext()
    val columns = expressions.zipWithIndex.map {
      case (e, i) =>
        s"private ${ctx.javaType(e.dataType)} c$i = ${ctx.defaultValue(e.dataType)};\n"
    }.mkString("\n      ")

    val initColumns = expressions.zipWithIndex.map {
      case (e, i) =>
        val eval = e.gen(ctx)
        s"""
        {
          // column$i
          ${eval.code}
          nullBits[$i] = ${eval.isNull};
          if (!${eval.isNull}) {
            c$i = ${eval.primitive};
          }
        }
        """
    }.mkString("\n")

    val getCases = (0 until expressions.size).map { i =>
      s"case $i: return c$i;"
    }.mkString("\n        ")

    val updateCases = expressions.zipWithIndex.map { case (e, i) =>
      s"case $i: { c$i = (${ctx.boxedType(e.dataType)})value; return;}"
    }.mkString("\n        ")

    val specificAccessorFunctions = ctx.nativeTypes.map { dataType =>
      val cases = expressions.zipWithIndex.map {
        case (e, i) if e.dataType == dataType
          || dataType == IntegerType && e.dataType == DateType
          || dataType == LongType && e.dataType == TimestampType =>
          s"case $i: return c$i;"
        case _ => ""
      }.mkString("\n        ")
      if (cases.count(_ != '\n') > 0) {
        s"""
      @Override
      public ${ctx.javaType(dataType)} ${ctx.accessorForType(dataType)}(int i) {
        if (isNullAt(i)) {
          return ${ctx.defaultValue(dataType)};
        }
        switch (i) {
        $cases
        }
        return ${ctx.defaultValue(dataType)};
      }"""
      } else {
        ""
      }
    }.mkString("\n")

    val specificMutatorFunctions = ctx.nativeTypes.map { dataType =>
      val cases = expressions.zipWithIndex.map {
        case (e, i) if e.dataType == dataType
          || dataType == IntegerType && e.dataType == DateType
          || dataType == LongType && e.dataType == TimestampType =>
          s"case $i: { c$i = value; return; }"
        case _ => ""
      }.mkString("\n")
      if (cases.count(_ != '\n') > 0) {
        s"""
      @Override
      public void ${ctx.mutatorForType(dataType)}(int i, ${ctx.javaType(dataType)} value) {
        nullBits[i] = false;
        switch (i) {
        $cases
        }
      }"""
      } else {
        ""
      }
    }.mkString("\n")

    val hashValues = expressions.zipWithIndex.map { case (e, i) =>
      val col = newTermName(s"c$i")
      val nonNull = e.dataType match {
        case BooleanType => s"$col ? 0 : 1"
        case ByteType | ShortType | IntegerType | DateType => s"$col"
        case LongType | TimestampType => s"$col ^ ($col >>> 32)"
        case FloatType => s"Float.floatToIntBits($col)"
        case DoubleType =>
            s"(int)(Double.doubleToLongBits($col) ^ (Double.doubleToLongBits($col) >>> 32))"
        case _ => s"$col.hashCode()"
      }
      s"isNullAt($i) ? 0 : ($nonNull)"
    }

    val hashUpdates: String = hashValues.map( v =>
      s"""
        result *= 37; result += $v;"""
    ).mkString("\n")

    val columnChecks = expressions.zipWithIndex.map { case (e, i) =>
      s"""
          if (isNullAt($i) != row.isNullAt($i) || !isNullAt($i) && !get($i).equals(row.get($i))) {
            return false;
          }
      """
    }.mkString("\n")

    val code = s"""
    import org.apache.spark.sql.Row;

    public SpecificProjection generate($exprType[] expr) {
      return new SpecificProjection(expr);
    }

    class SpecificProjection extends ${typeOf[BaseProject]} {
      private $exprType[] expressions = null;

      public SpecificProjection($exprType[] expr) {
        expressions = expr;
      }

      @Override
      public Object apply(Object r) {
        return new SpecificRow(expressions, (Row) r);
      }
    }

    final class SpecificRow extends ${typeOf[BaseMutableRow]} {

      $columns

      public SpecificRow($exprType[] expressions, Row i) {
        $initColumns
      }

      public int size() { return ${expressions.length};}
      private boolean[] nullBits = new boolean[${expressions.length}];
      public void setNullAt(int i) { nullBits[i] = true; }
      public boolean isNullAt(int i) { return nullBits[i]; }

      public Object get(int i) {
        if (isNullAt(i)) return null;
        switch (i) {
        $getCases
        }
        return null;
      }
      public void update(int i, Object value) {
        if (value == null) {
          setNullAt(i);
          return;
        }
        nullBits[i] = false;
        switch (i) {
        $updateCases
        }
      }
      $specificAccessorFunctions
      $specificMutatorFunctions

      @Override
      public int hashCode() {
        int result = 37;
        $hashUpdates
        return result;
      }

      @Override
      public boolean equals(Object other) {
        if (other instanceof Row) {
          Row row = (Row) other;
          if (row.length() != size()) return false;
          $columnChecks
          return true;
        }
        return super.equals(other);
      }
    }
    """

    logDebug(s"MutableRow, initExprs: ${expressions.mkString(",")} code:\n${code}")

    val c = compile(code)
    // fetch the only one method `generate(Expression[])`
    val m = c.getDeclaredMethods()(0)
    m.invoke(c.newInstance(), ctx.references.toArray).asInstanceOf[Projection]
  }
}
