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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

/**
 * Java can not access Projection (in package object)
 */
abstract class BaseProjection extends Projection {}

abstract class CodeGenMutableRow extends MutableRow with BaseGenericInternalRow

/**
 * Generates bytecode that produces a new [[InternalRow]] object based on a fixed set of input
 * [[Expression Expressions]] and a given input [[InternalRow]].  The returned [[InternalRow]]
 * object is custom generated based on the output types of the [[Expression]] to avoid boxing of
 * primitive values.
 */
object GenerateProjection extends CodeGenerator[Seq[Expression], Projection] {

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

    val specificAccessorFunctions = ctx.primitiveTypes.map { jt =>
      val cases = expressions.zipWithIndex.flatMap {
        case (e, i) if ctx.javaType(e.dataType) == jt =>
          Some(s"case $i: return c$i;")
        case _ => None
      }.mkString("\n        ")
      if (cases.length > 0) {
        val getter = "get" + ctx.primitiveTypeName(jt)
        s"""
      @Override
      public $jt $getter(int i) {
        if (isNullAt(i)) {
          return ${ctx.defaultValue(jt)};
        }
        switch (i) {
        $cases
        }
        throw new IllegalArgumentException("Invalid index: " + i
          + " in $getter");
      }"""
      } else {
        ""
      }
    }.filter(_.length > 0).mkString("\n")

    val specificMutatorFunctions = ctx.primitiveTypes.map { jt =>
      val cases = expressions.zipWithIndex.flatMap {
        case (e, i) if ctx.javaType(e.dataType) == jt =>
          Some(s"case $i: { c$i = value; return; }")
        case _ => None
      }.mkString("\n        ")
      if (cases.length > 0) {
        val setter = "set" + ctx.primitiveTypeName(jt)
        s"""
      @Override
      public void $setter(int i, $jt value) {
        nullBits[i] = false;
        switch (i) {
        $cases
        }
        throw new IllegalArgumentException("Invalid index: " + i +
          " in $setter}");
      }"""
      } else {
        ""
      }
    }.filter(_.length > 0).mkString("\n")

    val hashValues = expressions.zipWithIndex.map { case (e, i) =>
      val col = s"c$i"
      val nonNull = e.dataType match {
        case BooleanType => s"$col ? 0 : 1"
        case ByteType | ShortType | IntegerType | DateType => s"$col"
        case LongType | TimestampType => s"$col ^ ($col >>> 32)"
        case FloatType => s"Float.floatToIntBits($col)"
        case DoubleType =>
            s"(int)(Double.doubleToLongBits($col) ^ (Double.doubleToLongBits($col) >>> 32))"
        case BinaryType => s"java.util.Arrays.hashCode($col)"
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
        if (nullBits[$i] != row.nullBits[$i] ||
          (!nullBits[$i] && !(${ctx.genEqual(e.dataType, s"c$i", s"row.c$i")}))) {
          return false;
        }
      """
    }.mkString("\n")

    val copyColumns = expressions.zipWithIndex.map { case (e, i) =>
        s"""if (!nullBits[$i]) arr[$i] = c$i;"""
    }.mkString("\n      ")

    val code = s"""
    public SpecificProjection generate($exprType[] expr) {
      return new SpecificProjection(expr);
    }

    class SpecificProjection extends ${classOf[BaseProjection].getName} {
      private $exprType[] expressions;
      ${declareMutableStates(ctx)}
      ${declareAddedFunctions(ctx)}

      public SpecificProjection($exprType[] expr) {
        expressions = expr;
        ${initMutableStates(ctx)}
      }

      @Override
      public Object apply(Object r) {
        return new SpecificRow((InternalRow) r);
      }

      final class SpecificRow extends ${classOf[CodeGenMutableRow].getName} {

        $columns

        public SpecificRow(InternalRow i) {
          $initColumns
        }

        public int numFields() { return ${expressions.length};}
        protected boolean[] nullBits = new boolean[${expressions.length}];
        public void setNullAt(int i) { nullBits[i] = true; }
        public boolean isNullAt(int i) { return nullBits[i]; }

        @Override
        public Object genericGet(int i) {
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
          if (other instanceof SpecificRow) {
            SpecificRow row = (SpecificRow) other;
            $columnChecks
            return true;
          }
          return super.equals(other);
        }

        @Override
        public InternalRow copy() {
          Object[] arr = new Object[${expressions.length}];
          ${copyColumns}
          return new ${classOf[GenericInternalRow].getName}(arr);
        }
      }
    }
    """

    logDebug(s"MutableRow, initExprs: ${expressions.mkString(",")} code:\n" +
      CodeFormatter.format(code))

    compile(code).generate(ctx.references.toArray).asInstanceOf[Projection]
  }
}
