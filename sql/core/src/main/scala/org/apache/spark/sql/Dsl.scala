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

package org.apache.spark.sql

import java.util.{List => JList}

import scala.language.implicitConversions
import scala.reflect.runtime.universe.{TypeTag, typeTag}
import scala.collection.JavaConversions._

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._


/**
 * Domain specific functions available for [[DataFrame]].
 */
object Dsl {

  /** An implicit conversion that turns a Scala `Symbol` into a [[Column]]. */
  implicit def symbolToColumn(s: Symbol): ColumnName = new ColumnName(s.name)

  //  /**
  //   * An implicit conversion that turns a RDD of product into a [[DataFrame]].
  //   *
  //   * This method requires an implicit SQLContext in scope. For example:
  //   * {{{
  //   *   implicit val sqlContext: SQLContext = ...
  //   *   val rdd: RDD[(Int, String)] = ...
  //   *   rdd.toDataFrame  // triggers the implicit here
  //   * }}}
  //   */
  //  implicit def rddToDataFrame[A <: Product: TypeTag](rdd: RDD[A])(implicit context: SQLContext)
  //    : DataFrame = {
  //    context.createDataFrame(rdd)
  //  }

  /** Converts $"col name" into an [[Column]]. */
  implicit class StringToColumn(val sc: StringContext) extends AnyVal {
    def $(args: Any*): ColumnName = {
      new ColumnName(sc.s(args :_*))
    }
  }

  private[this] implicit def toColumn(expr: Expression): Column = Column(expr)

  /**
   * Returns a [[Column]] based on the given column name.
   */
  def col(colName: String): Column = Column(colName)

  /**
   * Returns a [[Column]] based on the given column name. Alias of [[col]].
   */
  def column(colName: String): Column = Column(colName)

  /**
   * Creates a [[Column]] of literal value.
   */
  def lit(literal: Any): Column = {
    if (literal.isInstanceOf[Symbol]) {
      return new ColumnName(literal.asInstanceOf[Symbol].name)
    }

    val literalExpr = literal match {
      case v: Boolean => Literal(v, BooleanType)
      case v: Byte => Literal(v, ByteType)
      case v: Short => Literal(v, ShortType)
      case v: Int => Literal(v, IntegerType)
      case v: Long => Literal(v, LongType)
      case v: Float => Literal(v, FloatType)
      case v: Double => Literal(v, DoubleType)
      case v: String => Literal(v, StringType)
      case v: BigDecimal => Literal(Decimal(v), DecimalType.Unlimited)
      case v: java.math.BigDecimal => Literal(Decimal(v), DecimalType.Unlimited)
      case v: Decimal => Literal(v, DecimalType.Unlimited)
      case v: java.sql.Timestamp => Literal(v, TimestampType)
      case v: java.sql.Date => Literal(v, DateType)
      case v: Array[Byte] => Literal(v, BinaryType)
      case null => Literal(null, NullType)
      case _ =>
        throw new RuntimeException("Unsupported literal type " + literal.getClass + " " + literal)
    }
    Column(literalExpr)
  }

  def sum(e: Column): Column = Sum(e.expr)
  def sumDistinct(e: Column): Column = SumDistinct(e.expr)
  def count(e: Column): Column = Count(e.expr)

  @scala.annotation.varargs
  def countDistinct(expr: Column, exprs: Column*): Column =
    CountDistinct((expr +: exprs).map(_.expr))

  def approxCountDistinct(e: Column): Column = ApproxCountDistinct(e.expr)
  def approxCountDistinct(e: Column, rsd: Double): Column =
    ApproxCountDistinct(e.expr, rsd)

  def avg(e: Column): Column = Average(e.expr)
  def first(e: Column): Column = First(e.expr)
  def last(e: Column): Column = Last(e.expr)
  def min(e: Column): Column = Min(e.expr)
  def max(e: Column): Column = Max(e.expr)

  def upper(e: Column): Column = Upper(e.expr)
  def lower(e: Column): Column = Lower(e.expr)
  def sqrt(e: Column): Column = Sqrt(e.expr)
  def abs(e: Column): Column = Abs(e.expr)

  /**
   * This is a private API for Python
   * TODO: move this to a private package
   */
  def toColumns(cols: JList[Column]): Seq[Column] = {
    cols.toList.toSeq
  }

  // scalastyle:off

  /* Use the following code to generate:
  (0 to 22).map { x =>
    val types = (1 to x).foldRight("RT")((i, s) => {s"A$i, $s"})
    val typeTags = (1 to x).map(i => s"A$i: TypeTag").foldLeft("RT: TypeTag")(_ + ", " + _)
    val args = (1 to x).map(i => s"arg$i: Column").mkString(", ")
    val argsInUdf = (1 to x).map(i => s"arg$i.expr").mkString(", ")
    println(s"""
    /**
     * Call a Scala function of ${x} arguments as user-defined function (UDF), and automatically
     * infer the data types based on the function's signature.
     */
    def callUDF[$typeTags](f: Function$x[$types]${if (args.length > 0) ", " + args else ""}): Column = {
      ScalaUdf(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, Seq($argsInUdf))
    }""")
  }

  (0 to 22).map { x =>
    val args = (1 to x).map(i => s"arg$i: Column").mkString(", ")
    val fTypes = Seq.fill(x + 1)("_").mkString(", ")
    val argsInUdf = (1 to x).map(i => s"arg$i.expr").mkString(", ")
    println(s"""
    /**
     * Call a Scala function of ${x} arguments as user-defined function (UDF). This requires
     * you to specify the return data type.
     */
    def callUDF(f: Function$x[$fTypes], returnType: DataType${if (args.length > 0) ", " + args else ""}): Column = {
      ScalaUdf(f, returnType, Seq($argsInUdf))
    }""")
  }
  }
  */
  /**
   * Call a Scala function of 0 arguments as user-defined function (UDF), and automatically
   * infer the data types based on the function's signature.
   */
  def callUDF[RT: TypeTag](f: Function0[RT]): Column = {
    ScalaUdf(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, Seq())
  }

  /**
   * Call a Scala function of 1 arguments as user-defined function (UDF), and automatically
   * infer the data types based on the function's signature.
   */
  def callUDF[RT: TypeTag, A1: TypeTag](f: Function1[A1, RT], arg1: Column): Column = {
    ScalaUdf(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, Seq(arg1.expr))
  }

  /**
   * Call a Scala function of 2 arguments as user-defined function (UDF), and automatically
   * infer the data types based on the function's signature.
   */
  def callUDF[RT: TypeTag, A1: TypeTag, A2: TypeTag](f: Function2[A1, A2, RT], arg1: Column, arg2: Column): Column = {
    ScalaUdf(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, Seq(arg1.expr, arg2.expr))
  }

  /**
   * Call a Scala function of 3 arguments as user-defined function (UDF), and automatically
   * infer the data types based on the function's signature.
   */
  def callUDF[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag](f: Function3[A1, A2, A3, RT], arg1: Column, arg2: Column, arg3: Column): Column = {
    ScalaUdf(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, Seq(arg1.expr, arg2.expr, arg3.expr))
  }

  /**
   * Call a Scala function of 4 arguments as user-defined function (UDF), and automatically
   * infer the data types based on the function's signature.
   */
  def callUDF[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag](f: Function4[A1, A2, A3, A4, RT], arg1: Column, arg2: Column, arg3: Column, arg4: Column): Column = {
    ScalaUdf(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr))
  }

  /**
   * Call a Scala function of 5 arguments as user-defined function (UDF), and automatically
   * infer the data types based on the function's signature.
   */
  def callUDF[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag](f: Function5[A1, A2, A3, A4, A5, RT], arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column): Column = {
    ScalaUdf(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr))
  }

  /**
   * Call a Scala function of 6 arguments as user-defined function (UDF), and automatically
   * infer the data types based on the function's signature.
   */
  def callUDF[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag](f: Function6[A1, A2, A3, A4, A5, A6, RT], arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column): Column = {
    ScalaUdf(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr))
  }

  /**
   * Call a Scala function of 7 arguments as user-defined function (UDF), and automatically
   * infer the data types based on the function's signature.
   */
  def callUDF[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag](f: Function7[A1, A2, A3, A4, A5, A6, A7, RT], arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column): Column = {
    ScalaUdf(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr))
  }

  /**
   * Call a Scala function of 8 arguments as user-defined function (UDF), and automatically
   * infer the data types based on the function's signature.
   */
  def callUDF[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag](f: Function8[A1, A2, A3, A4, A5, A6, A7, A8, RT], arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column): Column = {
    ScalaUdf(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr))
  }

  /**
   * Call a Scala function of 9 arguments as user-defined function (UDF), and automatically
   * infer the data types based on the function's signature.
   */
  def callUDF[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag](f: Function9[A1, A2, A3, A4, A5, A6, A7, A8, A9, RT], arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column): Column = {
    ScalaUdf(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr))
  }

  /**
   * Call a Scala function of 10 arguments as user-defined function (UDF), and automatically
   * infer the data types based on the function's signature.
   */
  def callUDF[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag](f: Function10[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT], arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column, arg10: Column): Column = {
    ScalaUdf(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr, arg10.expr))
  }

  /**
   * Call a Scala function of 11 arguments as user-defined function (UDF), and automatically
   * infer the data types based on the function's signature.
   */
  def callUDF[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag](f: Function11[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, RT], arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column, arg10: Column, arg11: Column): Column = {
    ScalaUdf(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr, arg10.expr, arg11.expr))
  }

  /**
   * Call a Scala function of 12 arguments as user-defined function (UDF), and automatically
   * infer the data types based on the function's signature.
   */
  def callUDF[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag](f: Function12[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, RT], arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column, arg10: Column, arg11: Column, arg12: Column): Column = {
    ScalaUdf(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr, arg10.expr, arg11.expr, arg12.expr))
  }

  /**
   * Call a Scala function of 13 arguments as user-defined function (UDF), and automatically
   * infer the data types based on the function's signature.
   */
  def callUDF[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag](f: Function13[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, RT], arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column, arg10: Column, arg11: Column, arg12: Column, arg13: Column): Column = {
    ScalaUdf(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr, arg10.expr, arg11.expr, arg12.expr, arg13.expr))
  }

  /**
   * Call a Scala function of 14 arguments as user-defined function (UDF), and automatically
   * infer the data types based on the function's signature.
   */
  def callUDF[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag](f: Function14[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, RT], arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column, arg10: Column, arg11: Column, arg12: Column, arg13: Column, arg14: Column): Column = {
    ScalaUdf(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr, arg10.expr, arg11.expr, arg12.expr, arg13.expr, arg14.expr))
  }

  /**
   * Call a Scala function of 15 arguments as user-defined function (UDF), and automatically
   * infer the data types based on the function's signature.
   */
  def callUDF[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag](f: Function15[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, RT], arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column, arg10: Column, arg11: Column, arg12: Column, arg13: Column, arg14: Column, arg15: Column): Column = {
    ScalaUdf(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr, arg10.expr, arg11.expr, arg12.expr, arg13.expr, arg14.expr, arg15.expr))
  }

  /**
   * Call a Scala function of 16 arguments as user-defined function (UDF), and automatically
   * infer the data types based on the function's signature.
   */
  def callUDF[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag](f: Function16[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, RT], arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column, arg10: Column, arg11: Column, arg12: Column, arg13: Column, arg14: Column, arg15: Column, arg16: Column): Column = {
    ScalaUdf(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr, arg10.expr, arg11.expr, arg12.expr, arg13.expr, arg14.expr, arg15.expr, arg16.expr))
  }

  /**
   * Call a Scala function of 17 arguments as user-defined function (UDF), and automatically
   * infer the data types based on the function's signature.
   */
  def callUDF[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag](f: Function17[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, RT], arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column, arg10: Column, arg11: Column, arg12: Column, arg13: Column, arg14: Column, arg15: Column, arg16: Column, arg17: Column): Column = {
    ScalaUdf(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr, arg10.expr, arg11.expr, arg12.expr, arg13.expr, arg14.expr, arg15.expr, arg16.expr, arg17.expr))
  }

  /**
   * Call a Scala function of 18 arguments as user-defined function (UDF), and automatically
   * infer the data types based on the function's signature.
   */
  def callUDF[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag](f: Function18[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, RT], arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column, arg10: Column, arg11: Column, arg12: Column, arg13: Column, arg14: Column, arg15: Column, arg16: Column, arg17: Column, arg18: Column): Column = {
    ScalaUdf(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr, arg10.expr, arg11.expr, arg12.expr, arg13.expr, arg14.expr, arg15.expr, arg16.expr, arg17.expr, arg18.expr))
  }

  /**
   * Call a Scala function of 19 arguments as user-defined function (UDF), and automatically
   * infer the data types based on the function's signature.
   */
  def callUDF[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag, A19: TypeTag](f: Function19[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, RT], arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column, arg10: Column, arg11: Column, arg12: Column, arg13: Column, arg14: Column, arg15: Column, arg16: Column, arg17: Column, arg18: Column, arg19: Column): Column = {
    ScalaUdf(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr, arg10.expr, arg11.expr, arg12.expr, arg13.expr, arg14.expr, arg15.expr, arg16.expr, arg17.expr, arg18.expr, arg19.expr))
  }

  /**
   * Call a Scala function of 20 arguments as user-defined function (UDF), and automatically
   * infer the data types based on the function's signature.
   */
  def callUDF[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag, A19: TypeTag, A20: TypeTag](f: Function20[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, RT], arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column, arg10: Column, arg11: Column, arg12: Column, arg13: Column, arg14: Column, arg15: Column, arg16: Column, arg17: Column, arg18: Column, arg19: Column, arg20: Column): Column = {
    ScalaUdf(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr, arg10.expr, arg11.expr, arg12.expr, arg13.expr, arg14.expr, arg15.expr, arg16.expr, arg17.expr, arg18.expr, arg19.expr, arg20.expr))
  }

  /**
   * Call a Scala function of 21 arguments as user-defined function (UDF), and automatically
   * infer the data types based on the function's signature.
   */
  def callUDF[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag, A19: TypeTag, A20: TypeTag, A21: TypeTag](f: Function21[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21, RT], arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column, arg10: Column, arg11: Column, arg12: Column, arg13: Column, arg14: Column, arg15: Column, arg16: Column, arg17: Column, arg18: Column, arg19: Column, arg20: Column, arg21: Column): Column = {
    ScalaUdf(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr, arg10.expr, arg11.expr, arg12.expr, arg13.expr, arg14.expr, arg15.expr, arg16.expr, arg17.expr, arg18.expr, arg19.expr, arg20.expr, arg21.expr))
  }

  /**
   * Call a Scala function of 22 arguments as user-defined function (UDF), and automatically
   * infer the data types based on the function's signature.
   */
  def callUDF[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag, A19: TypeTag, A20: TypeTag, A21: TypeTag, A22: TypeTag](f: Function22[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21, A22, RT], arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column, arg10: Column, arg11: Column, arg12: Column, arg13: Column, arg14: Column, arg15: Column, arg16: Column, arg17: Column, arg18: Column, arg19: Column, arg20: Column, arg21: Column, arg22: Column): Column = {
    ScalaUdf(f, ScalaReflection.schemaFor(typeTag[RT]).dataType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr, arg10.expr, arg11.expr, arg12.expr, arg13.expr, arg14.expr, arg15.expr, arg16.expr, arg17.expr, arg18.expr, arg19.expr, arg20.expr, arg21.expr, arg22.expr))
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Call a Scala function of 0 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   */
  def callUDF(f: Function0[_], returnType: DataType): Column = {
    ScalaUdf(f, returnType, Seq())
  }

  /**
   * Call a Scala function of 1 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   */
  def callUDF(f: Function1[_, _], returnType: DataType, arg1: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr))
  }

  /**
   * Call a Scala function of 2 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   */
  def callUDF(f: Function2[_, _, _], returnType: DataType, arg1: Column, arg2: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr))
  }

  /**
   * Call a Scala function of 3 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   */
  def callUDF(f: Function3[_, _, _, _], returnType: DataType, arg1: Column, arg2: Column, arg3: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr, arg3.expr))
  }

  /**
   * Call a Scala function of 4 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   */
  def callUDF(f: Function4[_, _, _, _, _], returnType: DataType, arg1: Column, arg2: Column, arg3: Column, arg4: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr))
  }

  /**
   * Call a Scala function of 5 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   */
  def callUDF(f: Function5[_, _, _, _, _, _], returnType: DataType, arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr))
  }

  /**
   * Call a Scala function of 6 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   */
  def callUDF(f: Function6[_, _, _, _, _, _, _], returnType: DataType, arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr))
  }

  /**
   * Call a Scala function of 7 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   */
  def callUDF(f: Function7[_, _, _, _, _, _, _, _], returnType: DataType, arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr))
  }

  /**
   * Call a Scala function of 8 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   */
  def callUDF(f: Function8[_, _, _, _, _, _, _, _, _], returnType: DataType, arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr))
  }

  /**
   * Call a Scala function of 9 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   */
  def callUDF(f: Function9[_, _, _, _, _, _, _, _, _, _], returnType: DataType, arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr))
  }

  /**
   * Call a Scala function of 10 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   */
  def callUDF(f: Function10[_, _, _, _, _, _, _, _, _, _, _], returnType: DataType, arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column, arg10: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr, arg10.expr))
  }

  /**
   * Call a Scala function of 11 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   */
  def callUDF(f: Function11[_, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType, arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column, arg10: Column, arg11: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr, arg10.expr, arg11.expr))
  }

  /**
   * Call a Scala function of 12 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   */
  def callUDF(f: Function12[_, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType, arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column, arg10: Column, arg11: Column, arg12: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr, arg10.expr, arg11.expr, arg12.expr))
  }

  /**
   * Call a Scala function of 13 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   */
  def callUDF(f: Function13[_, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType, arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column, arg10: Column, arg11: Column, arg12: Column, arg13: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr, arg10.expr, arg11.expr, arg12.expr, arg13.expr))
  }

  /**
   * Call a Scala function of 14 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   */
  def callUDF(f: Function14[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType, arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column, arg10: Column, arg11: Column, arg12: Column, arg13: Column, arg14: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr, arg10.expr, arg11.expr, arg12.expr, arg13.expr, arg14.expr))
  }

  /**
   * Call a Scala function of 15 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   */
  def callUDF(f: Function15[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType, arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column, arg10: Column, arg11: Column, arg12: Column, arg13: Column, arg14: Column, arg15: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr, arg10.expr, arg11.expr, arg12.expr, arg13.expr, arg14.expr, arg15.expr))
  }

  /**
   * Call a Scala function of 16 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   */
  def callUDF(f: Function16[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType, arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column, arg10: Column, arg11: Column, arg12: Column, arg13: Column, arg14: Column, arg15: Column, arg16: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr, arg10.expr, arg11.expr, arg12.expr, arg13.expr, arg14.expr, arg15.expr, arg16.expr))
  }

  /**
   * Call a Scala function of 17 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   */
  def callUDF(f: Function17[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType, arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column, arg10: Column, arg11: Column, arg12: Column, arg13: Column, arg14: Column, arg15: Column, arg16: Column, arg17: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr, arg10.expr, arg11.expr, arg12.expr, arg13.expr, arg14.expr, arg15.expr, arg16.expr, arg17.expr))
  }

  /**
   * Call a Scala function of 18 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   */
  def callUDF(f: Function18[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType, arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column, arg10: Column, arg11: Column, arg12: Column, arg13: Column, arg14: Column, arg15: Column, arg16: Column, arg17: Column, arg18: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr, arg10.expr, arg11.expr, arg12.expr, arg13.expr, arg14.expr, arg15.expr, arg16.expr, arg17.expr, arg18.expr))
  }

  /**
   * Call a Scala function of 19 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   */
  def callUDF(f: Function19[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType, arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column, arg10: Column, arg11: Column, arg12: Column, arg13: Column, arg14: Column, arg15: Column, arg16: Column, arg17: Column, arg18: Column, arg19: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr, arg10.expr, arg11.expr, arg12.expr, arg13.expr, arg14.expr, arg15.expr, arg16.expr, arg17.expr, arg18.expr, arg19.expr))
  }

  /**
   * Call a Scala function of 20 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   */
  def callUDF(f: Function20[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType, arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column, arg10: Column, arg11: Column, arg12: Column, arg13: Column, arg14: Column, arg15: Column, arg16: Column, arg17: Column, arg18: Column, arg19: Column, arg20: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr, arg10.expr, arg11.expr, arg12.expr, arg13.expr, arg14.expr, arg15.expr, arg16.expr, arg17.expr, arg18.expr, arg19.expr, arg20.expr))
  }

  /**
   * Call a Scala function of 21 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   */
  def callUDF(f: Function21[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType, arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column, arg10: Column, arg11: Column, arg12: Column, arg13: Column, arg14: Column, arg15: Column, arg16: Column, arg17: Column, arg18: Column, arg19: Column, arg20: Column, arg21: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr, arg10.expr, arg11.expr, arg12.expr, arg13.expr, arg14.expr, arg15.expr, arg16.expr, arg17.expr, arg18.expr, arg19.expr, arg20.expr, arg21.expr))
  }

  /**
   * Call a Scala function of 22 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   */
  def callUDF(f: Function22[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType, arg1: Column, arg2: Column, arg3: Column, arg4: Column, arg5: Column, arg6: Column, arg7: Column, arg8: Column, arg9: Column, arg10: Column, arg11: Column, arg12: Column, arg13: Column, arg14: Column, arg15: Column, arg16: Column, arg17: Column, arg18: Column, arg19: Column, arg20: Column, arg21: Column, arg22: Column): Column = {
    ScalaUdf(f, returnType, Seq(arg1.expr, arg2.expr, arg3.expr, arg4.expr, arg5.expr, arg6.expr, arg7.expr, arg8.expr, arg9.expr, arg10.expr, arg11.expr, arg12.expr, arg13.expr, arg14.expr, arg15.expr, arg16.expr, arg17.expr, arg18.expr, arg19.expr, arg20.expr, arg21.expr, arg22.expr))
  }

  // scalastyle:on
}
