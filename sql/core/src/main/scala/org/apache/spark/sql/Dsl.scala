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

import scala.language.implicitConversions
import scala.reflect.runtime.universe.{TypeTag, typeTag}

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._


/**
 * Domain specific functions available for [[DataFrame]].
 */
object Dsl {

  /** An implicit conversion that turns a Scala `Symbol` into a [[Column]]. */
  implicit def symbolToColumn(s: Symbol): ColumnName = new ColumnName(s.name)

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
   *
   * The passed in object is returned directly if it is already a [[Column]].
   * If the object is a Scala Symbol, it is converted into a [[Column]] also.
   * Otherwise, a new [[Column]] is created to represent the literal value.
   */
  def lit(literal: Any): Column = {
    literal match {
      case c: Column => return c
      case s: Symbol => return new ColumnName(literal.asInstanceOf[Symbol].name)
      case _ =>  // continue
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

  //////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////

  /** Aggregate function: returns the sum of all values in the expression. */
  def sum(e: Column): Column = Sum(e.expr)

  /** Aggregate function: returns the sum of all values in the given column. */
  def sum(columnName: String): Column = sum(Column(columnName))

  /** Aggregate function: returns the sum of distinct values in the expression. */
  def sumDistinct(e: Column): Column = SumDistinct(e.expr)

  /** Aggregate function: returns the sum of distinct values in the expression. */
  def sumDistinct(columnName: String): Column = sumDistinct(Column(columnName))

  /** Aggregate function: returns the number of items in a group. */
  def count(e: Column): Column = Count(e.expr)

  /** Aggregate function: returns the number of items in a group. */
  def count(columnName: String): Column = count(Column(columnName))

  /** Aggregate function: returns the number of distinct items in a group. */
  @scala.annotation.varargs
  def countDistinct(expr: Column, exprs: Column*): Column =
    CountDistinct((expr +: exprs).map(_.expr))

  /** Aggregate function: returns the number of distinct items in a group. */
  @scala.annotation.varargs
  def countDistinct(columnName: String, columnNames: String*): Column =
    countDistinct(Column(columnName), columnNames.map(Column.apply) :_*)

  /** Aggregate function: returns the approximate number of distinct items in a group. */
  def approxCountDistinct(e: Column): Column = ApproxCountDistinct(e.expr)

  /** Aggregate function: returns the approximate number of distinct items in a group. */
  def approxCountDistinct(columnName: String): Column = approxCountDistinct(column(columnName))

  /** Aggregate function: returns the approximate number of distinct items in a group. */
  def approxCountDistinct(e: Column, rsd: Double): Column = ApproxCountDistinct(e.expr, rsd)

  /** Aggregate function: returns the approximate number of distinct items in a group. */
  def approxCountDistinct(columnName: String, rsd: Double): Column = {
    approxCountDistinct(Column(columnName), rsd)
  }

  /** Aggregate function: returns the average of the values in a group. */
  def avg(e: Column): Column = Average(e.expr)

  /** Aggregate function: returns the average of the values in a group. */
  def avg(columnName: String): Column = avg(Column(columnName))

  /** Aggregate function: returns the first value in a group. */
  def first(e: Column): Column = First(e.expr)

  /** Aggregate function: returns the first value of a column in a group. */
  def first(columnName: String): Column = first(Column(columnName))

  /** Aggregate function: returns the last value in a group. */
  def last(e: Column): Column = Last(e.expr)

  /** Aggregate function: returns the last value of the column in a group. */
  def last(columnName: String): Column = last(Column(columnName))

  /** Aggregate function: returns the minimum value of the expression in a group. */
  def min(e: Column): Column = Min(e.expr)

  /** Aggregate function: returns the minimum value of the column in a group. */
  def min(columnName: String): Column = min(Column(columnName))

  /** Aggregate function: returns the maximum value of the expression in a group. */
  def max(e: Column): Column = Max(e.expr)

  /** Aggregate function: returns the maximum value of the column in a group. */
  def max(columnName: String): Column = max(Column(columnName))

  //////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Returns the first column that is not null.
   * {{{
   *   df.select(coalesce(df("a"), df("b")))
   * }}}
   */
  @scala.annotation.varargs
  def coalesce(e: Column*): Column = Coalesce(e.map(_.expr))

  /**
   * Unary minus, i.e. negate the expression.
   * {{{
   *   // Select the amount column and negates all values.
   *   // Scala:
   *   df.select( -df("amount") )
   *
   *   // Java:
   *   df.select( negate(df.col("amount")) );
   * }}}
   */
  def negate(e: Column): Column = -e

  /**
   * Inversion of boolean expression, i.e. NOT.
   * {{
   *   // Scala: select rows that are not active (isActive === false)
   *   df.filter( !df("isActive") )
   *
   *   // Java:
   *   df.filter( not(df.col("isActive")) );
   * }}
   */
  def not(e: Column): Column = !e

  /** Converts a string expression to upper case. */
  def upper(e: Column): Column = Upper(e.expr)

  /** Converts a string exprsesion to lower case. */
  def lower(e: Column): Column = Lower(e.expr)

  /** Computes the square root of the specified float value. */
  def sqrt(e: Column): Column = Sqrt(e.expr)

  /** Computes the absolutle value. */
  def abs(e: Column): Column = Abs(e.expr)

  //////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////

  // scalastyle:off

  /* Use the following code to generate:
  (0 to 10).map { x =>
    val types = (1 to x).foldRight("RT")((i, s) => {s"A$i, $s"})
    val typeTags = (1 to x).map(i => s"A$i: TypeTag").foldLeft("RT: TypeTag")(_ + ", " + _)
    println(s"""
    /**
     * Defines a user-defined function of ${x} arguments as user-defined function (UDF).
     * The data types are automatically inferred based on the function's signature.
     */
    def udf[$typeTags](f: Function$x[$types]): UserDefinedFunction = {
      UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType)
    }""")
  }

  (0 to 10).map { x =>
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
   * Defines a user-defined function of 0 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   */
  def udf[RT: TypeTag](f: Function0[RT]): UserDefinedFunction = {
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType)
  }

  /**
   * Defines a user-defined function of 1 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   */
  def udf[RT: TypeTag, A1: TypeTag](f: Function1[A1, RT]): UserDefinedFunction = {
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType)
  }

  /**
   * Defines a user-defined function of 2 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag](f: Function2[A1, A2, RT]): UserDefinedFunction = {
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType)
  }

  /**
   * Defines a user-defined function of 3 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag](f: Function3[A1, A2, A3, RT]): UserDefinedFunction = {
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType)
  }

  /**
   * Defines a user-defined function of 4 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag](f: Function4[A1, A2, A3, A4, RT]): UserDefinedFunction = {
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType)
  }

  /**
   * Defines a user-defined function of 5 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag](f: Function5[A1, A2, A3, A4, A5, RT]): UserDefinedFunction = {
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType)
  }

  /**
   * Defines a user-defined function of 6 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag](f: Function6[A1, A2, A3, A4, A5, A6, RT]): UserDefinedFunction = {
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType)
  }

  /**
   * Defines a user-defined function of 7 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag](f: Function7[A1, A2, A3, A4, A5, A6, A7, RT]): UserDefinedFunction = {
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType)
  }

  /**
   * Defines a user-defined function of 8 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag](f: Function8[A1, A2, A3, A4, A5, A6, A7, A8, RT]): UserDefinedFunction = {
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType)
  }

  /**
   * Defines a user-defined function of 9 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag](f: Function9[A1, A2, A3, A4, A5, A6, A7, A8, A9, RT]): UserDefinedFunction = {
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType)
  }

  /**
   * Defines a user-defined function of 10 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   */
  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag](f: Function10[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT]): UserDefinedFunction = {
    UserDefinedFunction(f, ScalaReflection.schemaFor(typeTag[RT]).dataType)
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

  // scalastyle:on
}
