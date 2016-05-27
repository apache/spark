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

package org.apache.spark.sql.catalyst.analysis

import scala.language.existentials
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.util.StringKeyHashMap


/**
 * A catalog for looking up user defined functions, used by an [[Analyzer]].
 *
 * Note: The implementation should be thread-safe to allow concurrent access.
 */
trait FunctionRegistry {

  final def registerFunction(name: String, builder: FunctionBuilder): Unit = {
    registerFunction(name, new ExpressionInfo(builder.getClass.getCanonicalName, name), builder)
  }

  def registerFunction(name: String, info: ExpressionInfo, builder: FunctionBuilder): Unit

  @throws[AnalysisException]("If function does not exist")
  def lookupFunction(name: String, children: Seq[Expression]): Expression

  /* List all of the registered function names. */
  def listFunction(): Seq[String]

  /* Get the class of the registered function by specified name. */
  def lookupFunction(name: String): Option[ExpressionInfo]

  /* Get the builder of the registered function by specified name. */
  def lookupFunctionBuilder(name: String): Option[FunctionBuilder]

  /** Drop a function and return whether the function existed. */
  def dropFunction(name: String): Boolean

  /** Checks if a function with a given name exists. */
  def functionExists(name: String): Boolean = lookupFunction(name).isDefined

  /** Clear all registered functions. */
  def clear(): Unit

}

class SimpleFunctionRegistry extends FunctionRegistry {

  protected val functionBuilders =
    StringKeyHashMap[(ExpressionInfo, FunctionBuilder)](caseSensitive = false)

  override def registerFunction(
      name: String,
      info: ExpressionInfo,
      builder: FunctionBuilder): Unit = synchronized {
    functionBuilders.put(name, (info, builder))
  }

  override def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    val func = synchronized {
      functionBuilders.get(name).map(_._2).getOrElse {
        throw new AnalysisException(s"undefined function $name")
      }
    }
    func(children)
  }

  override def listFunction(): Seq[String] = synchronized {
    functionBuilders.iterator.map(_._1).toList.sorted
  }

  override def lookupFunction(name: String): Option[ExpressionInfo] = synchronized {
    functionBuilders.get(name).map(_._1)
  }

  override def lookupFunctionBuilder(name: String): Option[FunctionBuilder] = synchronized {
    functionBuilders.get(name).map(_._2)
  }

  override def dropFunction(name: String): Boolean = synchronized {
    functionBuilders.remove(name).isDefined
  }

  override def clear(): Unit = synchronized {
    functionBuilders.clear()
  }

  def copy(): SimpleFunctionRegistry = synchronized {
    val registry = new SimpleFunctionRegistry
    functionBuilders.iterator.foreach { case (name, (info, builder)) =>
      registry.registerFunction(name, info, builder)
    }
    registry
  }
}

/**
 * A trivial catalog that returns an error when a function is requested. Used for testing when all
 * functions are already filled in and the analyzer needs only to resolve attribute references.
 */
object EmptyFunctionRegistry extends FunctionRegistry {
  override def registerFunction(name: String, info: ExpressionInfo, builder: FunctionBuilder)
  : Unit = {
    throw new UnsupportedOperationException
  }

  override def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    throw new UnsupportedOperationException
  }

  override def listFunction(): Seq[String] = {
    throw new UnsupportedOperationException
  }

  override def lookupFunction(name: String): Option[ExpressionInfo] = {
    throw new UnsupportedOperationException
  }

  override def lookupFunctionBuilder(name: String): Option[FunctionBuilder] = {
    throw new UnsupportedOperationException
  }

  override def dropFunction(name: String): Boolean = {
    throw new UnsupportedOperationException
  }

  override def clear(): Unit = {
    throw new UnsupportedOperationException
  }

}


object FunctionRegistry {

  type FunctionBuilder = Seq[Expression] => Expression

  // Note: Whenever we add a new entry here, make sure we also update ExpressionToSQLSuite
  val expressions: Map[String, (ExpressionInfo, FunctionBuilder)] = Map(
    // misc non-aggregate functions
    expression[Abs]("abs"),
    expression[CreateArray]("array"),
    expression[Coalesce]("coalesce"),
    expression[Explode]("explode"),
    expression[Greatest]("greatest"),
    expression[If]("if"),
    expression[IsNaN]("isnan"),
    expression[IfNull]("ifnull"),
    expression[IsNull]("isnull"),
    expression[IsNotNull]("isnotnull"),
    expression[Least]("least"),
    expression[CreateMap]("map"),
    expression[CreateNamedStruct]("named_struct"),
    expression[NaNvl]("nanvl"),
    expression[NullIf]("nullif"),
    expression[Nvl]("nvl"),
    expression[Nvl2]("nvl2"),
    expression[Rand]("rand"),
    expression[Randn]("randn"),
    expression[CreateStruct]("struct"),
    expression[CaseWhen]("when"),

    // math functions
    expression[Acos]("acos"),
    expression[Asin]("asin"),
    expression[Atan]("atan"),
    expression[Atan2]("atan2"),
    expression[Bin]("bin"),
    expression[BRound]("bround"),
    expression[Cbrt]("cbrt"),
    expression[Ceil]("ceil"),
    expression[Ceil]("ceiling"),
    expression[Cos]("cos"),
    expression[Cosh]("cosh"),
    expression[Conv]("conv"),
    expression[ToDegrees]("degrees"),
    expression[EulerNumber]("e"),
    expression[Exp]("exp"),
    expression[Expm1]("expm1"),
    expression[Floor]("floor"),
    expression[Factorial]("factorial"),
    expression[Hex]("hex"),
    expression[Hypot]("hypot"),
    expression[Logarithm]("log"),
    expression[Log10]("log10"),
    expression[Log1p]("log1p"),
    expression[Log2]("log2"),
    expression[Log]("ln"),
    expression[UnaryMinus]("negative"),
    expression[Pi]("pi"),
    expression[Pmod]("pmod"),
    expression[UnaryPositive]("positive"),
    expression[Pow]("pow"),
    expression[Pow]("power"),
    expression[ToRadians]("radians"),
    expression[Rint]("rint"),
    expression[Round]("round"),
    expression[ShiftLeft]("shiftleft"),
    expression[ShiftRight]("shiftright"),
    expression[ShiftRightUnsigned]("shiftrightunsigned"),
    expression[Signum]("sign"),
    expression[Signum]("signum"),
    expression[Sin]("sin"),
    expression[Sinh]("sinh"),
    expression[Sqrt]("sqrt"),
    expression[Tan]("tan"),
    expression[Tanh]("tanh"),

    expression[Add]("+"),
    expression[Subtract]("-"),
    expression[Multiply]("*"),
    expression[Divide]("/"),
    expression[Remainder]("%"),

    // aggregate functions
    expression[HyperLogLogPlusPlus]("approx_count_distinct"),
    expression[Average]("avg"),
    expression[Corr]("corr"),
    expression[Count]("count"),
    expression[CovPopulation]("covar_pop"),
    expression[CovSample]("covar_samp"),
    expression[First]("first"),
    expression[First]("first_value"),
    expression[Kurtosis]("kurtosis"),
    expression[Last]("last"),
    expression[Last]("last_value"),
    expression[Max]("max"),
    expression[Average]("mean"),
    expression[Min]("min"),
    expression[Skewness]("skewness"),
    expression[StddevSamp]("stddev"),
    expression[StddevPop]("stddev_pop"),
    expression[StddevSamp]("stddev_samp"),
    expression[Sum]("sum"),
    expression[VarianceSamp]("variance"),
    expression[VariancePop]("var_pop"),
    expression[VarianceSamp]("var_samp"),
    expression[CollectList]("collect_list"),
    expression[CollectSet]("collect_set"),

    // string functions
    expression[Ascii]("ascii"),
    expression[Base64]("base64"),
    expression[Concat]("concat"),
    expression[ConcatWs]("concat_ws"),
    expression[Decode]("decode"),
    expression[Encode]("encode"),
    expression[FindInSet]("find_in_set"),
    expression[FormatNumber]("format_number"),
    expression[FormatString]("format_string"),
    expression[GetJsonObject]("get_json_object"),
    expression[InitCap]("initcap"),
    expression[StringInstr]("instr"),
    expression[Lower]("lcase"),
    expression[Length]("length"),
    expression[Levenshtein]("levenshtein"),
    expression[Like]("like"),
    expression[Lower]("lower"),
    expression[StringLocate]("locate"),
    expression[StringLPad]("lpad"),
    expression[StringTrimLeft]("ltrim"),
    expression[JsonTuple]("json_tuple"),
    expression[FormatString]("printf"),
    expression[RegExpExtract]("regexp_extract"),
    expression[RegExpReplace]("regexp_replace"),
    expression[StringRepeat]("repeat"),
    expression[StringReverse]("reverse"),
    expression[RLike]("rlike"),
    expression[StringRPad]("rpad"),
    expression[StringTrimRight]("rtrim"),
    expression[SoundEx]("soundex"),
    expression[StringSpace]("space"),
    expression[StringSplit]("split"),
    expression[Substring]("substr"),
    expression[Substring]("substring"),
    expression[SubstringIndex]("substring_index"),
    expression[StringTranslate]("translate"),
    expression[StringTrim]("trim"),
    expression[Upper]("ucase"),
    expression[UnBase64]("unbase64"),
    expression[Unhex]("unhex"),
    expression[Upper]("upper"),

    // datetime functions
    expression[AddMonths]("add_months"),
    expression[CurrentDate]("current_date"),
    expression[CurrentTimestamp]("current_timestamp"),
    expression[DateDiff]("datediff"),
    expression[DateAdd]("date_add"),
    expression[DateFormatClass]("date_format"),
    expression[DateSub]("date_sub"),
    expression[DayOfMonth]("day"),
    expression[DayOfYear]("dayofyear"),
    expression[DayOfMonth]("dayofmonth"),
    expression[FromUnixTime]("from_unixtime"),
    expression[FromUTCTimestamp]("from_utc_timestamp"),
    expression[Hour]("hour"),
    expression[LastDay]("last_day"),
    expression[Minute]("minute"),
    expression[Month]("month"),
    expression[MonthsBetween]("months_between"),
    expression[NextDay]("next_day"),
    expression[CurrentTimestamp]("now"),
    expression[Quarter]("quarter"),
    expression[Second]("second"),
    expression[ToDate]("to_date"),
    expression[ToUnixTimestamp]("to_unix_timestamp"),
    expression[ToUTCTimestamp]("to_utc_timestamp"),
    expression[TruncDate]("trunc"),
    expression[UnixTimestamp]("unix_timestamp"),
    expression[WeekOfYear]("weekofyear"),
    expression[Year]("year"),
    expression[TimeWindow]("window"),

    // collection functions
    expression[ArrayContains]("array_contains"),
    expression[Size]("size"),
    expression[SortArray]("sort_array"),

    // misc functions
    expression[AssertTrue]("assert_true"),
    expression[Crc32]("crc32"),
    expression[Md5]("md5"),
    expression[Murmur3Hash]("hash"),
    expression[Sha1]("sha"),
    expression[Sha1]("sha1"),
    expression[Sha2]("sha2"),
    expression[SparkPartitionID]("spark_partition_id"),
    expression[InputFileName]("input_file_name"),
    expression[MonotonicallyIncreasingID]("monotonically_increasing_id"),
    expression[CurrentDatabase]("current_database"),

    // grouping sets
    expression[Cube]("cube"),
    expression[Rollup]("rollup"),
    expression[Grouping]("grouping"),
    expression[GroupingID]("grouping_id"),

    // window functions
    expression[Lead]("lead"),
    expression[Lag]("lag"),
    expression[RowNumber]("row_number"),
    expression[CumeDist]("cume_dist"),
    expression[NTile]("ntile"),
    expression[Rank]("rank"),
    expression[DenseRank]("dense_rank"),
    expression[PercentRank]("percent_rank"),

    // predicates
    expression[And]("and"),
    expression[In]("in"),
    expression[Not]("not"),
    expression[Or]("or"),

    // comparison operators
    expression[EqualNullSafe]("<=>"),
    expression[EqualTo]("="),
    expression[EqualTo]("=="),
    expression[GreaterThan](">"),
    expression[GreaterThanOrEqual](">="),
    expression[LessThan]("<"),
    expression[LessThanOrEqual]("<="),
    expression[Not]("!"),

    // bitwise
    expression[BitwiseAnd]("&"),
    expression[BitwiseNot]("~"),
    expression[BitwiseOr]("|"),
    expression[BitwiseXor]("^")

  )

  val builtin: SimpleFunctionRegistry = {
    val fr = new SimpleFunctionRegistry
    expressions.foreach { case (name, (info, builder)) => fr.registerFunction(name, info, builder) }
    fr
  }

  /** See usage above. */
  private def expression[T <: Expression](name: String)
      (implicit tag: ClassTag[T]): (String, (ExpressionInfo, FunctionBuilder)) = {

    // See if we can find a constructor that accepts Seq[Expression]
    val varargCtor = Try(tag.runtimeClass.getDeclaredConstructor(classOf[Seq[_]])).toOption
    val builder = (expressions: Seq[Expression]) => {
      if (varargCtor.isDefined) {
        // If there is an apply method that accepts Seq[Expression], use that one.
        Try(varargCtor.get.newInstance(expressions).asInstanceOf[Expression]) match {
          case Success(e) => e
          case Failure(e) => throw new AnalysisException(e.getMessage)
        }
      } else {
        // Otherwise, find an ctor method that matches the number of arguments, and use that.
        val params = Seq.fill(expressions.size)(classOf[Expression])
        val f = Try(tag.runtimeClass.getDeclaredConstructor(params : _*)) match {
          case Success(e) =>
            e
          case Failure(e) =>
            throw new AnalysisException(s"Invalid number of arguments for function $name")
        }
        Try(f.newInstance(expressions : _*).asInstanceOf[Expression]) match {
          case Success(e) => e
          case Failure(e) =>
            // the exception is an invocation exception. To get a meaningful message, we need the
            // cause.
            throw new AnalysisException(e.getCause.getMessage)
        }
      }
    }

    val clazz = tag.runtimeClass
    val df = clazz.getAnnotation(classOf[ExpressionDescription])
    if (df != null) {
      (name,
        (new ExpressionInfo(clazz.getCanonicalName, name, df.usage(), df.extended()),
        builder))
    } else {
      (name, (new ExpressionInfo(clazz.getCanonicalName, name), builder))
    }
  }
}
