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

import java.util.Locale
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.FUNCTION_NAME
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.variant._
import org.apache.spark.sql.catalyst.expressions.xml._
import org.apache.spark.sql.catalyst.plans.logical.{FunctionBuilderBase, Generate, LogicalPlan, OneRowRelation, Range}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types._
import org.apache.spark.util.ArrayImplicits._

/**
 * A catalog for looking up user defined functions, used by an [[Analyzer]].
 *
 * Note:
 *   1) The implementation should be thread-safe to allow concurrent access.
 *   2) the database name is always case-sensitive here, callers are responsible to
 *      format the database name w.r.t. case-sensitive config.
 */
trait FunctionRegistryBase[T] {

  type FunctionBuilder = Seq[Expression] => T

  final def registerFunction(
      name: FunctionIdentifier, builder: FunctionBuilder, source: String): Unit = {
    val info = new ExpressionInfo(
      // SPARK-43099: getCanonicalName would return null on JDK15+
      Option(builder.getClass.getCanonicalName).getOrElse(builder.getClass.getName),
      name.database.orNull,
      name.funcName,
      null,
      "",
      "",
      "",
      "",
      "",
      "",
      source)
    registerFunction(name, info, builder)
  }

  def registerFunction(
    name: FunctionIdentifier,
    info: ExpressionInfo,
    builder: FunctionBuilder): Unit

  /* Create or replace a temporary function. */
  final def createOrReplaceTempFunction(
      name: String, builder: FunctionBuilder, source: String): Unit = {
    registerFunction(
      FunctionIdentifier(name),
      builder,
      source)
  }

  @throws[AnalysisException]("If function does not exist")
  def lookupFunction(name: FunctionIdentifier, children: Seq[Expression]): T

  /* List all of the registered function names. */
  def listFunction(): Seq[FunctionIdentifier]

  /* Get the class of the registered function by specified name. */
  def lookupFunction(name: FunctionIdentifier): Option[ExpressionInfo]

  /* Get the builder of the registered function by specified name. */
  def lookupFunctionBuilder(name: FunctionIdentifier): Option[FunctionBuilder]

  /** Drop a function and return whether the function existed. */
  def dropFunction(name: FunctionIdentifier): Boolean

  /** Checks if a function with a given name exists. */
  def functionExists(name: FunctionIdentifier): Boolean = lookupFunction(name).isDefined

  /** Clear all registered functions. */
  def clear(): Unit
}

object FunctionRegistryBase {

  /**
   * Return an expression info and a function builder for the function as defined by
   * T using the given name.
   */
  def build[T : ClassTag](
      name: String,
      since: Option[String]): (ExpressionInfo, Seq[Expression] => T) = {
    val runtimeClass = scala.reflect.classTag[T].runtimeClass
    // For `InheritAnalysisRules`, skip the constructor with most arguments, which is the main
    // constructor and contains non-parameter `replacement` and should not be used as
    // function builder.
    val isRuntime = classOf[InheritAnalysisRules].isAssignableFrom(runtimeClass)
    val constructors = if (isRuntime) {
      val all = runtimeClass.getConstructors
      val maxNumArgs = all.map(_.getParameterCount).max
      all.filterNot(_.getParameterCount == maxNumArgs)
    } else {
      runtimeClass.getConstructors
    }
    // See if we can find a constructor that accepts Seq[Expression]
    val varargCtor =
      constructors.find(_.getParameterTypes.toImmutableArraySeq == Seq(classOf[Seq[_]]))
    val builder = (expressions: Seq[Expression]) => {
      if (varargCtor.isDefined) {
        // If there is an apply method that accepts Seq[Expression], use that one.
        try {
          varargCtor.get.newInstance(expressions).asInstanceOf[T]
        } catch {
          // the exception is an invocation exception. To get a meaningful message, we need the
          // cause.
          case e: Exception => throw QueryCompilationErrors.funcBuildError(name, e)
        }
      } else {
        // Otherwise, find a constructor method that matches the number of arguments, and use that.
        val params = Seq.fill(expressions.size)(classOf[Expression])
        val f = constructors.find(_.getParameterTypes.toImmutableArraySeq == params).getOrElse {
          val validParametersCount = constructors
            .filter(_.getParameterTypes.forall(_ == classOf[Expression]))
            .map(_.getParameterCount).distinct.sorted
          throw QueryCompilationErrors.wrongNumArgsError(
            name, validParametersCount.toImmutableArraySeq, params.length)
        }
        try {
          f.newInstance(expressions : _*).asInstanceOf[T]
        } catch {
          // the exception is an invocation exception. To get a meaningful message, we need the
          // cause.
          case e: Exception => throw QueryCompilationErrors.funcBuildError(name, e)
        }
      }
    }

    (expressionInfo(name, since), builder)
  }

  /**
   * Creates an [[ExpressionInfo]] for the function as defined by T using the given name.
   */
  def expressionInfo[T : ClassTag](name: String, since: Option[String]): ExpressionInfo = {
    val clazz = scala.reflect.classTag[T].runtimeClass
    val df = clazz.getAnnotation(classOf[ExpressionDescription])
    if (df != null) {
      if (df.extended().isEmpty) {
        new ExpressionInfo(
          clazz.getCanonicalName.stripSuffix("$"),
          null,
          name,
          df.usage(),
          df.arguments(),
          df.examples(),
          df.note(),
          df.group(),
          since.getOrElse(df.since()),
          df.deprecated(),
          df.source())
      } else {
        // This exists for the backward compatibility with old `ExpressionDescription`s defining
        // the extended description in `extended()`.
        new ExpressionInfo(
          clazz.getCanonicalName.stripSuffix("$"), null, name, df.usage(), df.extended())
      }
    } else {
      new ExpressionInfo(clazz.getCanonicalName.stripSuffix("$"), name)
    }
  }
}

trait SimpleFunctionRegistryBase[T] extends FunctionRegistryBase[T] with Logging {

  @GuardedBy("this")
  protected val functionBuilders =
    new mutable.HashMap[FunctionIdentifier, (ExpressionInfo, FunctionBuilder)]

  // Resolution of the function name is always case insensitive, but the database name
  // depends on the caller
  private def normalizeFuncName(name: FunctionIdentifier): FunctionIdentifier = {
    FunctionIdentifier(name.funcName.toLowerCase(Locale.ROOT), name.database)
  }

  override def registerFunction(
      name: FunctionIdentifier,
      info: ExpressionInfo,
      builder: FunctionBuilder): Unit = {
    val normalizedName = normalizeFuncName(name)
    internalRegisterFunction(normalizedName, info, builder)
  }

  /**
   * Perform function registry without any preprocessing.
   * This is used when registering built-in functions and doing `FunctionRegistry.clone()`
   */
  def internalRegisterFunction(
      name: FunctionIdentifier,
      info: ExpressionInfo,
      builder: FunctionBuilder): Unit = synchronized {
    val newFunction = (info, builder)
    functionBuilders.put(name, newFunction) match {
      case Some(previousFunction) if previousFunction != newFunction =>
        logWarning(log"The function ${MDC(FUNCTION_NAME, name)} replaced a " +
          log"previously registered function.")
      case _ =>
    }
  }

  override def lookupFunction(name: FunctionIdentifier, children: Seq[Expression]): T = {
    val func = synchronized {
      functionBuilders.get(normalizeFuncName(name)).map(_._2).getOrElse {
        throw QueryCompilationErrors.unresolvedRoutineError(name, Seq("system.builtin"))
      }
    }
    func(children)
  }

  override def listFunction(): Seq[FunctionIdentifier] = synchronized {
    functionBuilders.iterator.map(_._1).toList
  }

  override def lookupFunction(name: FunctionIdentifier): Option[ExpressionInfo] = synchronized {
    functionBuilders.get(normalizeFuncName(name)).map(_._1)
  }

  override def lookupFunctionBuilder(
      name: FunctionIdentifier): Option[FunctionBuilder] = synchronized {
    functionBuilders.get(normalizeFuncName(name)).map(_._2)
  }

  override def dropFunction(name: FunctionIdentifier): Boolean = synchronized {
    functionBuilders.remove(normalizeFuncName(name)).isDefined
  }

  override def clear(): Unit = synchronized {
    functionBuilders.clear()
  }
}

/**
 * A trivial catalog that returns an error when a function is requested. Used for testing when all
 * functions are already filled in and the analyzer needs only to resolve attribute references.
 */
trait EmptyFunctionRegistryBase[T] extends FunctionRegistryBase[T] {
  override def registerFunction(
      name: FunctionIdentifier, info: ExpressionInfo, builder: FunctionBuilder): Unit = {
    throw SparkUnsupportedOperationException()
  }

  override def lookupFunction(name: FunctionIdentifier, children: Seq[Expression]): T = {
    throw SparkUnsupportedOperationException()
  }

  override def listFunction(): Seq[FunctionIdentifier] = {
    throw SparkUnsupportedOperationException()
  }

  override def lookupFunction(name: FunctionIdentifier): Option[ExpressionInfo] = {
    throw SparkUnsupportedOperationException()
  }

  override def lookupFunctionBuilder(name: FunctionIdentifier): Option[FunctionBuilder] = {
    throw SparkUnsupportedOperationException()
  }

  override def dropFunction(name: FunctionIdentifier): Boolean = {
    throw SparkUnsupportedOperationException()
  }

  override def clear(): Unit = {
    throw SparkUnsupportedOperationException()
  }
}

trait FunctionRegistry extends FunctionRegistryBase[Expression] {

  /** Create a copy of this registry with identical functions as this registry. */
  override def clone(): FunctionRegistry = throw new CloneNotSupportedException()
}

class SimpleFunctionRegistry
    extends SimpleFunctionRegistryBase[Expression]
    with FunctionRegistry {

  override def clone(): SimpleFunctionRegistry = synchronized {
    val registry = new SimpleFunctionRegistry
    functionBuilders.iterator.foreach { case (name, (info, builder)) =>
      registry.internalRegisterFunction(name, info, builder)
    }
    registry
  }
}

object EmptyFunctionRegistry
    extends EmptyFunctionRegistryBase[Expression]
    with FunctionRegistry {

  override def clone(): FunctionRegistry = this
}

object FunctionRegistry {

  type FunctionBuilder = Seq[Expression] => Expression

  val FUNC_ALIAS = TreeNodeTag[String]("functionAliasName")

  // ==============================================================================================
  //                          The guideline for adding SQL functions
  // ==============================================================================================
  // To add a SQL function, we usually need to create a new `Expression` for the function, and
  // implement the function logic in both the interpretation code path and codegen code path of the
  // `Expression`. We also need to define the type coercion behavior for the function inputs, by
  // extending `ImplicitCastInputTypes` or updating type coercion rules directly.
  //
  // It's much simpler if the SQL function can be implemented with existing expression(s). There are
  // a few cases:
  //   - The function is simply an alias of another function. We can just register the same
  //     expression with a different function name, e.g. `expression[Rand]("random", true)`.
  //   - The function is mostly the same with another function, but has a different parameter list.
  //     We can use `RuntimeReplaceable` to create a new expression, which can customize the
  //     parameter list and analysis behavior (type coercion). The `RuntimeReplaceable` expression
  //     will be replaced by the actual expression at the end of analysis. See `Left` as an example.
  //   - The function can be implemented by combining some existing expressions. We can use
  //     `RuntimeReplaceable` to define the combination. See `ParseToDate` as an example.
  //     To inherit the analysis behavior from the replacement expression
  //     mix-in `InheritAnalysisRules` with `RuntimeReplaceable`. See `TryAdd` as an example.
  //   - For `AggregateFunction`, `RuntimeReplaceableAggregate` should be mixed-in. See
  //     `CountIf` as an example.
  //
  // Sometimes, multiple functions share the same/similar expression replacement logic and it's
  // tedious to create many similar `RuntimeReplaceable` expressions. We can use `ExpressionBuilder`
  // to share the replacement logic. See `ParseToTimestampLTZExpressionBuilder` as an example.
  //
  // With these tools, we can even implement a new SQL function with a Java (static) method, and
  // then create a `RuntimeReplaceable` expression to call the Java method with `Invoke` or
  // `StaticInvoke` expression. By doing so we don't need to implement codegen for new functions
  // anymore. See `AesEncrypt`/`AesDecrypt` as an example.
  val expressions: Map[String, (ExpressionInfo, FunctionBuilder)] = Map(
    // misc non-aggregate functions
    expression[Abs]("abs"),
    expression[Coalesce]("coalesce"),
    expressionBuilder("explode", ExplodeExpressionBuilder),
    expressionGeneratorBuilderOuter("explode_outer", ExplodeExpressionBuilder),
    expression[Greatest]("greatest"),
    expression[If]("if"),
    expression[Inline]("inline"),
    expressionGeneratorOuter[Inline]("inline_outer"),
    expression[IsNaN]("isnan"),
    expression[Nvl]("ifnull", setAlias = true),
    expression[IsNull]("isnull"),
    expression[IsNotNull]("isnotnull"),
    expression[Least]("least"),
    expression[NaNvl]("nanvl"),
    expression[NullIf]("nullif"),
    expression[Nvl]("nvl"),
    expression[Nvl2]("nvl2"),
    expression[PosExplode]("posexplode"),
    expressionGeneratorOuter[PosExplode]("posexplode_outer"),
    expression[Rand]("rand"),
    expression[Rand]("random", true, Some("3.0.0")),
    expression[Randn]("randn"),
    expression[Stack]("stack"),
    CaseWhen.registryEntry,

    // math functions
    expression[Acos]("acos"),
    expression[Acosh]("acosh"),
    expression[Asin]("asin"),
    expression[Asinh]("asinh"),
    expression[Atan]("atan"),
    expression[Atan2]("atan2"),
    expression[Atanh]("atanh"),
    expression[Bin]("bin"),
    expression[BRound]("bround"),
    expression[Cbrt]("cbrt"),
    expressionBuilder("ceil", CeilExpressionBuilder),
    expressionBuilder("ceiling", CeilExpressionBuilder, true),
    expression[Cos]("cos"),
    expression[Sec]("sec"),
    expression[Cosh]("cosh"),
    expression[Conv]("conv"),
    expression[ToDegrees]("degrees"),
    expression[EulerNumber]("e"),
    expression[Exp]("exp"),
    expression[Expm1]("expm1"),
    expressionBuilder("floor", FloorExpressionBuilder),
    expression[Factorial]("factorial"),
    expression[Hex]("hex"),
    expression[Hypot]("hypot"),
    expression[Logarithm]("log"),
    expression[Log10]("log10"),
    expression[Log1p]("log1p"),
    expression[Log2]("log2"),
    expression[Log]("ln"),
    expression[Remainder]("mod", true, Some("2.3.0")),
    expression[UnaryMinus]("negative", true),
    expression[Pi]("pi"),
    expression[Pmod]("pmod"),
    expression[UnaryPositive]("positive"),
    expression[Pow]("pow", true),
    expression[Pow]("power"),
    expression[ToRadians]("radians"),
    expression[Rint]("rint"),
    expression[Round]("round"),
    expression[ShiftLeft]("shiftleft"),
    expression[ShiftRight]("shiftright"),
    expression[ShiftRightUnsigned]("shiftrightunsigned"),
    expression[Signum]("sign", true),
    expression[Signum]("signum"),
    expression[Sin]("sin"),
    expression[Csc]("csc"),
    expression[Sinh]("sinh"),
    expression[StringToMap]("str_to_map"),
    expression[Sqrt]("sqrt"),
    expression[Tan]("tan"),
    expression[Cot]("cot"),
    expression[Tanh]("tanh"),
    expression[WidthBucket]("width_bucket"),

    expression[Add]("+"),
    expression[Subtract]("-"),
    expression[Multiply]("*"),
    expression[Divide]("/"),
    expression[IntegralDivide]("div"),
    expression[Remainder]("%"),

    // "try_*" function which always return Null instead of runtime error.
    expression[TryAdd]("try_add"),
    expression[TryDivide]("try_divide"),
    expression[TryMod]("try_mod"),
    expression[TrySubtract]("try_subtract"),
    expression[TryMultiply]("try_multiply"),
    expression[TryElementAt]("try_element_at"),
    expressionBuilder("try_avg", TryAverageExpressionBuilder, setAlias = true),
    expressionBuilder("try_sum", TrySumExpressionBuilder, setAlias = true),
    expression[TryToBinary]("try_to_binary"),
    expressionBuilder("try_to_timestamp", TryToTimestampExpressionBuilder, setAlias = true),
    expression[TryAesDecrypt]("try_aes_decrypt"),
    expression[TryReflect]("try_reflect"),
    expression[TryUrlDecode]("try_url_decode"),

    // aggregate functions
    expression[HyperLogLogPlusPlus]("approx_count_distinct"),
    expression[Average]("avg"),
    expression[Corr]("corr"),
    expression[Count]("count"),
    expression[CountIf]("count_if"),
    expression[CovPopulation]("covar_pop"),
    expression[CovSample]("covar_samp"),
    expression[First]("first"),
    expression[First]("first_value", true),
    expression[AnyValue]("any_value"),
    expression[Kurtosis]("kurtosis"),
    expression[Last]("last"),
    expression[Last]("last_value", true),
    expression[Max]("max"),
    expression[MaxBy]("max_by"),
    expression[Average]("mean", true),
    expression[Min]("min"),
    expression[MinBy]("min_by"),
    expression[Percentile]("percentile"),
    expressionBuilder("percentile_cont", PercentileContBuilder),
    expressionBuilder("percentile_disc", PercentileDiscBuilder),
    expression[Median]("median"),
    expression[Skewness]("skewness"),
    expression[ApproximatePercentile]("percentile_approx"),
    expression[ApproximatePercentile]("approx_percentile", true),
    expression[HistogramNumeric]("histogram_numeric"),
    expression[StddevSamp]("std", true),
    expression[StddevSamp]("stddev", true),
    expression[StddevPop]("stddev_pop"),
    expression[StddevSamp]("stddev_samp"),
    expression[Sum]("sum"),
    expression[VarianceSamp]("variance", true),
    expression[VariancePop]("var_pop"),
    expression[VarianceSamp]("var_samp"),
    expression[CollectList]("collect_list"),
    expression[CollectList]("array_agg", true, Some("3.3.0")),
    expression[CollectSet]("collect_set"),
    expressionBuilder("count_min_sketch", CountMinSketchAggExpressionBuilder),
    expression[BoolAnd]("every", true),
    expression[BoolAnd]("bool_and"),
    expression[BoolOr]("any", true),
    expression[BoolOr]("some", true),
    expression[BoolOr]("bool_or"),
    expression[RegrCount]("regr_count"),
    expression[RegrAvgX]("regr_avgx"),
    expression[RegrAvgY]("regr_avgy"),
    expression[RegrR2]("regr_r2"),
    expression[RegrSXX]("regr_sxx"),
    expression[RegrSXY]("regr_sxy"),
    expression[RegrSYY]("regr_syy"),
    expression[RegrSlope]("regr_slope"),
    expression[RegrIntercept]("regr_intercept"),
    expressionBuilder("mode", ModeBuilder),
    expression[HllSketchAgg]("hll_sketch_agg"),
    expression[HllUnionAgg]("hll_union_agg"),

    // string functions
    expression[Ascii]("ascii"),
    expression[Chr]("char", true),
    expression[Chr]("chr"),
    expressionBuilder("collate", CollateExpressionBuilder),
    expression[Collation]("collation"),
    expressionBuilder("contains", ContainsExpressionBuilder),
    expressionBuilder("startswith", StartsWithExpressionBuilder),
    expressionBuilder("endswith", EndsWithExpressionBuilder),
    expression[Base64]("base64"),
    expression[BitLength]("bit_length"),
    expression[Length]("char_length", true, Some("2.3.0")),
    expression[Length]("character_length", true, Some("2.3.0")),
    expression[ConcatWs]("concat_ws"),
    expression[Decode]("decode"),
    expression[Elt]("elt"),
    expression[Encode]("encode"),
    expression[FindInSet]("find_in_set"),
    expression[FormatNumber]("format_number"),
    expression[FormatString]("format_string"),
    expression[ToNumber]("to_number"),
    expression[TryToNumber]("try_to_number"),
    expressionBuilder("to_char", ToCharacterBuilder),
    expressionBuilder("to_varchar", ToCharacterBuilder, setAlias = true, Some("3.5.0")),
    expression[GetJsonObject]("get_json_object"),
    expression[InitCap]("initcap"),
    expression[StringInstr]("instr"),
    expression[Lower]("lcase", true),
    expression[Length]("length"),
    expression[Length]("len", setAlias = true, Some("3.4.0")),
    expression[Levenshtein]("levenshtein"),
    expression[Luhncheck]("luhn_check"),
    expression[Like]("like"),
    expression[ILike]("ilike"),
    expression[Lower]("lower"),
    expression[OctetLength]("octet_length"),
    expression[StringLocate]("locate"),
    expressionBuilder("lpad", LPadExpressionBuilder),
    expression[StringTrimLeft]("ltrim"),
    expression[JsonTuple]("json_tuple"),
    expression[StringLocate]("position", true, Some("2.3.0")),
    expression[FormatString]("printf", true),
    expression[RegExpExtract]("regexp_extract"),
    expression[RegExpExtractAll]("regexp_extract_all"),
    expression[RegExpReplace]("regexp_replace"),
    expression[StringRepeat]("repeat"),
    expression[StringReplace]("replace"),
    expression[Overlay]("overlay"),
    expression[RLike]("rlike"),
    expression[RLike]("regexp_like", true, Some("3.2.0")),
    expression[RLike]("regexp", true, Some("3.2.0")),
    expressionBuilder("rpad", RPadExpressionBuilder),
    expression[StringTrimRight]("rtrim"),
    expression[Sentences]("sentences"),
    expression[SoundEx]("soundex"),
    expression[StringSpace]("space"),
    expression[StringSplit]("split"),
    expression[SplitPart]("split_part"),
    expression[Substring]("substr", true),
    expression[Substring]("substring"),
    expression[Left]("left"),
    expression[Right]("right"),
    expression[SubstringIndex]("substring_index"),
    expression[StringTranslate]("translate"),
    expression[StringTrim]("trim"),
    expression[StringTrimBoth]("btrim"),
    expression[Upper]("ucase", true),
    expression[UnBase64]("unbase64"),
    expression[Unhex]("unhex"),
    expression[Upper]("upper"),
    expression[XPathList]("xpath"),
    expression[XPathBoolean]("xpath_boolean"),
    expression[XPathDouble]("xpath_double"),
    expression[XPathDouble]("xpath_number", true),
    expression[XPathFloat]("xpath_float"),
    expression[XPathInt]("xpath_int"),
    expression[XPathLong]("xpath_long"),
    expression[XPathShort]("xpath_short"),
    expression[XPathString]("xpath_string"),
    expression[RegExpCount]("regexp_count"),
    expression[RegExpSubStr]("regexp_substr"),
    expression[RegExpInStr]("regexp_instr"),
    expression[IsValidUTF8]("is_valid_utf8"),
    expression[MakeValidUTF8]("make_valid_utf8"),
    expression[ValidateUTF8]("validate_utf8"),
    expression[TryValidateUTF8]("try_validate_utf8"),

    // url functions
    expression[UrlEncode]("url_encode"),
    expression[UrlDecode]("url_decode"),
    expression[ParseUrl]("parse_url"),

    // datetime functions
    expression[AddMonths]("add_months"),
    expression[CurrentDate]("current_date"),
    expressionBuilder("curdate", CurDateExpressionBuilder, setAlias = true),
    expression[CurrentTimestamp]("current_timestamp"),
    expression[CurrentTimeZone]("current_timezone"),
    expression[LocalTimestamp]("localtimestamp"),
    expression[DateDiff]("datediff"),
    expression[DateDiff]("date_diff", setAlias = true, Some("3.4.0")),
    expression[DateAdd]("date_add"),
    expression[DateAdd]("dateadd", setAlias = true, Some("3.4.0")),
    expression[DateFormatClass]("date_format"),
    expression[DateSub]("date_sub"),
    expression[DayOfMonth]("day", true),
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
    expression[Now]("now"),
    expression[Quarter]("quarter"),
    expression[Second]("second"),
    expression[ParseToTimestamp]("to_timestamp"),
    expression[ParseToDate]("to_date"),
    expression[ToBinary]("to_binary"),
    expression[ToUnixTimestamp]("to_unix_timestamp"),
    expression[ToUTCTimestamp]("to_utc_timestamp"),
    // We keep the 2 expression builders below to have different function docs.
    expressionBuilder("to_timestamp_ntz", ParseToTimestampNTZExpressionBuilder, setAlias = true),
    expressionBuilder("to_timestamp_ltz", ParseToTimestampLTZExpressionBuilder, setAlias = true),
    expression[TruncDate]("trunc"),
    expression[TruncTimestamp]("date_trunc"),
    expression[UnixTimestamp]("unix_timestamp"),
    expression[DayOfWeek]("dayofweek"),
    expression[WeekDay]("weekday"),
    expression[DayName]("dayname"),
    expression[WeekOfYear]("weekofyear"),
    expression[Year]("year"),
    expression[TimeWindow]("window"),
    expression[SessionWindow]("session_window"),
    expression[WindowTime]("window_time"),
    expression[MakeDate]("make_date"),
    expression[MakeTimestamp]("make_timestamp"),
    expression[MonthName]("monthname"),
    // We keep the 2 expression builders below to have different function docs.
    expressionBuilder("make_timestamp_ntz", MakeTimestampNTZExpressionBuilder, setAlias = true),
    expressionBuilder("make_timestamp_ltz", MakeTimestampLTZExpressionBuilder, setAlias = true),
    expression[MakeInterval]("make_interval"),
    expression[MakeDTInterval]("make_dt_interval"),
    expression[MakeYMInterval]("make_ym_interval"),
    expression[Extract]("extract"),
    // We keep the `DatePartExpressionBuilder` to have different function docs.
    expressionBuilder("date_part", DatePartExpressionBuilder, setAlias = true),
    expressionBuilder("datepart", DatePartExpressionBuilder, setAlias = true, Some("3.4.0")),
    expression[DateFromUnixDate]("date_from_unix_date"),
    expression[UnixDate]("unix_date"),
    expression[SecondsToTimestamp]("timestamp_seconds"),
    expression[MillisToTimestamp]("timestamp_millis"),
    expression[MicrosToTimestamp]("timestamp_micros"),
    expression[UnixSeconds]("unix_seconds"),
    expression[UnixMillis]("unix_millis"),
    expression[UnixMicros]("unix_micros"),
    expression[ConvertTimezone]("convert_timezone"),

    // collection functions
    expression[CreateArray]("array"),
    expression[ArrayContains]("array_contains"),
    expression[ArraysOverlap]("arrays_overlap"),
    expression[ArrayInsert]("array_insert"),
    expression[ArrayIntersect]("array_intersect"),
    expression[ArrayJoin]("array_join"),
    expression[ArrayPosition]("array_position"),
    expression[ArraySize]("array_size"),
    expression[ArraySort]("array_sort"),
    expression[ArrayExcept]("array_except"),
    expression[ArrayUnion]("array_union"),
    expression[ArrayCompact]("array_compact"),
    expression[CreateMap]("map"),
    expression[CreateNamedStruct]("named_struct"),
    expression[ElementAt]("element_at"),
    expression[MapContainsKey]("map_contains_key"),
    expression[MapFromArrays]("map_from_arrays"),
    expression[MapKeys]("map_keys"),
    expression[MapValues]("map_values"),
    expression[MapEntries]("map_entries"),
    expression[MapFromEntries]("map_from_entries"),
    expression[MapConcat]("map_concat"),
    expression[Size]("size"),
    expression[Slice]("slice"),
    expression[Size]("cardinality", true, Some("2.4.0")),
    expression[ArraysZip]("arrays_zip"),
    expression[SortArray]("sort_array"),
    expression[Shuffle]("shuffle"),
    expression[ArrayMin]("array_min"),
    expression[ArrayMax]("array_max"),
    expression[ArrayAppend]("array_append"),
    expression[Reverse]("reverse"),
    expression[Concat]("concat"),
    expression[Flatten]("flatten"),
    expression[Sequence]("sequence"),
    expression[ArrayRepeat]("array_repeat"),
    expression[ArrayRemove]("array_remove"),
    expression[ArrayPrepend]("array_prepend"),
    expression[ArrayDistinct]("array_distinct"),
    expression[ArrayTransform]("transform"),
    expression[MapFilter]("map_filter"),
    expression[ArrayFilter]("filter"),
    expression[ArrayExists]("exists"),
    expression[ArrayForAll]("forall"),
    expression[ArrayAggregate]("aggregate"),
    expression[ArrayAggregate]("reduce", setAlias = true, Some("3.4.0")),
    expression[TransformValues]("transform_values"),
    expression[TransformKeys]("transform_keys"),
    expression[MapZipWith]("map_zip_with"),
    expression[ZipWith]("zip_with"),
    expression[Get]("get"),

    CreateStruct.registryEntry,

    // misc functions
    expression[AssertTrue]("assert_true"),
    expressionBuilder("raise_error", RaiseErrorExpressionBuilder),
    expression[Crc32]("crc32"),
    expression[Md5]("md5"),
    expression[Uuid]("uuid"),
    expression[Murmur3Hash]("hash"),
    expression[XxHash64]("xxhash64"),
    expression[Sha1]("sha", true),
    expression[Sha1]("sha1"),
    expression[Sha2]("sha2"),
    expression[AesEncrypt]("aes_encrypt"),
    expression[AesDecrypt]("aes_decrypt"),
    expression[SparkPartitionID]("spark_partition_id"),
    expression[InputFileName]("input_file_name"),
    expression[InputFileBlockStart]("input_file_block_start"),
    expression[InputFileBlockLength]("input_file_block_length"),
    expression[MonotonicallyIncreasingID]("monotonically_increasing_id"),
    expression[CurrentDatabase]("current_database"),
    expression[CurrentDatabase]("current_schema", true, Some("3.4.0")),
    expression[CurrentCatalog]("current_catalog"),
    expression[CurrentUser]("current_user"),
    expression[CurrentUser]("user", true, Some("3.4.0")),
    expression[CurrentUser]("session_user", true, Some("4.0.0")),
    expression[CallMethodViaReflection]("reflect"),
    expression[CallMethodViaReflection]("java_method", true),
    expression[SparkVersion]("version"),
    expression[TypeOf]("typeof"),
    expression[EqualNull]("equal_null"),
    expression[HllSketchEstimate]("hll_sketch_estimate"),
    expression[HllUnion]("hll_union"),

    // grouping sets
    expression[Grouping]("grouping"),
    expression[GroupingID]("grouping_id"),

    // window functions
    expression[Lead]("lead"),
    expression[Lag]("lag"),
    expression[RowNumber]("row_number"),
    expression[CumeDist]("cume_dist"),
    expression[NthValue]("nth_value"),
    expression[NTile]("ntile"),
    expression[Rank]("rank"),
    expression[DenseRank]("dense_rank"),
    expression[PercentRank]("percent_rank"),

    // predicates
    expression[Between]("between"),
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
    expression[BitwiseXor]("^"),
    expression[ShiftLeft]("<<", true, Some("4.0.0")),
    expression[ShiftRight](">>", true, Some("4.0.0")),
    expression[ShiftRightUnsigned](">>>", true, Some("4.0.0")),
    expression[BitwiseCount]("bit_count"),
    expression[BitAndAgg]("bit_and"),
    expression[BitOrAgg]("bit_or"),
    expression[BitXorAgg]("bit_xor"),
    expression[BitwiseGet]("bit_get"),
    expression[BitwiseGet]("getbit", true),

    // bitmap functions and aggregates
    expression[BitmapBucketNumber]("bitmap_bucket_number"),
    expression[BitmapBitPosition]("bitmap_bit_position"),
    expression[BitmapConstructAgg]("bitmap_construct_agg"),
    expression[BitmapCount]("bitmap_count"),
    expression[BitmapOrAgg]("bitmap_or_agg"),

    // json
    expression[StructsToJson]("to_json"),
    expression[JsonToStructs]("from_json"),
    expression[SchemaOfJson]("schema_of_json"),
    expression[LengthOfJsonArray]("json_array_length"),
    expression[JsonObjectKeys]("json_object_keys"),

    // Variant
    expressionBuilder("parse_json", ParseJsonExpressionBuilder),
    expressionBuilder("try_parse_json", TryParseJsonExpressionBuilder),
    expression[IsVariantNull]("is_variant_null"),
    expressionBuilder("variant_get", VariantGetExpressionBuilder),
    expressionBuilder("try_variant_get", TryVariantGetExpressionBuilder),
    expression[SchemaOfVariant]("schema_of_variant"),
    expression[SchemaOfVariantAgg]("schema_of_variant_agg"),

    // cast
    expression[Cast]("cast"),
    // Cast aliases (SPARK-16730)
    castAlias("boolean", BooleanType),
    castAlias("tinyint", ByteType),
    castAlias("smallint", ShortType),
    castAlias("int", IntegerType),
    castAlias("bigint", LongType),
    castAlias("float", FloatType),
    castAlias("double", DoubleType),
    castAlias("decimal", DecimalType.USER_DEFAULT),
    castAlias("date", DateType),
    castAlias("timestamp", TimestampType),
    castAlias("binary", BinaryType),
    castAlias("string", StringType),

    // mask functions
    expressionBuilder("mask", MaskExpressionBuilder),

    // csv
    expression[CsvToStructs]("from_csv"),
    expression[SchemaOfCsv]("schema_of_csv"),
    expression[StructsToCsv]("to_csv"),

    // Xml
    expression[XmlToStructs]("from_xml"),
    expression[SchemaOfXml]("schema_of_xml"),
    expression[StructsToXml]("to_xml"),

    // Avro
    expression[FromAvro]("from_avro"),
    expression[ToAvro]("to_avro")
  )

  val builtin: SimpleFunctionRegistry = {
    val fr = new SimpleFunctionRegistry
    expressions.foreach {
      case (name, (info, builder)) =>
        fr.internalRegisterFunction(FunctionIdentifier(name), info, builder)
    }
    fr
  }

  val functionSet: Set[FunctionIdentifier] = builtin.listFunction().toSet

  /** Registry for internal functions used by Connect and the Column API. */
  private[sql] val internal: SimpleFunctionRegistry = new SimpleFunctionRegistry

  private def registerInternalExpression[T <: Expression : ClassTag](name: String): Unit = {
    val (info, builder) = FunctionRegistryBase.build(name, None)
    internal.internalRegisterFunction(FunctionIdentifier(name), info, builder)
  }

  registerInternalExpression[Product]("product")
  registerInternalExpression[BloomFilterAggregate]("bloom_filter_agg")
  registerInternalExpression[CollectTopK]("collect_top_k")
  registerInternalExpression[TimestampAdd]("timestampadd")
  registerInternalExpression[TimestampDiff]("timestampdiff")
  registerInternalExpression[Bucket]("bucket")
  registerInternalExpression[Years]("years")
  registerInternalExpression[Months]("months")
  registerInternalExpression[Days]("days")
  registerInternalExpression[Hours]("hours")
  registerInternalExpression[UnwrapUDT]("unwrap_udt")
  registerInternalExpression[DistributedSequenceID]("distributed_sequence_id")
  registerInternalExpression[PandasProduct]("pandas_product")
  registerInternalExpression[PandasStddev]("pandas_stddev")
  registerInternalExpression[PandasVariance]("pandas_var")
  registerInternalExpression[PandasSkewness]("pandas_skew")
  registerInternalExpression[PandasKurtosis]("pandas_kurt")
  registerInternalExpression[PandasCovar]("pandas_covar")
  registerInternalExpression[PandasMode]("pandas_mode")
  registerInternalExpression[EWM]("ewm")
  registerInternalExpression[NullIndex]("null_index")

  private def makeExprInfoForVirtualOperator(name: String, usage: String): ExpressionInfo = {
    new ExpressionInfo(
      null,
      null,
      name,
      usage,
      "",
      "",
      "",
      "",
      "",
      "",
      "built-in")
  }

  val builtinOperators: Map[String, ExpressionInfo] = Map(
    "<>" -> makeExprInfoForVirtualOperator("<>",
      "expr1 <> expr2 - Returns true if `expr1` is not equal to `expr2`."),
    "!=" -> makeExprInfoForVirtualOperator("!=",
      "expr1 != expr2 - Returns true if `expr1` is not equal to `expr2`."),
    "case" -> makeExprInfoForVirtualOperator("case",
      "CASE expr1 WHEN expr2 THEN expr3 [WHEN expr4 THEN expr5]* [ELSE expr6] END " +
        "- When `expr1` = `expr2`, returns `expr3`; when `expr1` = `expr4`, return `expr5`; " +
        "else return `expr6`."),
    "||" -> makeExprInfoForVirtualOperator("||",
      "expr1 || expr2 - Returns the concatenation of `expr1` and `expr2`.")
  )

  /**
   * Create a SQL function builder and corresponding `ExpressionInfo`.
   * @param name The function name.
   * @param setAlias The alias name used in SQL representation string.
   * @param since The Spark version since the function is added.
   * @tparam T The actual expression class.
   * @return (function name, (expression information, function builder))
   */
  private def expression[T <: Expression : ClassTag](
      name: String,
      setAlias: Boolean = false,
      since: Option[String] = None): (String, (ExpressionInfo, FunctionBuilder)) = {
    val (expressionInfo, builder) = FunctionRegistryBase.build[T](name, since)
    val newBuilder = (expressions: Seq[Expression]) => {
      if (expressions.exists(_.isInstanceOf[NamedArgumentExpression])) {
        throw QueryCompilationErrors.namedArgumentsNotSupported(name)
      }
      val expr = builder(expressions)
      if (setAlias) expr.setTagValue(FUNC_ALIAS, name)
      expr
    }
    (name, (expressionInfo, newBuilder))
  }

  /**
   * This method will be used to rearrange the arguments provided in function invocation
   * in the order defined by the function signature given in the builder instance.
   *
   * @param name The name of the function
   * @param builder The builder of the function expression
   * @param expressions The argument list passed in function invocation
   * @tparam T The class of the builder
   * @return An argument list in positional order defined by the builder
   */
  def rearrangeExpressions[T <: FunctionBuilderBase[_]](
      name: String,
      builder: T,
      expressions: Seq[Expression]) : Seq[Expression] = {
    val rearrangedExpressions = if (!builder.functionSignature.isEmpty) {
      val functionSignature = builder.functionSignature.get
      builder.rearrange(functionSignature, expressions, name)
    } else {
      expressions
    }
    if (rearrangedExpressions.exists(_.isInstanceOf[NamedArgumentExpression])) {
      throw QueryCompilationErrors.namedArgumentsNotSupported(name)
    }
    rearrangedExpressions
  }

  private def expressionBuilder[T <: ExpressionBuilder : ClassTag](
      name: String,
      builder: T,
      setAlias: Boolean = false,
      since: Option[String] = None): (String, (ExpressionInfo, FunctionBuilder)) = {
    val info = FunctionRegistryBase.expressionInfo[T](name, since)
    val funcBuilder = (expressions: Seq[Expression]) => {
      val (lambdas, others) = expressions.partition(_.isInstanceOf[LambdaFunction])
      if (lambdas.nonEmpty && !builder.supportsLambda) {
        throw new AnalysisException(
          errorClass = "INVALID_LAMBDA_FUNCTION_CALL.NON_HIGHER_ORDER_FUNCTION",
          messageParameters = Map(
            "class" -> builder.getClass.getCanonicalName))
      }
      assert(others.forall(_.resolved), "function arguments must be resolved.")
      val rearrangedExpressions = rearrangeExpressions(name, builder, expressions)
      val expr = builder.build(name, rearrangedExpressions)
      if (setAlias) expr.setTagValue(FUNC_ALIAS, name)
      expr
    }
    (name, (info, funcBuilder))
  }

  /**
   * Creates a function registry lookup entry for cast aliases (SPARK-16730).
   * For example, if name is "int", and dataType is IntegerType, this means int(x) would become
   * an alias for cast(x as IntegerType).
   * See usage above.
   */
  private def castAlias(
      name: String,
      dataType: DataType): (String, (ExpressionInfo, FunctionBuilder)) = {
    val builder = (args: Seq[Expression]) => {
      val argSize = args.size
      if (argSize != 1) {
        throw QueryCompilationErrors.wrongNumArgsError(name, Seq(1), argSize)
      }
      Cast(args.head, dataType)
    }
    val clazz = scala.reflect.classTag[Cast].runtimeClass
    val usage = "_FUNC_(expr) - Casts the value `expr` to the target data type `_FUNC_`."
    val expressionInfo =
      new ExpressionInfo(clazz.getCanonicalName, null, name, usage, "", "", "",
        "conversion_funcs", "2.0.1", "", "built-in")
    (name, (expressionInfo, builder))
  }

  private def expressionGeneratorOuter[T <: Generator : ClassTag](name: String)
    : (String, (ExpressionInfo, FunctionBuilder)) = {
    val (_, (info, builder)) = expression[T](name)
    val outerBuilder = (args: Seq[Expression]) => {
      GeneratorOuter(builder(args).asInstanceOf[Generator])
    }
    (name, (info, outerBuilder))
  }

  private def expressionGeneratorBuilderOuter[T <: ExpressionBuilder : ClassTag]
    (name: String, builder: T) : (String, (ExpressionInfo, FunctionBuilder)) = {
    val info = FunctionRegistryBase.expressionInfo[T](name, since = None)
    val outerBuilder = (args: Seq[Expression]) => {
      val rearrangedArgs =
        FunctionRegistry.rearrangeExpressions(name, builder, args)
      val generator = builder.build(name, rearrangedArgs)
      assert(generator.isInstanceOf[Generator])
      GeneratorOuter(generator.asInstanceOf[Generator])
    }
    (name, (info, outerBuilder))
  }
}

/**
 * A catalog for looking up table functions.
 */
trait TableFunctionRegistry extends FunctionRegistryBase[LogicalPlan] {

  /** Create a copy of this registry with identical functions as this registry. */
  override def clone(): TableFunctionRegistry = throw new CloneNotSupportedException()
}

class SimpleTableFunctionRegistry extends SimpleFunctionRegistryBase[LogicalPlan]
    with TableFunctionRegistry {

  override def clone(): SimpleTableFunctionRegistry = synchronized {
    val registry = new SimpleTableFunctionRegistry
    functionBuilders.iterator.foreach { case (name, (info, builder)) =>
      registry.internalRegisterFunction(name, info, builder)
    }
    registry
  }
}

object EmptyTableFunctionRegistry extends EmptyFunctionRegistryBase[LogicalPlan]
    with TableFunctionRegistry {

  override def clone(): TableFunctionRegistry = this
}

object TableFunctionRegistry {

  type TableFunctionBuilder = Seq[Expression] => LogicalPlan

  private def logicalPlan[T <: LogicalPlan : ClassTag](name: String)
      : (String, (ExpressionInfo, TableFunctionBuilder)) = {
    val (info, builder) = FunctionRegistryBase.build[T](name, since = None)
    (name, (info, (expressions: Seq[Expression]) => builder(expressions)))
  }

  /**
   * A function used for table-valued functions to return a builder that
   * when given input arguments, will return a function expression representing
   * the table-valued functions.
   *
   * @param name Name of the function
   * @param builder Object which will build the expression given input arguments
   * @param since Time of implementation
   * @tparam T Type of the builder
   * @return A tuple of the function name, expression info, and function builder
   */
  def generatorBuilder[T <: GeneratorBuilder : ClassTag](
      name: String,
      builder: T,
      since: Option[String] = None): (String, (ExpressionInfo, TableFunctionBuilder)) = {
    val info = FunctionRegistryBase.expressionInfo[T](name, since)
    val funcBuilder = (expressions: Seq[Expression]) => {
      assert(expressions.forall(_.resolved), "function arguments must be resolved.")
      val rearrangedExpressions = FunctionRegistry.rearrangeExpressions(name, builder, expressions)
      builder.build(name, rearrangedExpressions)
    }
    (name, (info, funcBuilder))
  }

  def generator[T <: Generator : ClassTag](name: String, outer: Boolean = false)
      : (String, (ExpressionInfo, TableFunctionBuilder)) = {
    val (info, builder) = FunctionRegistryBase.build[T](name, since = None)
    val newBuilder = (expressions: Seq[Expression]) => {
      val generator = builder(expressions)
      assert(generator.isInstanceOf[Generator])
      Generate(
        generator,
        unrequiredChildIndex = Nil,
        outer = outer,
        qualifier = None,
        generatorOutput = Nil,
        child = OneRowRelation())
    }
    (name, (info, newBuilder))
  }

  val logicalPlans: Map[String, (ExpressionInfo, TableFunctionBuilder)] = Map(
    logicalPlan[Range]("range"),
    generatorBuilder("explode", ExplodeGeneratorBuilder),
    generatorBuilder("explode_outer", ExplodeOuterGeneratorBuilder),
    generator[Inline]("inline"),
    generator[Inline]("inline_outer", outer = true),
    generator[JsonTuple]("json_tuple"),
    generator[PosExplode]("posexplode"),
    generator[PosExplode]("posexplode_outer", outer = true),
    generator[Stack]("stack"),
    generator[SQLKeywords]("sql_keywords"),
    generator[VariantExplode]("variant_explode"),
    generator[VariantExplode]("variant_explode_outer", outer = true)
  )

  val builtin: SimpleTableFunctionRegistry = {
    val fr = new SimpleTableFunctionRegistry
    logicalPlans.foreach {
      case (name, (info, builder)) =>
        fr.internalRegisterFunction(FunctionIdentifier(name), info, builder)
    }
    fr
  }

  val functionSet: Set[FunctionIdentifier] = builtin.listFunction().toSet
}

/**
 * This is a trait used for scalar valued functions that defines how their expression
 * representations are constructed in [[FunctionRegistry]].
 */
trait ExpressionBuilder extends FunctionBuilderBase[Expression]

/**
 * This is a trait used for table valued functions that defines how their expression
 * representations are constructed in [[TableFunctionRegistry]].
 */
trait GeneratorBuilder extends FunctionBuilderBase[LogicalPlan] {
  override final def build(funcName: String, expressions: Seq[Expression]) : LogicalPlan = {
    Generate(
      buildGenerator(funcName, expressions),
      unrequiredChildIndex = Nil,
      outer = isOuter,
      qualifier = None,
      generatorOutput = Nil,
      child = OneRowRelation())
  }

  def isOuter: Boolean

  def buildGenerator(funcName: String, expressions: Seq[Expression]) : Generator
}
