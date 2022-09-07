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

package org.apache.spark.ml.param

import java.lang.reflect.Modifier
import java.util.{List => JList}
import java.util.NoSuchElementException

import scala.annotation.varargs
import scala.collection.JavaConverters._
import scala.collection.mutable

import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkException
import org.apache.spark.annotation.Since
import org.apache.spark.ml.linalg.{JsonMatrixConverter, JsonVectorConverter, Matrix, Vector}
import org.apache.spark.ml.util.Identifiable

/**
 * A param with self-contained documentation and optionally default value. Primitive-typed param
 * should use the specialized versions, which are more friendly to Java users.
 *
 * @param parent parent object
 * @param name param name
 * @param doc documentation
 * @param isValid optional validation method which indicates if a value is valid.
 *                See [[ParamValidators]] for factory methods for common validation functions.
 * @tparam T param value type
 */
class Param[T](val parent: String, val name: String, val doc: String, val isValid: T => Boolean)
  extends Serializable {

  def this(parent: Identifiable, name: String, doc: String, isValid: T => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue[T])

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

  /**
   * Assert that the given value is valid for this parameter.
   *
   * Note: Parameter checks involving interactions between multiple parameters and input/output
   * columns should be implemented in [[org.apache.spark.ml.PipelineStage.transformSchema()]].
   *
   * DEVELOPERS: This method is only called by [[ParamPair]], which means that all parameters
   *             should be specified via [[ParamPair]].
   *
   * @throws IllegalArgumentException if the value is invalid
   */
  private[param] def validate(value: T): Unit = {
    if (!isValid(value)) {
      val valueToString = value match {
        case v: Array[_] => v.mkString("[", ",", "]")
        case _ => value.toString
      }
      throw new IllegalArgumentException(
        s"$parent parameter $name given invalid value $valueToString.")
    }
  }

  /** Creates a param pair with the given value (for Java). */
  def w(value: T): ParamPair[T] = this -> value

  /** Creates a param pair with the given value (for Scala). */
  // scalastyle:off
  def ->(value: T): ParamPair[T] = ParamPair(this, value)
  // scalastyle:on

  /** Encodes a param value into JSON, which can be decoded by `jsonDecode()`. */
  def jsonEncode(value: T): String = {
    value match {
      case x: String =>
        compact(render(JString(x)))
      case v: Vector =>
        JsonVectorConverter.toJson(v)
      case m: Matrix =>
        JsonMatrixConverter.toJson(m)
      case _ =>
        throw new UnsupportedOperationException(
          "The default jsonEncode only supports string, vector and matrix. " +
            s"${this.getClass.getName} must override jsonEncode for ${value.getClass.getName}.")
    }
  }

  /** Decodes a param value from JSON. */
  def jsonDecode(json: String): T = Param.jsonDecode[T](json)

  private[this] val stringRepresentation = s"${parent}__$name"

  override final def toString: String = stringRepresentation

  override final def hashCode: Int = toString.##

  override final def equals(obj: Any): Boolean = {
    obj match {
      case p: Param[_] => (p.parent == parent) && (p.name == name)
      case _ => false
    }
  }
}

private[ml] object Param {

  /** Decodes a param value from JSON. */
  def jsonDecode[T](json: String): T = {
    val jValue = parse(json)
    jValue match {
      case JString(x) =>
        x.asInstanceOf[T]
      case JObject(v) =>
        val keys = v.map(_._1)
        if (keys.contains("class")) {
          implicit val formats = DefaultFormats
          val className = (jValue \ "class").extract[String]
          className match {
            case JsonMatrixConverter.className =>
              val checkFields = Array("numRows", "numCols", "values", "isTransposed", "type")
              require(checkFields.forall(keys.contains), s"Expect a JSON serialized Matrix" +
                s" but cannot find fields ${checkFields.mkString(", ")} in $json.")
              JsonMatrixConverter.fromJson(json).asInstanceOf[T]

            case s => throw new SparkException(s"unrecognized class $s in $json")
          }
        } else {
          // "class" info in JSON was added in Spark 2.3(SPARK-22289). JSON support for Vector was
          // implemented before that and does not have "class" attribute.
          require(keys.contains("type") && keys.contains("values"), s"Expect a JSON serialized" +
            s" vector/matrix but cannot find fields 'type' and 'values' in $json.")
          JsonVectorConverter.fromJson(json).asInstanceOf[T]
        }

      case _ =>
        throw new UnsupportedOperationException(
          "The default jsonDecode only supports string, vector and matrix. " +
            s"${this.getClass.getName} must override jsonDecode to support its value type.")
    }
  }
}

/**
 * Factory methods for common validation functions for `Param.isValid`.
 * The numerical methods only support Int, Long, Float, and Double.
 */
object ParamValidators {

  /** (private[param]) Default validation always return true */
  private[param] def alwaysTrue[T]: T => Boolean = (_: T) => true

  /**
   * Private method for checking numerical types and converting to Double.
   * This is mainly for the sake of compilation; type checks are really handled
   * by [[Params]] setters and the [[ParamPair]] constructor.
   */
  private def getDouble[T](value: T): Double = value match {
    case x: Int => x.toDouble
    case x: Long => x.toDouble
    case x: Float => x.toDouble
    case x: Double => x.toDouble
    case _ =>
      // The type should be checked before this is ever called.
      throw new IllegalArgumentException("Numerical Param validation failed because" +
        s" of unexpected input type: ${value.getClass}")
  }

  /**
   * Check if value is greater than lowerBound
   */
  def gt[T](lowerBound: Double): T => Boolean = { (value: T) =>
    getDouble(value) > lowerBound
  }

  /**
   * Check if value is greater than or equal to lowerBound
   */
  def gtEq[T](lowerBound: Double): T => Boolean = { (value: T) =>
    getDouble(value) >= lowerBound
  }

  /**
   * Check if value is less than upperBound
   */
  def lt[T](upperBound: Double): T => Boolean = { (value: T) =>
    getDouble(value) < upperBound
  }

  /**
   * Check if value is less than or equal to upperBound
   */
  def ltEq[T](upperBound: Double): T => Boolean = { (value: T) =>
    getDouble(value) <= upperBound
  }

  /**
   * Check for value in range lowerBound to upperBound.
   *
   * @param lowerInclusive if true, range includes value = lowerBound
   * @param upperInclusive if true, range includes value = upperBound
   */
  def inRange[T](
      lowerBound: Double,
      upperBound: Double,
      lowerInclusive: Boolean,
      upperInclusive: Boolean): T => Boolean = { (value: T) =>
    val x: Double = getDouble(value)
    val lowerValid = if (lowerInclusive) x >= lowerBound else x > lowerBound
    val upperValid = if (upperInclusive) x <= upperBound else x < upperBound
    lowerValid && upperValid
  }

  /** Version of `inRange()` which uses inclusive be default: [lowerBound, upperBound] */
  def inRange[T](lowerBound: Double, upperBound: Double): T => Boolean = {
    inRange[T](lowerBound, upperBound, lowerInclusive = true, upperInclusive = true)
  }

  /** Check for value in an allowed set of values. */
  def inArray[T](allowed: Array[T]): T => Boolean = { (value: T) =>
    allowed.contains(value)
  }

  /** Check for value in an allowed set of values. */
  def inArray[T](allowed: java.util.List[T]): T => Boolean = { (value: T) =>
    allowed.contains(value)
  }

  /** Check that the array length is greater than lowerBound. */
  def arrayLengthGt[T](lowerBound: Double): Array[T] => Boolean = { (value: Array[T]) =>
    value.length > lowerBound
  }

  /**
   * Utility for Param validity checks for Transformers which have both single- and multi-column
   * support.  This utility assumes that `inputCol` indicates single-column usage and
   * that `inputCols` indicates multi-column usage.
   *
   * This checks to ensure that exactly one set of Params has been set, and it
   * raises an `IllegalArgumentException` if not.
   *
   * @param singleColumnParams Params which should be set (or have defaults) if `inputCol` has been
   *                           set.  This does not need to include `inputCol`.
   * @param multiColumnParams Params which should be set (or have defaults) if `inputCols` has been
   *                           set.  This does not need to include `inputCols`.
   */
  def checkSingleVsMultiColumnParams(
      model: Params,
      singleColumnParams: Seq[Param[_]],
      multiColumnParams: Seq[Param[_]]): Unit = {
    val name = s"${model.getClass.getSimpleName} $model"

    def checkExclusiveParams(
        isSingleCol: Boolean,
        requiredParams: Seq[Param[_]],
        excludedParams: Seq[Param[_]]): Unit = {
      val badParamsMsgBuilder = new mutable.StringBuilder()

      val mustUnsetParams = excludedParams.filter(p => model.isSet(p))
        .map(_.name).mkString(", ")
      if (mustUnsetParams.nonEmpty) {
        badParamsMsgBuilder ++=
          s"The following Params are not applicable and should not be set: $mustUnsetParams."
      }

      val mustSetParams = requiredParams.filter(p => !model.isDefined(p))
        .map(_.name).mkString(", ")
      if (mustSetParams.nonEmpty) {
        badParamsMsgBuilder ++=
          s"The following Params must be defined but are not set: $mustSetParams."
      }

      val badParamsMsg = badParamsMsgBuilder.toString()

      if (badParamsMsg.nonEmpty) {
        val errPrefix = if (isSingleCol) {
          s"$name has the inputCol Param set for single-column transform."
        } else {
          s"$name has the inputCols Param set for multi-column transform."
        }
        throw new IllegalArgumentException(s"$errPrefix $badParamsMsg")
      }
    }

    val inputCol = model.getParam("inputCol")
    val inputCols = model.getParam("inputCols")

    if (model.isSet(inputCol)) {
      require(!model.isSet(inputCols), s"$name requires " +
        s"exactly one of inputCol, inputCols Params to be set, but both are set.")

      checkExclusiveParams(isSingleCol = true, requiredParams = singleColumnParams,
        excludedParams = multiColumnParams)
    } else if (model.isSet(inputCols)) {
      checkExclusiveParams(isSingleCol = false, requiredParams = multiColumnParams,
        excludedParams = singleColumnParams)
    } else {
      throw new IllegalArgumentException(s"$name requires " +
        s"exactly one of inputCol, inputCols Params to be set, but neither is set.")
    }
  }
}

// specialize primitive-typed params because Java doesn't recognize scala.Double, scala.Int, ...

/**
 * Specialized version of `Param[Double]` for Java.
 */
class DoubleParam(parent: String, name: String, doc: String, isValid: Double => Boolean)
  extends Param[Double](parent, name, doc, isValid) {

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  def this(parent: Identifiable, name: String, doc: String, isValid: Double => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: Double): ParamPair[Double] = super.w(value)

  override def jsonEncode(value: Double): String = {
    compact(render(DoubleParam.jValueEncode(value)))
  }

  override def jsonDecode(json: String): Double = {
    DoubleParam.jValueDecode(parse(json))
  }
}

private[param] object DoubleParam {
  /** Encodes a param value into JValue. */
  def jValueEncode(value: Double): JValue = {
    value match {
      case _ if value.isNaN =>
        JString("NaN")
      case Double.NegativeInfinity =>
        JString("-Inf")
      case Double.PositiveInfinity =>
        JString("Inf")
      case _ =>
        JDouble(value)
    }
  }

  /** Decodes a param value from JValue. */
  def jValueDecode(jValue: JValue): Double = {
    jValue match {
      case JString("NaN") =>
        Double.NaN
      case JString("-Inf") =>
        Double.NegativeInfinity
      case JString("Inf") =>
        Double.PositiveInfinity
      case JDouble(x) =>
        x
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $jValue to Double.")
    }
  }
}

/**
 * Specialized version of `Param[Int]` for Java.
 */
class IntParam(parent: String, name: String, doc: String, isValid: Int => Boolean)
  extends Param[Int](parent, name, doc, isValid) {

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  def this(parent: Identifiable, name: String, doc: String, isValid: Int => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: Int): ParamPair[Int] = super.w(value)

  override def jsonEncode(value: Int): String = {
    compact(render(JInt(value)))
  }

  override def jsonDecode(json: String): Int = {
    implicit val formats = DefaultFormats
    parse(json).extract[Int]
  }
}

/**
 * Specialized version of `Param[Float]` for Java.
 */
class FloatParam(parent: String, name: String, doc: String, isValid: Float => Boolean)
  extends Param[Float](parent, name, doc, isValid) {

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  def this(parent: Identifiable, name: String, doc: String, isValid: Float => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: Float): ParamPair[Float] = super.w(value)

  override def jsonEncode(value: Float): String = {
    compact(render(FloatParam.jValueEncode(value)))
  }

  override def jsonDecode(json: String): Float = {
    FloatParam.jValueDecode(parse(json))
  }
}

private object FloatParam {

  /** Encodes a param value into JValue. */
  def jValueEncode(value: Float): JValue = {
    value match {
      case _ if value.isNaN =>
        JString("NaN")
      case Float.NegativeInfinity =>
        JString("-Inf")
      case Float.PositiveInfinity =>
        JString("Inf")
      case _ =>
        JDouble(value)
    }
  }

  /** Decodes a param value from JValue. */
  def jValueDecode(jValue: JValue): Float = {
    jValue match {
      case JString("NaN") =>
        Float.NaN
      case JString("-Inf") =>
        Float.NegativeInfinity
      case JString("Inf") =>
        Float.PositiveInfinity
      case JDouble(x) =>
        x.toFloat
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $jValue to Float.")
    }
  }
}

/**
 * Specialized version of `Param[Long]` for Java.
 */
class LongParam(parent: String, name: String, doc: String, isValid: Long => Boolean)
  extends Param[Long](parent, name, doc, isValid) {

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  def this(parent: Identifiable, name: String, doc: String, isValid: Long => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: Long): ParamPair[Long] = super.w(value)

  override def jsonEncode(value: Long): String = {
    compact(render(JInt(value)))
  }

  override def jsonDecode(json: String): Long = {
    implicit val formats = DefaultFormats
    parse(json).extract[Long]
  }
}

/**
 * Specialized version of `Param[Boolean]` for Java.
 */
class BooleanParam(parent: String, name: String, doc: String) // No need for isValid
  extends Param[Boolean](parent, name, doc) {

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: Boolean): ParamPair[Boolean] = super.w(value)

  override def jsonEncode(value: Boolean): String = {
    compact(render(JBool(value)))
  }

  override def jsonDecode(json: String): Boolean = {
    implicit val formats = DefaultFormats
    parse(json).extract[Boolean]
  }
}

/**
 * Specialized version of `Param[Array[String]]` for Java.
 */
class StringArrayParam(parent: Params, name: String, doc: String, isValid: Array[String] => Boolean)
  extends Param[Array[String]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  def w(value: java.util.List[String]): ParamPair[Array[String]] = w(value.asScala.toArray)

  override def jsonEncode(value: Array[String]): String = {
    import org.json4s.JsonDSL._
    compact(render(value.toSeq))
  }

  override def jsonDecode(json: String): Array[String] = {
    implicit val formats = DefaultFormats
    parse(json).extract[Seq[String]].toArray
  }
}

/**
 * Specialized version of `Param[Array[Double]]` for Java.
 */
class DoubleArrayParam(parent: Params, name: String, doc: String, isValid: Array[Double] => Boolean)
  extends Param[Array[Double]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  def w(value: java.util.List[java.lang.Double]): ParamPair[Array[Double]] =
    w(value.asScala.map(_.asInstanceOf[Double]).toArray)

  override def jsonEncode(value: Array[Double]): String = {
    import org.json4s.JsonDSL._
    compact(render(value.toSeq.map(DoubleParam.jValueEncode)))
  }

  override def jsonDecode(json: String): Array[Double] = {
    parse(json) match {
      case JArray(values) =>
        values.map(DoubleParam.jValueDecode).toArray
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to Array[Double].")
    }
  }
}

/**
 * Specialized version of `Param[Array[Array[Double]]]` for Java.
 */
class DoubleArrayArrayParam(
    parent: Params,
    name: String,
    doc: String,
    isValid: Array[Array[Double]] => Boolean)
  extends Param[Array[Array[Double]]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  def w(value: java.util.List[java.util.List[java.lang.Double]]): ParamPair[Array[Array[Double]]] =
    w(value.asScala.map(_.asScala.map(_.asInstanceOf[Double]).toArray).toArray)

  override def jsonEncode(value: Array[Array[Double]]): String = {
    import org.json4s.JsonDSL._
    compact(render(value.toSeq.map(_.toSeq.map(DoubleParam.jValueEncode))))
  }

  override def jsonDecode(json: String): Array[Array[Double]] = {
    parse(json) match {
      case JArray(values) =>
        values.map {
          case JArray(values) =>
            values.map(DoubleParam.jValueDecode).toArray
          case _ =>
            throw new IllegalArgumentException(s"Cannot decode $json to Array[Array[Double]].")
        }.toArray
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to Array[Array[Double]].")
    }
  }
}

/**
 * Specialized version of `Param[Array[Int]]` for Java.
 */
class IntArrayParam(parent: Params, name: String, doc: String, isValid: Array[Int] => Boolean)
  extends Param[Array[Int]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  def w(value: java.util.List[java.lang.Integer]): ParamPair[Array[Int]] =
    w(value.asScala.map(_.asInstanceOf[Int]).toArray)

  override def jsonEncode(value: Array[Int]): String = {
    import org.json4s.JsonDSL._
    compact(render(value.toSeq))
  }

  override def jsonDecode(json: String): Array[Int] = {
    implicit val formats = DefaultFormats
    parse(json).extract[Seq[Int]].toArray
  }
}

/**
 * A param and its value.
 */
@Since("1.2.0")
case class ParamPair[T] @Since("1.2.0") (
    @Since("1.2.0") param: Param[T],
    @Since("1.2.0") value: T) {
  // This is *the* place Param.validate is called.  Whenever a parameter is specified, we should
  // always construct a ParamPair so that validate is called.
  param.validate(value)
}

/**
 * Trait for components that take parameters. This also provides an internal param map to store
 * parameter values attached to the instance.
 */
trait Params extends Identifiable with Serializable {

  /**
   * Returns all params sorted by their names. The default implementation uses Java reflection to
   * list all public methods that have no arguments and return [[Param]].
   *
   * @note Developer should not use this method in constructor because we cannot guarantee that
   * this variable gets initialized before other params.
   */
  lazy val params: Array[Param[_]] = {
    val methods = this.getClass.getMethods
    methods.filter { m =>
        Modifier.isPublic(m.getModifiers) &&
          classOf[Param[_]].isAssignableFrom(m.getReturnType) &&
          m.getParameterTypes.isEmpty
      }.sortBy(_.getName)
      .map(m => m.invoke(this).asInstanceOf[Param[_]])
  }

  /**
   * Explains a param.
   * @param param input param, must belong to this instance.
   * @return a string that contains the input param name, doc, and optionally its default value and
   *         the user-supplied value
   */
  def explainParam(param: Param[_]): String = {
    shouldOwn(param)
    val valueStr = if (isDefined(param)) {
      val defaultValueStr = getDefault(param).map("default: " + _)
      val currentValueStr = get(param).map("current: " + _)
      (defaultValueStr ++ currentValueStr).mkString("(", ", ", ")")
    } else {
      "(undefined)"
    }
    s"${param.name}: ${param.doc} $valueStr"
  }

  /**
   * Explains all params of this instance. See `explainParam()`.
   */
  def explainParams(): String = {
    params.map(explainParam).mkString("\n")
  }

  /** Checks whether a param is explicitly set. */
  final def isSet(param: Param[_]): Boolean = {
    shouldOwn(param)
    paramMap.contains(param)
  }

  /** Checks whether a param is explicitly set or has a default value. */
  final def isDefined(param: Param[_]): Boolean = {
    shouldOwn(param)
    defaultParamMap.contains(param) || paramMap.contains(param)
  }

  /** Tests whether this instance contains a param with a given name. */
  def hasParam(paramName: String): Boolean = {
    params.exists(_.name == paramName)
  }

  /** Gets a param by its name. */
  def getParam(paramName: String): Param[Any] = {
    params.find(_.name == paramName).getOrElse {
      throw new NoSuchElementException(s"Param $paramName does not exist.")
    }.asInstanceOf[Param[Any]]
  }

  /**
   * Sets a parameter in the embedded param map.
   */
  final def set[T](param: Param[T], value: T): this.type = {
    set(param -> value)
  }

  /**
   * Sets a parameter (by name) in the embedded param map.
   */
  protected final def set(param: String, value: Any): this.type = {
    set(getParam(param), value)
  }

  /**
   * Sets a parameter in the embedded param map.
   */
  protected final def set(paramPair: ParamPair[_]): this.type = {
    shouldOwn(paramPair.param)
    paramMap.put(paramPair)
    this
  }

  /**
   * Optionally returns the user-supplied value of a param.
   */
  final def get[T](param: Param[T]): Option[T] = {
    shouldOwn(param)
    paramMap.get(param)
  }

  /**
   * Clears the user-supplied value for the input param.
   */
  final def clear(param: Param[_]): this.type = {
    shouldOwn(param)
    paramMap.remove(param)
    this
  }

  /**
   * Gets the value of a param in the embedded param map or its default value. Throws an exception
   * if neither is set.
   */
  final def getOrDefault[T](param: Param[T]): T = {
    shouldOwn(param)
    get(param).orElse(getDefault(param)).getOrElse(
      throw new NoSuchElementException(s"Failed to find a default value for ${param.name}"))
  }

  /**
   * An alias for `getOrDefault()`.
   */
  protected final def $[T](param: Param[T]): T = getOrDefault(param)

  /**
   * Sets a default value for a param.
   * @param param  param to set the default value. Make sure that this param is initialized before
   *               this method gets called.
   * @param value  the default value
   */
  protected final def setDefault[T](param: Param[T], value: T): this.type = {
    defaultParamMap.put(param -> value)
    this
  }

  /**
   * Sets default values for a list of params.
   *
   * Note: Java developers should use the single-parameter `setDefault`.
   *       Annotating this with varargs can cause compilation failures due to a Scala compiler bug.
   *       See SPARK-9268.
   *
   * @param paramPairs  a list of param pairs that specify params and their default values to set
   *                    respectively. Make sure that the params are initialized before this method
   *                    gets called.
   */
  protected final def setDefault(paramPairs: ParamPair[_]*): this.type = {
    paramPairs.foreach { p =>
      setDefault(p.param.asInstanceOf[Param[Any]], p.value)
    }
    this
  }

  /**
   * Gets the default value of a parameter.
   */
  final def getDefault[T](param: Param[T]): Option[T] = {
    shouldOwn(param)
    defaultParamMap.get(param)
  }

  /**
   * Tests whether the input param has a default value set.
   */
  final def hasDefault[T](param: Param[T]): Boolean = {
    shouldOwn(param)
    defaultParamMap.contains(param)
  }

  /**
   * Creates a copy of this instance with the same UID and some extra params.
   * Subclasses should implement this method and set the return type properly.
   * See `defaultCopy()`.
   */
  def copy(extra: ParamMap): Params

  /**
   * Default implementation of copy with extra params.
   * It tries to create a new instance with the same UID.
   * Then it copies the embedded and extra parameters over and returns the new instance.
   */
  protected final def defaultCopy[T <: Params](extra: ParamMap): T = {
    val that = this.getClass.getConstructor(classOf[String]).newInstance(uid)
    copyValues(that, extra).asInstanceOf[T]
  }

  /**
   * Extracts the embedded default param values and user-supplied values, and then merges them with
   * extra values from input into a flat param map, where the latter value is used if there exist
   * conflicts, i.e., with ordering:
   * default param values less than user-supplied values less than extra.
   */
  final def extractParamMap(extra: ParamMap): ParamMap = {
    defaultParamMap ++ paramMap ++ extra
  }

  /**
   * `extractParamMap` with no extra values.
   */
  final def extractParamMap(): ParamMap = {
    extractParamMap(ParamMap.empty)
  }

  /** Internal param map for user-supplied values. */
  private[ml] val paramMap: ParamMap = ParamMap.empty

  /** Internal param map for default values. */
  private[ml] val defaultParamMap: ParamMap = ParamMap.empty

  /** Validates that the input param belongs to this instance. */
  private def shouldOwn(param: Param[_]): Unit = {
    require(param.parent == uid && hasParam(param.name), s"Param $param does not belong to $this.")
  }

  /**
   * Copies param values from this instance to another instance for params shared by them.
   *
   * This handles default Params and explicitly set Params separately.
   * Default Params are copied from and to `defaultParamMap`, and explicitly set Params are
   * copied from and to `paramMap`.
   * Warning: This implicitly assumes that this [[Params]] instance and the target instance
   *          share the same set of default Params.
   *
   * @param to the target instance, which should work with the same set of default Params as this
   *           source instance
   * @param extra extra params to be copied to the target's `paramMap`
   * @return the target instance with param values copied
   */
  protected def copyValues[T <: Params](to: T, extra: ParamMap = ParamMap.empty): T = {
    val map = paramMap ++ extra
    params.foreach { param =>
      // copy default Params
      if (defaultParamMap.contains(param) && to.hasParam(param.name)) {
        to.defaultParamMap.put(to.getParam(param.name), defaultParamMap(param))
      }
      // copy explicitly set Params
      if (map.contains(param) && to.hasParam(param.name)) {
        to.set(param.name, map(param))
      }
    }
    to
  }
}

private[ml] object Params {
  /**
   * Sets a default param value for a `Params`.
   */
  private[ml] final def setDefault[T](params: Params, param: Param[T], value: T): Unit = {
    params.defaultParamMap.put(param -> value)
  }
}

/**
 * Java-friendly wrapper for [[Params]].
 * Java developers who need to extend [[Params]] should use this class instead.
 * If you need to extend an abstract class which already extends [[Params]], then that abstract
 * class should be Java-friendly as well.
 */
abstract class JavaParams extends Params

/**
 * A param to value map.
 */
@Since("1.2.0")
final class ParamMap private[ml] (private val map: mutable.Map[Param[Any], Any])
  extends Serializable {

  /* DEVELOPERS: About validating parameter values
   *   This and ParamPair are the only two collections of parameters.
   *   This class should always create ParamPairs when
   *   specifying new parameter values.  ParamPair will then call Param.validate().
   */

  /**
   * Creates an empty param map.
   */
  @Since("1.2.0")
  def this() = this(mutable.Map.empty)

  /**
   * Puts a (param, value) pair (overwrites if the input param exists).
   */
  @Since("1.2.0")
  def put[T](param: Param[T], value: T): this.type = put(param -> value)

  /**
   * Puts a list of param pairs (overwrites if the input params exists).
   */
  @varargs
  @Since("1.2.0")
  def put(paramPairs: ParamPair[_]*): this.type = {
    paramPairs.foreach { p =>
      map(p.param.asInstanceOf[Param[Any]]) = p.value
    }
    this
  }

  /** Put param pairs with a `java.util.List` of values for Python. */
  private[ml] def put(paramPairs: JList[ParamPair[_]]): this.type = {
    put(paramPairs.asScala.toSeq: _*)
  }

  /**
   * Optionally returns the value associated with a param.
   */
  @Since("1.2.0")
  def get[T](param: Param[T]): Option[T] = {
    map.get(param.asInstanceOf[Param[Any]]).asInstanceOf[Option[T]]
  }

  /**
   * Returns the value associated with a param or a default value.
   */
  @Since("1.4.0")
  def getOrElse[T](param: Param[T], default: T): T = {
    get(param).getOrElse(default)
  }

  /**
   * Gets the value of the input param or its default value if it does not exist.
   * Raises a NoSuchElementException if there is no value associated with the input param.
   */
  @Since("1.2.0")
  def apply[T](param: Param[T]): T = {
    get(param).getOrElse {
      throw new NoSuchElementException(s"Cannot find param ${param.name}.")
    }
  }

  /**
   * Checks whether a parameter is explicitly specified.
   */
  @Since("1.2.0")
  def contains(param: Param[_]): Boolean = {
    map.contains(param.asInstanceOf[Param[Any]])
  }

  /**
   * Removes a key from this map and returns its value associated previously as an option.
   */
  @Since("1.4.0")
  def remove[T](param: Param[T]): Option[T] = {
    map.remove(param.asInstanceOf[Param[Any]]).asInstanceOf[Option[T]]
  }

  /**
   * Filters this param map for the given parent.
   */
  @Since("1.2.0")
  def filter(parent: Params): ParamMap = {
    // Don't use filterKeys because mutable.Map#filterKeys
    // returns the instance of collections.Map, not mutable.Map.
    // Otherwise, we get ClassCastException.
    // Not using filterKeys also avoid SI-6654
    val filtered = map.filter { case (k, _) => k.parent == parent.uid }
    new ParamMap(filtered)
  }

  /**
   * Creates a copy of this param map.
   */
  @Since("1.2.0")
  def copy: ParamMap = new ParamMap(map.clone())

  @Since("1.2.0")
  override def toString: String = {
    map.toSeq.sortBy(_._1.name).map { case (param, value) =>
      s"\t${param.parent}-${param.name}: $value"
    }.mkString("{\n", ",\n", "\n}")
  }

  /**
   * Returns a new param map that contains parameters in this map and the given map,
   * where the latter overwrites this if there exist conflicts.
   */
  @Since("1.2.0")
  def ++(other: ParamMap): ParamMap = {
    // TODO: Provide a better method name for Java users.
    new ParamMap(this.map ++ other.map)
  }

  /**
   * Adds all parameters from the input param map into this param map.
   */
  @Since("1.2.0")
  def ++=(other: ParamMap): this.type = {
    // TODO: Provide a better method name for Java users.
    this.map ++= other.map
    this
  }

  /**
   * Converts this param map to a sequence of param pairs.
   */
  @Since("1.2.0")
  def toSeq: Seq[ParamPair[_]] = {
    map.toSeq.map { case (param, value) =>
      ParamPair(param, value)
    }
  }

  /** Java-friendly method for Python API */
  private[ml] def toList: java.util.List[ParamPair[_]] = {
    this.toSeq.asJava
  }

  /**
   * Number of param pairs in this map.
   */
  @Since("1.3.0")
  def size: Int = map.size
}

@Since("1.2.0")
object ParamMap {

  /**
   * Returns an empty param map.
   */
  @Since("1.2.0")
  def empty: ParamMap = new ParamMap()

  /**
   * Constructs a param map by specifying its entries.
   */
  @varargs
  @Since("1.2.0")
  def apply(paramPairs: ParamPair[_]*): ParamMap = {
    new ParamMap().put(paramPairs: _*)
  }
}
