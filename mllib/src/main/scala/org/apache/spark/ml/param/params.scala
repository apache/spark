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
import java.util.NoSuchElementException

import scala.annotation.varargs
import scala.collection.mutable

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.util.Identifiable

/**
 * :: AlphaComponent ::
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
@AlphaComponent
class Param[T] (val parent: Params, val name: String, val doc: String, val isValid: T => Boolean)
  extends Serializable {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue[T])

  /**
   * Assert that the given value is valid for this parameter.
   *
   * Note: Parameter checks involving interactions between multiple parameters should be
   *       implemented in [[Params.validateParams()]].  Checks for input/output columns should be
   *       implemented in [[org.apache.spark.ml.PipelineStage.transformSchema()]].
   *
   * DEVELOPERS: This method is only called by [[ParamPair]], which means that all parameters
   *             should be specified via [[ParamPair]].
   *
   * @throws IllegalArgumentException if the value is invalid
   */
  private[param] def validate(value: T): Unit = {
    if (!isValid(value)) {
      throw new IllegalArgumentException(s"$parent parameter $name given invalid value $value." +
        s" Parameter description: $toString")
    }
  }

  /**
   * Creates a param pair with the given value (for Java).
   */
  def w(value: T): ParamPair[T] = this -> value

  /**
   * Creates a param pair with the given value (for Scala).
   */
  def ->(value: T): ParamPair[T] = ParamPair(this, value)

  /**
   * Converts this param's name, doc, and optionally its default value and the user-supplied
   * value in its parent to string.
   */
  override def toString: String = {
    val valueStr = if (parent.isDefined(this)) {
      val defaultValueStr = parent.getDefault(this).map("default: " + _)
      val currentValueStr = parent.get(this).map("current: " + _)
      (defaultValueStr ++ currentValueStr).mkString("(", ", ", ")")
    } else {
      "(undefined)"
    }
    s"$name: $doc $valueStr"
  }
}

/**
 * Factory methods for common validation functions for [[Param.isValid]].
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

  /** Check if value > lowerBound */
  def gt[T](lowerBound: Double): T => Boolean = { (value: T) =>
    getDouble(value) > lowerBound
  }

  /** Check if value >= lowerBound */
  def gtEq[T](lowerBound: Double): T => Boolean = { (value: T) =>
    getDouble(value) >= lowerBound
  }

  /** Check if value < upperBound */
  def lt[T](upperBound: Double): T => Boolean = { (value: T) =>
    getDouble(value) < upperBound
  }

  /** Check if value <= upperBound */
  def ltEq[T](upperBound: Double): T => Boolean = { (value: T) =>
    getDouble(value) <= upperBound
  }

  /**
   * Check for value in range lowerBound to upperBound.
   * @param lowerInclusive  If true, check for value >= lowerBound.
   *                        If false, check for value > lowerBound.
   * @param upperInclusive  If true, check for value <= upperBound.
   *                        If false, check for value < upperBound.
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

  /** Version of [[inRange()]] which uses inclusive be default: [lowerBound, upperBound] */
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
}

// specialize primitive-typed params because Java doesn't recognize scala.Double, scala.Int, ...

/** Specialized version of [[Param[Double]]] for Java. */
class DoubleParam(parent: Params, name: String, doc: String, isValid: Double => Boolean)
  extends Param[Double](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  override def w(value: Double): ParamPair[Double] = super.w(value)
}

/** Specialized version of [[Param[Int]]] for Java. */
class IntParam(parent: Params, name: String, doc: String, isValid: Int => Boolean)
  extends Param[Int](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  override def w(value: Int): ParamPair[Int] = super.w(value)
}

/** Specialized version of [[Param[Float]]] for Java. */
class FloatParam(parent: Params, name: String, doc: String, isValid: Float => Boolean)
  extends Param[Float](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  override def w(value: Float): ParamPair[Float] = super.w(value)
}

/** Specialized version of [[Param[Long]]] for Java. */
class LongParam(parent: Params, name: String, doc: String, isValid: Long => Boolean)
  extends Param[Long](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  override def w(value: Long): ParamPair[Long] = super.w(value)
}

/** Specialized version of [[Param[Boolean]]] for Java. */
class BooleanParam(parent: Params, name: String, doc: String) // No need for isValid
  extends Param[Boolean](parent, name, doc) {

  override def w(value: Boolean): ParamPair[Boolean] = super.w(value)
}

/**
 * A param amd its value.
 */
case class ParamPair[T](param: Param[T], value: T) {
  // This is *the* place Param.validate is called.  Whenever a parameter is specified, we should
  // always construct a ParamPair so that validate is called.
  param.validate(value)
}

/**
 * :: AlphaComponent ::
 * Trait for components that take parameters. This also provides an internal param map to store
 * parameter values attached to the instance.
 */
@AlphaComponent
trait Params extends Identifiable with Serializable {

  /**
   * Returns all params sorted by their names. The default implementation uses Java reflection to
   * list all public methods that have no arguments and return [[Param]].
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
   * Validates parameter values stored internally plus the input parameter map.
   * Raises an exception if any parameter is invalid.
   *
   * This only needs to check for interactions between parameters.
   * Parameter value checks which do not depend on other parameters are handled by
   * [[Param.validate()]].  This method does not handle input/output column parameters;
   * those are checked during schema validation.
   */
  def validateParams(paramMap: ParamMap): Unit = {
    copy(paramMap).validateParams()
  }

  /**
   * Validates parameter values stored internally.
   * Raise an exception if any parameter value is invalid.
   *
   * This only needs to check for interactions between parameters.
   * Parameter value checks which do not depend on other parameters are handled by
   * [[Param.validate()]].  This method does not handle input/output column parameters;
   * those are checked during schema validation.
   */
  def validateParams(): Unit = {
    params.filter(isDefined _).foreach { param =>
      param.asInstanceOf[Param[Any]].validate($(param))
    }
  }

  /**
   * Returns the documentation of all params.
   */
  def explainParams(): String = params.mkString("\n")

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
  protected final def set[T](param: Param[T], value: T): this.type = {
    shouldOwn(param)
    paramMap.put(param.asInstanceOf[Param[Any]], value)
    this
  }

  /**
   * Sets a parameter (by name) in the embedded param map.
   */
  protected final def set(param: String, value: Any): this.type = {
    set(getParam(param), value)
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
  protected final def clear(param: Param[_]): this.type = {
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
    get(param).orElse(getDefault(param)).get
  }

  /** An alias for [[getOrDefault()]]. */
  protected final def $[T](param: Param[T]): T = getOrDefault(param)

  /**
   * Sets a default value for a param.
   * @param param  param to set the default value. Make sure that this param is initialized before
   *               this method gets called.
   * @param value  the default value
   */
  protected final def setDefault[T](param: Param[T], value: T): this.type = {
    shouldOwn(param)
    defaultParamMap.put(param, value)
    this
  }

  /**
   * Sets default values for a list of params.
   *
   * Note: Java developers should use the single-parameter [[setDefault()]].
   *       Annotating this with varargs causes compilation failures.
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
   * Creates a copy of this instance with a randomly generated uid and some extra params.
   * The default implementation calls the default constructor to create a new instance, then
   * copies the embedded and extra parameters over and returns the new instance.
   * Subclasses should override this method if the default approach is not sufficient.
   */
  def copy(extra: ParamMap): Params = {
    val that = this.getClass.newInstance()
    copyValues(that, extra)
    that
  }

  /**
   * Extracts the embedded default param values and user-supplied values, and then merges them with
   * extra values from input into a flat param map, where the latter value is used if there exist
   * conflicts, i.e., with ordering: default param values < user-supplied values < extraParamMap.
   */
  final def extractParamMap(extraParamMap: ParamMap): ParamMap = {
    defaultParamMap ++ paramMap ++ extraParamMap
  }

  /**
   * [[extractParamMap]] with no extra values.
   */
  final def extractParamMap(): ParamMap = {
    extractParamMap(ParamMap.empty)
  }

  /** Internal param map for user-supplied values. */
  private val paramMap: ParamMap = ParamMap.empty

  /** Internal param map for default values. */
  private val defaultParamMap: ParamMap = ParamMap.empty

  /** Validates that the input param belongs to this instance. */
  private def shouldOwn(param: Param[_]): Unit = {
    require(param.parent.eq(this), s"Param $param does not belong to $this.")
  }

  /**
   * Copies param values from this instance to another instance for params shared by them.
   * @param to the target instance
   * @param extra extra params to be copied
   * @return the target instance with param values copied
   */
  protected def copyValues[T <: Params](to: T, extra: ParamMap = ParamMap.empty): T = {
    val map = extractParamMap(extra)
    params.foreach { param =>
      if (map.contains(param) && to.hasParam(param.name)) {
        to.set(param.name, map(param))
      }
    }
    to
  }
}

/**
 * Java-friendly wrapper for [[Params]].
 * Java developers who need to extend [[Params]] should use this class instead.
 * If you need to extend a abstract class which already extends [[Params]], then that abstract
 * class should be Java-friendly as well.
 */
abstract class JavaParams extends Params

/**
 * :: AlphaComponent ::
 * A param to value map.
 */
@AlphaComponent
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
  def this() = this(mutable.Map.empty)

  /**
   * Puts a (param, value) pair (overwrites if the input param exists).
   */
  def put[T](param: Param[T], value: T): this.type = put(ParamPair(param, value))

  /**
   * Puts a list of param pairs (overwrites if the input params exists).
   */
  @varargs
  def put(paramPairs: ParamPair[_]*): this.type = {
    paramPairs.foreach { p =>
      map(p.param.asInstanceOf[Param[Any]]) = p.value
    }
    this
  }

  /**
   * Optionally returns the value associated with a param.
   */
  def get[T](param: Param[T]): Option[T] = {
    map.get(param.asInstanceOf[Param[Any]]).asInstanceOf[Option[T]]
  }

  /**
   * Returns the value associated with a param or a default value.
   */
  def getOrElse[T](param: Param[T], default: T): T = {
    get(param).getOrElse(default)
  }

  /**
   * Gets the value of the input param or its default value if it does not exist.
   * Raises a NoSuchElementException if there is no value associated with the input param.
   */
  def apply[T](param: Param[T]): T = {
    get(param).getOrElse {
      throw new NoSuchElementException(s"Cannot find param ${param.name}.")
    }
  }

  /**
   * Checks whether a parameter is explicitly specified.
   */
  def contains(param: Param[_]): Boolean = {
    map.contains(param.asInstanceOf[Param[Any]])
  }

  /**
   * Removes a key from this map and returns its value associated previously as an option.
   */
  def remove[T](param: Param[T]): Option[T] = {
    map.remove(param.asInstanceOf[Param[Any]]).asInstanceOf[Option[T]]
  }

  /**
   * Filters this param map for the given parent.
   */
  def filter(parent: Params): ParamMap = {
    val filtered = map.filterKeys(_.parent == parent)
    new ParamMap(filtered.asInstanceOf[mutable.Map[Param[Any], Any]])
  }

  /**
   * Creates a copy of this param map.
   */
  def copy: ParamMap = new ParamMap(map.clone())

  override def toString: String = {
    map.toSeq.sortBy(_._1.name).map { case (param, value) =>
      s"\t${param.parent.uid}-${param.name}: $value"
    }.mkString("{\n", ",\n", "\n}")
  }

  /**
   * Returns a new param map that contains parameters in this map and the given map,
   * where the latter overwrites this if there exist conflicts.
   */
  def ++(other: ParamMap): ParamMap = {
    // TODO: Provide a better method name for Java users.
    new ParamMap(this.map ++ other.map)
  }

  /**
   * Adds all parameters from the input param map into this param map.
   */
  def ++=(other: ParamMap): this.type = {
    // TODO: Provide a better method name for Java users.
    this.map ++= other.map
    this
  }

  /**
   * Converts this param map to a sequence of param pairs.
   */
  def toSeq: Seq[ParamPair[_]] = {
    map.toSeq.map { case (param, value) =>
      ParamPair(param, value)
    }
  }

  /**
   * Number of param pairs in this map.
   */
  def size: Int = map.size
}

object ParamMap {

  /**
   * Returns an empty param map.
   */
  def empty: ParamMap = new ParamMap()

  /**
   * Constructs a param map by specifying its entries.
   */
  @varargs
  def apply(paramPairs: ParamPair[_]*): ParamMap = {
    new ParamMap().put(paramPairs: _*)
  }
}
