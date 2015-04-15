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

import org.apache.spark.annotation.{AlphaComponent, DeveloperApi}
import org.apache.spark.ml.Identifiable

/**
 * :: AlphaComponent ::
 * A param with self-contained documentation and optionally default value. Primitive-typed param
 * should use the specialized versions, which are more friendly to Java users.
 *
 * @param parent parent object
 * @param name param name
 * @param doc documentation
 * @tparam T param value type
 */
@AlphaComponent
class Param[T] (val parent: Params, val name: String, val doc: String) extends Serializable {

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

// specialize primitive-typed params because Java doesn't recognize scala.Double, scala.Int, ...

/** Specialized version of [[Param[Double]]] for Java. */
class DoubleParam(parent: Params, name: String, doc: String)
  extends Param[Double](parent, name, doc) {

  override def w(value: Double): ParamPair[Double] = super.w(value)
}

/** Specialized version of [[Param[Int]]] for Java. */
class IntParam(parent: Params, name: String, doc: String)
  extends Param[Int](parent, name, doc) {

  override def w(value: Int): ParamPair[Int] = super.w(value)
}

/** Specialized version of [[Param[Float]]] for Java. */
class FloatParam(parent: Params, name: String, doc: String)
  extends Param[Float](parent, name, doc) {

  override def w(value: Float): ParamPair[Float] = super.w(value)
}

/** Specialized version of [[Param[Long]]] for Java. */
class LongParam(parent: Params, name: String, doc: String)
  extends Param[Long](parent, name, doc) {

  override def w(value: Long): ParamPair[Long] = super.w(value)
}

/** Specialized version of [[Param[Boolean]]] for Java. */
class BooleanParam(parent: Params, name: String, doc: String)
  extends Param[Boolean](parent, name, doc) {

  override def w(value: Boolean): ParamPair[Boolean] = super.w(value)
}

/**
 * A param amd its value.
 */
case class ParamPair[T](param: Param[T], value: T)

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
   */
  def validate(paramMap: ParamMap): Unit = {}

  /**
   * Validates parameter values stored internally.
   * Raise an exception if any parameter value is invalid.
   */
  def validate(): Unit = validate(ParamMap.empty)

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
   * Extracts the embedded default param values and user-supplied values, and then merges them with
   * extra values from input into a flat param map, where the latter value is used if there exist
   * conflicts, i.e., with ordering: default param values < user-supplied values < extraParamMap.
   */
  protected final def extractParamMap(extraParamMap: ParamMap): ParamMap = {
    defaultParamMap ++ paramMap ++ extraParamMap
  }

  /**
   * [[extractParamMap]] with no extra values.
   */
  protected final def extractParamMap(): ParamMap = {
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
}

/**
 * :: DeveloperApi ::
 *
 * Helper functionality for developers.
 *
 * NOTE: This is currently private[spark] but will be made public later once it is stabilized.
 */
@DeveloperApi
private[spark] object Params {

  /**
   * Copies parameter values from the parent estimator to the child model it produced.
   * @param paramMap the param map that holds parameters of the parent
   * @param parent the parent estimator
   * @param child the child model
   */
  def inheritValues[E <: Params, M <: E](
      paramMap: ParamMap,
      parent: E,
      child: M): Unit = {
    val childParams = child.params.map(_.name).toSet
    parent.params.foreach { param =>
      if (paramMap.contains(param) && childParams.contains(param.name)) {
        child.set(child.getParam(param.name), paramMap(param))
      }
    }
  }
}

/**
 * :: AlphaComponent ::
 * A param to value map.
 */
@AlphaComponent
final class ParamMap private[ml] (private val map: mutable.Map[Param[Any], Any])
  extends Serializable {

  /**
   * Creates an empty param map.
   */
  def this() = this(mutable.Map.empty)

  /**
   * Puts a (param, value) pair (overwrites if the input param exists).
   */
  def put[T](param: Param[T], value: T): this.type = {
    map(param.asInstanceOf[Param[Any]]) = value
    this
  }

  /**
   * Puts a list of param pairs (overwrites if the input params exists).
   */
  @varargs
  def put(paramPairs: ParamPair[_]*): this.type = {
    paramPairs.foreach { p =>
      put(p.param.asInstanceOf[Param[Any]], p.value)
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
