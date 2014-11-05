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

import scala.collection.mutable
import scala.language.implicitConversions

/**
 * A param with self-contained documentation and optionally default value.
 *
 * @param parent parent object
 * @param name param name
 * @param doc documentation
 * @tparam T param value type
 */
class Param[T] private (
    val parent: Params,
    val name: String,
    val doc: String,
    val default: Option[T]) extends Serializable {

  /**
   * Creates a param without a default value.
   *
   * @param parent parent object
   * @param name param name
   * @param doc documentation
   */
  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, None)

  /**
   * Creates a param with a default value.
   *
   * @param parent parent object
   * @param name param name
   * @param doc documentation
   * @param default default value
   */
  def this(parent: Params, name: String, doc: String, default: T) =
    this(parent, name, doc, Some(default))

  /**
   * Creates a param pair with the given value (for Java).
   */
  def w(value: T): ParamPair[T] = ParamPair(this, value)

  /**
   * Creates a param pair with the given value (for Scala).
   */
  def ->(value: T): ParamPair[T] = ParamPair(this, value)

  override def toString: String = {
    if (default.isDefined) {
      s"$name: $doc (default: ${default.get})"
    } else {
      s"$name: $doc"
    }
  }
}

/**
 * A param amd its value.
 */
case class ParamPair[T](param: Param[T], value: T)

/**
 * Trait for components that take parameters.
 */
trait Params extends Identifiable {

  /** Returns all params. */
  def params: Array[Param[_]] = {
    val methods = this.getClass.getMethods
    methods.filter { m =>
      Modifier.isPublic(m.getModifiers) &&
        classOf[Param[_]].isAssignableFrom(m.getReturnType) &&
        m.getParameterTypes.isEmpty
    }.map(m => m.invoke(this).asInstanceOf[Param[_]])
  }

  /** Gets a param by its name. */
  def getParam(paramName: String): Param[Any] = {
    val m = this.getClass.getMethod(paramName)
    assert(Modifier.isPublic(m.getModifiers) &&
      classOf[Param[_]].isAssignableFrom(m.getReturnType))
    m.invoke(this).asInstanceOf[Param[Any]]
  }

  /**
   * Validates parameters specified by the input parameter map.
   * Raises an exception if any parameter belongs to this object is invalid.
   */
  def validateParams(paramMap: ParamMap): Unit = {}

  /**
   * Returns the documentation of all params.
   */
  def explainParams(): String = params.mkString("\n")

  /**
   * Internal param map.
   */
  val paramMap: ParamMap = ParamMap.empty

  /**
   * Sets a parameter in the own parameter map.
   */
  protected def set[T](param: Param[T], value: T): this.type = {
    paramMap.put(param.asInstanceOf[Param[Any]], value)
    this
  }

  protected def get[T](param: Param[T]): T = {
    paramMap(param)
  }
}

private[ml] object Params {

  /**
   * Returns a Params implementation without any
   */
  val empty: Params = new Params {
    override def params: Array[Param[_]] = Array.empty
  }
}

/**
 * A param to value map.
 */
class ParamMap private[ml] (
    private val params: mutable.Map[Param[Any], Any]) extends Serializable {

  /**
   * Creates an empty param map.
   */
  def this() = this(mutable.Map.empty[Param[Any], Any])

  /**
   * Puts a (param, value) pair (overwrites if the input param exists).
   */
  def put[T](param: Param[T], value: T): this.type = {
    params(param.asInstanceOf[Param[Any]]) = value
    this
  }

  /**
   * Puts a param pair (overwrites if the input param exists).
   */
  def put(firstParamPair: ParamPair[_], otherParamPairs: ParamPair[_]*): this.type = {
    put(firstParamPair.param.asInstanceOf[Param[Any]], firstParamPair.value)
    otherParamPairs.foreach { p =>
      put(p.param.asInstanceOf[Param[Any]], p.value)
    }
    this
  }

  def get[T](param: Param[T]): Option[T] = {
    params.get(param.asInstanceOf[Param[Any]]).asInstanceOf[Option[T]]
  }

  /**
   * Gets the value of the input param or the default value if it does not exist.
   * Raises a NoSuchElementException if there is no value associated with the input param.
   */
  def apply[T](param: Param[T]): T = {
    val value = params.get(param.asInstanceOf[Param[Any]]).orElse(param.default)
    if (value.isDefined) {
      value.get.asInstanceOf[T]
    } else {
      throw new NoSuchElementException(s"Cannot find param ${param.name}.")
    }
  }

  /**
   * Checks whether a parameter is specified.
   */
  def contains(param: Param[_]): Boolean = {
    params.contains(param.asInstanceOf[Param[Any]])
  }

  /**
   * Filter this param map for the given parent.
   */
  def filter(parent: Identifiable): ParamMap = {
    val map = params.filterKeys(_.parent == parent)
    new ParamMap(map.asInstanceOf[mutable.Map[Param[Any], Any]])
  }

  /**
   * Make a deep copy of this param map.
   */
  def copy: ParamMap = new ParamMap(params.clone())

  override def toString: String = {
    params.map { case (param, value) =>
      s"\t${param.parent.uid}-${param.name}: $value"
    }.mkString("{\n", ",\n", "\n}")
  }

  /**
   * Returns a new param map that contains parameters in this map and the given map,
   * where the latter overwrites this if there exists conflicts.
   */
  private[ml] def ++(other: ParamMap): ParamMap = {
    new ParamMap(this.params ++ other.params)
  }

  /**
   * Implicitly maps a param to its value defined in the map or its default value.
   */
  private[ml] implicit def implicitMapping[T](param: Param[T]): T = apply(param)
}

object ParamMap {

  /**
   * Returns an empty param map.
   */
  def empty: ParamMap = new ParamMap()
}

/**
 * Builder for a param grid used in grid search.
 */
class ParamGridBuilder {

  private val paramGrid = mutable.Map.empty[Param[_], Iterable[_]]

  /**
   * Adds a param with a single value (overwrites if the input param exists).
   */
  def add[T](param: Param[T], value: T): this.type = {
    paramGrid.put(param, Seq(value))
    this
  }

  /**
   * Adds a param with multiple values (overwrites if the input param exists).
   */
  def addMulti[T](param: Param[T], values: Iterable[T]): this.type = {
    paramGrid.put(param, values)
    this
  }

  def build(): Array[ParamMap] = {
    var paramSets = Array(new ParamMap)
    paramGrid.foreach { case (param, values) =>
      val newParamSets = values.flatMap { v =>
        paramSets.map(_.copy.put(param.asInstanceOf[Param[Any]], v))
      }
      paramSets = newParamSets.toArray
    }
    paramSets
  }
}
