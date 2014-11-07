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

import scala.annotation.varargs
import scala.collection.mutable
import scala.language.implicitConversions

import org.apache.spark.ml.Identifiable

/**
 * A param with self-contained documentation and optionally default value.
 *
 * @param parent parent object
 * @param name param name
 * @param doc documentation
 * @tparam T param value type
 */
class Param[T] (
    val parent: Params,
    val name: String,
    val doc: String,
    val default: Option[T] = None) extends Serializable {

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

// specialize primitive-typed params because Java doesn't recognize scala.Double, scala.Int, ...

class DoubleParam(parent: Params, name: String, doc: String, default: Option[Double] = None)
    extends Param[Double](parent, name, doc, default) {
  override def w(value: Double): ParamPair[Double] = ParamPair(this, value)
}

class IntParam(parent: Params, name: String, doc: String, default: Option[Int] = None)
    extends Param[Int](parent, name, doc, default) {
  override def w(value: Int): ParamPair[Int] = ParamPair(this, value)
}

class FloatParam(parent: Params, name: String, doc: String, default: Option[Float] = None)
    extends Param[Float](parent, name, doc, default) {
  override def w(value: Float): ParamPair[Float] = ParamPair(this, value)
}

class LongParam(parent: Params, name: String, doc: String, default: Option[Long] = None)
    extends Param[Long](parent, name, doc, default) {
  override def w(value: Long): ParamPair[Long] = ParamPair(this, value)
}

class BooleanParam(parent: Params, name: String, doc: String, default: Option[Boolean] = None)
    extends Param[Boolean](parent, name, doc, default) {
  override def w(value: Boolean): ParamPair[Boolean] = ParamPair(this, value)
}

/**
 * A param amd its value.
 */
case class ParamPair[T](param: Param[T], value: T)

/**
 * Trait for components that take parameters. This also provides an internal param map to store
 * parameter values attached to the instance.
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

  /**
   * Validates parameters specified by the input parameter map.
   * Raises an exception if any parameter belongs to this object is invalid.
   */
  def validate(paramMap: ParamMap): Unit = {}

  /**
   * Returns the documentation of all params.
   */
  def explainParams(): String = params.mkString("\n")

  /** Gets a param by its name. */
  private[ml] def getParam(paramName: String): Param[Any] = {
    val m = this.getClass.getMethod(paramName)
    assert(Modifier.isPublic(m.getModifiers) &&
      classOf[Param[_]].isAssignableFrom(m.getReturnType))
    m.invoke(this).asInstanceOf[Param[Any]]
  }

  /** Checks whether a param is explicitly set. */
  private[ml] def isSet(param: Param[_]): Boolean = paramMap.contains(param)

  /**
   * Internal param map.
   */
  protected val paramMap: ParamMap = ParamMap.empty

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
   * Returns an empty Params implementation without any params.
   */
  val empty: Params = new Params {}

  /**
   * Copy parameter values that are explicitly set from one Params instance to another.
   */
  private[ml] def copyValues[F <: Params, T <: F](from: F, to: T): Unit = {
    from.params.foreach { param =>
      if (from.isSet(param)) {
        to.set(to.getParam(param.name), from.get(param))
      }
    }
  }
}

/**
 * A param to value map.
 */
class ParamMap private[ml] (private val map: mutable.Map[Param[Any], Any]) extends Serializable {

  /**
   * Creates an empty param map.
   */
  def this() = this(mutable.Map.empty[Param[Any], Any])

  /**
   * Puts a (param, value) pair (overwrites if the input param exists).
   */
  def put[T](param: Param[T], value: T): this.type = {
    map(param.asInstanceOf[Param[Any]]) = value
    this
  }

  /**
   * Puts a param pair (overwrites if the input param exists).
   */
  def put(paramPairs: ParamPair[_]*): this.type = {
    paramPairs.foreach { p =>
      put(p.param.asInstanceOf[Param[Any]], p.value)
    }
    this
  }

  /**
   * Optionally returns the value associated with a param or its default.
   */
  def get[T](param: Param[T]): Option[T] = {
    map.get(param.asInstanceOf[Param[Any]])
      .orElse(param.default)
      .asInstanceOf[Option[T]]
  }

  /**
   * Gets the value of the input param or its default value if it does not exist.
   * Raises a NoSuchElementException if there is no value associated with the input param.
   */
  def apply[T](param: Param[T]): T = {
    val value = get(param)
    if (value.isDefined) {
      value.get
    } else {
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
   * Filters this param map for the given parent.
   */
  def filter(parent: Params): ParamMap = {
    val filtered = map.filterKeys(_.parent == parent)
    new ParamMap(filtered.asInstanceOf[mutable.Map[Param[Any], Any]])
  }

  /**
   * Make a copy of this param map.
   */
  def copy: ParamMap = new ParamMap(map.clone())

  override def toString: String = {
    map.map { case (param, value) =>
      s"\t${param.parent.uid}-${param.name}: $value"
    }.mkString("{\n", ",\n", "\n}")
  }

  /**
   * Returns a new param map that contains parameters in this map and the given map,
   * where the latter overwrites this if there exists conflicts.
   */
  private[ml] def ++(other: ParamMap): ParamMap = {
    new ParamMap(this.map ++ other.map)
  }


  private[ml] def ++=(other: ParamMap): this.type = {
    this.map ++= other.map
    this
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

  private val fixedParamMap = ParamMap.empty
  private val paramGrid = mutable.Map.empty[Param[_], Iterable[_]]

  /**
   * Builds with fixed parameters.
   */
  def withFixed(paramMap: ParamMap): this.type = {
    fixedParamMap ++= paramMap
    this
  }

  /**
   * Builds with fixed parameters.
   */
  @varargs
  def withFixed(paramPairs: ParamPair[_]*): this.type = {
    fixedParamMap.put(paramPairs: _*)
    this
  }

  /**
   * Adds a param with multiple values (overwrites if the input param exists).
   */
  def addMulti[T](param: Param[T], values: Iterable[T]): this.type = {
    paramGrid.put(param, values)
    this
  }

  /**
   * Adds a double param with multiple values.
   */
  def addMulti(param: DoubleParam, values: Array[Double]): this.type = {
    addMulti[Double](param, values)
  }

  /**
   * Adds a int param with multiple values.
   */
  def addMulti(param: IntParam, values: Array[Int]): this.type = {
    addMulti[Int](param, values)
  }

  /**
   * Adds a float param with multiple values.
   */
  def addMulti(param: FloatParam, values: Array[Float]): this.type = {
    addMulti[Float](param, values)
  }

  /**
   * Adds a long param with multiple values.
   */
  def addMulti(param: LongParam, values: Array[Long]): this.type = {
    addMulti[Long](param, values)
  }

  /**
   * Adds a boolean param with true and false.
   */
  def addMulti(param: BooleanParam): this.type = {
    addMulti[Boolean](param, Array(true, false))
  }

  /**
   * Builds and returns all combinations of parameters specified by the param grid.
   */
  def build(): Array[ParamMap] = {
    var paramMaps = Array(new ParamMap)
    paramGrid.foreach { case (param, values) =>
      val newParamMaps = values.flatMap { v =>
        paramMaps.map(_.copy.put(param.asInstanceOf[Param[Any]], v))
      }
      paramMaps = newParamMaps.toArray
    }
    paramMaps.map(_ ++= fixedParamMap)
  }
}
