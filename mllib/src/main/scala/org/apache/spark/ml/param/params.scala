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

import org.apache.spark.ml.Identifiable

/**
 * A param with self-contained documentation and optionally default value. Primitive-typed param
 * should use the specialized versions, which are more friendly to Java users.
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
  def w(value: T): ParamPair[T] = this -> value

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

/** Specialized version of [[Param[Double]]] for Java. */
class DoubleParam(parent: Params, name: String, doc: String, default: Option[Double] = None)
    extends Param[Double](parent, name, doc, default) {
  override def w(value: Double): ParamPair[Double] = super.w(value)
}

/** Specialized version of [[Param[Int]]] for Java. */
class IntParam(parent: Params, name: String, doc: String, default: Option[Int] = None)
    extends Param[Int](parent, name, doc, default) {
  override def w(value: Int): ParamPair[Int] = super.w(value)
}

/** Specialized version of [[Param[Float]]] for Java. */
class FloatParam(parent: Params, name: String, doc: String, default: Option[Float] = None)
    extends Param[Float](parent, name, doc, default) {
  override def w(value: Float): ParamPair[Float] = super.w(value)
}

/** Specialized version of [[Param[Long]]] for Java. */
class LongParam(parent: Params, name: String, doc: String, default: Option[Long] = None)
    extends Param[Long](parent, name, doc, default) {
  override def w(value: Long): ParamPair[Long] = super.w(value)
}

/** Specialized version of [[Param[Boolean]]] for Java. */
class BooleanParam(parent: Params, name: String, doc: String, default: Option[Boolean] = None)
    extends Param[Boolean](parent, name, doc, default) {
  override def w(value: Boolean): ParamPair[Boolean] = super.w(value)
}

/**
 * A param amd its value.
 */
case class ParamPair[T](param: Param[T], value: T)

/**
 * Trait for components that take parameters. This also provides an internal param map to store
 * parameter values attached to the instance.
 */
trait Params extends Identifiable with Serializable {

  /** Returns all params. */
  def params: Array[Param[_]] = {
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
  def isSet(param: Param[_]): Boolean = {
    require(param.parent.eq(this))
    paramMap.contains(param)
  }

  /** Gets a param by its name. */
  private[ml] def getParam(paramName: String): Param[Any] = {
    val m = this.getClass.getMethod(paramName)
    assert(Modifier.isPublic(m.getModifiers) &&
      classOf[Param[_]].isAssignableFrom(m.getReturnType))
    m.invoke(this).asInstanceOf[Param[Any]]
  }

  /**
   * Internal param map.
   */
  protected val paramMap: ParamMap = ParamMap.empty

  /**
   * Sets a parameter in the own parameter map.
   */
  protected def set[T](param: Param[T], value: T): this.type = {
    require(param.parent.eq(this))
    paramMap.put(param.asInstanceOf[Param[Any]], value)
    this
  }

  /**
   * Gets the value of a parameter.
   */
  protected def get[T](param: Param[T]): T = {
    require(param.parent.eq(this))
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
  def ++(other: ParamMap): ParamMap = {
    new ParamMap(this.map ++ other.map)
  }


  /**
   * Adds all parameters from the input param map into this param map.
   */
  def ++=(other: ParamMap): this.type = {
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
