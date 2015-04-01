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

import scala.annotation.varargs
import scala.collection.mutable

import java.lang.reflect.Modifier

import org.apache.spark.annotation.{AlphaComponent, DeveloperApi}
import org.apache.spark.ml.Identifiable
import org.apache.spark.sql.types.{DataType, StructField, StructType}


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
class Param[T] (
    val parent: Params,
    val name: String,
    val doc: String,
    val defaultValue: Option[T] = None)
  extends Serializable {

  /**
   * Creates a param pair with the given value (for Java).
   */
  def w(value: T): ParamPair[T] = this -> value

  /**
   * Creates a param pair with the given value (for Scala).
   */
  def ->(value: T): ParamPair[T] = ParamPair(this, value)

  override def toString: String = {
    if (defaultValue.isDefined) {
      s"$name: $doc (default: ${defaultValue.get})"
    } else {
      s"$name: $doc"
    }
  }
}

// specialize primitive-typed params because Java doesn't recognize scala.Double, scala.Int, ...

/** Specialized version of [[Param[Double]]] for Java. */
class DoubleParam(parent: Params, name: String, doc: String, defaultValue: Option[Double])
  extends Param[Double](parent, name, doc, defaultValue) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, None)

  override def w(value: Double): ParamPair[Double] = super.w(value)
}

/** Specialized version of [[Param[Int]]] for Java. */
class IntParam(parent: Params, name: String, doc: String, defaultValue: Option[Int])
  extends Param[Int](parent, name, doc, defaultValue) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, None)

  override def w(value: Int): ParamPair[Int] = super.w(value)
}

/** Specialized version of [[Param[Float]]] for Java. */
class FloatParam(parent: Params, name: String, doc: String, defaultValue: Option[Float])
  extends Param[Float](parent, name, doc, defaultValue) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, None)

  override def w(value: Float): ParamPair[Float] = super.w(value)
}

/** Specialized version of [[Param[Long]]] for Java. */
class LongParam(parent: Params, name: String, doc: String, defaultValue: Option[Long])
  extends Param[Long](parent, name, doc, defaultValue) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, None)

  override def w(value: Long): ParamPair[Long] = super.w(value)
}

/** Specialized version of [[Param[Boolean]]] for Java. */
class BooleanParam(parent: Params, name: String, doc: String, defaultValue: Option[Boolean])
  extends Param[Boolean](parent, name, doc, defaultValue) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, None)

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
      classOf[Param[_]].isAssignableFrom(m.getReturnType) &&
      m.getParameterTypes.isEmpty)
    m.invoke(this).asInstanceOf[Param[Any]]
  }

  /**
   * Sets a parameter in the embedded param map.
   */
  protected def set[T](param: Param[T], value: T): this.type = {
    require(param.parent.eq(this))
    paramMap.put(param.asInstanceOf[Param[Any]], value)
    this
  }

  /**
   * Sets a parameter (by name) in the embedded param map.
   */
  private[ml] def set(param: String, value: Any): this.type = {
    set(getParam(param), value)
  }

  /**
   * Gets the value of a parameter in the embedded param map.
   */
  protected def get[T](param: Param[T]): T = {
    require(param.parent.eq(this))
    paramMap(param)
  }

  /**
   * Internal param map.
   */
  protected val paramMap: ParamMap = ParamMap.empty

  /**
   * Check whether the given schema contains an input column.
   * @param colName  Parameter name for the input column.
   * @param dataType  SQL DataType of the input column.
   */
  protected def checkInputColumn(schema: StructType, colName: String, dataType: DataType): Unit = {
    val actualDataType = schema(colName).dataType
    require(actualDataType.equals(dataType),
      s"Input column $colName must be of type $dataType" +
        s" but was actually $actualDataType.  Column param description: ${getParam(colName)}")
  }

  protected def addOutputColumn(
      schema: StructType,
      colName: String,
      dataType: DataType): StructType = {
    if (colName.length == 0) return schema
    val fieldNames = schema.fieldNames
    require(!fieldNames.contains(colName), s"Prediction column $colName already exists.")
    val outputFields = schema.fields ++ Seq(StructField(colName, dataType, nullable = false))
    StructType(outputFields)
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
    parent.params.foreach { param =>
      if (paramMap.contains(param)) {
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
   * Optionally returns the value associated with a param or its default.
   */
  def get[T](param: Param[T]): Option[T] = {
    map.get(param.asInstanceOf[Param[Any]])
      .orElse(param.defaultValue)
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
    map.toSeq.sortBy(_._1.name).map { case (param, value) =>
      s"\t${param.parent.uid}-${param.name}: $value"
    }.mkString("{\n", ",\n", "\n}")
  }

  /**
   * Returns a new param map that contains parameters in this map and the given map,
   * where the latter overwrites this if there exists conflicts.
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
   * Number of param pairs in this set.
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
