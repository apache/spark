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

package org.apache.spark.sql.execution.python

import java.util.{List => JList, Map => JMap}
import javax.script._

import scala.collection.JavaConverters._

import org.python.core._

import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

/**
 * A serialized version of a Python lambda function to be executed in Jython.
 *
 * TODO: Consider if extending ScalaUDF is the right path forward
 * TODO: Consider if pipelining multiple JythonUDFs is important
 */
private[sql] class JythonUDF(
    name: String,
    func: JythonFunction,
    sfunc: AnyRef,
    dataType: DataType,
    children: Seq[Expression])
  extends ScalaUDF(sfunc, dataType, children) {

  // For the copy constructor keep the same ScalaFunc.
  def this(sfunc: AnyRef, dataType: DataType, children: Seq[Expression],
    inputTypes: Seq[DataType], name: String, func: JythonFunction) = {
    this(name, func, sfunc, dataType, children)
  }

  def this(name: String, func: JythonFunction, dataType: DataType, children: Seq[Expression]) {
    this(name, func, func.toScalaFunc(JythonConverter.build(dataType), children.size), dataType,
      children)
  }

  override def toString: String = s"$name(${children.mkString(", ")})"

  override def nullable: Boolean = true

  // Pass the name and function when copying
  override protected def otherCopyArgs: Seq[AnyRef] = {
    List(name, func)
  }
}

/**
 * A wrapper for a Jython function, contains all necessary context to run the function in Jython.
 *
 *
 * @param src  Python lambda expression as a string
 * @param pythonVars  Variables to be set before the function, as a base 64 encoded pickle map of
 *                    name and value.
 * @param imports  Python imports as a base 64 encoded pickle set of module, name, target.
 */
private[sql] case class JythonFunction(src: String, pythonVars: String, imports: String) {
  val className = s"__reservedPandaClass"
  val code = s"""
              |from base64 import b64decode
              |import pickle
              |import os
              |import sys
              |sys.path.extend(os.environ["PYTHONPATH"].split(":"))
              |
              |pythonVars = pickle.loads(b64decode('${pythonVars}'))
              |imports = pickle.loads(b64decode('${imports}'))
              |if imports is not None:
              |  for module, name, target in imports:
              |      exec "from %s import %s as %s" % (module, name, target)
              |if pythonVars is not None:
              |  for k, v in pythonVars.iteritems():
              |    exec "%s = v" % k
              |class ${className}(object):
              |  def __init__(self):
              |    self.call = ${src}
              |${className}_instance = ${className}()""".stripMargin('|')
  val lazyFunc = new LazyJythonFunc(code, className)

  /**
   * Compile this function to a Scala function.
   */
  def toScalaFunc(converter: Any => Any, children: Int): AnyRef = {
    children match {
      case 0 => () => converter(lazyFunc.scalaFunc())
      case 1 => (ar1: AnyRef) => converter(lazyFunc.scalaFunc(ar1))
      case 2 => (ar1: AnyRef, ar2: AnyRef) => converter(lazyFunc.scalaFunc(ar1, ar2))
      case _ => throw new Exception("Unsupported number of children " + children)
    }
  }
}

/**
 * Since the compiled code functions aren't, delay compilation till we get to the worker
 * but we also want to minimize the number of compiles we do on the workers.
 *
 * @params code  The code representing the python class to be evaluated.
 * @params className  The name of the primary class to be called.
 */
private[sql] class LazyJythonFunc(code: String, className: String) extends Serializable {
  @transient lazy val jython = JythonFunc.jython
  @transient lazy val ctx = {
    val sctx = new SimpleScriptContext()
    sctx.setBindings(jython.createBindings(), ScriptContext.ENGINE_SCOPE)
    sctx
  }
  @transient lazy val scope = ctx.getBindings(ScriptContext.ENGINE_SCOPE)
  @transient lazy val func = {
    jython.eval(code, ctx)
    scope.get(s"${className}_instance")
  }

  def scalaFunc(ar: AnyRef*): Any = {
    val pythonRet = jython.asInstanceOf[Invocable].invokeMethod(func, "call", ar : _*)
    pythonRet
  }
}

/**
 * Constructs converters for Jython return types to Scala types based on the specified data type.
 */
private[sql] object JythonConverter {
  // Needs to be on the worker - not properly serializable.
  @transient lazy val fieldsPyStr = new PyString("__fields__")

  def build(dt: DataType): Any => Any = {
    dt match {
      case LongType => x => x.asInstanceOf[java.math.BigInteger].longValue()
      case IntegerType => x => x.asInstanceOf[java.math.BigInteger].intValue()
      case arrayType: ArrayType =>
        val innerConv = build(arrayType.elementType)
        x => {
          val arr = x.asInstanceOf[JList[_]].asScala
          arr.map(innerConv)
        }
      case mapType: MapType =>
        val keyConverter = build(mapType.keyType)
        val valueConverter = build(mapType.valueType)
        x => {
          val dict = x.asInstanceOf[JMap[_, _]].asScala
          dict.map{case (k, v) => (keyConverter(k), valueConverter(v))}
        }
      case structType: StructType =>
        val converters = structType.fields.map(f => build(f.dataType))
        x => {
          val rez = x.asInstanceOf[PyTupleDerived]
          // Determine if the Row is named, or not.
          val dict = rez.getDict().asInstanceOf[PyStringMap]
          if (dict.has_key(fieldsPyStr)) {
            val pyFields = dict.get(fieldsPyStr).asInstanceOf[JList[String]].asScala
            val pyFieldsArray = pyFields.toArray
            val structFields = structType.fields.map(_.name)
            val rezArray = rez.toArray()
            val elements = structFields.zip(converters).map{case (name, conv) =>
              val idx = pyFieldsArray.indexOf(name)
              conv(rezArray(idx))
            }
            Row(elements : _*)
          } else {
            val itr = rez.asScala
            Row(converters.zip(itr).map{case (conv, v) => conv(v)} : _*)
          }
        }
      case _ => x => x
    }
  }
}

/**
 * Companion object for Jython script engine requirements.
 *
 * Starting the Jython script engine is slow, so try and do it infrequently.
 */
private[sql] object JythonFunc {
  lazy val mgr = new ScriptEngineManager()
  lazy val jython = mgr.getEngineByName("python")
}
