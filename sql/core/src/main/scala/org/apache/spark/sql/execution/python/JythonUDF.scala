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

import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

/**
 * A serialized version of a Python lambda function to be executed in Jython.
 */
private[spark] class JythonUDF(
    name: String,
    func: JythonFunction,
    sfunc: AnyRef,
    dataType: DataType,
    children: Seq[Expression])
  extends ScalaUDF(sfunc, dataType, children) {

  // For the copy constructor
  def this(sfunc: AnyRef, dataType: DataType, children: Seq[Expression],
    inputTypes: Seq[DataType], name: String, func: JythonFunction) = {
    this(name, func, sfunc, dataType, children)
  }

  def this(name: String, func: JythonFunction, dataType: DataType, children: Seq[Expression]) {
    this(name, func, func.toScalaFunc(JythonConverter.build(dataType), children.size), dataType,
      children)
  }

  override def toString: String = s"JythonUDF_$name(${children.mkString(", ")})"

  override def nullable: Boolean = true

  override protected def otherCopyArgs: Seq[AnyRef] = {
    List(name, func)
  }
}

/**
 * A wrapper for a Jython function, contains all necessary context to run the function in Jython.
 */
case class JythonFunction(src: String, pythonVars: String, imports: String) {
  val className = s"__reservedPandaClass"
  val code = s"""
from base64 import b64decode
import pickle
import os
import sys
sys.path.extend(os.environ["PYTHONPATH"].split(":"))
# A mini version of Row for complex types
class Row(tuple):
    def __new__(self, *args, **kwargs):
        if args and kwargs:
            raise ValueError("Can not use both args "
                             "and kwargs to create Row")
        if args:
            # create row class or objects
            return tuple.__new__(self, args)

        elif kwargs:
            # create row objects
            names = sorted(kwargs.keys())
            row = tuple.__new__(self, [kwargs[n] for n in names])
            row.__fields__ = names
            return row

        else:
            raise ValueError("No args or kwargs")


pythonVars = pickle.loads(b64decode('${pythonVars}'))
imports = pickle.loads(b64decode('${imports}'))
if imports is not None:
  for k, v in imports.iteritems():
      exec "import %s as %s" % (v, k)
if pythonVars is not None:
  for k, v in pythonVars.iteritems():
    exec "%s = v" % k
class ${className}(object):
  def __init__(self):
    self.call = ${src}
${className}_instance = ${className}()
"""
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
 */
class LazyJythonFunc(code: String, className: String) extends Serializable {
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

object JythonConverter {
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
          val itr = x.asInstanceOf[org.python.core.PyTupleDerived].asScala
          Row(converters.zip(itr).map{case (conv, v) => conv(v)} : _*)
        }
      case _ => x => x
    }
  }
}

/**
 * Companion object for Jython script engine requirements.
 *
 * Starting the Jython script engine is slow, so try and do it infrequently
 */
object JythonFunc {
  lazy val mgr = new ScriptEngineManager()
  lazy val jython = mgr.getEngineByName("python")
}
