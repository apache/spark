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
import org.apache.spark.sql.types.DataType

/**
 * A serialized version of a Python lambda function to be executed in Jython.
 */
private[spark] class JythonUDF(
    name: String,
    func: JythonFunction,
    dataType: DataType,
    children: Seq[Expression])
  extends ScalaUDF(func.toScalaFunc(), dataType, children) {

  override def toString: String = s"JythonUDF_$name(${children.mkString(", ")})"

  override def nullable: Boolean = true
}

/**
 * A wrapper for a Jython function, contains all necessary context to run the function in Jython.
 */
case class JythonFunction(src: String, pythonVars: String) {
  /**
   * Compile this function to a Scala function.
   */
  def toScalaFunc(): AnyRef = {
    val jython = JythonFunc.jython
    val className = s"__reservedPandaClass"
    val code = s"""
for k, v in ${pythonVars}:
  exec "%s = v" % k
class ${className}(object):
  def __init__(self):
    self.call = ${src}
${className}_instance = ${className}()
"""
    val lazyFunc = new LazyJythonFunc(code, className)
    lazyFunc.scalaFunc _
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
  def scalaFunc(ar: AnyRef*): AnyRef = {
    jython.asInstanceOf[Invocable].invokeMethod(func, "call", ar : _*)
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
