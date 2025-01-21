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

package org.apache.spark.sql.hive

import scala.jdk.CollectionConverters._

import org.apache.hadoop.hive.ql.exec.{FunctionRegistry, UDF}
import org.apache.hadoop.hive.ql.udf.{UDFType => HiveUDFType}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF._
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ConversionHelper
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper
import org.apache.spark.sql.types.DataType

abstract class HiveUDFEvaluatorBase[UDFType <: AnyRef](
    funcWrapper: HiveFunctionWrapper, children: Seq[Expression])
  extends HiveInspectors with Serializable {

  @transient
  lazy val function = funcWrapper.createFunction[UDFType]()

  @transient
  lazy val isUDFDeterministic = {
    val udfType = function.getClass.getAnnotation(classOf[HiveUDFType])
    udfType != null && udfType.deterministic() && !udfType.stateful()
  }

  def returnType: DataType

  def setArg(index: Int, arg: Any): Unit

  def doEvaluate(): Any

  final def evaluate(): Any = {
    try {
      doEvaluate()
    } catch {
      case e: Throwable =>
        throw QueryExecutionErrors.failedExecuteUserDefinedFunctionError(
          s"${funcWrapper.functionClassName}",
          s"${children.map(_.dataType.catalogString).mkString(", ")}",
          s"${returnType.catalogString}",
          e)
    }
  }
}

class HiveSimpleUDFEvaluator(
    funcWrapper: HiveFunctionWrapper, children: Seq[Expression])
  extends HiveUDFEvaluatorBase[UDF](funcWrapper, children) {

  @transient
  lazy val method = function.getResolver.
    getEvalMethod(children.map(_.dataType.toTypeInfo).asJava)

  @transient
  private lazy val wrappers = children.map(x => wrapperFor(toInspector(x), x.dataType)).toArray

  @transient
  private lazy val arguments = children.map(toInspector).toArray

  // Create parameter converters
  @transient
  private lazy val conversionHelper = new ConversionHelper(method, arguments)

  @transient
  private lazy val inputs: Array[AnyRef] = new Array[AnyRef](children.length)

  override def returnType: DataType = javaTypeToDataType(method.getGenericReturnType)

  override def setArg(index: Int, arg: Any): Unit = {
    inputs(index) = wrappers(index)(arg).asInstanceOf[AnyRef]
  }

  @transient
  private lazy val unwrapper: Any => Any =
    unwrapperFor(ObjectInspectorFactory.getReflectionObjectInspector(
      method.getGenericReturnType, ObjectInspectorOptions.JAVA))

  override def doEvaluate(): Any = {
    val ret = FunctionRegistry.invoke(
      method,
      function,
      conversionHelper.convertIfNecessary(inputs: _*): _*)
    unwrapper(ret)
  }
}

class HiveGenericUDFEvaluator(
    funcWrapper: HiveFunctionWrapper, children: Seq[Expression])
  extends HiveUDFEvaluatorBase[GenericUDF](funcWrapper, children) {

  @transient
  private lazy val argumentInspectors = children.map(toInspector)

  @transient
  lazy val returnInspector = {
    function.initializeAndFoldConstants(argumentInspectors.toArray)
  }

  @transient
  private lazy val deferredObjects: Array[DeferredObject] = argumentInspectors.zip(children).map {
    case (inspect, child) => new DeferredObjectAdapter(inspect, child.dataType)
  }.toArray[DeferredObject]

  @transient
  private lazy val unwrapper: Any => Any = unwrapperFor(returnInspector)

  override def returnType: DataType = inspectorToDataType(returnInspector)

  def setArg(index: Int, arg: Any): Unit =
    deferredObjects(index).asInstanceOf[DeferredObjectAdapter].set(() => arg)

  def setException(index: Int, exp: Throwable): Unit = {
    deferredObjects(index).asInstanceOf[DeferredObjectAdapter].set(() => throw exp)
  }

  override def doEvaluate(): Any = unwrapper(function.evaluate(deferredObjects))
}

// Adapter from Catalyst ExpressionResult to Hive DeferredObject
private[hive] class DeferredObjectAdapter(oi: ObjectInspector, dataType: DataType)
  extends DeferredObject with HiveInspectors {

  private val wrapper = wrapperFor(oi, dataType)
  private var func: () => Any = _
  def set(func: () => Any): Unit = {
    this.func = func
  }
  override def prepare(i: Int): Unit = {}
  override def get(): AnyRef = wrapper(func()).asInstanceOf[AnyRef]
}
