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

package org.apache.spark.sql.catalyst.bytecode

import org.apache.spark.sql.catalyst.expressions.{Cast, Concat, EqualTo, Expression}
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}

// TODO: code style
// TODO: this is temporary, we need a proper solution, edge cases are not handled
private[bytecode] object SpecialStaticMethodHandler {

  private val handler: PartialFunction[Behavior, LocalVarArray => Option[Expression]] = {
    case b if b.declaringClass.getName == JAVA_LONG_CLASS && b.name == "valueOf" &&
      (b.parameterTypes sameElements Array(CtClassPool.getCtClass("long"))) =>
      localVars =>
        val value = localVars(0)
        require(value.dataType == LongType)
        Some(ObjectRef(value, classOf[java.lang.Long]))
    case b if b.declaringClass.getName == JAVA_LONG_CLASS && b.name == "valueOf" =>
      localVars =>
        val value = Cast(localVars(0), LongType)
        Some(ObjectRef(value, classOf[java.lang.Long]))
    case b if b.declaringClass.getName == JAVA_LONG_CLASS && b.name == "toString" =>
      localVars =>
        val value = Cast(localVars(0), StringType)
        Some(ObjectRef(value, classOf[String]))
    case b if b.declaringClass.getName == JAVA_INTEGER_CLASS && b.name == "valueOf" &&
      (b.parameterTypes sameElements Array(CtClassPool.getCtClass("int"))) =>
      localVars =>
        val value = localVars(0)
        require(value.dataType == IntegerType)
        Some(ObjectRef(value, classOf[java.lang.Integer]))
    case b if b.declaringClass.getName == JAVA_INTEGER_CLASS && b.name == "valueOf" =>
      localVars =>
        val value = Cast(localVars(0), IntegerType)
        Some(ObjectRef(value, classOf[java.lang.Integer]))
    case b if b.declaringClass.getName == JAVA_INTEGER_CLASS && b.name == "toString" =>
      localVars =>
        val value = Cast(localVars(0), StringType)
        Some(ObjectRef(value, classOf[String]))
  }

  def canHandle(behavior: Behavior): Boolean = {
    behavior.isStatic && handler.isDefinedAt(behavior)
  }

  def handle(behavior: Behavior, localVars: LocalVarArray): Option[Expression] = {
    handler(behavior)(localVars)
  }
}

// TODO: code style
// TODO: this is temporary, we need a proper solution, edge cases are not handled
private[bytecode] object SpecialInstanceMethodHandler {

  private val handler: PartialFunction[Behavior, LocalVarArray => Option[Expression]] = {
    case b if b.declaringClass.getName == JAVA_STRING_CLASS && b.name == "concat" =>
      localVars =>
        val value = Concat(Seq(AssertNotNull(localVars(0)), AssertNotNull(localVars(1))))
        Some(ObjectRef(value, classOf[String]))
    case b if b.declaringClass.getName == SPARK_UTF8_STRING_CLASS && b.name == "toString" =>
      localVars =>
        val value = localVars(0)
        Some(ObjectRef(AssertNotNull(value), classOf[String]))
    case b if b.declaringClass.getName == JAVA_STRING_CLASS && b.name == "equals" =>
      localVars =>
        val value = localVars(0)
        val anotherValue = localVars(1)
        val equalsExpr = EqualTo(AssertNotNull(value), anotherValue)
        Some(ObjectRef(equalsExpr, classOf[String]))
  }

  def canHandle(behavior: Behavior): Boolean = {
    !behavior.isStatic && handler.isDefinedAt(behavior)
  }

  def handle(behavior: Behavior, localVars: LocalVarArray): Option[Expression] = {
    handler(behavior)(localVars)
  }
}
