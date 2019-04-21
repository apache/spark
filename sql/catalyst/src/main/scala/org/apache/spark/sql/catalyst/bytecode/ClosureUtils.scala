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

import scala.util.Try

import javassist.CtClass
import javassist.bytecode.Descriptor

import org.apache.spark.api.java.function.{FilterFunction, MapFunction}
import org.apache.spark.util.Utils

/**
 * The utility object for working with closures.
 */
private[bytecode] object ClosureUtils {

  def getMethodDescriptor(argClasses: Seq[Class[_]], returnClass: Class[_]): String = {
    val argCtClasses = argClasses.map(CtClassPool.getCtClass)
    val returnCtClass = CtClassPool.getCtClass(returnClass)
    Descriptor.ofMethod(returnCtClass, argCtClasses.toArray)
  }

  def getConstructorDescriptor(argClasses: Seq[Class[_]]): String = {
    val argCtClasses = argClasses.map(CtClassPool.getCtClass)
    Descriptor.ofConstructor(argCtClasses.toArray)
  }

  // TODO: Java operations
  // TODO: cases when Scala closures are still compiled into separate classes in 2.12
  def getMethod(closure: AnyRef, argClass: Class[_], returnClass: Class[_]): Behavior = {
    closure match {
      case _: MapFunction[_, _] =>
        throw new RuntimeException("Java map closures are not supported now")
      case _: FilterFunction[_] =>
        throw new RuntimeException("Java filter closures are not supported now")
      case _ =>
        Utils.getSerializedLambda(closure) match {
          case Some(lambda) if lambda.getCapturedArgCount > 0 =>
            // TODO: support outer immutable/stateless classes/objects
            throw new RuntimeException("Closures with outer variables are not supported now")
          case Some(lambda) =>
            val capturingClassName = lambda.getCapturingClass.replace('/', '.')
            val capturingClass = Utils.classForName(capturingClassName)
            CtClassPool.addClassPathFor(capturingClass)
            val ctClass = CtClassPool.getCtClass(capturingClass)
            val implMethodName = lambda.getImplMethodName
            val implMethodSignature = lambda.getImplMethodSignature
            val implMethod = ctClass.getMethod(implMethodName, implMethodSignature)
            // boxing semantics in Scala is different from Java (e.g., unboxing nulls),
            // so Scala relies on "adapted" methods to encapsulate this difference
            // "adapted" methods are used for LMF
            // if we know our args are primitive, we can fetch the non-adapted
            // method and call it directly to avoid a round of boxing/unboxing
            if (implMethodName.endsWith("$adapted")) {
              val nonAdaptedMethod = getNonAdaptedMethod(ctClass, implMethod, argClass, returnClass)
              nonAdaptedMethod.getOrElse(implMethod)
            } else {
              implMethod
            }
          case _ =>
            throw new RuntimeException("Could not get a serialized lambda from the closure")
        }
    }
  }

  private def getNonAdaptedMethod(
      ctClass: CtClass,
      adaptedMethod: Behavior,
      argClass: Class[_],
      returnClass: Class[_]): Option[Behavior] = {

    val Array(adaptedParamCtClass) = adaptedMethod.parameterTypes
    val paramCtClass = argClass match {
      case c if c.isPrimitive => CtClassPool.getCtClass(argClass)
      case _ => adaptedParamCtClass
    }
    val returnCtClass = returnClass match {
      case c if c.isPrimitive => CtClassPool.getCtClass(returnClass)
      case _ => adaptedMethod.returnType.get
    }
    val nonAdaptedMethodDescriptor = Descriptor.ofMethod(returnCtClass, Array(paramCtClass))
    val nonAdaptedMethodName = adaptedMethod.name.stripSuffix("$adapted")
    Try[Behavior](ctClass.getMethod(nonAdaptedMethodName, nonAdaptedMethodDescriptor)).toOption
  }
}
