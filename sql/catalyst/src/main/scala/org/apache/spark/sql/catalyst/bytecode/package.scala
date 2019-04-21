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

package org.apache.spark.sql.catalyst

import scala.collection.mutable

import javassist.{CtBehavior, CtClass, CtConstructor, CtMethod, Modifier}
import javassist.bytecode.{CodeIterator, ConstPool}
import javassist.bytecode.InstructionPrinter.instructionString

import org.apache.spark.sql.catalyst.expressions.Expression

// TODO: what about accessibility?
package object bytecode {

  type LocalVarArray = Array[Expression]
  type OperandStack = mutable.ArrayStack[Expression]

  // TODO: what about other ways to represent this? classOf?
  val JAVA_BOOLEAN_CLASS = "java.lang.Boolean"
  val JAVA_BYTE_CLASS = "java.lang.Byte"
  val JAVA_SHORT_CLASS = "java.lang.Short"
  val JAVA_INTEGER_CLASS = "java.lang.Integer"
  val JAVA_LONG_CLASS = "java.lang.Long"
  val JAVA_FLOAT_CLASS = "java.lang.Float"
  val JAVA_DOUBLE_CLASS = "java.lang.Double"
  val JAVA_STRING_CLASS = "java.lang.String"
  val JAVA_OBJECT_CLASS = "java.lang.Object"
  val SPARK_UTF8_STRING_CLASS = "org.apache.spark.unsafe.types.UTF8String"

  val boxedPrimitiveClasses: Seq[String] = Seq(JAVA_BOOLEAN_CLASS, JAVA_BYTE_CLASS,
    JAVA_SHORT_CLASS, JAVA_INTEGER_CLASS, JAVA_LONG_CLASS, JAVA_FLOAT_CLASS, JAVA_DOUBLE_CLASS)

  /**
   * This is a wrapper around [[CtBehavior]] to simplify the interaction.
   */
  // TODO: refactor
  implicit class Behavior(ctBehavior: CtBehavior) {

    lazy val name: String = ctBehavior.getName
    lazy val declaringClass: CtClass = ctBehavior.getDeclaringClass
    lazy val localVarArraySize: Int = ctBehavior.getMethodInfo.getCodeAttribute.getMaxLocals
    lazy val constPool: ConstPool = ctBehavior.getMethodInfo.getConstPool
    lazy val isStatic: Boolean = Modifier.isStatic(ctBehavior.getModifiers)
    lazy val isConstructor: Boolean = ctBehavior.isInstanceOf[CtConstructor]
    lazy val numParameters: Int = ctBehavior.getParameterTypes.length
    lazy val signature: String = ctBehavior.getSignature
    lazy val parameterTypes: Array[CtClass] = ctBehavior.getParameterTypes
    lazy val returnType: Option[CtClass] = ctBehavior match {
      case method: CtMethod => Some(method.getReturnType)
      case _ => None
    }

    def newCodeIterator(): CodeIterator =
      ctBehavior.getMethodInfo.getCodeAttribute.iterator()

    def toDebugString: String = {
      val stringBuilder = new StringBuilder()

      stringBuilder.append(
        s"""### Behavior ###
           |name: $name,
           |class: ${declaringClass.getName},
           |isStatic: $isStatic,
           |isConstructor: $isConstructor,
           |signature: $signature\n""".stripMargin)

      val codeIter = newCodeIterator()
      while (codeIter.hasNext) {
        val pos = codeIter.next()
        val code = instructionString(codeIter, pos, constPool)
        stringBuilder.append(s"$pos: $code\n")
      }

      stringBuilder.toString()
    }
  }

  def newLocalVarArray(
      behavior: Behavior,
      thisRef: Option[Expression],
      args: Seq[Expression]): LocalVarArray = {

    // `thisRef` must be None for static behaviors
    require(behavior.isStatic == thisRef.isEmpty)

    val localVars = new LocalVarArray(behavior.localVarArraySize)
    var localVarIndex = 0

    thisRef.foreach { ref =>
      localVars(0) = ref
      localVarIndex += 1
    }

    args.zip(behavior.parameterTypes).foreach { case (arg, argCtClass) =>
      localVars(localVarIndex) = arg
      localVarIndex += 1
      // primitive longs and doubles occupy two slots in the local variable array
      if (argCtClass.getName == "long" || argCtClass.getName == "double") {
        localVars(localVarIndex) = null
        localVarIndex += 1
      }
    }

    localVars
  }

  def resolveRefs(e: Expression): Expression = e.transformUp {
    case e: Ref => e.resolve()
  }
}
