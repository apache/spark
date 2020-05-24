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

package org.apache.spark.sql.catalyst.expressions

import scala.annotation.tailrec

import org.clapper.classutil.ClassFinder
import org.objectweb.asm.Opcodes

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.util.Utils

class NullIntolerantCheckerSuite extends SparkFunSuite {

  // Do not check these Expressions
  private val whiteList = List(
    classOf[IntegralDivide], classOf[Divide], classOf[Remainder], classOf[Pmod],
    classOf[CheckOverflow], classOf[NormalizeNaNAndZero],
    classOf[InSet],
    classOf[PrintToStderr], classOf[CodegenFallbackExpression]).map(_.getName)

  private val finder = ClassFinder(maybeOverrideAsmVersion = Some(Opcodes.ASM7))

  private val nullIntolerantName = classOf[NullIntolerant].getName

  Seq(classOf[UnaryExpression], classOf[BinaryExpression],
    classOf[TernaryExpression], classOf[QuaternaryExpression],
    classOf[SeptenaryExpression]).map(_.getName).foreach { expName =>
    ClassFinder.concreteSubclasses(expName, finder.getClasses)
      .filterNot(c => whiteList.exists(_.equals(c.name))).foreach { classInfo =>
      test(s"${classInfo.name}") {
        // Do not check NonSQLExpressions
        if (!classInfo.interfaces.exists(_.equals(classOf[NonSQLExpression].getName))) {
          val evalExist = overrodeEval(classInfo.name, expName)
          val nullIntolerantExist = implementedNullIntolerant(classInfo.name, expName)
          if (evalExist && nullIntolerantExist) {
            fail(s"${classInfo.name} should not extend $nullIntolerantName")
          } else if (!evalExist && !nullIntolerantExist) {
            fail(s"${classInfo.name} should extend $nullIntolerantName")
          } else {
            assert((!evalExist && nullIntolerantExist) || (evalExist && !nullIntolerantExist))
          }
        }
      }
    }
  }

  @tailrec
  private def implementedNullIntolerant(className: String, endClassName: String): Boolean = {
    val clazz = Utils.classForName(className)
    val nullIntolerant = clazz.getInterfaces.exists(_.getName.equals(nullIntolerantName)) ||
      clazz.getInterfaces.exists { i =>
        Utils.classForName(i.getName).getInterfaces.exists(_.getName.equals(nullIntolerantName))
      }
    val superClassName = clazz.getSuperclass.getName
    if (!nullIntolerant && !superClassName.equals(endClassName)) {
      implementedNullIntolerant(superClassName, endClassName)
    } else {
      nullIntolerant
    }
  }

  @tailrec
  private def overrodeEval(className: String, endClassName: String): Boolean = {
    val clazz = Utils.classForName(className)
    val eval = clazz.getDeclaredMethods.exists(_.getName.equals("eval"))
    val superClassName = clazz.getSuperclass.getName
    if (!eval && !superClassName.equals(endClassName)) {
      overrodeEval(superClassName, endClassName)
    } else {
      eval
    }
  }
}
