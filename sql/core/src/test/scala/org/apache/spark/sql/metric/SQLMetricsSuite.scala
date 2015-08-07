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

package org.apache.spark.sql.metric

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import scala.collection.mutable

import com.esotericsoftware.reflectasm.shaded.org.objectweb.asm._
import com.esotericsoftware.reflectasm.shaded.org.objectweb.asm.Opcodes._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.util.Utils


class SQLMetricsSuite extends SparkFunSuite {

  test("LongSQLMetric should not box Long") {
    val l = SQLMetrics.createLongMetric(TestSQLContext.sparkContext, "long")
    val f = () => { l += 1L }
    BoxingFinder.getClassReader(f.getClass).foreach { cl =>
      val boxingFinder = new BoxingFinder()
      cl.accept(boxingFinder, 0)
      assert(boxingFinder.boxingInvokes.isEmpty, s"Found boxing: ${boxingFinder.boxingInvokes}")
    }
  }

  test("IntSQLMetric should not box Int") {
    val l = SQLMetrics.createIntMetric(TestSQLContext.sparkContext, "Int")
    val f = () => { l += 1 }
    BoxingFinder.getClassReader(f.getClass).foreach { cl =>
      val boxingFinder = new BoxingFinder()
      cl.accept(boxingFinder, 0)
      assert(boxingFinder.boxingInvokes.isEmpty, s"Found boxing: ${boxingFinder.boxingInvokes}")
    }
  }

  test("Normal accumulator should do boxing") {
    // We need this test to make sure BoxingFinder works.
    val l = TestSQLContext.sparkContext.accumulator(0L)
    val f = () => { l += 1L }
    BoxingFinder.getClassReader(f.getClass).foreach { cl =>
      val boxingFinder = new BoxingFinder()
      cl.accept(boxingFinder, 0)
      assert(boxingFinder.boxingInvokes.nonEmpty, "Found find boxing in this test")
    }
  }
}

private case class MethodIdentifier[T](cls: Class[T], name: String, desc: String)

/**
 * If `method` is null, search all methods of this class recursively to find if they do some boxing.
 * If `method` is specified, only search this method of the class to speed up the searching.
 *
 * This method will skip the methods in `visitedMethods` to avoid potential infinite cycles.
 */
private class BoxingFinder(
    method: MethodIdentifier[_] = null,
    val boxingInvokes: mutable.Set[String] = mutable.Set.empty,
    visitedMethods: mutable.Set[MethodIdentifier[_]] = mutable.Set.empty)
  extends ClassVisitor(ASM4) {

  private val primitiveBoxingClassName =
    Set("java/lang/Long",
      "java/lang/Double",
      "java/lang/Integer",
      "java/lang/Float",
      "java/lang/Short",
      "java/lang/Character",
      "java/lang/Byte",
      "java/lang/Boolean")

  override def visitMethod(
      access: Int, name: String, desc: String, sig: String, exceptions: Array[String]):
    MethodVisitor = {
    if (method != null && (method.name != name || method.desc != desc)) {
      // If method is specified, skip other methods.
      return new MethodVisitor(ASM4) {}
    }

    new MethodVisitor(ASM4) {
      override def visitMethodInsn(op: Int, owner: String, name: String, desc: String) {
        if (op == INVOKESPECIAL && name == "<init>" || op == INVOKESTATIC && name == "valueOf") {
          if (primitiveBoxingClassName.contains(owner)) {
            // Find boxing methods, e.g, new java.lang.Long(l) or java.lang.Long.valueOf(l)
            boxingInvokes.add(s"$owner.$name")
          }
        } else {
          // scalastyle:off classforname
          val classOfMethodOwner = Class.forName(owner.replace('/', '.'), false,
            Thread.currentThread.getContextClassLoader)
          // scalastyle:on classforname
          val m = MethodIdentifier(classOfMethodOwner, name, desc)
          if (!visitedMethods.contains(m)) {
            // Keep track of visited methods to avoid potential infinite cycles
            visitedMethods += m
            BoxingFinder.getClassReader(classOfMethodOwner).foreach { cl =>
              visitedMethods += m
              cl.accept(new BoxingFinder(m, boxingInvokes, visitedMethods), 0)
            }
          }
        }
      }
    }
  }
}

private object BoxingFinder {

  def getClassReader(cls: Class[_]): Option[ClassReader] = {
    val className = cls.getName.replaceFirst("^.*\\.", "") + ".class"
    val resourceStream = cls.getResourceAsStream(className)
    val baos = new ByteArrayOutputStream(128)
    // Copy data over, before delegating to ClassReader -
    // else we can run out of open file handles.
    Utils.copyStream(resourceStream, baos, true)
    // ASM4 doesn't support Java 8 classes, which requires ASM5.
    // So if the class is ASM5 (E.g., java.lang.Long when using JDK8 runtime to run these codes),
    // then ClassReader will throw IllegalArgumentException,
    // However, since this is only for testing, it's safe to skip these classes.
    try {
      Some(new ClassReader(new ByteArrayInputStream(baos.toByteArray)))
    } catch {
      case _: IllegalArgumentException => None
    }
  }

}
