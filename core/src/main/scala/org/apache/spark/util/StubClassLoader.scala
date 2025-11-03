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
package org.apache.spark.util

import org.apache.xbean.asm9.{ClassWriter, Opcodes}

import org.apache.spark.internal.Logging

/**
 * [[ClassLoader]] that replaces missing classes with stubs, if the cannot be found. It will only
 * do this for classes that are marked for stubbing.
 *
 * While this is generally not a good idea. In this particular case this is used to load lambda's
 * whose capturing class contains unknown (and unneeded) classes. The lambda itself does not need
 * the class and therefor is safe to replace by a stub.
 */
private[spark] class StubClassLoader(parent: ClassLoader, shouldStub: String => Boolean)
  extends ClassLoader(parent) with Logging {
  override def findClass(name: String): Class[_] = {
    if (!shouldStub(name)) {
      throw new ClassNotFoundException(name)
    }
    logDebug(s"Generating stub for $name")
    val bytes = StubClassLoader.generateStub(name)
    defineClass(name, bytes, 0, bytes.length)
  }
}

private[spark] object StubClassLoader {
  def apply(parent: ClassLoader, binaryName: Seq[String]): StubClassLoader = {
    new StubClassLoader(parent, name => binaryName.exists(p => name.startsWith(p)))
  }

  def generateStub(binaryName: String): Array[Byte] = {
    // Convert binary names to internal names.
    val name = binaryName.replace('.', '/')
    val classWriter = new ClassWriter(0)
    classWriter.visit(
      49,
      Opcodes.ACC_PUBLIC + Opcodes.ACC_SUPER,
      name,
      null,
      "java/lang/Object",
      null)
    classWriter.visitSource(name + ".java", null)

    // Generate constructor.
    val ctorWriter = classWriter.visitMethod(
      Opcodes.ACC_PUBLIC,
      "<init>",
      "()V",
      null,
      null)
    ctorWriter.visitVarInsn(Opcodes.ALOAD, 0)
    ctorWriter.visitMethodInsn(
      Opcodes.INVOKESPECIAL,
      "java/lang/Object",
      "<init>",
      "()V",
      false)

    val internalException: String = "java/lang/ClassNotFoundException"
    ctorWriter.visitTypeInsn(Opcodes.NEW, internalException)
    ctorWriter.visitInsn(Opcodes.DUP)
    ctorWriter.visitLdcInsn(
      s"Fail to initiate the class $binaryName because it is stubbed. " +
        "Please install the artifact of the missing class by calling session.addArtifact.")
    // Invoke throwable constructor
    ctorWriter.visitMethodInsn(
      Opcodes.INVOKESPECIAL,
      internalException,
      "<init>",
      "(Ljava/lang/String;)V",
      false)

    ctorWriter.visitInsn(Opcodes.ATHROW)
    ctorWriter.visitMaxs(3, 3)
    ctorWriter.visitEnd()
    classWriter.visitEnd()
    classWriter.toByteArray
  }
}
