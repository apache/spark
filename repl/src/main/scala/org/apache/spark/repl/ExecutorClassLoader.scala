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

package org.apache.spark.repl

import java.io.{ByteArrayOutputStream, InputStream}
import java.net.{URI, URL, URLClassLoader, URLEncoder}
import java.util.concurrent.{Executors, ExecutorService}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.objectweb.asm._
import org.objectweb.asm.Opcodes._


/**
 * A ClassLoader that reads classes from a Hadoop FileSystem or HTTP URI,
 * used to load classes defined by the interpreter when the REPL is used
 */
class ExecutorClassLoader(classUri: String, parent: ClassLoader)
extends ClassLoader(parent) {
  val uri = new URI(classUri)
  val directory = uri.getPath

  // Hadoop FileSystem object for our URI, if it isn't using HTTP
  var fileSystem: FileSystem = {
    if (uri.getScheme() == "http")
      null
    else
      FileSystem.get(uri, new Configuration())
  }
  
  override def findClass(name: String): Class[_] = {
    try {
      val pathInDirectory = name.replace('.', '/') + ".class"
      val inputStream = {
        if (fileSystem != null)
          fileSystem.open(new Path(directory, pathInDirectory))
        else
          new URL(classUri + "/" + urlEncode(pathInDirectory)).openStream()
      }
      val bytes = readAndTransformClass(name, inputStream)
      inputStream.close()
      return defineClass(name, bytes, 0, bytes.length)
    } catch {
      case e: Exception => throw new ClassNotFoundException(name, e)
    }
  }
  
  def readAndTransformClass(name: String, in: InputStream): Array[Byte] = {
    if (name.startsWith("line") && name.endsWith("$iw$")) {
      // Class seems to be an interpreter "wrapper" object storing a val or var.
      // Replace its constructor with a dummy one that does not run the
      // initialization code placed there by the REPL. The val or var will
      // be initialized later through reflection when it is used in a task.
      val cr = new ClassReader(in)
      val cw = new ClassWriter(
        ClassWriter.COMPUTE_FRAMES + ClassWriter.COMPUTE_MAXS)
      val cleaner = new ConstructorCleaner(name, cw)
      cr.accept(cleaner, 0)
      return cw.toByteArray
    } else {
      // Pass the class through unmodified
      val bos = new ByteArrayOutputStream
      val bytes = new Array[Byte](4096)
      var done = false
      while (!done) {
        val num = in.read(bytes)
        if (num >= 0)
          bos.write(bytes, 0, num)
        else
          done = true
      }
      return bos.toByteArray
    }
  }

  /**
   * URL-encode a string, preserving only slashes
   */
  def urlEncode(str: String): String = {
    str.split('/').map(part => URLEncoder.encode(part, "UTF-8")).mkString("/")
  }
}

class ConstructorCleaner(className: String, cv: ClassVisitor)
extends ClassVisitor(ASM4, cv) {
  override def visitMethod(access: Int, name: String, desc: String,
      sig: String, exceptions: Array[String]): MethodVisitor = {
    val mv = cv.visitMethod(access, name, desc, sig, exceptions)
    if (name == "<init>" && (access & ACC_STATIC) == 0) {
      // This is the constructor, time to clean it; just output some new
      // instructions to mv that create the object and set the static MODULE$
      // field in the class to point to it, but do nothing otherwise.
      mv.visitCode()
      mv.visitVarInsn(ALOAD, 0) // load this
      mv.visitMethodInsn(INVOKESPECIAL, "java/lang/Object", "<init>", "()V")
      mv.visitVarInsn(ALOAD, 0) // load this
      //val classType = className.replace('.', '/')
      //mv.visitFieldInsn(PUTSTATIC, classType, "MODULE$", "L" + classType + ";")
      mv.visitInsn(RETURN)
      mv.visitMaxs(-1, -1) // stack size and local vars will be auto-computed
      mv.visitEnd()
      return null
    } else {
      return mv
    }
  }
}
