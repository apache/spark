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

package org.apache.spark.executor

import java.io.{ByteArrayOutputStream, FileNotFoundException, FilterInputStream, InputStream}
import java.net.{URI, URL, URLEncoder}
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets.UTF_8

import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.xbean.asm9._
import org.apache.xbean.asm9.Opcodes._

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.util.ParentClassLoader

/**
 * A ClassLoader that reads classes from a Hadoop FileSystem or Spark RPC endpoint, used to load
 * classes defined by the interpreter when the REPL is used. Allows the user to specify if user
 * class path should be first.
 * This class loader delegates getting/finding resources to parent loader, which makes sense because
 * the REPL never produce resources dynamically. One exception is when getting a Class file as
 * resource stream, in which case we will try to fetch the Class file in the same way as loading
 * the class, so that dynamically generated Classes from the REPL can be picked up.
 *
 * Note: [[ClassLoader]] will preferentially load class from parent. Only when parent is null or
 * the load failed, that it will call the overridden `findClass` function. To avoid the potential
 * issue caused by loading class using inappropriate class loader, we should set the parent of
 * ClassLoader to null, so that we can fully control which class loader is used. For detailed
 * discussion, see SPARK-18646.
 */
class ExecutorClassLoader(
    conf: SparkConf,
    env: SparkEnv,
    classUri: String,
    parent: ClassLoader,
    userClassPathFirst: Boolean) extends ClassLoader(null) with Logging {
  val uri = new URI(classUri)
  val directory = uri.getPath

  val parentLoader = new ParentClassLoader(parent)

  private val fetchFn: (String) => InputStream = uri.getScheme() match {
    case "spark" => getClassFileInputStreamFromSparkRPC
    case _ =>
      val fileSystem = FileSystem.get(uri, SparkHadoopUtil.get.newConfiguration(conf))
      getClassFileInputStreamFromFileSystem(fileSystem)
  }

  override def getResource(name: String): URL = {
    parentLoader.getResource(name)
  }

  override def getResources(name: String): java.util.Enumeration[URL] = {
    parentLoader.getResources(name)
  }

  override def getResourceAsStream(name: String): InputStream = {
    if (userClassPathFirst) {
      val res = getClassResourceAsStreamLocally(name)
      if (res != null) res else parentLoader.getResourceAsStream(name)
    } else {
      val res = parentLoader.getResourceAsStream(name)
      if (res != null) res else getClassResourceAsStreamLocally(name)
    }
  }

  private def getClassResourceAsStreamLocally(name: String): InputStream = {
    // Class files can be dynamically generated from the REPL. Allow this class loader to
    // load such files for purposes other than loading the class.
    try {
      if (name.endsWith(".class")) fetchFn(name) else null
    } catch {
      // The helper functions referenced by fetchFn throw CNFE to indicate failure to fetch
      // the class. It matches what IOException was supposed to be used for, and
      // ClassLoader.getResourceAsStream() catches IOException and returns null in that case.
      // So we follow that model and handle CNFE here.
      case _: ClassNotFoundException => null
    }
  }

  override def findClass(name: String): Class[_] = {
    if (userClassPathFirst) {
      findClassLocally(name).getOrElse(parentLoader.loadClass(name))
    } else {
      try {
        parentLoader.loadClass(name)
      } catch {
        case e: ClassNotFoundException =>
          val classOption = try {
            findClassLocally(name)
          } catch {
            case e: RemoteClassLoaderError =>
              throw e
            case NonFatal(e) =>
              // Wrap the error to include the class name
              // scalastyle:off throwerror
              throw new RemoteClassLoaderError(name, e)
              // scalastyle:on throwerror
          }
          classOption match {
            case None => throw new ClassNotFoundException(name, e)
            case Some(a) => a
          }
      }
    }
  }

  // See org.apache.spark.network.server.TransportRequestHandler.processStreamRequest.
  private val STREAM_NOT_FOUND_REGEX = s"Stream '.*' was not found.".r.pattern

  private def getClassFileInputStreamFromSparkRPC(path: String): InputStream = {
    val channel = env.rpcEnv.openChannel(s"$classUri/${urlEncode(path)}")
    new FilterInputStream(Channels.newInputStream(channel)) {

      override def read(): Int = toClassNotFound(super.read())

      override def read(b: Array[Byte], offset: Int, len: Int) =
        toClassNotFound(super.read(b, offset, len))

      private def toClassNotFound(fn: => Int): Int = {
        try {
          fn
        } catch {
          case e: RuntimeException if e.getMessage != null
            && STREAM_NOT_FOUND_REGEX.matcher(e.getMessage).matches() =>
            // Convert a stream not found error to ClassNotFoundException.
            // Driver sends this explicit acknowledgment to tell us that the class was missing.
            throw new ClassNotFoundException(path, e)
          case NonFatal(e) =>
            // scalastyle:off throwerror
            throw new RemoteClassLoaderError(path, e)
            // scalastyle:on throwerror
        }
      }
    }
  }

  private def getClassFileInputStreamFromFileSystem(fileSystem: FileSystem)(
      pathInDirectory: String): InputStream = {
    val path = new Path(directory, pathInDirectory)
    try {
      fileSystem.open(path)
    } catch {
      case _: FileNotFoundException =>
        throw new ClassNotFoundException(s"Class file not found at path $path")
    }
  }

  def findClassLocally(name: String): Option[Class[_]] = {
    val pathInDirectory = name.replace('.', '/') + ".class"
    var inputStream: InputStream = null
    try {
      inputStream = fetchFn(pathInDirectory)
      val bytes = readAndTransformClass(name, inputStream)
      Some(defineClass(name, bytes, 0, bytes.length))
    } catch {
      case e: ClassNotFoundException =>
        // We did not find the class
        logDebug(s"Did not load class $name from REPL class server at $uri", e)
        None
      case e: Exception =>
        // Something bad happened while checking if the class exists
        logError(log"Failed to check existence of class ${MDC(LogKeys.CLASS_NAME, name)} " +
          log"on REPL class server at ${MDC(LogKeys.URI, uri)}", e)
        if (userClassPathFirst) {
          // Allow to try to load from "parentLoader"
          None
        } else {
          throw e
        }
    } finally {
      if (inputStream != null) {
        try {
          inputStream.close()
        } catch {
          case e: Exception =>
            logError("Exception while closing inputStream", e)
        }
      }
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
      cw.toByteArray
    } else {
      // Pass the class through unmodified
      val bos = new ByteArrayOutputStream
      val bytes = new Array[Byte](4096)
      var done = false
      while (!done) {
        val num = in.read(bytes)
        if (num >= 0) {
          bos.write(bytes, 0, num)
        } else {
          done = true
        }
      }
      bos.toByteArray
    }
  }

  /**
   * URL-encode a string, preserving only slashes
   */
  def urlEncode(str: String): String = {
    str.split('/').map(part => URLEncoder.encode(part, UTF_8.name())).mkString("/")
  }
}

class ConstructorCleaner(className: String, cv: ClassVisitor)
extends ClassVisitor(ASM9, cv) {
  override def visitMethod(access: Int, name: String, desc: String,
      sig: String, exceptions: Array[String]): MethodVisitor = {
    val mv = cv.visitMethod(access, name, desc, sig, exceptions)
    if (name == "<init>" && (access & ACC_STATIC) == 0) {
      // This is the constructor, time to clean it; just output some new
      // instructions to mv that create the object and set the static MODULE$
      // field in the class to point to it, but do nothing otherwise.
      mv.visitCode()
      mv.visitVarInsn(ALOAD, 0) // load this
      mv.visitMethodInsn(INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false)
      mv.visitVarInsn(ALOAD, 0) // load this
      // val classType = className.replace('.', '/')
      // mv.visitFieldInsn(PUTSTATIC, classType, "MODULE$", "L" + classType + ";")
      mv.visitInsn(RETURN)
      mv.visitMaxs(-1, -1) // stack size and local vars will be auto-computed
      mv.visitEnd()
      null
    } else {
      mv
    }
  }
}

/**
 * An error when we cannot load a class due to exceptions. We don't know if this class exists, so
 * throw a special one that's neither [[LinkageError]] nor [[ClassNotFoundException]] to make JVM
 * retry to load this class later.
 */
private[executor] class RemoteClassLoaderError(className: String, cause: Throwable)
  extends Error(className, cause)
