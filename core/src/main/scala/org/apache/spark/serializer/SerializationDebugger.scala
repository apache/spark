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

package org.apache.spark.serializer

import java.io._
import java.lang.reflect.Field
import java.security.AccessController

import scala.collection.mutable
import scala.util.control.NonFatal


private[serializer]
object SerializationDebugger {

  /**
   * Write an object to the [[ObjectOutputStream]]. If a NotSerializableException is encountered,
   * use our debug stream to capture the serialization stack leading to the problematic object.
   *
   * The debug stream is more expensive to write to, so we only write to it when we encounter
   * an exception. This ensures no performance impact.
   */
  def writeObject(out: ObjectOutputStream, obj: Any): Unit = {
    try {
      out.writeObject(obj)
    } catch {
      case e: NotSerializableException =>
        if (enableDebugging) throw improveException(obj, e) else throw e
    }
  }

  /**
   * Improve the given NotSerializableException with the serialization stack leading from the given
   * object to the problematic object.
   */
  private def improveException(obj: Any, e: NotSerializableException): NotSerializableException = {
    if (depthField != null) {
      val out = new DebugStream(new ByteArrayOutputStream)
      try {
        out.writeObject(obj)
        e
      } catch {
        case nse: NotSerializableException =>
          new NotSerializableException(
            nse.getMessage + "\n" +
            s"\tSerialization stack (${out.stack.size}):\n" +
            out.stack.map(o => s"\t- $o (class ${o.getClass.getName})").mkString("\n") + "\n" +
            "\tRun the JVM with sun.io.serialization.extendedDebugInfo for more information.")
        case _: Throwable => e
      }
    } else {
      e
    }
  }

  /** Reference to the private depth field in ObjectOutputStream. */
  private val depthField: Field = try {
    val f = classOf[ObjectOutputStream].getDeclaredField("depth")
    f.setAccessible(true)
    f
  } catch {
    case NonFatal(e) => null
  }

  /**
   * Whether to enable this debugging or not. By default, the special debugging feature is disabled
   * if the JVM is run with sun.io.serialization.extendedDebugInfo.
   */
  private[serializer] var enableDebugging: Boolean = {
    !AccessController.doPrivileged(new sun.security.action.GetBooleanAction(
      "sun.io.serialization.extendedDebugInfo")).booleanValue()
  }

  /**
   * An [[ObjectOutputStream]] that collects the serialization stack when a NotSerializableException
   * is thrown.
   *
   * This works by hooking into ObjectOutputStream internals using replaceObject method and the
   * private depth field. Inspired by Bob Lee's DebuggingObjectOutputStream.
   */
  private class DebugStream(underlying: OutputStream) extends ObjectOutputStream(underlying) {

    // Enable replacement so replaceObject is called whenever an object is being serialized.
    enableReplaceObject(true)

    val stack = new mutable.Stack[Object]

    private var foundNotSerializableObject = false

    /**
     * Called when [[ObjectOutputStream]] tries to serialize any object.
     */
    override protected def replaceObject(obj: Object): Object = obj match {
      case _: NotSerializableException if depth == 1 =>
        // When an object is not serializable, ObjectOutputStream resets the depth to 1 and writes
        // an NotSerializableException to the stream, and we will catch it here.
        // Once we reach here, the stack is what we want to return back to the caller.
        foundNotSerializableObject = true
        obj
      case _ =>
        if (!foundNotSerializableObject) {
          // Once ObjectOutputStream finishes serializing an object (and its fields), it will
          // decrease the depth field and serialize the next object. We pop the stack since
          // everything above depth has been successfully serialized.
          while (depth < stack.size) {
            stack.pop()
          }
          stack.push(obj)
        }
        obj
    }

    /** Return the value of the private depth field in [[ObjectOutputStream]]. */
    private def depth: Int = SerializationDebugger.depthField.get(this).asInstanceOf[Int]
  }
}
