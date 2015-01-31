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

import java.io.{NotSerializableException, ObjectOutput, ObjectStreamClass, ObjectStreamField}
import java.lang.reflect.{Field, Method}
import java.security.AccessController

import scala.annotation.tailrec
import scala.collection.mutable

import org.apache.spark.Logging

private[serializer] object SerializationDebugger extends Logging {

  /**
   * Improve the given NotSerializableException with the serialization path leading from the given
   * object to the problematic object. This is turned off automatically if
   * `sun.io.serialization.extendedDebugInfo` flag is turned on for the JVM.
   */
  def improveException(obj: Any, e: NotSerializableException): NotSerializableException = {
    if (enableDebugging && reflect != null) {
      new NotSerializableException(
        e.getMessage + "\nSerialization stack:\n" + find(obj).map("\t- " + _).mkString("\n"))
    } else {
      e
    }
  }

  /**
   * Find the path leading to a not serializable object. This method is modeled after OpenJDK's
   * serialization mechanism, and handles the following cases:
   * - primitives
   * - arrays of primitives
   * - arrays of non-primitive objects
   * - Serializable objects
   * - Externalizable objects
   * - writeReplace
   *
   * It does not yet handle writeObject override, but that shouldn't be too hard to do either.
   */
  def find(obj: Any): List[String] = {
    new SerializationDebugger().visit(obj, List.empty)
  }

  private[serializer] var enableDebugging: Boolean = {
    !AccessController.doPrivileged(new sun.security.action.GetBooleanAction(
      "sun.io.serialization.extendedDebugInfo")).booleanValue()
  }

  private class SerializationDebugger {

    /** A set to track the list of objects we have visited, to avoid cycles in the graph. */
    private val visited = new mutable.HashSet[Any]

    /**
     * Visit the object and its fields and stop when we find an object that is not serializable.
     * Return the path as a list. If everything can be serialized, return an empty list.
     */
    def visit(o: Any, stack: List[String]): List[String] = {
      if (o == null) {
        List.empty
      } else if (visited.contains(o)) {
        List.empty
      } else {
        visited += o
        o match {
          // Primitive value, string, and primitive arrays are always serializable
          case _ if o.getClass.isPrimitive => List.empty
          case _: String => List.empty
          case _ if o.getClass.isArray && o.getClass.getComponentType.isPrimitive => List.empty

          // Traverse non primitive array.
          case a: Array[_] if o.getClass.isArray && !o.getClass.getComponentType.isPrimitive =>
            val elem = s"array (class ${a.getClass.getName}, size ${a.length})"
            visitArray(o.asInstanceOf[Array[_]], elem :: stack)

          case e: java.io.Externalizable =>
            val elem = s"externalizable object (class ${e.getClass.getName}, $e)"
            visitExternalizable(e, elem :: stack)

          case s: Object with java.io.Serializable =>
            val elem = s"object (class ${s.getClass.getName}, $s)"
            visitSerializable(s, elem :: stack)

          case _ =>
            // Found an object that is not serializable!
            s"object not serializable (class: ${o.getClass.getName}, value: $o)" :: stack
        }
      }
    }

    private def visitArray(o: Array[_], stack: List[String]): List[String] = {
      var i = 0
      while (i < o.length) {
        val childStack = visit(o(i), s"element of array (index: $i)" :: stack)
        if (childStack.nonEmpty) {
          return childStack
        }
        i += 1
      }
      return List.empty
    }

    private def visitExternalizable(o: java.io.Externalizable, stack: List[String]): List[String] =
    {
      val fieldList = new ListObjectOutput
      o.writeExternal(fieldList)
      val childObjects = fieldList.outputArray
      var i = 0
      while (i < childObjects.length) {
        val childStack = visit(childObjects(i), "writeExternal data" :: stack)
        if (childStack.nonEmpty) {
          return childStack
        }
        i += 1
      }
      return List.empty
    }

    private def visitSerializable(o: Object, stack: List[String]): List[String] = {
      // An object contains multiple slots in serialization.
      // Get the slots and visit fields in all of them.
      val (finalObj, desc) = findObjectAndDescriptor(o)
      val slotDescs = desc.getSlotDescs
      var i = 0
      while (i < slotDescs.length) {
        val slotDesc = slotDescs(i)
        if (slotDesc.hasWriteObjectMethod) {
          // TODO: Handle classes that specify writeObject method.
        } else {
          val fields: Array[ObjectStreamField] = slotDesc.getFields
          val objFieldValues: Array[Object] = new Array[Object](slotDesc.getNumObjFields)
          val numPrims = fields.length - objFieldValues.length
          desc.getObjFieldValues(finalObj, objFieldValues)

          var j = 0
          while (j < objFieldValues.length) {
            val fieldDesc = fields(numPrims + j)
            val elem = s"field (class: ${slotDesc.getName}" +
              s", name: ${fieldDesc.getName}" +
              s", type: ${fieldDesc.getType})"
            val childStack = visit(objFieldValues(j), elem :: stack)
            if (childStack.nonEmpty) {
              return childStack
            }
            j += 1
          }

        }
        i += 1
      }
      return List.empty
    }
  }

  /**
   * Find the object to serialize and the associated [[ObjectStreamClass]]. This method handles
   * writeReplace in Serializable. It starts with the object itself, and keeps calling the
   * writeReplace method until there is no more
   */
  @tailrec
  private def findObjectAndDescriptor(o: Object): (Object, ObjectStreamClass) = {
    val cl = o.getClass
    val desc = ObjectStreamClass.lookupAny(cl)
    if (!desc.hasWriteReplaceMethod) {
      (o, desc)
    } else {
      // write place
      findObjectAndDescriptor(desc.invokeWriteReplace(o))
    }
  }

  /**
   * A dummy [[ObjectOutput]] that simply saves the list of objects written by a writeExternal
   * call, and returns them through `outputArray`.
   */
  private class ListObjectOutput extends ObjectOutput {
    private val output = new mutable.ArrayBuffer[Any]
    def outputArray: Array[Any] = output.toArray
    override def writeObject(o: Any): Unit = output += o
    override def flush(): Unit = {}
    override def write(i: Int): Unit = {}
    override def write(bytes: Array[Byte]): Unit = {}
    override def write(bytes: Array[Byte], i: Int, i1: Int): Unit = {}
    override def close(): Unit = {}
    override def writeFloat(v: Float): Unit = {}
    override def writeChars(s: String): Unit = {}
    override def writeDouble(v: Double): Unit = {}
    override def writeUTF(s: String): Unit = {}
    override def writeShort(i: Int): Unit = {}
    override def writeInt(i: Int): Unit = {}
    override def writeBoolean(b: Boolean): Unit = {}
    override def writeBytes(s: String): Unit = {}
    override def writeChar(i: Int): Unit = {}
    override def writeLong(l: Long): Unit = {}
    override def writeByte(i: Int): Unit = {}
  }

  /** An implicit class that allows us to call private methods of ObjectStreamClass. */
  implicit class ObjectStreamClassMethods(val desc: ObjectStreamClass) extends AnyVal {
    def getSlotDescs: Array[ObjectStreamClass] = {
      reflect.GetClassDataLayout.invoke(desc).asInstanceOf[Array[Object]].map {
        classDataSlot => reflect.DescField.get(classDataSlot).asInstanceOf[ObjectStreamClass]
      }
    }

    def hasWriteObjectMethod: Boolean = {
      reflect.HasWriteObjectMethod.invoke(desc).asInstanceOf[Boolean]
    }

    def hasWriteReplaceMethod: Boolean = {
      reflect.HasWriteReplaceMethod.invoke(desc).asInstanceOf[Boolean]
    }

    def invokeWriteReplace(obj: Object): Object = {
      reflect.InvokeWriteReplace.invoke(desc, obj)
    }

    def getNumObjFields: Int = {
      reflect.GetNumObjFields.invoke(desc).asInstanceOf[Int]
    }

    def getObjFieldValues(obj: Object, out: Array[Object]): Unit = {
      reflect.GetObjFieldValues.invoke(desc, obj, out)
    }
  }

  /**
   * Object to hold all the reflection objects. If we run on a JVM that we cannot understand,
   * this field will be null and this the debug helper should be disabled.
   */
  private val reflect: ObjectStreamClassReflection = try {
    new ObjectStreamClassReflection
  } catch {
    case e: Exception =>
      logWarning("Cannot find private methods using reflection", e)
      null
  }

  private class ObjectStreamClassReflection {
    /** ObjectStreamClass.getClassDataLayout */
    val GetClassDataLayout: Method = {
      val f = classOf[ObjectStreamClass].getDeclaredMethod("getClassDataLayout")
      f.setAccessible(true)
      f
    }

    /** ObjectStreamClass.hasWriteObjectMethod */
    val HasWriteObjectMethod: Method = {
      val f = classOf[ObjectStreamClass].getDeclaredMethod("hasWriteObjectMethod")
      f.setAccessible(true)
      f
    }

    /** ObjectStreamClass.hasWriteReplaceMethod */
    val HasWriteReplaceMethod: Method = {
      val f = classOf[ObjectStreamClass].getDeclaredMethod("hasWriteReplaceMethod")
      f.setAccessible(true)
      f
    }

    /** ObjectStreamClass.invokeWriteReplace */
    val InvokeWriteReplace: Method = {
      val f = classOf[ObjectStreamClass].getDeclaredMethod("invokeWriteReplace", classOf[Object])
      f.setAccessible(true)
      f
    }

    /** ObjectStreamClass.getNumObjFields */
    val GetNumObjFields: Method = {
      val f = classOf[ObjectStreamClass].getDeclaredMethod("getNumObjFields")
      f.setAccessible(true)
      f
    }

    /** ObjectStreamClass.getObjFieldValues */
    val GetObjFieldValues: Method = {
      val f = classOf[ObjectStreamClass].getDeclaredMethod(
        "getObjFieldValues", classOf[Object], classOf[Array[Object]])
      f.setAccessible(true)
      f
    }

    /** ObjectStreamClass$ClassDataSlot.desc field */
    val DescField: Field = {
      val f = Class.forName("java.io.ObjectStreamClass$ClassDataSlot").getDeclaredField("desc")
      f.setAccessible(true)
      f
    }
  }
}
