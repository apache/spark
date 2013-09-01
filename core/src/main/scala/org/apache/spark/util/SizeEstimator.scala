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

import java.lang.reflect.Field
import java.lang.reflect.Modifier
import java.lang.reflect.{Array => JArray}
import java.util.IdentityHashMap
import java.util.concurrent.ConcurrentHashMap
import java.util.Random

import javax.management.MBeanServer
import java.lang.management.ManagementFactory

import scala.collection.mutable.ArrayBuffer

import it.unimi.dsi.fastutil.ints.IntOpenHashSet
import org.apache.spark.Logging

/**
 * Estimates the sizes of Java objects (number of bytes of memory they occupy), for use in 
 * memory-aware caches.
 *
 * Based on the following JavaWorld article:
 * http://www.javaworld.com/javaworld/javaqa/2003-12/02-qa-1226-sizeof.html
 */
private[spark] object SizeEstimator extends Logging {

  // Sizes of primitive types
  private val BYTE_SIZE    = 1
  private val BOOLEAN_SIZE = 1
  private val CHAR_SIZE    = 2
  private val SHORT_SIZE   = 2
  private val INT_SIZE     = 4
  private val LONG_SIZE    = 8
  private val FLOAT_SIZE   = 4
  private val DOUBLE_SIZE  = 8

  // Alignment boundary for objects
  // TODO: Is this arch dependent ?
  private val ALIGN_SIZE = 8

  // A cache of ClassInfo objects for each class
  private val classInfos = new ConcurrentHashMap[Class[_], ClassInfo]

  // Object and pointer sizes are arch dependent
  private var is64bit = false

  // Size of an object reference
  // Based on https://wikis.oracle.com/display/HotSpotInternals/CompressedOops
  private var isCompressedOops = false
  private var pointerSize = 4

  // Minimum size of a java.lang.Object
  private var objectSize = 8

  initialize()

  // Sets object size, pointer size based on architecture and CompressedOops settings
  // from the JVM.
  private def initialize() {
    is64bit = System.getProperty("os.arch").contains("64")
    isCompressedOops = getIsCompressedOops

    objectSize = if (!is64bit) 8 else {
      if(!isCompressedOops) {
        16
      } else {
        12
      }
    }
    pointerSize = if (is64bit && !isCompressedOops) 8 else 4
    classInfos.clear()
    classInfos.put(classOf[Object], new ClassInfo(objectSize, Nil))
  }

  private def getIsCompressedOops : Boolean = {
    if (System.getProperty("spark.test.useCompressedOops") != null) {
      return System.getProperty("spark.test.useCompressedOops").toBoolean 
    }

    try {
      val hotSpotMBeanName = "com.sun.management:type=HotSpotDiagnostic"
      val server = ManagementFactory.getPlatformMBeanServer()

      // NOTE: This should throw an exception in non-Sun JVMs
      val hotSpotMBeanClass = Class.forName("com.sun.management.HotSpotDiagnosticMXBean")
      val getVMMethod = hotSpotMBeanClass.getDeclaredMethod("getVMOption",
          Class.forName("java.lang.String"))

      val bean = ManagementFactory.newPlatformMXBeanProxy(server, 
        hotSpotMBeanName, hotSpotMBeanClass)
      // TODO: We could use reflection on the VMOption returned ?
      return getVMMethod.invoke(bean, "UseCompressedOops").toString.contains("true")
    } catch {
      case e: Exception => {
        // Guess whether they've enabled UseCompressedOops based on whether maxMemory < 32 GB
        val guess = Runtime.getRuntime.maxMemory < (32L*1024*1024*1024)
        val guessInWords = if (guess) "yes" else "not"
        logWarning("Failed to check whether UseCompressedOops is set; assuming " + guessInWords)
        return guess
      }
    }
  }

  /**
   * The state of an ongoing size estimation. Contains a stack of objects to visit as well as an
   * IdentityHashMap of visited objects, and provides utility methods for enqueueing new objects
   * to visit.
   */
  private class SearchState(val visited: IdentityHashMap[AnyRef, AnyRef]) {
    val stack = new ArrayBuffer[AnyRef]
    var size = 0L

    def enqueue(obj: AnyRef) {
      if (obj != null && !visited.containsKey(obj)) {
        visited.put(obj, null)
        stack += obj
      }
    }

    def isFinished(): Boolean = stack.isEmpty

    def dequeue(): AnyRef = {
      val elem = stack.last
      stack.trimEnd(1)
      return elem
    }
  }

  /**
   * Cached information about each class. We remember two things: the "shell size" of the class
   * (size of all non-static fields plus the java.lang.Object size), and any fields that are
   * pointers to objects.
   */
  private class ClassInfo(
    val shellSize: Long,
    val pointerFields: List[Field]) {}

  def estimate(obj: AnyRef): Long = estimate(obj, new IdentityHashMap[AnyRef, AnyRef])

  private def estimate(obj: AnyRef, visited: IdentityHashMap[AnyRef, AnyRef]): Long = {
    val state = new SearchState(visited)
    state.enqueue(obj)
    while (!state.isFinished) {
      visitSingleObject(state.dequeue(), state)
    }
    return state.size
  }

  private def visitSingleObject(obj: AnyRef, state: SearchState) {
    val cls = obj.getClass
    if (cls.isArray) {
      visitArray(obj, cls, state)
    } else if (obj.isInstanceOf[ClassLoader] || obj.isInstanceOf[Class[_]]) {
      // Hadoop JobConfs created in the interpreter have a ClassLoader, which greatly confuses
      // the size estimator since it references the whole REPL. Do nothing in this case. In
      // general all ClassLoaders and Classes will be shared between objects anyway.
    } else {
      val classInfo = getClassInfo(cls)
      state.size += classInfo.shellSize
      for (field <- classInfo.pointerFields) {
        state.enqueue(field.get(obj))
      }
    }
  }

  // Estimat the size of arrays larger than ARRAY_SIZE_FOR_SAMPLING by sampling.
  private val ARRAY_SIZE_FOR_SAMPLING = 200
  private val ARRAY_SAMPLE_SIZE = 100 // should be lower than ARRAY_SIZE_FOR_SAMPLING

  private def visitArray(array: AnyRef, cls: Class[_], state: SearchState) {
    val length = JArray.getLength(array)
    val elementClass = cls.getComponentType

    // Arrays have object header and length field which is an integer
    var arrSize: Long = alignSize(objectSize + INT_SIZE)

    if (elementClass.isPrimitive) {
      arrSize += alignSize(length * primitiveSize(elementClass))
      state.size += arrSize
    } else {
      arrSize += alignSize(length * pointerSize)
      state.size += arrSize

      if (length <= ARRAY_SIZE_FOR_SAMPLING) {
        for (i <- 0 until length) {
          state.enqueue(JArray.get(array, i))
        }
      } else {
        // Estimate the size of a large array by sampling elements without replacement.
        var size = 0.0
        val rand = new Random(42)
        val drawn = new IntOpenHashSet(ARRAY_SAMPLE_SIZE)
        for (i <- 0 until ARRAY_SAMPLE_SIZE) {
          var index = 0
          do {
            index = rand.nextInt(length)
          } while (drawn.contains(index))
          drawn.add(index)
          val elem = JArray.get(array, index)
          size += SizeEstimator.estimate(elem, state.visited)
        }
        state.size += ((length / (ARRAY_SAMPLE_SIZE * 1.0)) * size).toLong
      }
    }
  }

  private def primitiveSize(cls: Class[_]): Long = {
    if (cls == classOf[Byte])
      BYTE_SIZE
    else if (cls == classOf[Boolean])
      BOOLEAN_SIZE
    else if (cls == classOf[Char])
      CHAR_SIZE
    else if (cls == classOf[Short])
      SHORT_SIZE
    else if (cls == classOf[Int])
      INT_SIZE
    else if (cls == classOf[Long])
      LONG_SIZE
    else if (cls == classOf[Float])
      FLOAT_SIZE
    else if (cls == classOf[Double])
      DOUBLE_SIZE
    else throw new IllegalArgumentException(
      "Non-primitive class " + cls + " passed to primitiveSize()")
  }

  /**
   * Get or compute the ClassInfo for a given class.
   */
  private def getClassInfo(cls: Class[_]): ClassInfo = {
    // Check whether we've already cached a ClassInfo for this class
    val info = classInfos.get(cls)
    if (info != null) {
      return info
    }
    
    val parent = getClassInfo(cls.getSuperclass)
    var shellSize = parent.shellSize
    var pointerFields = parent.pointerFields

    for (field <- cls.getDeclaredFields) {
      if (!Modifier.isStatic(field.getModifiers)) {
        val fieldClass = field.getType
        if (fieldClass.isPrimitive) {
          shellSize += primitiveSize(fieldClass)
        } else {
          field.setAccessible(true) // Enable future get()'s on this field
          shellSize += pointerSize
          pointerFields = field :: pointerFields
        }
      }
    }

    shellSize = alignSize(shellSize)

    // Create and cache a new ClassInfo
    val newInfo = new ClassInfo(shellSize, pointerFields)
    classInfos.put(cls, newInfo)
    return newInfo
  }

  private def alignSize(size: Long): Long = {
    val rem = size % ALIGN_SIZE
    return if (rem == 0) size else (size + ALIGN_SIZE - rem)
  }
}
