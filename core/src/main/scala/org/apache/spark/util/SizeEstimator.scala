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

import java.lang.management.ManagementFactory
import java.lang.reflect.{Field, Modifier}
import java.util.{IdentityHashMap, WeakHashMap}
import java.util.concurrent.ThreadLocalRandom

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.util.Unsafe

import com.google.common.collect.MapMaker

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.OpenHashSet

/**
 * A trait that allows a class to give [[SizeEstimator]] more accurate size estimation.
 * When a class extends it, [[SizeEstimator]] will query the `estimatedSize` first.
 * If `estimatedSize` does not return [[None]], [[SizeEstimator]] will use the returned size
 * as the size of the object. Otherwise, [[SizeEstimator]] will do the estimation work.
 * The difference between a [[KnownSizeEstimation]] and
 * [[org.apache.spark.util.collection.SizeTracker]] is that, a
 * [[org.apache.spark.util.collection.SizeTracker]] still uses [[SizeEstimator]] to
 * estimate the size. However, a [[KnownSizeEstimation]] can provide a better estimation without
 * using [[SizeEstimator]].
 */
private[spark] trait KnownSizeEstimation {
  def estimatedSize: Long
}

/**
 * :: DeveloperApi ::
 * Estimates the sizes of Java objects (number of bytes of memory they occupy), for use in
 * memory-aware caches.
 *
 * Based on the following JavaWorld article:
 * http://www.javaworld.com/javaworld/javaqa/2003-12/02-qa-1226-sizeof.html
 */
@DeveloperApi
object SizeEstimator extends Logging {

  /**
   * Estimate the number of bytes that the given object takes up on the JVM heap. The estimate
   * includes space taken up by objects referenced by the given object, their references, and so on
   * and so forth.
   *
   * This is useful for determining the amount of heap space a broadcast variable will occupy on
   * each executor or the amount of space each object will take when caching objects in
   * deserialized form. This is not the same as the serialized size of the object, which will
   * typically be much smaller.
   */
  def estimate(obj: AnyRef): Long = estimate(obj, new IdentityHashMap[AnyRef, AnyRef])

  // Sizes of primitive types
  private val BYTE_SIZE = 1
  private val BOOLEAN_SIZE = 1
  private val CHAR_SIZE = 2
  private val SHORT_SIZE = 2
  private val INT_SIZE = 4
  private val LONG_SIZE = 8
  private val FLOAT_SIZE = 4
  private val DOUBLE_SIZE = 8

  // Fields can be primitive types, sizes are: 1, 2, 4, 8. Or fields can be pointers. The size of
  // a pointer is 4 or 8 depending on the JVM (32-bit or 64-bit) and UseCompressedOops flag.
  // The sizes should be in descending order, as we will use that information for fields placement.
  private val fieldSizes = List(8, 4, 2, 1)

  // Alignment boundary for objects
  // TODO: Is this arch dependent ?
  private val ALIGN_SIZE = 8

  // A cache of ClassInfo objects for each class
  // We use weakKeys to allow GC of dynamically created classes
  private val classInfos = new ThreadLocal[WeakHashMap[Class[_], ClassInfo]] {
    override def initialValue(): java.util.WeakHashMap[Class[_], ClassInfo] = {
      val toReturn = new WeakHashMap[Class[_], ClassInfo]()
      toReturn.put(classOf[Object], new ClassInfo(objectSize, new Array[Int](0)))
      return toReturn
    }
  }

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
    val arch = System.getProperty("os.arch")
    is64bit = arch.contains("64") || arch.contains("s390x")
    isCompressedOops = getIsCompressedOops

    objectSize = if (!is64bit) 8 else {
      if (!isCompressedOops) {
        16
      } else {
        12
      }
    }
    pointerSize = if (is64bit && !isCompressedOops) 8 else 4
  }

  private def getIsCompressedOops: Boolean = {
    // This is only used by tests to override the detection of compressed oops. The test
    // actually uses a system property instead of a SparkConf, so we'll stick with that.
    if (System.getProperty("spark.test.useCompressedOops") != null) {
      return System.getProperty("spark.test.useCompressedOops").toBoolean
    }

    // java.vm.info provides compressed ref info for IBM JDKs
    if (System.getProperty("java.vendor").contains("IBM")) {
      return System.getProperty("java.vm.info").contains("Compressed Ref")
    }

    try {
      val hotSpotMBeanName = "com.sun.management:type=HotSpotDiagnostic"
      val server = ManagementFactory.getPlatformMBeanServer()

      // NOTE: This should throw an exception in non-Sun JVMs
      // scalastyle:off classforname
      val hotSpotMBeanClass = Class.forName("com.sun.management.HotSpotDiagnosticMXBean")
      val getVMMethod = hotSpotMBeanClass.getDeclaredMethod("getVMOption",
          Class.forName("java.lang.String"))
      // scalastyle:on classforname

      val bean = ManagementFactory.newPlatformMXBeanProxy(server,
        hotSpotMBeanName, hotSpotMBeanClass)
      // TODO: We could use reflection on the VMOption returned ?
      getVMMethod.invoke(bean, "UseCompressedOops").toString.contains("true")
    } catch {
      case e: Exception =>
        // Guess whether they've enabled UseCompressedOops based on whether maxMemory < 32 GB
        val guess = Runtime.getRuntime.maxMemory < (32L*1024*1024*1024)
        val guessInWords = if (guess) "yes" else "not"
        logWarning("Failed to check whether UseCompressedOops is set; assuming " + guessInWords)
        return guess
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
      elem
    }
  }

  /**
   * Cached information about each class. We remember two things: the "shell size" of the class
   * (size of all non-static fields plus the java.lang.Object size), and any fields that are
   * pointers to objects.
   */
  private class ClassInfo(
    val shellSize: Long,
    val fieldOffsets: Array[Int]) {}

  private def estimate(obj: AnyRef, visited: IdentityHashMap[AnyRef, AnyRef]): Long = {
    val state = new SearchState(visited)
    state.enqueue(obj)
    while (!state.isFinished) {
      visitSingleObject(state.dequeue(), state)
    }
    state.size
  }

  private def visitSingleObject(obj: AnyRef, state: SearchState) {
    val cls = obj.getClass
    if (cls.isArray) {
      visitArray(obj, cls, state)
    } else if (cls.getName.startsWith("scala.reflect")) {
      // Many objects in the scala.reflect package reference global reflection objects which, in
      // turn, reference many other large global objects. Do nothing in this case.
    } else if (obj.isInstanceOf[ClassLoader] || obj.isInstanceOf[Class[_]]) {
      // Hadoop JobConfs created in the interpreter have a ClassLoader, which greatly confuses
      // the size estimator since it references the whole REPL. Do nothing in this case. In
      // general all ClassLoaders and Classes will be shared between objects anyway.
    } else {
      obj match {
        case s: KnownSizeEstimation =>
          state.size += s.estimatedSize
        case _ =>
          val classInfo = getClassInfo(cls)
          state.size += alignSize(classInfo.shellSize)
          // avoid an iterator based for loop for performance
          var index = 0
          val fieldCount = classInfo.fieldOffsets.length
          val us = Unsafe.instance
          while (index < fieldCount) {
            state.enqueue(us.getObject(obj, classInfo.fieldOffsets(index).toLong))
            index += 1
          }
      }
    }
  }

  // Estimate the size of arrays larger than ARRAY_SIZE_FOR_SAMPLING by sampling.
  private val ARRAY_SIZE_FOR_SAMPLING = 400
  private val ARRAY_SAMPLE_SIZE = 100 // should be lower than ARRAY_SIZE_FOR_SAMPLING

  private def visitArray(array: AnyRef, arrayClass: Class[_], state: SearchState) {
    val length = java.lang.reflect.Array.getLength(array)
    val elementClass = arrayClass.getComponentType()

    // Arrays have object header and length field which is an integer
    var arrSize: Long = alignSize(objectSize + INT_SIZE)

    if (elementClass.isPrimitive) {
      arrSize += alignSize(length.toLong * primitiveSize(elementClass))
      state.size += arrSize
    } else {
      // We know that the array we are dealing with is an array of references
      // so explicitly expose this type so we can directly manipulate the array
      // without help form the Scala runtime for efficency
      arrSize += alignSize(length.toLong * pointerSize)
      state.size += arrSize

      val objArray = array.asInstanceOf[Array[AnyRef]]

      if (length <= ARRAY_SIZE_FOR_SAMPLING) {
        var arrayIndex = 0
        while (arrayIndex < length) {
          state.enqueue(objArray(arrayIndex))
          arrayIndex += 1
        }
      } else {
        // Estimate the size of a large array by sampling elements without replacement.
        // To exclude the shared objects that the array elements may link, sample twice
        //  and use the min one to calculate array size.
        //  Use ThreadLocalRandom here since the random is only accessed from 1 thread
        // and we can save the overhead of the full thread-safe Random
        val rand = ThreadLocalRandom.current
        val drawn = new OpenHashSet[Int](2 * ARRAY_SAMPLE_SIZE)
        val s1 = sampleArray(objArray, state, rand, drawn, length)
        val s2 = sampleArray(objArray, state, rand, drawn, length)
        val size = math.min(s1, s2)

        state.size += math.max(s1, s2) +
          (size * ((length - ARRAY_SAMPLE_SIZE) / (ARRAY_SAMPLE_SIZE))).toLong
      }
    }
  }

  private def sampleArray(
      array: Array[AnyRef],
      state: SearchState,
      rand: ThreadLocalRandom,
      drawn: OpenHashSet[Int],
      length: Int): Long = {
    var size = 0L
    // avoid the use of an iterator derrived from the range syntax here for performance
    var count = 0
    val end = ARRAY_SAMPLE_SIZE
    while (count <= end) {
      var index = 0
      do {
        index = rand.nextInt(length)
      } while (drawn.contains(index))
      drawn.add(index)
      val obj = array(index)
      if (obj != null) {
        size += SizeEstimator.estimate(obj, state.visited).toLong
      }
      count += 1
    }
    size
  }

  private def primitiveSize(cls: Class[_]): Int = {
    if (cls == classOf[Byte]) {
      BYTE_SIZE
    } else if (cls == classOf[Boolean]) {
      BOOLEAN_SIZE
    } else if (cls == classOf[Char]) {
      CHAR_SIZE
    } else if (cls == classOf[Short]) {
      SHORT_SIZE
    } else if (cls == classOf[Int]) {
      INT_SIZE
    } else if (cls == classOf[Long]) {
      LONG_SIZE
    } else if (cls == classOf[Float]) {
      FLOAT_SIZE
    } else if (cls == classOf[Double]) {
      DOUBLE_SIZE
    } else {
      throw new IllegalArgumentException(
      "Non-primitive class " + cls + " passed to primitiveSize()")
    }
  }

  /**
   * Get or compute the ClassInfo for a given class.
   */
  private def getClassInfo(cls: Class[_]): ClassInfo = {
    // Check whether we've already cached a ClassInfo for this class
    val info = classInfos.get().get(cls)
    if (info != null) {
      return info
    }

    val parent = getClassInfo(cls.getSuperclass)
    val fields = cls.getDeclaredFields
    val fieldCount = fields.length
    var shellSize = parent.shellSize
    var fieldOffsets = parent.fieldOffsets.toList

    var index = 0

    while (index < fieldCount) {
      val field = fields(index)
      if (!Modifier.isStatic(field.getModifiers)) {
        val fieldClass = field.getType
        if (fieldClass.isPrimitive) {
          if (cls == classOf[Double] || cls == classOf[Long]) {
            shellSize += 8
          } else {
            shellSize += 4
          }
        } else {
          shellSize += pointerSize
          fieldOffsets = Unsafe.instance.objectFieldOffset(field).toInt :: fieldOffsets
        }
      }
      index += 1
    }

    // Create and cache a new ClassInfo
    val newInfo = new ClassInfo(shellSize, fieldOffsets.toArray)
    classInfos.get().put(cls, newInfo)
    newInfo
  }

  private def alignSize(size: Long): Long = alignSizeUp(size, ALIGN_SIZE)

  /**
   * Compute aligned size. The alignSize must be 2^n, otherwise the result will be wrong.
   * When alignSize = 2^n, alignSize - 1 = 2^n - 1. The binary representation of (alignSize - 1)
   * will only have n trailing 1s(0b00...001..1). ~(alignSize - 1) will be 0b11..110..0. Hence,
   * (size + alignSize - 1) & ~(alignSize - 1) will set the last n bits to zeros, which leads to
   * multiple of alignSize.
   */
  private def alignSizeUp(size: Long, alignSize: Int): Long =
    (size + alignSize - 1) & ~(alignSize - 1)
}
