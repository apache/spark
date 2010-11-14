package spark

import java.lang.reflect.Field
import java.lang.reflect.Modifier
import java.lang.reflect.{Array => JArray}
import java.util.IdentityHashMap
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ArrayBuffer


/**
 * Estimates the sizes of Java objects (number of bytes of memory they occupy),
 * for use in memory-aware caches.
 *
 * Based on the following JavaWorld article:
 * http://www.javaworld.com/javaworld/javaqa/2003-12/02-qa-1226-sizeof.html
 */
object SizeEstimator {
  private val OBJECT_SIZE  = 8 // Minimum size of a java.lang.Object
  private val POINTER_SIZE = 4 // Size of an object reference

  // Sizes of primitive types
  private val BYTE_SIZE    = 1
  private val BOOLEAN_SIZE = 1
  private val CHAR_SIZE    = 2
  private val SHORT_SIZE   = 2
  private val INT_SIZE     = 4
  private val LONG_SIZE    = 8
  private val FLOAT_SIZE   = 4
  private val DOUBLE_SIZE  = 8

  // A cache of ClassInfo objects for each class
  private val classInfos = new ConcurrentHashMap[Class[_], ClassInfo]
  classInfos.put(classOf[Object], new ClassInfo(OBJECT_SIZE, Nil))

  /**
   * The state of an ongoing size estimation. Contains a stack of objects
   * to visit as well as an IdentityHashMap of visited objects, and provides
   * utility methods for enqueueing new objects to visit.
   */
  private class SearchState {
    val visited = new IdentityHashMap[AnyRef, AnyRef]
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
   * Cached information about each class. We remember two things: the
   * "shell size" of the class (size of all non-static fields plus the
   * java.lang.Object size), and any fields that are pointers to objects.
   */
  private class ClassInfo(
    val shellSize: Long,
    val pointerFields: List[Field]) {}

  def estimate(obj: AnyRef): Long = {
    val state = new SearchState
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
    } else {
      val classInfo = getClassInfo(cls)
      state.size += classInfo.shellSize
      for (field <- classInfo.pointerFields) {
        state.enqueue(field.get(obj))
      }
    }
  }

  private def visitArray(array: AnyRef, cls: Class[_], state: SearchState) {
    val length = JArray.getLength(array)
    val elementClass = cls.getComponentType
    if (elementClass.isPrimitive) {
      state.size += length * primitiveSize(elementClass)
    } else {
      state.size += length * POINTER_SIZE
      for (i <- 0 until length) {
        state.enqueue(JArray.get(array, i))
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
          shellSize += POINTER_SIZE
          pointerFields = field :: pointerFields
        }
      }
    }

    // Create and cache a new ClassInfo
    val newInfo = new ClassInfo(shellSize, pointerFields)
    classInfos.put(cls, newInfo)
    return newInfo
  }
}
