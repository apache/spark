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

package org.apache.spark.unsafe;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import sun.misc.Unsafe;

public final class Platform {

  private static final Unsafe _UNSAFE;

  public static final int BOOLEAN_ARRAY_OFFSET;

  public static final int BYTE_ARRAY_OFFSET;

  public static final int SHORT_ARRAY_OFFSET;

  public static final int INT_ARRAY_OFFSET;

  public static final int LONG_ARRAY_OFFSET;

  public static final int FLOAT_ARRAY_OFFSET;

  public static final int DOUBLE_ARRAY_OFFSET;

  private static final boolean unaligned;

  // Split java.version on non-digit chars:
  private static final int majorVersion =
    Integer.parseInt(System.getProperty("java.version").split("\\D+")[0]);

  // Access fields and constructors once and store them, for performance:
  private static final Constructor<?> DBB_CONSTRUCTOR;
  private static final Field DBB_CLEANER_FIELD;
  private static final Method CLEANER_CREATE_METHOD;

  static {
    // At the end of this block, CLEANER_CREATE_METHOD should be non-null iff it's possible to use
    // reflection to invoke it, which is not necessarily possible by default in Java 9+.
    // Code below can test for null to see whether to use it.

    // The implementation of Cleaner changed from JDK 8 to 9
    String cleanerClassName;
    if (majorVersion < 9) {
      cleanerClassName = "sun.misc.Cleaner";
    } else {
      cleanerClassName = "jdk.internal.ref.Cleaner";
    }
    try {
      Class<?> cls = Class.forName("java.nio.DirectByteBuffer");
      Constructor<?> constructor = cls.getDeclaredConstructor(Long.TYPE, Integer.TYPE);
      Field cleanerField = cls.getDeclaredField("cleaner");
      try {
        constructor.setAccessible(true);
        cleanerField.setAccessible(true);
      } catch (RuntimeException re) {
        // This is a Java 9+ exception, so needs to be handled without importing it
        if ("InaccessibleObjectException".equals(re.getClass().getSimpleName())) {
          // Continue, but the constructor/field are not available
          // See comment below for more context
          constructor = null;
          cleanerField = null;
        } else {
          throw re;
        }
      }
      // Have to set these values no matter what:
      DBB_CONSTRUCTOR = constructor;
      DBB_CLEANER_FIELD = cleanerField;

      // no point continuing if the above failed:
      if (DBB_CONSTRUCTOR != null && DBB_CLEANER_FIELD != null) {
        Class<?> cleanerClass = Class.forName(cleanerClassName);
        Method createMethod = cleanerClass.getMethod("create", Object.class, Runnable.class);
        // Accessing jdk.internal.ref.Cleaner should actually fail by default in JDK 9+,
        // unfortunately, unless the user has allowed access with something like
        // --add-opens java.base/java.lang=ALL-UNNAMED  If not, we can't really use the Cleaner
        // hack below. It doesn't break, just means the user might run into the default JVM limit
        // on off-heap memory and increase it or set the flag above. This tests whether it's
        // available:
        try {
          createMethod.invoke(null, null, null);
        } catch (IllegalAccessException e) {
          // Don't throw an exception, but can't log here?
          createMethod = null;
        }
        CLEANER_CREATE_METHOD = createMethod;
      } else {
        CLEANER_CREATE_METHOD = null;
      }
    } catch (ClassNotFoundException | NoSuchMethodException | NoSuchFieldException e) {
      // These are all fatal in any Java version - rethrow (have to wrap as this is a static block)
      throw new IllegalStateException(e);
    } catch (InvocationTargetException ite) {
      throw new IllegalStateException(ite.getCause());
    }
  }

  /**
   * @return true when running JVM is having sun's Unsafe package available in it and underlying
   *         system having unaligned-access capability.
   */
  public static boolean unaligned() {
    return unaligned;
  }

  public static int getInt(Object object, long offset) {
    return _UNSAFE.getInt(object, offset);
  }

  public static void putInt(Object object, long offset, int value) {
    _UNSAFE.putInt(object, offset, value);
  }

  public static boolean getBoolean(Object object, long offset) {
    return _UNSAFE.getBoolean(object, offset);
  }

  public static void putBoolean(Object object, long offset, boolean value) {
    _UNSAFE.putBoolean(object, offset, value);
  }

  public static byte getByte(Object object, long offset) {
    return _UNSAFE.getByte(object, offset);
  }

  public static void putByte(Object object, long offset, byte value) {
    _UNSAFE.putByte(object, offset, value);
  }

  public static short getShort(Object object, long offset) {
    return _UNSAFE.getShort(object, offset);
  }

  public static void putShort(Object object, long offset, short value) {
    _UNSAFE.putShort(object, offset, value);
  }

  public static long getLong(Object object, long offset) {
    return _UNSAFE.getLong(object, offset);
  }

  public static void putLong(Object object, long offset, long value) {
    _UNSAFE.putLong(object, offset, value);
  }

  public static float getFloat(Object object, long offset) {
    return _UNSAFE.getFloat(object, offset);
  }

  public static void putFloat(Object object, long offset, float value) {
    _UNSAFE.putFloat(object, offset, value);
  }

  public static double getDouble(Object object, long offset) {
    return _UNSAFE.getDouble(object, offset);
  }

  public static void putDouble(Object object, long offset, double value) {
    _UNSAFE.putDouble(object, offset, value);
  }

  public static Object getObjectVolatile(Object object, long offset) {
    return _UNSAFE.getObjectVolatile(object, offset);
  }

  public static void putObjectVolatile(Object object, long offset, Object value) {
    _UNSAFE.putObjectVolatile(object, offset, value);
  }

  public static long allocateMemory(long size) {
    return _UNSAFE.allocateMemory(size);
  }

  public static void freeMemory(long address) {
    _UNSAFE.freeMemory(address);
  }

  public static long reallocateMemory(long address, long oldSize, long newSize) {
    long newMemory = _UNSAFE.allocateMemory(newSize);
    copyMemory(null, address, null, newMemory, oldSize);
    freeMemory(address);
    return newMemory;
  }

  /**
   * Allocate a DirectByteBuffer, potentially bypassing the JVM's MaxDirectMemorySize limit.
   */
  public static ByteBuffer allocateDirectBuffer(int size) {
    try {
      if (CLEANER_CREATE_METHOD == null) {
        // Can't set a Cleaner (see comments on field), so need to allocate via normal Java APIs
        try {
          return ByteBuffer.allocateDirect(size);
        } catch (OutOfMemoryError oome) {
          // checkstyle.off: RegexpSinglelineJava
          throw new OutOfMemoryError("Failed to allocate direct buffer (" + oome.getMessage() +
              "); try increasing -XX:MaxDirectMemorySize=... to, for example, your heap size");
          // checkstyle.on: RegexpSinglelineJava
        }
      }
      // Otherwise, use internal JDK APIs to allocate a DirectByteBuffer while ignoring the JVM's
      // MaxDirectMemorySize limit (the default limit is too low and we do not want to
      // require users to increase it).
      long memory = allocateMemory(size);
      ByteBuffer buffer = (ByteBuffer) DBB_CONSTRUCTOR.newInstance(memory, size);
      try {
        DBB_CLEANER_FIELD.set(buffer,
            CLEANER_CREATE_METHOD.invoke(null, buffer, (Runnable) () -> freeMemory(memory)));
      } catch (IllegalAccessException | InvocationTargetException e) {
        freeMemory(memory);
        throw new IllegalStateException(e);
      }
      return buffer;
    } catch (Exception e) {
      throwException(e);
    }
    throw new IllegalStateException("unreachable");
  }

  public static void setMemory(Object object, long offset, long size, byte value) {
    _UNSAFE.setMemory(object, offset, size, value);
  }

  public static void setMemory(long address, byte value, long size) {
    _UNSAFE.setMemory(address, size, value);
  }

  public static void copyMemory(
    Object src, long srcOffset, Object dst, long dstOffset, long length) {
    // Check if dstOffset is before or after srcOffset to determine if we should copy
    // forward or backwards. This is necessary in case src and dst overlap.
    if (dstOffset < srcOffset) {
      while (length > 0) {
        long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
        _UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
        length -= size;
        srcOffset += size;
        dstOffset += size;
      }
    } else {
      srcOffset += length;
      dstOffset += length;
      while (length > 0) {
        long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
        srcOffset -= size;
        dstOffset -= size;
        _UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
        length -= size;
      }

    }
  }

  /**
   * Raises an exception bypassing compiler checks for checked exceptions.
   */
  public static void throwException(Throwable t) {
    _UNSAFE.throwException(t);
  }

  /**
   * Limits the number of bytes to copy per {@link Unsafe#copyMemory(long, long, long)} to
   * allow safepoint polling during a large copy.
   */
  private static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;

  static {
    sun.misc.Unsafe unsafe;
    try {
      Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
      unsafeField.setAccessible(true);
      unsafe = (sun.misc.Unsafe) unsafeField.get(null);
    } catch (Throwable cause) {
      unsafe = null;
    }
    _UNSAFE = unsafe;

    if (_UNSAFE != null) {
      BOOLEAN_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(boolean[].class);
      BYTE_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(byte[].class);
      SHORT_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(short[].class);
      INT_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(int[].class);
      LONG_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(long[].class);
      FLOAT_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(float[].class);
      DOUBLE_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(double[].class);
    } else {
      BOOLEAN_ARRAY_OFFSET = 0;
      BYTE_ARRAY_OFFSET = 0;
      SHORT_ARRAY_OFFSET = 0;
      INT_ARRAY_OFFSET = 0;
      LONG_ARRAY_OFFSET = 0;
      FLOAT_ARRAY_OFFSET = 0;
      DOUBLE_ARRAY_OFFSET = 0;
    }
  }

  // This requires `majorVersion` and `_UNSAFE`.
  static {
    boolean _unaligned;
    String arch = System.getProperty("os.arch", "");
    if (arch.equals("ppc64le") || arch.equals("ppc64") || arch.equals("s390x")) {
      // Since java.nio.Bits.unaligned() doesn't return true on ppc (See JDK-8165231), but
      // ppc64 and ppc64le support it
      _unaligned = true;
    } else {
      try {
        Class<?> bitsClass =
          Class.forName("java.nio.Bits", false, ClassLoader.getSystemClassLoader());
        if (_UNSAFE != null && majorVersion >= 9) {
          // Java 9/10 and 11/12 have different field names.
          Field unalignedField =
            bitsClass.getDeclaredField(majorVersion >= 11 ? "UNALIGNED" : "unaligned");
          _unaligned = _UNSAFE.getBoolean(
            _UNSAFE.staticFieldBase(unalignedField), _UNSAFE.staticFieldOffset(unalignedField));
        } else {
          Method unalignedMethod = bitsClass.getDeclaredMethod("unaligned");
          unalignedMethod.setAccessible(true);
          _unaligned = Boolean.TRUE.equals(unalignedMethod.invoke(null));
        }
      } catch (Throwable t) {
        // We at least know x86 and x64 support unaligned access.
        //noinspection DynamicRegexReplaceableByCompiledPattern
        _unaligned = arch.matches("^(i[3-6]86|x86(_64)?|x64|amd64|aarch64)$");
      }
    }
    unaligned = _unaligned;
  }
}
