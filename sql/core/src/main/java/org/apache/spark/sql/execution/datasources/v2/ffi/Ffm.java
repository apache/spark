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

package org.apache.spark.sql.execution.datasources.v2.ffi;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Array;
import java.nio.file.Path;
import java.util.Optional;

/**
 * The Foreign Function and Memory (FFM) API of Java 22 and later, for native data sources.
 * <p>
 * Spark compiles for Java 17, where the FFM API does not exist, so the API is only reached
 * through method handles, looked up when this class is initialized, the first time that a native
 * data source library is loaded. The C functions are bound to method handles that take and return
 * primitive values only, with pointers as {@code long} addresses, so calling them uses no
 * reflection.
 */
final class Ffm {

  private Ffm() {}

  /** The C types of the parameters and the results of functions. */
  enum C { VOID, BOOL, INT32, INT64, POINTER }

  private static final Class<?> LAYOUT_CLASS;
  private static final Object LINKER;
  private static final Object GLOBAL_ARENA;
  private static final Object NO_OPTIONS;
  private static final MethodHandle LIBRARY_LOOKUP;
  private static final MethodHandle FIND;
  private static final MethodHandle DOWNCALL;
  private static final MethodHandle POINTER_DOWNCALL;
  private static final MethodHandle UPCALL;
  private static final MethodHandle FUNCTION_OF;
  private static final MethodHandle FUNCTION_OF_VOID;
  // (long) -> MemorySegment and (MemorySegment) -> long.
  private static final MethodHandle OF_ADDRESS;
  private static final MethodHandle ADDRESS_OF;
  private static final Object BOOL;
  private static final Object INT32;
  private static final Object INT64;
  private static final Object POINTER;

  static {
    try {
      // A full-privilege lookup, so that the restricted methods of the FFM API see this class as
      // their caller, in the unnamed module that Spark enables native access for.
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      Class<?> linker = Class.forName("java.lang.foreign.Linker");
      Class<?> option = Class.forName("java.lang.foreign.Linker$Option");
      Class<?> symbolLookup = Class.forName("java.lang.foreign.SymbolLookup");
      Class<?> arena = Class.forName("java.lang.foreign.Arena");
      Class<?> segment = Class.forName("java.lang.foreign.MemorySegment");
      Class<?> layout = Class.forName("java.lang.foreign.MemoryLayout");
      Class<?> layoutArray = Array.newInstance(layout, 0).getClass();
      Class<?> valueLayout = Class.forName("java.lang.foreign.ValueLayout");
      Class<?> descriptor = Class.forName("java.lang.foreign.FunctionDescriptor");
      Class<?> options = Array.newInstance(option, 0).getClass();

      LAYOUT_CLASS = layout;
      LINKER =
        lookup.findStatic(linker, "nativeLinker", MethodType.methodType(linker)).invoke();
      GLOBAL_ARENA = lookup.findStatic(arena, "global", MethodType.methodType(arena)).invoke();
      NO_OPTIONS = Array.newInstance(option, 0);
      LIBRARY_LOOKUP = lookup.findStatic(symbolLookup, "libraryLookup",
        MethodType.methodType(symbolLookup, Path.class, arena));
      FIND = lookup.findVirtual(symbolLookup, "find",
        MethodType.methodType(Optional.class, String.class));
      DOWNCALL = lookup.findVirtual(linker, "downcallHandle",
        MethodType.methodType(MethodHandle.class, segment, descriptor, options)).asFixedArity();
      POINTER_DOWNCALL = lookup.findVirtual(linker, "downcallHandle",
        MethodType.methodType(MethodHandle.class, descriptor, options)).asFixedArity();
      UPCALL = lookup.findVirtual(linker, "upcallStub",
        MethodType.methodType(segment, MethodHandle.class, descriptor, arena, options))
        .asFixedArity();
      FUNCTION_OF = lookup.findStatic(descriptor, "of",
        MethodType.methodType(descriptor, layout, layoutArray)).asFixedArity();
      FUNCTION_OF_VOID = lookup.findStatic(descriptor, "ofVoid",
        MethodType.methodType(descriptor, layoutArray)).asFixedArity();
      OF_ADDRESS =
        lookup.findStatic(segment, "ofAddress", MethodType.methodType(segment, long.class));
      ADDRESS_OF = lookup.findVirtual(segment, "address", MethodType.methodType(long.class));
      BOOL = valueLayout.getField("JAVA_BOOLEAN").get(null);
      INT32 = valueLayout.getField("JAVA_INT").get(null);
      INT64 = valueLayout.getField("JAVA_LONG").get(null);
      POINTER = valueLayout.getField("ADDRESS").get(null);
    } catch (Throwable e) {
      // The message names the cause, because the JVM only keeps this message for the later uses
      // of the class.
      throw new IllegalStateException("Cannot initialize the FFM API: " + rootCause(e), e);
    }
  }

  static Throwable rootCause(Throwable e) {
    Throwable cause = e;
    while (cause.getCause() != null && cause.getCause() != cause) {
      cause = cause.getCause();
    }
    return cause;
  }

  /**
   * Returns the symbols of the library at the given path, which stays loaded until the JVM exits.
   *
   * @throws IllegalArgumentException if the file is not a library
   * @throws IllegalCallerException if native access is not enabled for Spark
   */
  static Object libraryLookup(Path path) throws Throwable {
    return LIBRARY_LOOKUP.invoke(path, GLOBAL_ARENA);
  }

  /**
   * Binds a function of a library, looked up with {@link #libraryLookup}, to a method handle with
   * the Java types of the given C types. Returns null if the library does not export the function.
   */
  static MethodHandle bind(Object library, String name, C result, C... parameters)
      throws Throwable {
    Optional<?> symbol = (Optional<?>) FIND.invoke(library, name);
    if (symbol.isEmpty()) {
      return null;
    }
    MethodHandle handle = (MethodHandle)
      DOWNCALL.invoke(LINKER, symbol.get(), descriptor(result, parameters), NO_OPTIONS);
    return withAddresses(handle, 0, result, parameters);
  }

  /**
   * Returns a method handle that calls the C function whose address is its first argument, with
   * the Java types of the given C types, such as a callback of an Arrow C struct.
   */
  static MethodHandle bindPointer(C result, C... parameters) throws Throwable {
    MethodHandle handle = (MethodHandle)
      POINTER_DOWNCALL.invoke(LINKER, descriptor(result, parameters), NO_OPTIONS);
    handle = MethodHandles.filterArguments(handle, 0, OF_ADDRESS);
    return withAddresses(handle, 1, result, parameters);
  }

  /**
   * Returns the address of a C function with the given C types that calls the given method handle,
   * whose parameters and result have the Java types of the C types. The function lives as long as
   * the JVM. The method handle must not throw: an exception thrown by an upcall crashes the JVM.
   */
  static long upcall(MethodHandle target, C result, C... parameters) throws Throwable {
    for (int i = 0; i < parameters.length; i++) {
      if (parameters[i] == C.POINTER) {
        target = MethodHandles.filterArguments(target, i, ADDRESS_OF);
      }
    }
    if (result == C.POINTER) {
      target = MethodHandles.filterReturnValue(target, OF_ADDRESS);
    }
    Object stub =
      UPCALL.invoke(LINKER, target, descriptor(result, parameters), GLOBAL_ARENA, NO_OPTIONS);
    return (long) ADDRESS_OF.invoke(stub);
  }

  /** Passes and returns the pointers, which are MemorySegments in the FFM API, as addresses. */
  private static MethodHandle withAddresses(
      MethodHandle handle, int firstParameter, C result, C... parameters) {
    for (int i = 0; i < parameters.length; i++) {
      if (parameters[i] == C.POINTER) {
        handle = MethodHandles.filterArguments(handle, firstParameter + i, OF_ADDRESS);
      }
    }
    if (result == C.POINTER) {
      handle = MethodHandles.filterReturnValue(handle, ADDRESS_OF);
    }
    return handle;
  }

  private static Object descriptor(C result, C... parameters) throws Throwable {
    Object layouts = Array.newInstance(LAYOUT_CLASS, parameters.length);
    for (int i = 0; i < parameters.length; i++) {
      Array.set(layouts, i, layout(parameters[i]));
    }
    return result == C.VOID
      ? FUNCTION_OF_VOID.invoke(layouts)
      : FUNCTION_OF.invoke(layout(result), layouts);
  }

  private static Object layout(C type) {
    return switch (type) {
      case BOOL -> BOOL;
      case INT32 -> INT32;
      case INT64 -> INT64;
      case POINTER -> POINTER;
      case VOID -> throw new IllegalArgumentException("void has no layout");
    };
  }
}
