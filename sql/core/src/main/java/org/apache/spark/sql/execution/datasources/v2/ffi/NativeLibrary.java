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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;
import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.sql.execution.datasources.v2.ffi.proto.CallReply;
import org.apache.spark.sql.execution.datasources.v2.ffi.proto.CallRequest;
import org.apache.spark.sql.execution.datasources.v2.ffi.proto.OpenReply;
import org.apache.spark.sql.execution.datasources.v2.ffi.proto.OpenRequest;
import org.apache.spark.sql.execution.datasources.v2.ffi.proto.StreamRequest;
import org.apache.spark.unsafe.Platform;

/**
 * A native data source library, which implements the C interface in
 * {@code org/apache/spark/sql/datasource/spark_datasource.h}, called through the Foreign Function
 * and Memory (FFM) API of Java 22 and later, with the messages of
 * {@code org/apache/spark/sql/datasource/spark_datasource.proto}.
 * <p>
 * Spark compiles for Java 17, where the FFM API does not exist, so the API is only reached
 * through method handles, looked up once when the first library is loaded. Each of the six
 * functions that the library exports is bound to a method handle that takes and returns primitive
 * values only, with pointers as {@code long} addresses, so the calls themselves use no
 * reflection. The memory that Spark passes to the library is allocated with {@link Platform}, at
 * the offsets of the structs of the header, which {@link #layout()} lists for the tests.
 * <p>
 * A function that fails throws a {@link RuntimeException} with the message of
 * {@code spark_ds_last_error}.
 */
final class NativeLibrary {

  private static final SparkLogger LOGGER = SparkLoggerFactory.getLogger(NativeLibrary.class);

  /** The version of the interface that Spark supports, {@code SPARK_DS_ABI_VERSION}. */
  static final int ABI_VERSION = 1;

  /** The newest version of the messages that Spark supports. */
  static final int PROTOCOL_VERSION = 1;

  /** The first version of Java with the final FFM API. */
  static final int MINIMUM_JAVA_VERSION = 22;

  /** Whether this JVM can load native data source libraries. */
  static boolean isSupported() {
    return Runtime.version().feature() >= MINIMUM_JAVA_VERSION;
  }

  // The C types of the parameters and the results of the functions.
  private enum C { VOID, BOOL, I32, I64, PTR }

  // The layout of SparkDsHost on the 64-bit platforms that Spark supports.
  private static final long HOST_SIZE = 24;
  private static final long HOST_LOG = 8;
  private static final long HOST_IS_INTERRUPTED = 16;

  /**
   * The offsets of the fields of the structs that Spark writes, by {@code struct.field}, and the
   * sizes of the structs, by {@code struct}, for the tests to compare with the header.
   */
  static Map<String, Long> layout() {
    Map<String, Long> layout = new LinkedHashMap<>();
    layout.put("SparkDsHost", HOST_SIZE);
    layout.put("SparkDsHost.size", 0L);
    layout.put("SparkDsHost.log", HOST_LOG);
    layout.put("SparkDsHost.is_interrupted", HOST_IS_INTERRUPTED);
    return layout;
  }

  private final long host;
  private final int abiVersion;
  private final MethodHandle lastError;
  private final MethodHandle open;
  private final MethodHandle call;
  private final MethodHandle stream;
  private final MethodHandle close;

  private NativeLibrary(Object lookup, String path) throws Throwable {
    abiVersion = (int) function(lookup, "spark_ds_abi_version", C.I32).invokeExact();
    // The functions of another version of the interface may differ: the caller rejects the
    // library before any of them is called.
    boolean supported = abiVersion == ABI_VERSION;
    lastError = supported ? optionalFunction(lookup, "spark_ds_last_error", C.PTR) : null;
    open = supported
      ? function(lookup, "spark_ds_open", C.I32, C.PTR, C.PTR, C.I64, C.PTR, C.PTR, C.PTR)
      : null;
    call = supported
      ? function(lookup, "spark_ds_call", C.I32, C.PTR, C.PTR, C.I64, C.PTR, C.PTR)
      : null;
    stream = supported
      ? function(lookup, "spark_ds_stream", C.I32, C.PTR, C.PTR, C.I64, C.PTR, C.PTR)
      : null;
    close = supported ? function(lookup, "spark_ds_close", C.VOID, C.PTR) : null;
    // The library may keep the host until the JVM exits.
    host = supported ? Ffm.host(path) : 0;
  }

  private static MethodHandle function(Object lookup, String name, C result, C... parameters)
      throws Throwable {
    MethodHandle function = optionalFunction(lookup, name, result, parameters);
    if (function == null) {
      throw new LoadException(
        "The library does not export the function " + name + ".", null, true);
    }
    return function;
  }

  private static MethodHandle optionalFunction(
      Object lookup, String name, C result, C... parameters) throws Throwable {
    Optional<?> address = (Optional<?>) Ffm.FIND.invoke(lookup, name);
    return address.isEmpty() ? null : Ffm.downcall(address.get(), result, parameters);
  }

  /**
   * Loads the library at the given path. The library stays loaded until the JVM exits.
   *
   * @throws LoadException if the library cannot be loaded
   */
  static NativeLibrary load(String path) throws Throwable {
    if (!isSupported()) {
      throw new UnsupportedOperationException(
        "Native data sources require Java " + MINIMUM_JAVA_VERSION + " or later.");
    }
    Object lookup;
    try {
      lookup = Ffm.LIBRARY_LOOKUP.invoke(Path.of(path), Ffm.GLOBAL_ARENA);
    } catch (IllegalArgumentException | IllegalCallerException e) {
      // The library is not a valid library, or native access is not enabled for Spark.
      throw new LoadException(e.getMessage(), e);
    } catch (LinkageError e) {
      // The FFM API could not be initialized, see Ffm: report why, such as an
      // IllegalCallerException when native access is not enabled for Spark.
      throw new LoadException(
        "Cannot use the Foreign Function and Memory API: " + rootCause(e), e);
    }
    return new NativeLibrary(lookup, path);
  }

  /** The method handles of the FFM API, initialized when the first library is loaded. */
  private static final class Ffm {
    static final Class<?> LAYOUT_CLASS;
    static final Object LINKER;
    static final Object GLOBAL_ARENA;
    static final Object NO_OPTIONS;
    static final MethodHandle LIBRARY_LOOKUP;
    static final MethodHandle FIND;
    static final MethodHandle DOWNCALL;
    static final MethodHandle UPCALL;
    static final MethodHandle FUNCTION_OF;
    static final MethodHandle FUNCTION_OF_VOID;
    // (long) -> MemorySegment and (MemorySegment) -> long.
    static final MethodHandle OF_ADDRESS;
    static final MethodHandle ADDRESS_OF;
    static final Object BOOL;
    static final Object INT32;
    static final Object INT64;
    static final Object POINTER;
    // NativeLibrary.log, which SparkDsHost.log calls with the path of its library.
    static final MethodHandle LOG;
    // The addresses of the C functions that call Java, which live as long as the JVM:
    // SparkDsSink, which calls NativeLibrary.sink, and SparkDsHost.is_interrupted, which calls
    // NativeLibrary.isInterrupted.
    static final long SINK;
    static final long IS_INTERRUPTED;

    static {
      try {
        // A full-privilege lookup, so that the restricted methods of the FFM API see this class
        // as their caller, in the unnamed module that Spark enables native access for.
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
        LINKER = lookup.findStatic(linker, "nativeLinker", MethodType.methodType(linker))
          .invoke();
        GLOBAL_ARENA = lookup.findStatic(arena, "global", MethodType.methodType(arena)).invoke();
        NO_OPTIONS = Array.newInstance(option, 0);
        LIBRARY_LOOKUP = lookup.findStatic(symbolLookup, "libraryLookup",
          MethodType.methodType(symbolLookup, Path.class, arena));
        FIND = lookup.findVirtual(symbolLookup, "find",
          MethodType.methodType(Optional.class, String.class));
        DOWNCALL = lookup.findVirtual(linker, "downcallHandle",
          MethodType.methodType(MethodHandle.class, segment, descriptor, options))
          .asFixedArity();
        UPCALL = lookup.findVirtual(linker, "upcallStub",
          MethodType.methodType(segment, MethodHandle.class, descriptor, arena, options))
          .asFixedArity();
        FUNCTION_OF = lookup.findStatic(descriptor, "of",
          MethodType.methodType(descriptor, layout, layoutArray)).asFixedArity();
        FUNCTION_OF_VOID = lookup.findStatic(descriptor, "ofVoid",
          MethodType.methodType(descriptor, layoutArray)).asFixedArity();
        OF_ADDRESS = lookup.findStatic(segment, "ofAddress",
          MethodType.methodType(segment, long.class));
        ADDRESS_OF = lookup.findVirtual(segment, "address", MethodType.methodType(long.class));
        BOOL = valueLayout.getField("JAVA_BOOLEAN").get(null);
        INT32 = valueLayout.getField("JAVA_INT").get(null);
        INT64 = valueLayout.getField("JAVA_LONG").get(null);
        POINTER = valueLayout.getField("ADDRESS").get(null);

        LOG = lookup.findStatic(NativeLibrary.class, "log",
          MethodType.methodType(void.class, String.class, int.class, long.class));
        SINK = upcall(
          lookup.findStatic(NativeLibrary.class, "sink",
            MethodType.methodType(void.class, long.class, long.class, long.class)),
          C.VOID, C.PTR, C.PTR, C.I64);
        IS_INTERRUPTED = upcall(
          lookup.findStatic(NativeLibrary.class, "isInterrupted",
            MethodType.methodType(boolean.class, long.class)),
          C.BOOL, C.I64);
      } catch (Throwable e) {
        // The message names the cause, because the JVM only keeps this message for the later
        // uses of the class.
        throw new IllegalStateException("Cannot initialize the FFM API: " + rootCause(e), e);
      }
    }

    /** Returns the FunctionDescriptor of a C function with the given types. */
    static Object descriptor(C result, C... parameters) throws Throwable {
      Object layouts = Array.newInstance(LAYOUT_CLASS, parameters.length);
      for (int i = 0; i < parameters.length; i++) {
        Array.set(layouts, i, layout(parameters[i]));
      }
      return result == C.VOID
        ? FUNCTION_OF_VOID.invoke(layouts)
        : FUNCTION_OF.invoke(layout(result), layouts);
    }

    static Object layout(C type) {
      return switch (type) {
        case BOOL -> BOOL;
        case I32 -> INT32;
        case I64 -> INT64;
        case PTR -> POINTER;
        case VOID -> throw new IllegalArgumentException("void has no layout");
      };
    }

    /**
     * Binds the C function at the given address, a MemorySegment, to a method handle whose
     * parameters and result have the Java types of the given C types, with {@code long} for
     * pointers.
     */
    static MethodHandle downcall(Object address, C result, C... parameters) throws Throwable {
      MethodHandle handle = (MethodHandle) DOWNCALL.invoke(
        LINKER, address, descriptor(result, parameters), NO_OPTIONS);
      // Passes and returns the pointers, which are MemorySegments in the FFM API, as addresses.
      for (int i = 0; i < parameters.length; i++) {
        if (parameters[i] == C.PTR) {
          handle = MethodHandles.filterArguments(handle, i, OF_ADDRESS);
        }
      }
      if (result == C.PTR) {
        handle = MethodHandles.filterReturnValue(handle, ADDRESS_OF);
      }
      return handle;
    }

    /**
     * Returns the address of a C function with the given types that calls `target`, whose
     * parameters have the Java types of the C types, with {@code long} for pointers. The
     * function lives as long as the JVM.
     */
    static long upcall(MethodHandle target, C result, C... parameters) throws Throwable {
      for (int i = 0; i < parameters.length; i++) {
        if (parameters[i] == C.PTR) {
          target = MethodHandles.filterArguments(target, i, ADDRESS_OF);
        }
      }
      Object stub = UPCALL.invoke(
        LINKER, target, descriptor(result, parameters), GLOBAL_ARENA, NO_OPTIONS);
      return (long) ADDRESS_OF.invoke(stub);
    }

    /**
     * Allocates the SparkDsHost of a library. The library may keep it until the JVM exits, so it
     * is never freed.
     */
    static long host(String path) throws Throwable {
      long log = upcall(MethodHandles.insertArguments(LOG, 0, path), C.VOID, C.I32, C.PTR);
      long host = Platform.allocateMemory(HOST_SIZE);
      Platform.setMemory(host, (byte) 0, HOST_SIZE);
      Platform.putLong(null, host, HOST_SIZE);
      Platform.putLong(null, host + HOST_LOG, log);
      Platform.putLong(null, host + HOST_IS_INTERRUPTED, IS_INTERRUPTED);
      return host;
    }
  }

  private static Throwable rootCause(Throwable e) {
    Throwable cause = e;
    while (cause.getCause() != null && cause.getCause() != cause) {
      cause = cause.getCause();
    }
    return cause;
  }

  /** Thrown when a library cannot be loaded, or cannot be used. */
  static final class LoadException extends RuntimeException {
    /**
     * Whether the library was loaded, in which case it stays loaded: Spark found that it cannot
     * use it afterwards, for example because it does not export a function.
     */
    final boolean loaded;

    LoadException(String message, Throwable cause) {
      this(message, cause, false);
    }

    LoadException(String message, Throwable cause, boolean loaded) {
      super(message, cause);
      this.loaded = loaded;
    }
  }

  /**
   * Called by the library, through {@code SparkDsHost.log}, with the path of the library. It must
   * not throw: an exception thrown by an upcall crashes the JVM.
   */
  private static void log(String path, int level, long message) {
    try {
      String text = message == 0 ? "" : new String(readCString(message), StandardCharsets.UTF_8);
      switch (level) {
        case 1 -> LOGGER.error("Native data source library {}: {}",
          MDC.of(LogKeys.PATH, path), MDC.of(LogKeys.MESSAGE, text));
        case 2 -> LOGGER.warn("Native data source library {}: {}",
          MDC.of(LogKeys.PATH, path), MDC.of(LogKeys.MESSAGE, text));
        case 4 -> LOGGER.debug("Native data source library {}: {}", path, text);
        case 5 -> LOGGER.trace("Native data source library {}: {}", path, text);
        default -> LOGGER.info("Native data source library {}: {}",
          MDC.of(LogKeys.PATH, path), MDC.of(LogKeys.MESSAGE, text));
      }
    } catch (Throwable e) {
      // The message is lost.
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Calls
  // ---------------------------------------------------------------------------------------------

  /** The version of the interface that the library implements. */
  int abiVersion() {
    return abiVersion;
  }

  /** Throws the error that the library reported if a function failed. */
  private void check(int status, String function) throws Throwable {
    if (status == 0) {
      return;
    }
    long address = lastError == null ? 0 : (long) lastError.invokeExact();
    String message = address == 0 ? "" : new String(readCString(address), StandardCharsets.UTF_8);
    throw new RuntimeException(message.isEmpty()
      ? function + " failed with the error code " + status + "."
      : message);
  }

  /** An object that the library opened, with its handle and the reply of the library. */
  static final class Opened {
    final long handle;
    final OpenReply reply;

    Opened(long handle, OpenReply reply) {
      this.handle = handle;
      this.reply = reply;
    }
  }

  /** Opens an object. */
  Opened open(OpenRequest request) throws Throwable {
    try (Memory memory = new Memory(); Sink sink = new Sink()) {
      byte[] bytes = request.toByteArray();
      long out = memory.zeroed(8);
      check((int) open.invokeExact(host, memory.bytes(bytes), (long) bytes.length, Ffm.SINK,
        sink.id, out), "spark_ds_open");
      long handle = Platform.getLong(null, out);
      if (handle == 0) {
        throw new IllegalStateException("spark_ds_open returned a NULL handle.");
      }
      try {
        return new Opened(handle, OpenReply.parseFrom(sink.single("spark_ds_open")));
      } catch (InvalidProtocolBufferException e) {
        // The library opened the object, but its reply is invalid.
        close(handle);
        throw new IllegalStateException(invalidReply("spark_ds_open", e), e);
      } catch (Throwable e) {
        close(handle);
        throw e;
      }
    }
  }

  /** Runs a control operation on an object. */
  CallReply call(long handle, CallRequest request) throws Throwable {
    return call(handle, request, null);
  }

  /**
   * Runs a control operation on an object. Then {@code succeeded} runs right away, before the
   * reply is checked: for example, a data writer that committed must not be aborted, even if its
   * reply is invalid.
   */
  CallReply call(long handle, CallRequest request, Runnable succeeded) throws Throwable {
    try (Memory memory = new Memory(); Sink sink = new Sink()) {
      byte[] bytes = request.toByteArray();
      check((int) call.invokeExact(handle, memory.bytes(bytes), (long) bytes.length, Ffm.SINK,
        sink.id), "spark_ds_call");
      if (succeeded != null) {
        succeeded.run();
      }
      try {
        return CallReply.parseFrom(sink.single("spark_ds_call"));
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalStateException(invalidReply("spark_ds_call", e), e);
      }
    }
  }

  /**
   * Exchanges the data of an object.
   *
   * @param in the address of the input stream, or 0
   * @param out the address of the output stream, or 0
   */
  void stream(long handle, StreamRequest request, long in, long out) throws Throwable {
    try (Memory memory = new Memory()) {
      byte[] bytes = request.toByteArray();
      check((int) stream.invokeExact(handle, memory.bytes(bytes), (long) bytes.length, in, out),
        "spark_ds_stream");
    }
  }

  /** Releases the handle of an object. */
  void close(long handle) throws Throwable {
    close.invokeExact(handle);
  }

  // ---------------------------------------------------------------------------------------------
  // Memory
  // ---------------------------------------------------------------------------------------------

  /** Native memory for the arguments of a call, freed after the call. */
  private static final class Memory implements AutoCloseable {
    private final List<Long> allocations = new ArrayList<>();

    long zeroed(long size) {
      long address = Platform.allocateMemory(size);
      allocations.add(address);
      Platform.setMemory(address, (byte) 0, size);
      return address;
    }

    /** Copies the bytes. Empty bytes are passed as a valid pointer too. */
    long bytes(byte[] bytes) {
      long address = zeroed(Math.max(1, bytes.length));
      Platform.copyMemory(bytes, Platform.BYTE_ARRAY_OFFSET, null, address, bytes.length);
      return address;
    }

    @Override
    public void close() {
      allocations.forEach(Platform::freeMemory);
    }
  }

  private static byte[] readCString(long address) {
    int length = 0;
    while (Platform.getByte(null, address + length) != 0) {
      length++;
    }
    byte[] bytes = new byte[length];
    Platform.copyMemory(null, address, bytes, Platform.BYTE_ARRAY_OFFSET, length);
    return bytes;
  }

  // ---------------------------------------------------------------------------------------------
  // Tasks
  // ---------------------------------------------------------------------------------------------

  /** A task that uses the library, and the number of objects that it opened. */
  private static final class Task {
    final BooleanSupplier interrupted;
    int objects;

    Task(BooleanSupplier interrupted) {
      this.interrupted = interrupted;
    }
  }

  // The tasks that use a library, by the ID of their task attempt, for SparkDsHost.is_interrupted.
  private static final ConcurrentHashMap<Long, Task> tasks = new ConcurrentHashMap<>();

  /**
   * Registers a task for {@code SparkDsHost.is_interrupted}, until the returned
   * {@link AutoCloseable} is closed, once the library no longer uses the object of the task.
   */
  static AutoCloseable registerTask(long taskAttemptId, BooleanSupplier interrupted) {
    tasks.compute(taskAttemptId, (id, task) -> {
      Task registered = task == null ? new Task(interrupted) : task;
      registered.objects++;
      return registered;
    });
    return () -> tasks.computeIfPresent(taskAttemptId, (id, task) ->
      --task.objects == 0 ? null : task);
  }

  /**
   * Called by the library, through {@code SparkDsHost.is_interrupted}, with the ID of a task
   * attempt. It must not throw: an exception thrown by an upcall crashes the JVM.
   */
  private static boolean isInterrupted(long taskAttemptId) {
    try {
      Task task = tasks.get(taskAttemptId);
      // A task that is not registered has ended, or is not a task of this executor.
      return task == null || task.interrupted.getAsBoolean();
    } catch (Throwable e) {
      return false;
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Sink
  // ---------------------------------------------------------------------------------------------

  private static final AtomicLong nextSinkId = new AtomicLong(1);
  private static final ConcurrentHashMap<Long, Sink> sinks = new ConcurrentHashMap<>();

  /**
   * Collects the reply that a library returns during a call, through the C function
   * {@link Ffm#SINK}. The library passes {@link #id} back to identify it.
   */
  private static final class Sink implements AutoCloseable {
    final long id = nextSinkId.getAndIncrement();
    final List<byte[]> values = new ArrayList<>();
    Throwable error;

    Sink() {
      sinks.put(id, this);
    }

    /** Returns the reply, or no bytes, an empty message, if the library did not reply. */
    byte[] single(String function) throws Throwable {
      if (error != null) {
        throw error;
      }
      if (values.size() > 1) {
        throw new IllegalStateException(
          function + " replied " + values.size() + " times instead of once.");
      }
      return values.isEmpty() ? new byte[0] : values.get(0);
    }

    @Override
    public void close() {
      sinks.remove(id);
    }
  }

  /**
   * Called by the library, through the upcall stub {@link Ffm#SINK}, with a reply to copy. It
   * must not throw: an exception thrown by an upcall crashes the JVM.
   */
  private static void sink(long id, long data, long length) {
    Sink sink = null;
    try {
      sink = sinks.get(id);
      if (sink == null || sink.error != null) {
        return;
      }
      if (length < 0 || length > Integer.MAX_VALUE - 8 || (data == 0 && length > 0)) {
        throw new IllegalStateException("The library returned an invalid reply, of " +
          length + " bytes at the address " + data + ".");
      }
      byte[] bytes = new byte[(int) length];
      Platform.copyMemory(null, data, bytes, Platform.BYTE_ARRAY_OFFSET, length);
      sink.values.add(bytes);
    } catch (Throwable e) {
      if (sink != null) {
        sink.error = e;
      }
    }
  }

  /** Describes an invalid reply of the library. */
  private static String invalidReply(String function, InvalidProtocolBufferException e) {
    return function + " returned an invalid reply: " + e.getMessage();
  }
}
