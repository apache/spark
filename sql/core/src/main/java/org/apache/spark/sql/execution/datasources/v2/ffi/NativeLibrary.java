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

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.spark.sql.datasource.NativeBridge;

/**
 * A native data source library, loaded into its own copy of {@link NativeBridge}.
 * <p>
 * JNI links the native methods of a class to the libraries loaded by the class loader of that
 * class, and a library belongs to the class loader of the class that loads it. Defining a copy of
 * {@link NativeBridge} in a separate class loader for each library therefore lets all the
 * libraries export the same JNI functions. Spark calls the methods of a copy through method
 * handles, because the copy is a different class than the {@link NativeBridge} it is compiled
 * against. Calling a method that the library does not export throws
 * {@link UnsatisfiedLinkError}.
 */
final class NativeLibrary {

  private static final String BRIDGE_CLASS = NativeBridge.class.getName();

  private final MethodHandle abiVersion;
  private final MethodHandle createDataSource;
  private final MethodHandle schema;
  private final MethodHandle createReader;
  private final MethodHandle createStreamReader;
  private final MethodHandle createWriter;
  private final MethodHandle createStreamWriter;
  private final MethodHandle closeDataSource;
  private final MethodHandle pushPredicates;
  private final MethodHandle pushLimit;
  private final MethodHandle pruneColumns;
  private final MethodHandle partitions;
  private final MethodHandle serializeReader;
  private final MethodHandle closeReader;
  private final MethodHandle initialOffset;
  private final MethodHandle latestOffset;
  private final MethodHandle streamPartitions;
  private final MethodHandle serializeStreamReader;
  private final MethodHandle commitOffset;
  private final MethodHandle closeStreamReader;
  private final MethodHandle read;
  private final MethodHandle serializeWriter;
  private final MethodHandle commit;
  private final MethodHandle abort;
  private final MethodHandle closeWriter;
  private final MethodHandle createDataWriter;
  private final MethodHandle write;
  private final MethodHandle commitDataWriter;
  private final MethodHandle abortDataWriter;

  private NativeLibrary(Class<?> bridge) throws ReflectiveOperationException {
    Class<?> jlong = long.class;
    Class<?> bytes = byte[].class;
    Class<?> strings = String[].class;
    abiVersion = find(bridge, "abiVersion", int.class);
    createDataSource = find(bridge, "createDataSource", jlong, String.class, strings, strings);
    schema = find(bridge, "schema", void.class, jlong, jlong);
    createReader = find(bridge, "createReader", jlong, jlong, jlong);
    createStreamReader = find(bridge, "createStreamReader", jlong, jlong, jlong);
    createWriter = find(bridge, "createWriter", jlong, jlong, jlong, boolean.class);
    createStreamWriter = find(bridge, "createStreamWriter", jlong, jlong, jlong, boolean.class);
    closeDataSource = find(bridge, "closeDataSource", void.class, jlong);
    pushPredicates = find(bridge, "pushPredicates", boolean[].class, jlong, strings);
    pushLimit = find(bridge, "pushLimit", boolean.class, jlong, int.class);
    pruneColumns = find(bridge, "pruneColumns", boolean.class, jlong, strings);
    partitions = find(bridge, "partitions", byte[][].class, jlong);
    serializeReader = find(bridge, "serializeReader", bytes, jlong);
    closeReader = find(bridge, "closeReader", void.class, jlong);
    initialOffset = find(bridge, "initialOffset", String.class, jlong);
    latestOffset = find(bridge, "latestOffset", String.class, jlong);
    streamPartitions =
      find(bridge, "streamPartitions", byte[][].class, jlong, String.class, String.class);
    serializeStreamReader = find(bridge, "serializeStreamReader", bytes, jlong);
    commitOffset = find(bridge, "commitOffset", void.class, jlong, String.class);
    closeStreamReader = find(bridge, "closeStreamReader", void.class, jlong);
    read = find(bridge, "read", void.class, bytes, bytes, jlong);
    serializeWriter = find(bridge, "serializeWriter", bytes, jlong);
    commit = find(bridge, "commit", void.class, jlong, jlong, byte[][].class);
    abort = find(bridge, "abort", void.class, jlong, jlong, byte[][].class);
    closeWriter = find(bridge, "closeWriter", void.class, jlong);
    createDataWriter = find(bridge, "createDataWriter", jlong, bytes, int.class, jlong, jlong);
    write = find(bridge, "write", void.class, jlong, jlong, jlong);
    commitDataWriter = find(bridge, "commitDataWriter", bytes, jlong);
    abortDataWriter = find(bridge, "abortDataWriter", void.class, jlong);
  }

  private static MethodHandle find(
      Class<?> bridge, String name, Class<?> returnType, Class<?>... parameterTypes)
      throws ReflectiveOperationException {
    return MethodHandles.publicLookup()
      .findStatic(bridge, name, MethodType.methodType(returnType, parameterTypes));
  }

  /**
   * Loads the library at the given path into a new copy of {@link NativeBridge}.
   */
  static NativeLibrary load(String path) throws Throwable {
    Class<?> bridge = new BridgeClassLoader(NativeBridge.class.getClassLoader())
      .loadClass(BRIDGE_CLASS);
    Method load = bridge.getDeclaredMethod("load", String.class);
    load.setAccessible(true);
    try {
      load.invoke(null, path);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
    return new NativeLibrary(bridge);
  }

  /**
   * A class loader that defines its own copy of {@link NativeBridge}, and delegates all the other
   * classes to its parent.
   */
  private static final class BridgeClassLoader extends ClassLoader {
    static {
      ClassLoader.registerAsParallelCapable();
    }

    BridgeClassLoader(ClassLoader parent) {
      super(parent);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      if (!BRIDGE_CLASS.equals(name)) {
        return super.loadClass(name, resolve);
      }
      synchronized (getClassLoadingLock(name)) {
        Class<?> loaded = findLoadedClass(name);
        if (loaded == null) {
          String resource = name.replace('.', '/') + ".class";
          try (InputStream in = getParent().getResourceAsStream(resource)) {
            if (in == null) {
              throw new ClassNotFoundException(name);
            }
            byte[] bytes = in.readAllBytes();
            loaded = defineClass(name, bytes, 0, bytes.length);
          } catch (IOException e) {
            throw new ClassNotFoundException(name, e);
          }
        }
        if (resolve) {
          resolveClass(loaded);
        }
        return loaded;
      }
    }
  }

  int abiVersion() throws Throwable {
    return (int) abiVersion.invokeExact();
  }

  long createDataSource(String name, String[] optionKeys, String[] optionValues)
      throws Throwable {
    return (long) createDataSource.invokeExact(name, optionKeys, optionValues);
  }

  void schema(long dataSource, long schemaAddress) throws Throwable {
    schema.invokeExact(dataSource, schemaAddress);
  }

  long createReader(long dataSource, long schemaAddress) throws Throwable {
    return (long) createReader.invokeExact(dataSource, schemaAddress);
  }

  long createStreamReader(long dataSource, long schemaAddress) throws Throwable {
    return (long) createStreamReader.invokeExact(dataSource, schemaAddress);
  }

  long createWriter(long dataSource, long schemaAddress, boolean overwrite) throws Throwable {
    return (long) createWriter.invokeExact(dataSource, schemaAddress, overwrite);
  }

  long createStreamWriter(long dataSource, long schemaAddress, boolean overwrite)
      throws Throwable {
    return (long) createStreamWriter.invokeExact(dataSource, schemaAddress, overwrite);
  }

  void closeDataSource(long dataSource) throws Throwable {
    closeDataSource.invokeExact(dataSource);
  }

  boolean[] pushPredicates(long reader, String[] predicates) throws Throwable {
    return (boolean[]) pushPredicates.invokeExact(reader, predicates);
  }

  boolean pushLimit(long reader, int limit) throws Throwable {
    return (boolean) pushLimit.invokeExact(reader, limit);
  }

  boolean pruneColumns(long reader, String[] columnNames) throws Throwable {
    return (boolean) pruneColumns.invokeExact(reader, columnNames);
  }

  byte[][] partitions(long reader) throws Throwable {
    return (byte[][]) partitions.invokeExact(reader);
  }

  byte[] serializeReader(long reader) throws Throwable {
    return (byte[]) serializeReader.invokeExact(reader);
  }

  void closeReader(long reader) throws Throwable {
    closeReader.invokeExact(reader);
  }

  String initialOffset(long streamReader) throws Throwable {
    return (String) initialOffset.invokeExact(streamReader);
  }

  String latestOffset(long streamReader) throws Throwable {
    return (String) latestOffset.invokeExact(streamReader);
  }

  byte[][] streamPartitions(long streamReader, String start, String end) throws Throwable {
    return (byte[][]) streamPartitions.invokeExact(streamReader, start, end);
  }

  byte[] serializeStreamReader(long streamReader) throws Throwable {
    return (byte[]) serializeStreamReader.invokeExact(streamReader);
  }

  void commitOffset(long streamReader, String end) throws Throwable {
    commitOffset.invokeExact(streamReader, end);
  }

  void closeStreamReader(long streamReader) throws Throwable {
    closeStreamReader.invokeExact(streamReader);
  }

  void read(byte[] readerState, byte[] partition, long streamAddress) throws Throwable {
    read.invokeExact(readerState, partition, streamAddress);
  }

  byte[] serializeWriter(long writer) throws Throwable {
    return (byte[]) serializeWriter.invokeExact(writer);
  }

  void commit(long writer, long epochId, byte[][] messages) throws Throwable {
    commit.invokeExact(writer, epochId, messages);
  }

  void abort(long writer, long epochId, byte[][] messages) throws Throwable {
    abort.invokeExact(writer, epochId, messages);
  }

  void closeWriter(long writer) throws Throwable {
    closeWriter.invokeExact(writer);
  }

  long createDataWriter(byte[] writerState, int partitionId, long taskId, long epochId)
      throws Throwable {
    return (long) createDataWriter.invokeExact(writerState, partitionId, taskId, epochId);
  }

  void write(long dataWriter, long arrayAddress, long schemaAddress) throws Throwable {
    write.invokeExact(dataWriter, arrayAddress, schemaAddress);
  }

  byte[] commitDataWriter(long dataWriter) throws Throwable {
    return (byte[]) commitDataWriter.invokeExact(dataWriter);
  }

  void abortDataWriter(long dataWriter) throws Throwable {
    abortDataWriter.invokeExact(dataWriter);
  }
}
