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

package org.apache.spark.util.kvstore;

import com.google.common.io.ByteStreams;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Set;

public class KVStoreSnapshotter {
  private static final int MARKER_END_OF_TYPE = -2;
  private static final int MARKER_END_OF_FILE = -1;

  private final KVStoreSerializer serializer;

  public KVStoreSnapshotter(KVStoreSerializer serializer) {
    this.serializer = serializer;
  }

  public void dump(KVStore store, File snapshotFile) throws Exception {
    DataOutputStream output = new DataOutputStream(new FileOutputStream(snapshotFile));

    // store metadata if it exists
    Class<?> metadataType = store.metadataType();
    if (metadataType != null) {
      writeClassName(metadataType, output);
      Object metadata = store.getMetadata(metadataType);
      writeObject(metadata, output);
      writeEndOfType(output);
    } else {
      writeEndOfType(output);
    }

    Set<Class<?>> types = store.types();
    for (Class<?> clazz : types) {
      writeClassName(clazz, output);

      KVStoreView<?> view = store.view(clazz);
      for (Object obj : view) {
        writeObject(obj, output);
      }

      writeEndOfType(output);
    }

    writeEndOfFile(output);
    output.close();
  }

  public void restore(File snapshotFile, KVStore store) throws Exception {
    DataInputStream input = new DataInputStream(new FileInputStream(snapshotFile));

    // first one would be metadata
    int metadataClazzLen = input.readInt();
    if (metadataClazzLen > 0) {
      Class<?> metadataClazz = readClassName(input, metadataClazzLen);
      // metadata presented
      int objLen = input.readInt();
      Object metadata = readObj(input, metadataClazz, objLen);
      store.setMetadata(metadata);

      // additionally read -2 as end of type
      consumeEndOfType(input);
    }

    boolean eof = false;
    while (!eof) {
      int typeClazzNameLen = input.readInt();
      if (typeClazzNameLen == MARKER_END_OF_FILE) {
        eof = true;
      } else {
        Class<?> typeClazz = readClassName(input, typeClazzNameLen);
        boolean eot = false;
        while (!eot) {
          int objLen = input.readInt();
          if (objLen == MARKER_END_OF_TYPE) {
            eot = true;
          } else {
            Object obj = readObj(input, typeClazz, objLen);
            store.write(obj);
          }
        }
      }
    }

    input.close();
  }

  private void writeClassName(Class<?> clazz, DataOutputStream output) throws IOException {
    byte[] clazzName = clazz.getCanonicalName().getBytes(StandardCharsets.UTF_8);
    output.writeInt(clazzName.length);
    output.write(clazzName);
  }

  private void writeObject(Object obj, DataOutputStream output) throws Exception {
    byte[] ser = serializer.serialize(obj);
    output.writeInt(ser.length);
    output.write(ser);
  }

  private void writeEndOfType(DataOutputStream output) throws IOException {
    output.writeInt(MARKER_END_OF_TYPE);
  }

  private void writeEndOfFile(DataOutputStream output) throws IOException {
    output.writeInt(MARKER_END_OF_FILE);
  }

  private Class<?> readClassName(
      DataInputStream input,
      int classNameLen) throws IOException, ClassNotFoundException {
    byte[] classNameBuffer = new byte[classNameLen];
    ByteStreams.readFully(input, classNameBuffer, 0, classNameLen);
    String className = new String(classNameBuffer, StandardCharsets.UTF_8);
    return Class.forName(className);
  }

  private Object readObj(DataInputStream input, Class<?> clazz, int objLen) throws Exception {
    byte[] objBuffer = new byte[objLen];
    ByteStreams.readFully(input, objBuffer, 0, objLen);
    return serializer.deserialize(objBuffer, clazz);
  }

  private void consumeEndOfType(DataInputStream input) throws IOException {
    int eotCode = input.readInt();
    if (eotCode != MARKER_END_OF_TYPE) {
      throw new IllegalStateException("The notion of 'end of type' is expected here, but got " +
          eotCode + " instead");
    }
  }
}
