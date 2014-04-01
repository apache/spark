/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.typedbytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;

/**
 * Writable for typed bytes.
 */
public class TypedBytesWritable extends BytesWritable {

  /** Create a TypedBytesWritable. */
  public TypedBytesWritable() {
    super();
  }

  /** Create a TypedBytesWritable with a given byte array as initial value. */
  public TypedBytesWritable(byte[] bytes) {
    super(bytes);
  }

  /** Set the typed bytes from a given Java object. */
  public void setValue(Object obj) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      TypedBytesOutput tbo = TypedBytesOutput.get(new DataOutputStream(baos));
      tbo.write(obj);
      byte[] bytes = baos.toByteArray();
      set(bytes, 0, bytes.length);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Get the typed bytes as a Java object. */
  public Object getValue() {
    try {
      ByteArrayInputStream bais = new ByteArrayInputStream(getBytes());
      TypedBytesInput tbi = TypedBytesInput.get(new DataInputStream(bais));
      Object obj = tbi.read();
      return obj;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Get the type code embedded in the first byte. */
  public Type getType() {
    byte[] bytes = getBytes();
    if (bytes == null || bytes.length == 0) {
      return null;
    }
    for (Type type : Type.values()) {
      if (type.code == (int) bytes[0]) {
        return type;
      }
    }
    return null;
  }

  /** Generate a suitable string representation. */
  public String toString() {
    return getValue().toString();
  }

}
