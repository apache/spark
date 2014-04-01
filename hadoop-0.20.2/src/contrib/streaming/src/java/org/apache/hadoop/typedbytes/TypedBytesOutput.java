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

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.record.Buffer;

/**
 * Provides functionality for writing typed bytes.
 */
public class TypedBytesOutput {

  private DataOutput out;

  private TypedBytesOutput() {}

  private void setDataOutput(DataOutput out) {
    this.out = out;
  }

  private static ThreadLocal tbOut = new ThreadLocal() {
    protected synchronized Object initialValue() {
      return new TypedBytesOutput();
    }
  };

  /**
   * Get a thread-local typed bytes output for the supplied {@link DataOutput}.
   * 
   * @param out data output object
   * @return typed bytes output corresponding to the supplied 
   * {@link DataOutput}.
   */
  public static TypedBytesOutput get(DataOutput out) {
    TypedBytesOutput bout = (TypedBytesOutput) tbOut.get();
    bout.setDataOutput(out);
    return bout;
  }

  /** Creates a new instance of TypedBytesOutput. */
  public TypedBytesOutput(DataOutput out) {
    this.out = out;
  }
  
  /**
   * Writes a Java object as a typed bytes sequence.
   * 
   * @param obj the object to be written
   * @throws IOException
   */
  public void write(Object obj) throws IOException {
    if (obj instanceof Buffer) {
      writeBytes(((Buffer) obj).get());
    } else if (obj instanceof Byte) {
      writeByte((Byte) obj);
    } else if (obj instanceof Boolean) {
      writeBool((Boolean) obj);
    } else if (obj instanceof Integer) {
      writeInt((Integer) obj);
    } else if (obj instanceof Long) {
      writeLong((Long) obj);
    } else if (obj instanceof Float) {
      writeFloat((Float) obj);
    } else if (obj instanceof Double) {
      writeDouble((Double) obj);
    } else if (obj instanceof String) {
      writeString((String) obj);
    } else if (obj instanceof ArrayList) {
      writeVector((ArrayList) obj);
    } else if (obj instanceof List) {
      writeList((List) obj);
    } else if (obj instanceof Map) {
      writeMap((Map) obj);
    } else {
      throw new RuntimeException("cannot write objects of this type");
    }
  }

  /**
   * Writes a raw sequence of typed bytes.
   * 
   * @param bytes the bytes to be written
   * @throws IOException
   */
  public void writeRaw(byte[] bytes) throws IOException {
    out.write(bytes);
  }

  /**
   * Writes a raw sequence of typed bytes.
   * 
   * @param bytes the bytes to be written
   * @param offset an offset in the given array
   * @param length number of bytes from the given array to write
   * @throws IOException
   */
  public void writeRaw(byte[] bytes, int offset, int length)
    throws IOException {
    out.write(bytes, offset, length);
  }

  /**
   * Writes a bytes array as a typed bytes sequence, using a given typecode.
   * 
   * @param bytes the bytes array to be written
   * @param code the typecode to use
   * @throws IOException
   */
  public void writeBytes(byte[] bytes, int code) throws IOException {
    out.write(code);
    out.writeInt(bytes.length);
    out.write(bytes);
  }
  
  /**
   * Writes a bytes array as a typed bytes sequence.
   * 
   * @param bytes the bytes array to be written
   * @throws IOException
   */
  public void writeBytes(byte[] bytes) throws IOException {
    writeBytes(bytes, Type.BYTES.code);
  }

  /**
   * Writes a byte as a typed bytes sequence.
   * 
   * @param b the byte to be written
   * @throws IOException
   */
  public void writeByte(byte b) throws IOException {
    out.write(Type.BYTE.code);
    out.write(b);
  }

  /**
   * Writes a boolean as a typed bytes sequence.
   * 
   * @param b the boolean to be written
   * @throws IOException
   */
  public void writeBool(boolean b) throws IOException {
    out.write(Type.BOOL.code);
    out.writeBoolean(b);
  }

  /**
   * Writes an integer as a typed bytes sequence.
   * 
   * @param i the integer to be written
   * @throws IOException
   */
  public void writeInt(int i) throws IOException {
    out.write(Type.INT.code);
    out.writeInt(i);
  }

  /**
   * Writes a long as a typed bytes sequence.
   * 
   * @param l the long to be written
   * @throws IOException
   */
  public void writeLong(long l) throws IOException {
    out.write(Type.LONG.code);
    out.writeLong(l);
  }

  /**
   * Writes a float as a typed bytes sequence.
   * 
   * @param f the float to be written
   * @throws IOException
   */
  public void writeFloat(float f) throws IOException {
    out.write(Type.FLOAT.code);
    out.writeFloat(f);
  }

  /**
   * Writes a double as a typed bytes sequence.
   * 
   * @param d the double to be written
   * @throws IOException
   */
  public void writeDouble(double d) throws IOException {
    out.write(Type.DOUBLE.code);
    out.writeDouble(d);
  }

  /**
   * Writes a string as a typed bytes sequence.
   * 
   * @param s the string to be written
   * @throws IOException
   */
  public void writeString(String s) throws IOException {
    out.write(Type.STRING.code);
    WritableUtils.writeString(out, s);
  }

  /**
   * Writes a vector as a typed bytes sequence.
   * 
   * @param vector the vector to be written
   * @throws IOException
   */
  public void writeVector(ArrayList vector) throws IOException {
    writeVectorHeader(vector.size());
    for (Object obj : vector) {
      write(obj);
    }
  }

  /**
   * Writes a vector header.
   * 
   * @param length the number of elements in the vector
   * @throws IOException
   */
  public void writeVectorHeader(int length) throws IOException {
    out.write(Type.VECTOR.code);
    out.writeInt(length);
  }

  /**
   * Writes a list as a typed bytes sequence.
   * 
   * @param list the list to be written
   * @throws IOException
   */
  public void writeList(List list) throws IOException {
    writeListHeader();
    for (Object obj : list) {
      write(obj);
    }
    writeListFooter();
  }

  /**
   * Writes a list header.
   * 
   * @throws IOException
   */
  public void writeListHeader() throws IOException {
    out.write(Type.LIST.code);
  }

  /**
   * Writes a list footer.
   * 
   * @throws IOException
   */
  public void writeListFooter() throws IOException {
    out.write(Type.MARKER.code);
  }

  /**
   * Writes a map as a typed bytes sequence.
   * 
   * @param map the map to be written
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public void writeMap(Map map) throws IOException {
    writeMapHeader(map.size());
    Set<Entry> entries = map.entrySet();
    for (Entry entry : entries) {
      write(entry.getKey());
      write(entry.getValue());
    }
  }

  /**
   * Writes a map header.
   * 
   * @param length the number of key-value pairs in the map
   * @throws IOException
   */
  public void writeMapHeader(int length) throws IOException {
    out.write(Type.MAP.code);
    out.writeInt(length);
  }

}
