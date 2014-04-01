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
import java.util.TreeMap;

import org.apache.hadoop.record.Buffer;
import org.apache.hadoop.record.Record;
import org.apache.hadoop.record.RecordOutput;

/**
 * Deserialized for records that reads typed bytes.
 */
public class TypedBytesRecordOutput implements RecordOutput {

  private TypedBytesOutput out;

  private TypedBytesRecordOutput() {}

  private void setTypedBytesOutput(TypedBytesOutput out) {
    this.out = out;
  }

  private static ThreadLocal tbOut = new ThreadLocal() {
    protected synchronized Object initialValue() {
      return new TypedBytesRecordOutput();
    }
  };

  /**
   * Get a thread-local typed bytes record input for the supplied
   * {@link TypedBytesOutput}.
   * 
   * @param out typed bytes output object
   * @return typed bytes record output corresponding to the supplied
   *         {@link TypedBytesOutput}.
   */
  public static TypedBytesRecordOutput get(TypedBytesOutput out) {
    TypedBytesRecordOutput bout = (TypedBytesRecordOutput) tbOut.get();
    bout.setTypedBytesOutput(out);
    return bout;
  }

  /**
   * Get a thread-local typed bytes record output for the supplied
   * {@link DataOutput}.
   * 
   * @param out data output object
   * @return typed bytes record output corresponding to the supplied
   *         {@link DataOutput}.
   */
  public static TypedBytesRecordOutput get(DataOutput out) {
    return get(TypedBytesOutput.get(out));
  }

  /** Creates a new instance of TypedBytesRecordOutput. */
  public TypedBytesRecordOutput(TypedBytesOutput out) {
    this.out = out;
  }

  /** Creates a new instance of TypedBytesRecordOutput. */
  public TypedBytesRecordOutput(DataOutput out) {
    this(new TypedBytesOutput(out));
  }

  public void writeBool(boolean b, String tag) throws IOException {
    out.writeBool(b);
  }

  public void writeBuffer(Buffer buf, String tag) throws IOException {
    out.writeBytes(buf.get());
  }

  public void writeByte(byte b, String tag) throws IOException {
    out.writeByte(b);
  }

  public void writeDouble(double d, String tag) throws IOException {
    out.writeDouble(d);
  }

  public void writeFloat(float f, String tag) throws IOException {
    out.writeFloat(f);
  }

  public void writeInt(int i, String tag) throws IOException {
    out.writeInt(i);
  }

  public void writeLong(long l, String tag) throws IOException {
    out.writeLong(l);
  }

  public void writeString(String s, String tag) throws IOException {
    out.writeString(s);
  }

  public void startRecord(Record r, String tag) throws IOException {
    out.writeListHeader();
  }

  public void startVector(ArrayList v, String tag) throws IOException {
    out.writeVectorHeader(v.size());
  }

  public void startMap(TreeMap m, String tag) throws IOException {
    out.writeMapHeader(m.size());
  }

  public void endRecord(Record r, String tag) throws IOException {
    out.writeListFooter();
  }

  public void endVector(ArrayList v, String tag) throws IOException {}

  public void endMap(TreeMap m, String tag) throws IOException {}

}
