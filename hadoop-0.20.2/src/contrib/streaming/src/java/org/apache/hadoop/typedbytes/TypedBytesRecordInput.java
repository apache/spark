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

import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.record.Buffer;
import org.apache.hadoop.record.Index;
import org.apache.hadoop.record.RecordInput;

/**
 * Serializer for records that writes typed bytes.
 */
public class TypedBytesRecordInput implements RecordInput {

  private TypedBytesInput in;

  private TypedBytesRecordInput() {}

  private void setTypedBytesInput(TypedBytesInput in) {
    this.in = in;
  }

  private static ThreadLocal tbIn = new ThreadLocal() {
    protected synchronized Object initialValue() {
      return new TypedBytesRecordInput();
    }
  };

  /**
   * Get a thread-local typed bytes record input for the supplied
   * {@link TypedBytesInput}.
   * 
   * @param in typed bytes input object
   * @return typed bytes record input corresponding to the supplied
   *         {@link TypedBytesInput}.
   */
  public static TypedBytesRecordInput get(TypedBytesInput in) {
    TypedBytesRecordInput bin = (TypedBytesRecordInput) tbIn.get();
    bin.setTypedBytesInput(in);
    return bin;
  }

  /**
   * Get a thread-local typed bytes record input for the supplied
   * {@link DataInput}.
   * 
   * @param in data input object
   * @return typed bytes record input corresponding to the supplied
   *         {@link DataInput}.
   */
  public static TypedBytesRecordInput get(DataInput in) {
    return get(TypedBytesInput.get(in));
  }

  /** Creates a new instance of TypedBytesRecordInput. */
  public TypedBytesRecordInput(TypedBytesInput in) {
    this.in = in;
  }

  /** Creates a new instance of TypedBytesRecordInput. */
  public TypedBytesRecordInput(DataInput in) {
    this(new TypedBytesInput(in));
  }

  public boolean readBool(String tag) throws IOException {
    in.skipType();
    return in.readBool();
  }

  public Buffer readBuffer(String tag) throws IOException {
    in.skipType();
    return new Buffer(in.readBytes());
  }

  public byte readByte(String tag) throws IOException {
    in.skipType();
    return in.readByte();
  }

  public double readDouble(String tag) throws IOException {
    in.skipType();
    return in.readDouble();
  }

  public float readFloat(String tag) throws IOException {
    in.skipType();
    return in.readFloat();
  }

  public int readInt(String tag) throws IOException {
    in.skipType();
    return in.readInt();
  }

  public long readLong(String tag) throws IOException {
    in.skipType();
    return in.readLong();
  }

  public String readString(String tag) throws IOException {
    in.skipType();
    return in.readString();
  }

  public void startRecord(String tag) throws IOException {
    in.skipType();
  }

  public Index startVector(String tag) throws IOException {
    in.skipType();
    return new TypedBytesIndex(in.readVectorHeader());
  }

  public Index startMap(String tag) throws IOException {
    in.skipType();
    return new TypedBytesIndex(in.readMapHeader());
  }

  public void endRecord(String tag) throws IOException {}

  public void endVector(String tag) throws IOException {}

  public void endMap(String tag) throws IOException {}

  private static  final class TypedBytesIndex implements Index {
    private int nelems;

    private TypedBytesIndex(int nelems) {
      this.nelems = nelems;
    }

    public boolean done() {
      return (nelems <= 0);
    }

    public void incr() {
      nelems--;
    }
  }

}
