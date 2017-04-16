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

package org.apache.hadoop.record.meta;

import java.io.IOException;
import org.apache.hadoop.record.RecordOutput;

/** 
 * Represents typeID for basic types. 
 */
public class TypeID {

  /**
   * constants representing the IDL types we support
   */
  public static final class RIOType {
    public static final byte BOOL   = 1;
    public static final byte BUFFER = 2;
    public static final byte BYTE   = 3;
    public static final byte DOUBLE = 4;
    public static final byte FLOAT  = 5;
    public static final byte INT    = 6;
    public static final byte LONG   = 7;
    public static final byte MAP    = 8;
    public static final byte STRING = 9;
    public static final byte STRUCT = 10;
    public static final byte VECTOR = 11;
  }

  /**
   * Constant classes for the basic types, so we can share them.
   */
  public static final TypeID BoolTypeID = new TypeID(RIOType.BOOL);
  public static final TypeID BufferTypeID = new TypeID(RIOType.BUFFER);
  public static final TypeID ByteTypeID = new TypeID(RIOType.BYTE);
  public static final TypeID DoubleTypeID = new TypeID(RIOType.DOUBLE);
  public static final TypeID FloatTypeID = new TypeID(RIOType.FLOAT);
  public static final TypeID IntTypeID = new TypeID(RIOType.INT);
  public static final TypeID LongTypeID = new TypeID(RIOType.LONG);
  public static final TypeID StringTypeID = new TypeID(RIOType.STRING);
  
  protected byte typeVal;

  /**
   * Create a TypeID object 
   */
  TypeID(byte typeVal) {
    this.typeVal = typeVal;
  }

  /**
   * Get the type value. One of the constants in RIOType.
   */
  public byte getTypeVal() {
    return typeVal;
  }

  /**
   * Serialize the TypeID object
   */
  void write(RecordOutput rout, String tag) throws IOException {
    rout.writeByte(typeVal, tag);
  }
  
  /**
   * Two base typeIDs are equal if they refer to the same type
   */
  public boolean equals(Object o) {
    if (this == o) 
      return true;

    if (o == null)
      return false;

    if (this.getClass() != o.getClass())
      return false;

    TypeID oTypeID = (TypeID) o;
    return (this.typeVal == oTypeID.typeVal);
  }
  
  /**
   * We use a basic hashcode implementation, since this class will likely not
   * be used as a hashmap key 
   */
  public int hashCode() {
    // See 'Effectve Java' by Joshua Bloch
    return 37*17+(int)typeVal;
  }
}

