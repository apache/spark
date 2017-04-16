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
import java.util.*;

import org.apache.hadoop.record.RecordInput;
import org.apache.hadoop.record.RecordOutput;

/** 
 * Represents typeID for a struct 
 */
public class StructTypeID extends TypeID {
  private ArrayList<FieldTypeInfo> typeInfos = new ArrayList<FieldTypeInfo>();
  
  StructTypeID() {
    super(RIOType.STRUCT);
  }
  
  /**
   * Create a StructTypeID based on the RecordTypeInfo of some record
   */
  public StructTypeID(RecordTypeInfo rti) {
    super(RIOType.STRUCT);
    typeInfos.addAll(rti.getFieldTypeInfos());
  }

  void add (FieldTypeInfo ti) {
    typeInfos.add(ti);
  }
  
  public Collection<FieldTypeInfo> getFieldTypeInfos() {
    return typeInfos;
  }
  
  /* 
   * return the StructTypeiD, if any, of the given field 
   */
  StructTypeID findStruct(String name) {
    // walk through the list, searching. Not the most efficient way, but this
    // in intended to be used rarely, so we keep it simple. 
    // As an optimization, we can keep a hashmap of record name to its RTI, for later.
    for (FieldTypeInfo ti : typeInfos) {
      if ((0 == ti.getFieldID().compareTo(name)) && (ti.getTypeID().getTypeVal() == RIOType.STRUCT)) {
        return (StructTypeID) ti.getTypeID();
      }
    }
    return null;
  }
  
  void write(RecordOutput rout, String tag) throws IOException {
    rout.writeByte(typeVal, tag);
    writeRest(rout, tag);
  }

  /* 
   * Writes rest of the struct (excluding type value).
   * As an optimization, this method is directly called by RTI 
   * for the top level record so that we don't write out the byte
   * indicating that this is a struct (since top level records are
   * always structs).
   */
  void writeRest(RecordOutput rout, String tag) throws IOException {
    rout.writeInt(typeInfos.size(), tag);
    for (FieldTypeInfo ti : typeInfos) {
      ti.write(rout, tag);
    }
  }

  /* 
   * deserialize ourselves. Called by RTI. 
   */
  void read(RecordInput rin, String tag) throws IOException {
    // number of elements
    int numElems = rin.readInt(tag);
    for (int i=0; i<numElems; i++) {
      typeInfos.add(genericReadTypeInfo(rin, tag));
    }
  }
  
  // generic reader: reads the next TypeInfo object from stream and returns it
  private FieldTypeInfo genericReadTypeInfo(RecordInput rin, String tag) throws IOException {
    String fieldName = rin.readString(tag);
    TypeID id = genericReadTypeID(rin, tag);
    return new FieldTypeInfo(fieldName, id);
  }
  
  // generic reader: reads the next TypeID object from stream and returns it
  private TypeID genericReadTypeID(RecordInput rin, String tag) throws IOException {
    byte typeVal = rin.readByte(tag);
    switch (typeVal) {
    case TypeID.RIOType.BOOL: 
      return TypeID.BoolTypeID;
    case TypeID.RIOType.BUFFER: 
      return TypeID.BufferTypeID;
    case TypeID.RIOType.BYTE:
      return TypeID.ByteTypeID;
    case TypeID.RIOType.DOUBLE:
      return TypeID.DoubleTypeID;
    case TypeID.RIOType.FLOAT:
      return TypeID.FloatTypeID;
    case TypeID.RIOType.INT: 
      return TypeID.IntTypeID;
    case TypeID.RIOType.LONG:
      return TypeID.LongTypeID;
    case TypeID.RIOType.MAP:
    {
      TypeID tIDKey = genericReadTypeID(rin, tag);
      TypeID tIDValue = genericReadTypeID(rin, tag);
      return new MapTypeID(tIDKey, tIDValue);
    }
    case TypeID.RIOType.STRING: 
      return TypeID.StringTypeID;
    case TypeID.RIOType.STRUCT: 
    {
      StructTypeID stID = new StructTypeID();
      int numElems = rin.readInt(tag);
      for (int i=0; i<numElems; i++) {
        stID.add(genericReadTypeInfo(rin, tag));
      }
      return stID;
    }
    case TypeID.RIOType.VECTOR: 
    {
      TypeID tID = genericReadTypeID(rin, tag);
      return new VectorTypeID(tID);
    }
    default:
      // shouldn't be here
      throw new IOException("Unknown type read");
    }
  }

}
