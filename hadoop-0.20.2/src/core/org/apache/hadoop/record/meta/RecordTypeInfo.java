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
 * A record's Type Information object which can read/write itself. 
 * 
 * Type information for a record comprises metadata about the record, 
 * as well as a collection of type information for each field in the record. 
 */
public class RecordTypeInfo extends org.apache.hadoop.record.Record 
{

  private String name;
  // A RecordTypeInfo is really just a wrapper around StructTypeID
  StructTypeID sTid;
   // A RecordTypeInfo object is just a collection of TypeInfo objects for each of its fields.  
  //private ArrayList<FieldTypeInfo> typeInfos = new ArrayList<FieldTypeInfo>();
  // we keep a hashmap of struct/record names and their type information, as we need it to 
  // set filters when reading nested structs. This map is used during deserialization.
  //private Map<String, RecordTypeInfo> structRTIs = new HashMap<String, RecordTypeInfo>();

  /**
   * Create an empty RecordTypeInfo object.
   */
  public RecordTypeInfo() {
    sTid = new StructTypeID();
  }

  /**
   * Create a RecordTypeInfo object representing a record with the given name
   * @param name Name of the record
   */
  public RecordTypeInfo(String name) {
    this.name = name;
    sTid = new StructTypeID();
  }

  /*
   * private constructor
   */
  private RecordTypeInfo(String name, StructTypeID stid) {
    this.sTid = stid;
    this.name = name;
  }
  
  /**
   * return the name of the record
   */
  public String getName() {
    return name;
  }

  /**
   * set the name of the record
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Add a field. 
   * @param fieldName Name of the field
   * @param tid Type ID of the field
   */
  public void addField(String fieldName, TypeID tid) {
    sTid.getFieldTypeInfos().add(new FieldTypeInfo(fieldName, tid));
  }
  
  private void addAll(Collection<FieldTypeInfo> tis) {
    sTid.getFieldTypeInfos().addAll(tis);
  }

  /**
   * Return a collection of field type infos
   */
  public Collection<FieldTypeInfo> getFieldTypeInfos() {
    return sTid.getFieldTypeInfos();
  }
  
  /**
   * Return the type info of a nested record. We only consider nesting 
   * to one level. 
   * @param name Name of the nested record
   */
  public RecordTypeInfo getNestedStructTypeInfo(String name) {
    StructTypeID stid = sTid.findStruct(name);
    if (null == stid) return null;
    return new RecordTypeInfo(name, stid);
  }

  /**
   * Serialize the type information for a record
   */
  public void serialize(RecordOutput rout, String tag) throws IOException {
    // write out any header, version info, here
    rout.startRecord(this, tag);
    rout.writeString(name, tag);
    sTid.writeRest(rout, tag);
    rout.endRecord(this, tag);
  }

  /**
   * Deserialize the type information for a record
   */
  public void deserialize(RecordInput rin, String tag) throws IOException {
    // read in any header, version info 
    rin.startRecord(tag);
    // name
    this.name = rin.readString(tag);
    sTid.read(rin, tag);
    rin.endRecord(tag);
  }
  
  /**
   * This class doesn't implement Comparable as it's not meant to be used 
   * for anything besides de/serializing.
   * So we always throw an exception.
   * Not implemented. Always returns 0 if another RecordTypeInfo is passed in. 
   */
  public int compareTo (final Object peer_) throws ClassCastException {
    if (!(peer_ instanceof RecordTypeInfo)) {
      throw new ClassCastException("Comparing different types of records.");
    }
    throw new UnsupportedOperationException("compareTo() is not supported");
  }
}

