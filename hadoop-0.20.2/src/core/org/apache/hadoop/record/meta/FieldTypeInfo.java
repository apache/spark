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
 * Represents a type information for a field, which is made up of its 
 * ID (name) and its type (a TypeID object).
 */
public class FieldTypeInfo
{

  private String fieldID;
  private TypeID typeID;

  /**
   * Construct a FiledTypeInfo with the given field name and the type
   */
  FieldTypeInfo(String fieldID, TypeID typeID) {
    this.fieldID = fieldID;
    this.typeID = typeID;
  }

  /**
   * get the field's TypeID object
   */
  public TypeID getTypeID() {
    return typeID;
  }
  
  /**
   * get the field's id (name)
   */
  public String getFieldID() {
    return fieldID;
  }
  
  void write(RecordOutput rout, String tag) throws IOException {
    rout.writeString(fieldID, tag);
    typeID.write(rout, tag);
  }
  
  /**
   * Two FieldTypeInfos are equal if ach of their fields matches
   */
  public boolean equals(Object o) {
    if (this == o) 
      return true;
    if (!(o instanceof FieldTypeInfo))
      return false;
    FieldTypeInfo fti = (FieldTypeInfo) o;
    // first check if fieldID matches
    if (!this.fieldID.equals(fti.fieldID)) {
      return false;
    }
    // now see if typeID matches
    return (this.typeID.equals(fti.typeID));
  }
  
  /**
   * We use a basic hashcode implementation, since this class will likely not
   * be used as a hashmap key 
   */
  public int hashCode() {
    return 37*17+typeID.hashCode() + 37*17+fieldID.hashCode();
  }
  

  public boolean equals(FieldTypeInfo ti) {
    // first check if fieldID matches
    if (!this.fieldID.equals(ti.fieldID)) {
      return false;
    }
    // now see if typeID matches
    return (this.typeID.equals(ti.typeID));
  }

}

