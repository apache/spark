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

import org.apache.hadoop.record.RecordOutput;

/** 
 * Represents typeID for a Map 
 */
public class MapTypeID extends TypeID {
  
  private TypeID typeIDKey; 
  private TypeID typeIDValue; 
  
  public MapTypeID(TypeID typeIDKey, TypeID typeIDValue) {
    super(RIOType.MAP);
    this.typeIDKey = typeIDKey;
    this.typeIDValue = typeIDValue;
  }
  
  /**
   * get the TypeID of the map's key element
   */
  public TypeID getKeyTypeID() {
    return this.typeIDKey;
  }
  
  /**
   * get the TypeID of the map's value element
   */
  public TypeID getValueTypeID() {
    return this.typeIDValue;
  }
  
  void write(RecordOutput rout, String tag) throws IOException {
    rout.writeByte(typeVal, tag);
    typeIDKey.write(rout, tag);
    typeIDValue.write(rout, tag);
  }
  
  /**
   * Two map  typeIDs are equal if their constituent elements have the 
   * same type
   */
  public boolean equals(Object o) {
    if (!super.equals(o))
      return false;

    MapTypeID mti = (MapTypeID) o;

    return this.typeIDKey.equals(mti.typeIDKey) &&
           this.typeIDValue.equals(mti.typeIDValue);
  }
  
  /**
   * We use a basic hashcode implementation, since this class will likely not
   * be used as a hashmap key 
   */
  public int hashCode() {
    return 37*17+typeIDKey.hashCode() + 37*17+typeIDValue.hashCode();
  }
  
}
