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

#ifndef FIELDTYPEINFO_HH_
#define FIELDTYPEINFO_HH_

#include "recordio.hh"
#include "typeIDs.hh"

namespace hadoop {

class TypeID;

/** 
 * Represents a type information for a field, which is made up of its 
 * ID (name) and its type (a TypeID object).
 */
class FieldTypeInfo {
  
private: 
  // we own memory mgmt of these vars
  const std::string* pFieldID;
  const TypeID* pTypeID;

public: 
  FieldTypeInfo(const std::string* pFieldID, const TypeID* pTypeID) : 
    pFieldID(pFieldID), pTypeID(pTypeID) {}
  FieldTypeInfo(const FieldTypeInfo& ti);
  virtual ~FieldTypeInfo();

  const TypeID* getTypeID() const {return pTypeID;}
  const std::string* getFieldID() const {return pFieldID;}
  void serialize(::hadoop::OArchive& a_, const char* tag) const;
  bool operator==(const FieldTypeInfo& peer_) const;
  FieldTypeInfo* clone() const {return new FieldTypeInfo(*this);}

  void print(int space=0) const;

};

}

#endif // FIELDTYPEINFO_HH_

