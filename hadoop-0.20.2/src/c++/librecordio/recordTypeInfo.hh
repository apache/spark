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

#ifndef RECORDTYPEINFO_HH_
#define RECORDTYPEINFO_HH_

#include "recordio.hh"
#include <vector>
#include <map>
#include "fieldTypeInfo.hh"

namespace hadoop {

class RecordTypeInfo : public ::hadoop::Record {

private: 
  //std::vector<FieldTypeInfo* > typeInfos;
  std::string name;
  //std::map<std::string, RecordTypeInfo*> structRTIs;
  StructTypeID *pStid;

  RecordTypeInfo(const char * pName, StructTypeID* pStid): name(pName),pStid(pStid) {}

public: 
  RecordTypeInfo();
  RecordTypeInfo(const char *pName);
  //RecordTypeInfo(const RecordTypeInfo& rti);
  virtual ~RecordTypeInfo();

  void addField(const std::string* pFieldID, const TypeID* pTypeID);
  void addAll(std::vector<FieldTypeInfo*>& vec);
  const std::vector<FieldTypeInfo*>& getFieldTypeInfos() const;
  void serialize(::hadoop::OArchive& a_, const char* tag) const;
  void deserialize(::hadoop::IArchive& a_, const char* tag);
  RecordTypeInfo* clone() const {return new RecordTypeInfo(*this);}
  RecordTypeInfo* getNestedStructTypeInfo(const char *structName) const;

  const ::std::string& getName() const {return name;}
  void setName(const ::std::string& name) {this->name = name;}

  const ::std::string& type() const {return name;}
  const ::std::string& signature() const {return name;}

  void print(int space=0) const;


};



}
#endif // RECORDTYPEINFO_HH_

