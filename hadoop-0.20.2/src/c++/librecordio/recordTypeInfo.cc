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

#include "recordTypeInfo.hh"

using namespace hadoop;

RecordTypeInfo::RecordTypeInfo() 
{
  pStid = new StructTypeID();
}

RecordTypeInfo::RecordTypeInfo(const char *pName): name(pName)
{
  pStid = new StructTypeID();
}


/*RecordTypeInfo::RecordTypeInfo(const RecordTypeInfo& rti): name(rti.name)
{
  // clone the typeinfos from rti and add them
  for (unsigned int i=0; i<rti.typeInfos.size(); i++) {
    typeInfos.push_back(rti.typeInfos[i]->clone());
  }
  // clone the map
  for (std::map<std::string, RecordTypeInfo*>::const_iterator iter=rti.structRTIs.begin(); 
       iter!=rti.structRTIs.end(); ++iter) {
    structRTIs[iter->first] = iter->second->clone();
  }
}*/


RecordTypeInfo::~RecordTypeInfo()
{
  if (NULL != pStid) 
    delete pStid;

  /*for (unsigned int i=0; i<typeInfos.size(); i++) {
    delete typeInfos[i];
  }
  typeInfos.clear();
  for (std::map<std::string, RecordTypeInfo*>::const_iterator iter=structRTIs.begin(); 
       iter!=structRTIs.end(); ++iter) {
    // delete the RTI objects
    delete iter->second;
  }
  structRTIs.clear();*/
}

void RecordTypeInfo::addField(const std::string* pFieldID, const TypeID* pTypeID)
{
  pStid->getFieldTypeInfos().push_back(new FieldTypeInfo(pFieldID, pTypeID));
}

void RecordTypeInfo::addAll(std::vector<FieldTypeInfo*>& vec)
{
  // we need to copy object clones into our own vector
  for (unsigned int i=0; i<vec.size(); i++) {
    pStid->getFieldTypeInfos().push_back(vec[i]->clone());
  }
}

// make a copy of typeInfos and return it
/*std::vector<TypeInfo*>& RecordTypeInfo::getClonedTypeInfos()
{
  std::vector<TypeInfo*>* pNewVec = new std::vector<TypeInfo*>();
  for (unsigned int i=0; i<typeInfos.size(); i++) {
    pNewVec->push_back(typeInfos[i]->clone());
  }
  return *pNewVec;
} */

const std::vector<FieldTypeInfo*>& RecordTypeInfo::getFieldTypeInfos() const
{
  return pStid->getFieldTypeInfos();
}


RecordTypeInfo* RecordTypeInfo::getNestedStructTypeInfo(const char *structName) const
{
  StructTypeID* p = pStid->findStruct(structName);
  if (NULL == p) return NULL;
  return new RecordTypeInfo(structName, p);
  /*std::string s(structName);
  std::map<std::string, RecordTypeInfo*>::const_iterator iter = structRTIs.find(s);
  if (iter == structRTIs.end()) {
    return NULL;
  }
  return iter->second;*/
}

void RecordTypeInfo::serialize(::hadoop::OArchive& a_, const char* tag) const
{
  a_.startRecord(*this, tag);
  // name
  a_.serialize(name, tag);
  /*// number of elements
  a_.serialize((int32_t)typeInfos.size(), tag);
  // write out each element
  for (std::vector<FieldTypeInfo*>::const_iterator iter=typeInfos.begin();
       iter!=typeInfos.end(); ++iter) {
    (*iter)->serialize(a_, tag);
    }*/
  pStid->serializeRest(a_, tag);
  a_.endRecord(*this, tag);
}

void RecordTypeInfo::print(int space) const
{
  for (int i=0; i<space; i++) {
    printf(" ");
  }
  printf("RecordTypeInfo::%s\n", name.c_str());
  pStid->print(space);
  /*for (unsigned i=0; i<typeInfos.size(); i++) {
    typeInfos[i]->print(space+2);
    }*/
}

void RecordTypeInfo::deserialize(::hadoop::IArchive& a_, const char* tag)
{
  a_.startRecord(*this, tag);
  // name
  a_.deserialize(name, tag);
  pStid->deserialize(a_, tag);
  a_.endRecord(*this, tag);
}

