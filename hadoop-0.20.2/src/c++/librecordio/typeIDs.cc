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

#include "typeIDs.hh"

using namespace hadoop;

void TypeID::serialize(::hadoop::OArchive& a_, const char* tag) const
{
  a_.serialize(typeVal, tag);
}

bool TypeID::operator==(const TypeID& peer_) const 
{
  return (this->typeVal == peer_.typeVal);
}

void TypeID::print(int space) const
{
  for (int i=0; i<space; i++) {
    printf(" ");
  }
  printf("typeID(%lx) = %d\n", (long)this, typeVal);
}


/*StructTypeID::StructTypeID(const char *p): TypeID(RIOTYPE_STRUCT)
{
  pName = new std::string(p);
}

StructTypeID::StructTypeID(std::string* p): TypeID(RIOTYPE_STRUCT)
{
  this->pName = p;
}*/

StructTypeID::StructTypeID(const std::vector<FieldTypeInfo*>& vec) : 
  TypeID(RIOTYPE_STRUCT) 
{
  // we need to copy object clones into our own vector
  for (unsigned int i=0; i<vec.size(); i++) {
    typeInfos.push_back(vec[i]->clone());
  }
}

/*StructTypeID::StructTypeID(const StructTypeID& ti) :
  TypeID(RIOTYPE_STRUCT)
{
  // we need to copy object clones into our own vector
  for (unsigned int i=0; i<ti.typeInfos.size(); i++) {
    typeInfos.push_back(ti.typeInfos[i]->clone());
  }
} */ 

StructTypeID::~StructTypeID()
{
  for (unsigned int i=0; i<typeInfos.size(); i++) {
    delete typeInfos[i];
  }
}  

void StructTypeID::add(FieldTypeInfo *pti)
{
  typeInfos.push_back(pti);
}

// return the StructTypeiD, if any, of the given field
StructTypeID* StructTypeID::findStruct(const char *pStructName)
{
  // walk through the list, searching. Not the most efficient way, but this
  // in intended to be used rarely, so we keep it simple. 
  // As an optimization, we can keep a hashmap of record name to its RTI, for later.
  for (unsigned int i=0; i<typeInfos.size(); i++) {
    if ((0 == typeInfos[i]->getFieldID()->compare(pStructName)) && 
	(typeInfos[i]->getTypeID()->getTypeVal()==RIOTYPE_STRUCT)) {
      return (StructTypeID*)(typeInfos[i]->getTypeID()->clone());
    }
  }
  return NULL;
}

void StructTypeID::serialize(::hadoop::OArchive& a_, const char* tag) const
{
  a_.serialize(typeVal, tag);
  serializeRest(a_, tag);
}

/* 
 * Writes rest of the struct (excluding type value).
 * As an optimization, this method is directly called by RTI 
 * for the top level record so that we don't write out the byte
 * indicating that this is a struct (since top level records are
 * always structs).
 */
void StructTypeID::serializeRest(::hadoop::OArchive& a_, const char* tag) const
{
  a_.serialize((int32_t)typeInfos.size(), tag);
  for (unsigned int i=0; i<typeInfos.size(); i++) {
    typeInfos[i]->serialize(a_, tag);
  }
}


/* 
 * deserialize ourselves. Called by RTI. 
 */
void StructTypeID::deserialize(::hadoop::IArchive& a_, const char* tag)
{
  // number of elements
  int numElems;
  a_.deserialize(numElems, tag);
  for (int i=0; i<numElems; i++) {
    typeInfos.push_back(genericReadTypeInfo(a_, tag));
  }
}

// generic reader: reads the next TypeInfo object from stream and returns it
FieldTypeInfo* StructTypeID::genericReadTypeInfo(::hadoop::IArchive& a_, const char* tag)
{
  // read name of field
  std::string*  pName = new std::string();
  a_.deserialize(*pName, tag);
  TypeID* pti = genericReadTypeID(a_, tag);
  return new FieldTypeInfo(pName, pti);
}

// generic reader: reads the next TypeID object from stream and returns it
TypeID* StructTypeID::genericReadTypeID(::hadoop::IArchive& a_, const char* tag)
{
  int8_t typeVal;
  a_.deserialize(typeVal, tag);
  switch(typeVal) {
  case RIOTYPE_BOOL: 
  case RIOTYPE_BUFFER: 
  case RIOTYPE_BYTE: 
  case RIOTYPE_DOUBLE: 
  case RIOTYPE_FLOAT: 
  case RIOTYPE_INT: 
  case RIOTYPE_LONG: 
  case RIOTYPE_STRING: 
    return new TypeID(typeVal);
  case RIOTYPE_STRUCT: 
    {
      StructTypeID* pstID = new StructTypeID();
      int numElems;
      a_.deserialize(numElems, tag);
      for (int i=0; i<numElems; i++) {
	pstID->add(genericReadTypeInfo(a_, tag));
      }
      return pstID;
    }
  case RIOTYPE_VECTOR: 
    {
      TypeID* pti = genericReadTypeID(a_, tag);
      return new VectorTypeID(pti);
    }
  case RIOTYPE_MAP: 
    {
      TypeID* ptiKey = genericReadTypeID(a_, tag);
      TypeID* ptiValue = genericReadTypeID(a_, tag);
      return new MapTypeID(ptiKey, ptiValue);
    }
  default: 
    // shouldn't be here
    return NULL;
  }
}

void StructTypeID::print(int space) const
{
  TypeID::print(space);
  for (int i=0; i<space; i++) {
    printf(" ");
  }
  printf("StructTypeInfo(%lx): \n", (long)&typeInfos);
  for (unsigned int i=0; i<typeInfos.size(); i++) {
    typeInfos[i]->print(space+2);
  }
}


VectorTypeID::~VectorTypeID()
{
  delete ptiElement;
}

VectorTypeID::VectorTypeID(const VectorTypeID& ti): TypeID(RIOTYPE_VECTOR)
{
  ptiElement = ti.ptiElement->clone();
}

void VectorTypeID::serialize(::hadoop::OArchive& a_, const char* tag) const
{
  a_.serialize(typeVal, tag);
  ptiElement->serialize(a_, tag);
}

bool VectorTypeID::operator==(const TypeID& peer_) const
{
  if (typeVal != peer_.getTypeVal()) {
    return false;
  }
  // this must be a vector type id
  return (*ptiElement) == (*((VectorTypeID&)peer_).ptiElement);
}

void VectorTypeID::print(int space) const
{
  TypeID::print(space);
  for (int i=0; i<space; i++) {
    printf(" ");
  }
  printf("VectorTypeInfo(%lx): \n", (long)this);
  ptiElement->print(space+2);
}


MapTypeID::~MapTypeID()
{
  delete ptiKey;
  delete ptiValue;
}

MapTypeID::MapTypeID(const MapTypeID& ti): TypeID(RIOTYPE_MAP)
{
  ptiKey = ti.ptiKey->clone();
  ptiValue = ti.ptiValue->clone();
}

void MapTypeID::serialize(::hadoop::OArchive& a_, const char* tag) const
{
  a_.serialize(typeVal, tag);
  ptiKey->serialize(a_, tag);
  ptiValue->serialize(a_, tag);
}

bool MapTypeID::operator==(const TypeID& peer_) const
{
  if (typeVal != peer_.getTypeVal()) {
    return false;
  }
  // this must be a map type id
  MapTypeID& mti = (MapTypeID&) peer_;
  if (!(*ptiKey == *(mti.ptiKey))) {
    return false;
  }
  return ((*ptiValue == *(mti.ptiValue)));
}

void MapTypeID::print(int space) const
{
  TypeID::print(space);
  for (int i=0; i<space; i++) {
    printf(" ");
  }
  printf("MapTypeInfo(%lx): \n", (long)this);
  ptiKey->print(space+2);
  ptiValue->print(space+2);
}
