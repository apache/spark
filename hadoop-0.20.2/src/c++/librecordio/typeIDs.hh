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

#ifndef TYPEIDS_HH_
#define TYPEIDS_HH_

#include "recordio.hh"
#include "fieldTypeInfo.hh"

namespace hadoop {

class FieldTypeInfo;

/* 
 * enum of types. We define assign values to individual bytes, rather
 * than use enums because we want to make teh values consistent with
 * Java code, so we need to control the values.
 */
const int8_t RIOTYPE_BOOL   = 1;
const int8_t RIOTYPE_BUFFER = 2;
const int8_t RIOTYPE_BYTE   = 3;
const int8_t RIOTYPE_DOUBLE = 4;
const int8_t RIOTYPE_FLOAT  = 5;
const int8_t RIOTYPE_INT    = 6;
const int8_t RIOTYPE_LONG   = 7;
const int8_t RIOTYPE_MAP    = 8;
const int8_t RIOTYPE_STRING = 9;
const int8_t RIOTYPE_STRUCT = 10;
const int8_t RIOTYPE_VECTOR = 11;



/* 
 * Represents typeID for basic types. 
 * Serializes just the single int8_t.
 */
class TypeID {

public: 

  TypeID(int8_t typeVal) {this->typeVal = typeVal;}
  TypeID(const TypeID& t) {this->typeVal = t.typeVal;}
  virtual ~TypeID() {}

  int8_t getTypeVal() const {return typeVal;}
  virtual void serialize(::hadoop::OArchive& a_, const char* tag) const;

  virtual bool operator==(const TypeID& peer_) const;
  virtual TypeID* clone() const {return new TypeID(*this);}

  virtual void print(int space=0) const;
  
protected: 
  int8_t typeVal;
};


/* 
 * no predefined TypeID objects, since memory management becomes difficult. 
 * If some TypeID objects are consts and others are new-ed, becomes hard to 
 * destroy const objects without reference counting. 
 */
/*const TypeID TID_BoolTypeID(RIOTYPE_BOOL);
const TypeID TID_BufferTypeID(RIOTYPE_BUFFER);
const TypeID TID_ByteTypeID(RIOTYPE_BYTE);
const TypeID TID_DoubleTypeID(RIOTYPE_DOUBLE);
const TypeID TID_FloatTypeID(RIOTYPE_FLOAT);
const TypeID TID_IntTypeID(RIOTYPE_INT);
const TypeID TID_LongTypeID(RIOTYPE_LONG);
const TypeID TID_StringTypeID(RIOTYPE_STRING);*/


/* 
 * TypeID for structures
 */
class StructTypeID : public TypeID {

private: 
  // note: we own the memory mgmt of TypeInfo objects stored in the vector
  std::vector<FieldTypeInfo*> typeInfos;
  FieldTypeInfo* genericReadTypeInfo(::hadoop::IArchive& a_, const char* tag);
  TypeID* genericReadTypeID(::hadoop::IArchive& a_, const char* tag);

public: 
  /*StructTypeID(const char* p);
  StructTypeID(std::string* p);
  StructTypeID(const StructTypeID& ti);*/
  StructTypeID(): TypeID(RIOTYPE_STRUCT) {};
  StructTypeID(const std::vector<FieldTypeInfo*>& vec);
  virtual ~StructTypeID();

  void add(FieldTypeInfo *pti);
  std::vector<FieldTypeInfo*>& getFieldTypeInfos() {return typeInfos;}
  StructTypeID* findStruct(const char *pStructName);
  void serialize(::hadoop::OArchive& a_, const char* tag) const;
  void serializeRest(::hadoop::OArchive& a_, const char* tag) const;
  void deserialize(::hadoop::IArchive& a_, const char* tag);
  virtual TypeID* clone() const {return new StructTypeID(*this);}

  virtual void print(int space=0) const;

};


/* 
 * TypeID for vectors
 */
class VectorTypeID : public TypeID {

private: 
  // ptiElement's memory mgmt is owned by class
  TypeID* ptiElement;

public: 
  VectorTypeID(TypeID* ptiElement): TypeID(RIOTYPE_VECTOR), ptiElement(ptiElement) {}
  VectorTypeID(const VectorTypeID& ti);
  virtual ~VectorTypeID();

  const TypeID* getElementTypeID() {return ptiElement;}
  virtual TypeID* clone() const {return new VectorTypeID(*this);}
  void serialize(::hadoop::OArchive& a_, const char* tag) const;
  virtual bool operator==(const TypeID& peer_) const;
  
  virtual void print(int space=0) const;
};

/* 
 * TypeID for maps
 */
class MapTypeID : public TypeID {

private: 
  // ptiKay and ptiValue's memory mgmt is owned by class
  TypeID* ptiKey;
  TypeID* ptiValue;

public: 
  MapTypeID(TypeID* ptiKey, TypeID* ptiValue): 
    TypeID(RIOTYPE_MAP), ptiKey(ptiKey), ptiValue(ptiValue) {}
  MapTypeID(const MapTypeID& ti);
  virtual ~MapTypeID();

  const TypeID* getKeyTypeID() {return ptiKey;}
  const TypeID* getValueTypeID() {return ptiValue;}
  virtual TypeID* clone() const {return new MapTypeID(*this);}
  void serialize(::hadoop::OArchive& a_, const char* tag) const;
  virtual bool operator==(const TypeID& peer_) const;
  
  virtual void print(int space=0) const;
};

}
#endif // TYPEIDS_HH_

