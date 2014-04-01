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

#include "utils.hh"
#include "recordTypeInfo.hh"

using namespace hadoop;

void Utils::skip(IArchive& a, const char* tag, const TypeID& typeID)
{
  bool b;
  size_t len=0;
  ::std::string str;
  int8_t bt;
  double d;
  float f;
  int32_t i;
  int64_t l;

  switch(typeID.getTypeVal()) {
  case RIOTYPE_BOOL: 
    a.deserialize(b, tag);
    break;
  case RIOTYPE_BUFFER: 
    a.deserialize(str, len, tag);
    break;
  case RIOTYPE_BYTE: 
    a.deserialize(bt, tag);
    break;
  case RIOTYPE_DOUBLE: 
    a.deserialize(d, tag);
    break;
  case RIOTYPE_FLOAT: 
    a.deserialize(f, tag);
    break;
  case RIOTYPE_INT: 
    a.deserialize(i, tag);
    break;
  case RIOTYPE_LONG: 
    a.deserialize(l, tag);
    break;
  case RIOTYPE_MAP:
    {
      // since we don't know the key, value types, 
      // we need to deserialize in a generic manner
      Index* idx = a.startMap(tag);
      MapTypeID& mtID = (MapTypeID&) typeID;
      while (!idx->done()) {
	skip(a, tag, *(mtID.getKeyTypeID()));
	skip(a, tag, *(mtID.getValueTypeID()));
	idx->incr();
      }
      a.endMap(idx, tag);
    }
    break;
  case RIOTYPE_STRING: 
    a.deserialize(str, tag);
    break;
  case RIOTYPE_STRUCT: 
    {
      // since we don't know the key, value types, 
      // we need to deserialize in a generic manner
      // we need to pass a record in, though it's never used
      RecordTypeInfo rec;
      a.startRecord(rec, tag);
      StructTypeID& stID = (StructTypeID&) typeID;
      std::vector<FieldTypeInfo*>& typeInfos = stID.getFieldTypeInfos();
      for (unsigned int i=0; i<typeInfos.size(); i++) {
	skip(a, tag, *(typeInfos[i]->getTypeID()));
      }
      a.endRecord(rec, tag);
    }
    break;
  case RIOTYPE_VECTOR:
    {
      // since we don't know the key, value types, 
      // we need to deserialize in a generic manner
      Index* idx = a.startVector(tag);
      VectorTypeID& vtID = (VectorTypeID&) typeID;
      while (!idx->done()) {
	skip(a, tag, *(vtID.getElementTypeID()));
	idx->incr();
      }
      a.endVector(idx, tag);
    }
    break;
  default: 
    // shouldn't be here
    throw new IOException("Unknown typeID when skipping bytes");
    break;
  };

}

