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

#include "typeInfo.hh"

using namespace hadoop;

TypeInfo::~TypeInfo()
{
  delete pFieldID;
  delete pTypeID;
}

/*TypeInfo& TypeInfo::operator =(const TypeInfo& ti) {
  pFieldID = ti.pFieldID;
  pTypeID = ti.pTypeID;
  return *this;
  }*/

TypeInfo::TypeInfo(const TypeInfo& ti)
{
  pFieldID = new std::string(*ti.pFieldID);
  pTypeID = ti.pTypeID->clone();
}


void TypeInfo::serialize(::hadoop::OArchive& a_, const char* tag) const
{
  a_.serialize(*pFieldID, tag);
  pTypeID->serialize(a_, tag);
}

bool TypeInfo::operator==(const TypeInfo& peer_) const 
{
  // first check if fieldID matches
  if (0 != pFieldID->compare(*(peer_.pFieldID))) {
    return false;
  }
  // now see if typeID matches
  return (*pTypeID == *(peer_.pTypeID));
}

void TypeInfo::print(int space) const
{
  for (int i=0; i<space; i++) {
    printf(" ");
  }
  printf("TypeInfo(%lx):\n", (long)this);
  for (int i=0; i<space+2; i++) {
    printf(" ");
  }
  printf("field = \"%s\"\n", pFieldID->c_str());
  pTypeID->print(space+2);
}
