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

#ifndef RECORDIO_HH_
#define RECORDIO_HH_

#include <stdio.h>
#include <stdint.h>
#include <iostream>
#include <cstring>
#include <string>
#include <vector>
#include <map>
#include <bitset>

namespace hadoop {
  
class InStream {
public:
  virtual ssize_t read(void *buf, size_t buflen) = 0;
  virtual ~InStream() {}
};

class OutStream {
public:
  virtual ssize_t write(const void *buf, size_t len) = 0;
  virtual ~OutStream() {}
};

class IArchive;
class OArchive;

class Record {
public:
  virtual void serialize(OArchive& archive, const char* tag) const = 0;
  virtual void deserialize(IArchive& archive, const char* tag) = 0;
  virtual const std::string& type() const = 0;
  virtual const std::string& signature() const = 0;
  virtual ~Record() {}
};

enum RecFormat { kBinary, kXML, kCSV };

class RecordReader {
private:
  IArchive* mpArchive;
public:
  RecordReader(InStream& stream, RecFormat f);
  virtual void read(hadoop::Record& record);
  virtual ~RecordReader();
};

class RecordWriter {
private:
  OArchive* mpArchive;
public:
  RecordWriter(OutStream& stream, RecFormat f);
  virtual void write(const hadoop::Record& record);
  virtual ~RecordWriter();
};
}; // end namspace hadoop

#include "archive.hh"
#include "exception.hh"

#endif /*RECORDIO_HH_*/

