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

#include "csvarchive.hh"
#include <stdlib.h>

using namespace hadoop;

static std::string readUptoTerminator(PushBackInStream& stream)
{
  std::string s;
  while (1) {
    char c;
    if (1 != stream.read(&c, 1)) {
      throw new IOException("Error in deserialization.");
    }
    if (c == ',' || c == '\n' || c == '}') {
      if (c != ',') {
        stream.pushBack(c);
      }
      break;
    }
    s.push_back(c);
  }
  return s;
}

void hadoop::ICsvArchive::deserialize(int8_t& t, const char* tag)
{
  std::string s = readUptoTerminator(stream);
  t = (int8_t) strtol(s.c_str(), NULL, 10);
}

void hadoop::ICsvArchive::deserialize(bool& t, const char* tag)
{
  std::string s = readUptoTerminator(stream);
  t = (s == "T") ? true : false;
}

void hadoop::ICsvArchive::deserialize(int32_t& t, const char* tag)
{
  std::string s = readUptoTerminator(stream);
  t = strtol(s.c_str(), NULL, 10);
}

void hadoop::ICsvArchive::deserialize(int64_t& t, const char* tag)
{
  std::string s = readUptoTerminator(stream);
  t = strtoll(s.c_str(), NULL, 10);
}

void hadoop::ICsvArchive::deserialize(float& t, const char* tag)
{
  std::string s = readUptoTerminator(stream);
  t = strtof(s.c_str(), NULL);
}

void hadoop::ICsvArchive::deserialize(double& t, const char* tag)
{
  std::string s = readUptoTerminator(stream);
  t = strtod(s.c_str(), NULL);
}

void hadoop::ICsvArchive::deserialize(std::string& t, const char* tag)
{
  std::string temp = readUptoTerminator(stream);
  if (temp[0] != '\'') {
    throw new IOException("Errror deserializing string.");
  }
  t.clear();
  // skip first character, replace escaped characters 
  int len = temp.length();
  for (int i = 1; i < len; i++) {
    char c = temp.at(i);
    if (c == '%') {
      // since we escape '%', there have to be at least two chars following a '%'
      char ch1 = temp.at(i+1);
      char ch2 = temp.at(i+2);
      i += 2;
	  if (ch1 == '0' && ch2 == '0') {
	    t.append(1, '\0');
	  } else if (ch1 == '0' && ch2 == 'A') {
	    t.append(1, '\n');
	  } else if (ch1 == '0' && ch2 == 'D') {
	    t.append(1, '\r');
	  } else if (ch1 == '2' && ch2 == 'C') {
	    t.append(1, ',');
	  } else if (ch1 == '7' && ch2 == 'D') {
	    t.append(1, '}');
	  } else if (ch1 == '2' && ch2 == '5') {
	    t.append(1, '%');
	  } else {
	    throw new IOException("Error deserializing string.");
	  }
    } 
    else {
      t.append(1, c);
    }
  }
}

void hadoop::ICsvArchive::deserialize(std::string& t, size_t& len, const char* tag)
{
  std::string s = readUptoTerminator(stream);
  if (s[0] != '#') {
    throw new IOException("Errror deserializing buffer.");
  }
  s.erase(0, 1); /// erase first character
  len = s.length();
  if (len%2 == 1) { // len is guaranteed to be even
    throw new IOException("Errror deserializing buffer.");
  }
  len = len >> 1;
  for (size_t idx = 0; idx < len; idx++) {
    char buf[3];
    buf[0] = s[2*idx];
    buf[1] = s[2*idx+1];
    buf[2] = '\0';
    int i;
    if (1 != sscanf(buf, "%2x", &i)) {
      throw new IOException("Errror deserializing buffer.");
    }
    t.push_back((char) i);
  }
  len = t.length();
}

void hadoop::ICsvArchive::startRecord(Record& s, const char* tag)
{
  if (tag != NULL) {
    char mark[2];
    if (2 != stream.read(mark, 2)) {
      throw new IOException("Error deserializing record.");
    }
    if (mark[0] != 's' || mark[1] != '{') {
      throw new IOException("Error deserializing record.");
    }
  }
}

void hadoop::ICsvArchive::endRecord(Record& s, const char* tag)
{
  char mark;
  if (1 != stream.read(&mark, 1)) {
    throw new IOException("Error deserializing record.");
  }
  if (tag == NULL) {
    if (mark != '\n') {
      throw new IOException("Error deserializing record.");
    }
  } else if (mark != '}') {
    throw new IOException("Error deserializing record.");
  } else {
    readUptoTerminator(stream);
  }
}

Index* hadoop::ICsvArchive::startVector(const char* tag)
{
  char mark[2];
  if (2 != stream.read(mark, 2)) {
    throw new IOException("Error deserializing vector.");
  }
  if (mark[0] != 'v' || mark[1] != '{') {
    throw new IOException("Error deserializing vector.");
  }
  return new CsvIndex(stream);
}

void hadoop::ICsvArchive::endVector(Index* idx, const char* tag)
{
  delete idx;
  char mark;
  if (1 != stream.read(&mark, 1)) {
    throw new IOException("Error deserializing vector.");
  }
  if (mark != '}') {
    throw new IOException("Error deserializing vector.");
  }
  readUptoTerminator(stream);
}

Index* hadoop::ICsvArchive::startMap(const char* tag)
{
  char mark[2];
  if (2 != stream.read(mark, 2)) {
    throw new IOException("Error deserializing map.");
  }
  if (mark[0] != 'm' || mark[1] != '{') {
    throw new IOException("Error deserializing map.");
  }

  return new CsvIndex(stream);
}

void hadoop::ICsvArchive::endMap(Index* idx, const char* tag)
{
  delete idx;
  char mark;
  if (1 != stream.read(&mark, 1)) {
    throw new IOException("Error deserializing map.");
  }
  if (mark != '}') {
    throw new IOException("Error deserializing map.");
  }
  readUptoTerminator(stream);
}

hadoop::ICsvArchive::~ICsvArchive()
{
}

void hadoop::OCsvArchive::serialize(int8_t t, const char* tag)
{
  printCommaUnlessFirst();
  char sval[5];
  sprintf(sval, "%d", t);
  stream.write(sval, strlen(sval));
}

void hadoop::OCsvArchive::serialize(bool t, const char* tag)
{
  printCommaUnlessFirst();
  const char *sval = t ? "T" : "F";
  stream.write(sval,1);  
}

void hadoop::OCsvArchive::serialize(int32_t t, const char* tag)
{
  printCommaUnlessFirst();
  char sval[128];
  sprintf(sval, "%d", t);
  stream.write(sval, strlen(sval));
}

void hadoop::OCsvArchive::serialize(int64_t t, const char* tag)
{
  printCommaUnlessFirst();
  char sval[128];
  sprintf(sval, "%lld", t);
  stream.write(sval, strlen(sval));
}

void hadoop::OCsvArchive::serialize(float t, const char* tag)
{
  printCommaUnlessFirst();
  char sval[128];
  sprintf(sval, "%f", t);
  stream.write(sval, strlen(sval));
}

void hadoop::OCsvArchive::serialize(double t, const char* tag)
{
  printCommaUnlessFirst();
  char sval[128];
  sprintf(sval, "%lf", t);
  stream.write(sval, strlen(sval));
}

void hadoop::OCsvArchive::serialize(const std::string& t, const char* tag)
{
  printCommaUnlessFirst();
  stream.write("'",1);
  int len = t.length();
  for (int idx = 0; idx < len; idx++) {
    char c = t[idx];
    switch(c) {
      case '\0':
        stream.write("%00",3);
        break;
      case 0x0A:
        stream.write("%0A",3);
        break;
      case 0x0D:
        stream.write("%0D",3);
        break;
      case 0x25:
        stream.write("%25",3);
        break;
      case 0x2C:
        stream.write("%2C",3);
        break;
      case 0x7D:
        stream.write("%7D",3);
        break;
      default:
        stream.write(&c,1);
        break;
    }
  }
}

void hadoop::OCsvArchive::serialize(const std::string& t, size_t len, const char* tag)
{
  printCommaUnlessFirst();
  stream.write("#",1);
  for(size_t idx = 0; idx < len; idx++) {
    uint8_t b = t[idx];
    char sval[3];
    sprintf(sval,"%2x",b);
    stream.write(sval, 2);
  }
}

void hadoop::OCsvArchive::startRecord(const Record& s, const char* tag)
{
  printCommaUnlessFirst();
  if (tag != NULL && strlen(tag) != 0) {
    stream.write("s{",2);
  }
  isFirst = true;
}

void hadoop::OCsvArchive::endRecord(const Record& s, const char* tag)
{
  if (tag == NULL || strlen(tag) == 0) {
    stream.write("\n",1);
    isFirst = true;
  } else {
    stream.write("}",1);
    isFirst = false;
  }
}

void hadoop::OCsvArchive::startVector(size_t len, const char* tag)
{
  printCommaUnlessFirst();
  stream.write("v{",2);
  isFirst = true;
}

void hadoop::OCsvArchive::endVector(size_t len, const char* tag)
{
  stream.write("}",1);
  isFirst = false;
}

void hadoop::OCsvArchive::startMap(size_t len, const char* tag)
{
  printCommaUnlessFirst();
  stream.write("m{",2);
  isFirst = true;
}

void hadoop::OCsvArchive::endMap(size_t len, const char* tag)
{
  stream.write("}",1);
  isFirst = false;
}

hadoop::OCsvArchive::~OCsvArchive()
{
}
