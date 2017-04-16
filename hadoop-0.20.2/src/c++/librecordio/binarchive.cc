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

#include "binarchive.hh"
#include <rpc/types.h>
#include <rpc/xdr.h>


using namespace hadoop;

template <typename T>
static void serialize(T t, OutStream& stream)
{
  if (sizeof(T) != stream.write((const void *) &t, sizeof(T))) {
    throw new IOException("Error serializing data.");
  }
}

template <typename T>
static void deserialize(T& t, InStream& stream)
{
  if (sizeof(T) != stream.read((void *) &t, sizeof(T))) {
    throw new IOException("Error deserializing data.");
  }
}

static void serializeLong(int64_t t, OutStream& stream)
{
  if (t >= -112 && t <= 127) {
    int8_t b = t;
    stream.write(&b, 1);
    return;
  }
        
  int8_t len = -112;
  if (t < 0) {
    t ^= 0xFFFFFFFFFFFFFFFFLL; // take one's complement
    len = -120;
  }
        
  uint64_t tmp = t;
  while (tmp != 0) {
    tmp = tmp >> 8;
    len--;
  }
  
  stream.write(&len, 1);
        
  len = (len < -120) ? -(len + 120) : -(len + 112);
        
  for (uint32_t idx = len; idx != 0; idx--) {
    uint32_t shiftbits = (idx - 1) * 8;
    uint64_t mask = 0xFFLL << shiftbits;
    uint8_t b = (t & mask) >> shiftbits;
    stream.write(&b, 1);
  }
}

static void deserializeLong(int64_t& t, InStream& stream)
{
  int8_t b;
  if (1 != stream.read(&b, 1)) {
    throw new IOException("Error deserializing long.");
  }
  if (b >= -112) {
    t = b;
    return;
  }
  bool isNegative = (b < -120);
  b = isNegative ? -(b + 120) : -(b + 112);
  uint8_t barr[b];
  if (b != stream.read(barr, b)) {
    throw new IOException("Error deserializing long.");
  }
  t = 0;
  for (int idx = 0; idx < b; idx++) {
    t = t << 8;
    t |= (barr[idx] & 0xFF);
  }
  if (isNegative) {
    t ^= 0xFFFFFFFFFFFFFFFFLL;
  }
}

static void serializeInt(int32_t t, OutStream& stream)
{
  int64_t longVal = t;
  ::serializeLong(longVal, stream);
}

static void deserializeInt(int32_t& t, InStream& stream)
{
  int64_t longVal;
  ::deserializeLong(longVal, stream);
  t = longVal;
}

static void serializeFloat(float t, OutStream& stream)
{
  char buf[sizeof(float)];
  XDR xdrs;
  xdrmem_create(&xdrs, buf, sizeof(float), XDR_ENCODE);
  xdr_float(&xdrs, &t);
  stream.write(buf, sizeof(float));
}

static void deserializeFloat(float& t, InStream& stream)
{
  char buf[sizeof(float)];
  if (sizeof(float) != stream.read(buf, sizeof(float))) {
    throw new IOException("Error deserializing float.");
  }
  XDR xdrs;
  xdrmem_create(&xdrs, buf, sizeof(float), XDR_DECODE);
  xdr_float(&xdrs, &t);
}

static void serializeDouble(double t, OutStream& stream)
{
  char buf[sizeof(double)];
  XDR xdrs;
  xdrmem_create(&xdrs, buf, sizeof(double), XDR_ENCODE);
  xdr_double(&xdrs, &t);
  stream.write(buf, sizeof(double));
}

static void deserializeDouble(double& t, InStream& stream)
{
  char buf[sizeof(double)];
  stream.read(buf, sizeof(double));
  XDR xdrs;
  xdrmem_create(&xdrs, buf, sizeof(double), XDR_DECODE);
  xdr_double(&xdrs, &t);
}

static void serializeString(const std::string& t, OutStream& stream)
{
  ::serializeInt(t.length(), stream);
  if (t.length() > 0) {
    stream.write(t.data(), t.length());
  }
}

static void deserializeString(std::string& t, InStream& stream)
{
  int32_t len = 0;
  ::deserializeInt(len, stream);
  if (len > 0) {
    // resize the string to the right length
    t.resize(len);
    // read into the string in 64k chunks
    const int bufSize = 65536;
    int offset = 0;
    char buf[bufSize];
    while (len > 0) {
      int chunkLength = len > bufSize ? bufSize : len;
      stream.read((void *)buf, chunkLength);
      t.replace(offset, chunkLength, buf, chunkLength);
      offset += chunkLength;
      len -= chunkLength;
    }
  }
}

void hadoop::IBinArchive::deserialize(int8_t& t, const char* tag)
{
  ::deserialize(t, stream);
}

void hadoop::IBinArchive::deserialize(bool& t, const char* tag)
{
  ::deserialize(t, stream);
}

void hadoop::IBinArchive::deserialize(int32_t& t, const char* tag)
{
  int64_t longVal = 0LL;
  ::deserializeLong(longVal, stream);
  t = longVal;
}

void hadoop::IBinArchive::deserialize(int64_t& t, const char* tag)
{
  ::deserializeLong(t, stream);
}

void hadoop::IBinArchive::deserialize(float& t, const char* tag)
{
  ::deserializeFloat(t, stream);
}

void hadoop::IBinArchive::deserialize(double& t, const char* tag)
{
  ::deserializeDouble(t, stream);
}

void hadoop::IBinArchive::deserialize(std::string& t, const char* tag)
{
  ::deserializeString(t, stream);
}

void hadoop::IBinArchive::deserialize(std::string& t, size_t& len, const char* tag)
{
  ::deserializeString(t, stream);
  len = t.length();
}

void hadoop::IBinArchive::startRecord(Record& s, const char* tag)
{
}

void hadoop::IBinArchive::endRecord(Record& s, const char* tag)
{
}

Index* hadoop::IBinArchive::startVector(const char* tag)
{
  int32_t len;
  ::deserializeInt(len, stream);
  BinIndex *idx = new BinIndex((size_t) len);
  return idx;
}

void hadoop::IBinArchive::endVector(Index* idx, const char* tag)
{
  delete idx;
}

Index* hadoop::IBinArchive::startMap(const char* tag)
{
  int32_t len;
  ::deserializeInt(len, stream);
  BinIndex *idx = new BinIndex((size_t) len);
  return idx;
}

void hadoop::IBinArchive::endMap(Index* idx, const char* tag)
{
  delete idx;
}

hadoop::IBinArchive::~IBinArchive()
{
}

void hadoop::OBinArchive::serialize(int8_t t, const char* tag)
{
  ::serialize(t, stream);
}

void hadoop::OBinArchive::serialize(bool t, const char* tag)
{
  ::serialize(t, stream);
}

void hadoop::OBinArchive::serialize(int32_t t, const char* tag)
{
  int64_t longVal = t;
  ::serializeLong(longVal, stream);
}

void hadoop::OBinArchive::serialize(int64_t t, const char* tag)
{
  ::serializeLong(t, stream);
}

void hadoop::OBinArchive::serialize(float t, const char* tag)
{
  ::serializeFloat(t, stream);
}

void hadoop::OBinArchive::serialize(double t, const char* tag)
{
  ::serializeDouble(t, stream);
}

void hadoop::OBinArchive::serialize(const std::string& t, const char* tag)
{
  ::serializeString(t, stream);
}

void hadoop::OBinArchive::serialize(const std::string& t, size_t len, const char* tag)
{
  ::serializeString(t, stream);
}

void hadoop::OBinArchive::startRecord(const Record& s, const char* tag)
{
}

void hadoop::OBinArchive::endRecord(const Record& s, const char* tag)
{
}

void hadoop::OBinArchive::startVector(size_t len, const char* tag)
{
  ::serializeInt(len, stream);
}

void hadoop::OBinArchive::endVector(size_t len, const char* tag)
{
}

void hadoop::OBinArchive::startMap(size_t len, const char* tag)
{
  ::serializeInt(len, stream);
}

void hadoop::OBinArchive::endMap(size_t len, const char* tag)
{
}

hadoop::OBinArchive::~OBinArchive()
{
}
