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
#include "hadoop/SerialUtils.hh"
#include "hadoop/StringUtils.hh"

#include <errno.h>
#include <rpc/types.h>
#include <rpc/xdr.h>
#include <string>
#include <string.h>

using std::string;

namespace HadoopUtils {

  Error::Error(const std::string& msg): error(msg) {
  }

  Error::Error(const std::string& msg, 
               const std::string& file, int line, 
               const std::string& function) {
    error = msg + " at " + file + ":" + toString(line) + 
            " in " + function;
  }

  const std::string& Error::getMessage() const {
    return error;
  }

  FileInStream::FileInStream()
  {
    mFile = NULL;
    isOwned = false;
  }

  bool FileInStream::open(const std::string& name)
  {
    mFile = fopen(name.c_str(), "rb");
    isOwned = true;
    return (mFile != NULL);
  }

  bool FileInStream::open(FILE* file)
  {
    mFile = file;
    isOwned = false;
    return (mFile != NULL);
  }

  void FileInStream::read(void *buf, size_t len)
  {
    size_t result = fread(buf, len, 1, mFile);
    if (result == 0) {
      if (feof(mFile)) {
        HADOOP_ASSERT(false, "end of file");
      } else {
        HADOOP_ASSERT(false, string("read error on file: ") + strerror(errno));
      }
    }
  }

  bool FileInStream::skip(size_t nbytes)
  {
    return (0==fseek(mFile, nbytes, SEEK_CUR));
  }

  bool FileInStream::close()
  {
    int ret = 0;
    if (mFile != NULL && isOwned) {
      ret = fclose(mFile);
    }
    mFile = NULL;
    return (ret==0);
  }

  FileInStream::~FileInStream()
  {
    if (mFile != NULL) {
      close();
    }
  }

  FileOutStream::FileOutStream()
  {
    mFile = NULL;
    isOwned = false;
  }

  bool FileOutStream::open(const std::string& name, bool overwrite)
  {
    if (!overwrite) {
      mFile = fopen(name.c_str(), "rb");
      if (mFile != NULL) {
        fclose(mFile);
        return false;
      }
    }
    mFile = fopen(name.c_str(), "wb");
    isOwned = true;
    return (mFile != NULL);
  }

  bool FileOutStream::open(FILE* file)
  {
    mFile = file;
    isOwned = false;
    return (mFile != NULL);
  }

  void FileOutStream::write(const void* buf, size_t len)
  {
    size_t result = fwrite(buf, len, 1, mFile);
    HADOOP_ASSERT(result == 1,
                  string("write error to file: ") + strerror(errno));
  }

  bool FileOutStream::advance(size_t nbytes)
  {
    return (0==fseek(mFile, nbytes, SEEK_CUR));
  }

  bool FileOutStream::close()
  {
    int ret = 0;
    if (mFile != NULL && isOwned) {
      ret = fclose(mFile);
    }
    mFile = NULL;
    return (ret == 0);
  }

  void FileOutStream::flush()
  {
    fflush(mFile);
  }

  FileOutStream::~FileOutStream()
  {
    if (mFile != NULL) {
      close();
    }
  }

  StringInStream::StringInStream(const std::string& str): buffer(str) {
    itr = buffer.begin();
  }

  void StringInStream::read(void *buf, size_t buflen) {
    size_t bytes = 0;
    char* output = (char*) buf;
    std::string::const_iterator end = buffer.end();
    while (bytes < buflen) {
      output[bytes++] = *itr;
      ++itr;
      if (itr == end) {
        break;
      }
    }
    HADOOP_ASSERT(bytes == buflen, "unexpected end of string reached");
  }

  void serializeInt(int32_t t, OutStream& stream) {
    serializeLong(t,stream);
  }

  void serializeLong(int64_t t, OutStream& stream)
  {
    if (t >= -112 && t <= 127) {
      int8_t b = t;
      stream.write(&b, 1);
      return;
    }
        
    int8_t len = -112;
    if (t < 0) {
      t ^= -1ll; // reset the sign bit
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
      uint64_t mask = 0xFFll << shiftbits;
      uint8_t b = (t & mask) >> shiftbits;
      stream.write(&b, 1);
    }
  }

  int32_t deserializeInt(InStream& stream) {
    return deserializeLong(stream);
  }

  int64_t deserializeLong(InStream& stream)
  {
    int8_t b;
    stream.read(&b, 1);
    if (b >= -112) {
      return b;
    }
    bool negative;
    int len;
    if (b < -120) {
      negative = true;
      len = -120 - b;
    } else {
      negative = false;
      len = -112 - b;
    }
    uint8_t barr[len];
    stream.read(barr, len);
    int64_t t = 0;
    for (int idx = 0; idx < len; idx++) {
      t = t << 8;
      t |= (barr[idx] & 0xFF);
    }
    if (negative) {
      t ^= -1ll;
    }
    return t;
  }

  void serializeFloat(float t, OutStream& stream)
  {
    char buf[sizeof(float)];
    XDR xdrs;
    xdrmem_create(&xdrs, buf, sizeof(float), XDR_ENCODE);
    xdr_float(&xdrs, &t);
    stream.write(buf, sizeof(float));
  }

  void deserializeFloat(float& t, InStream& stream)
  {
    char buf[sizeof(float)];
    stream.read(buf, sizeof(float));
    XDR xdrs;
    xdrmem_create(&xdrs, buf, sizeof(float), XDR_DECODE);
    xdr_float(&xdrs, &t);
  }

  void serializeString(const std::string& t, OutStream& stream)
  {
    serializeInt(t.length(), stream);
    if (t.length() > 0) {
      stream.write(t.data(), t.length());
    }
  }

  void deserializeString(std::string& t, InStream& stream)
  {
    int32_t len = deserializeInt(stream);
    if (len > 0) {
      // resize the string to the right length
      t.resize(len);
      // read into the string in 64k chunks
      const int bufSize = 65536;
      int offset = 0;
      char buf[bufSize];
      while (len > 0) {
        int chunkLength = len > bufSize ? bufSize : len;
        stream.read(buf, chunkLength);
        t.replace(offset, chunkLength, buf, chunkLength);
        offset += chunkLength;
        len -= chunkLength;
      }
    } else {
      t.clear();
    }
  }

}
