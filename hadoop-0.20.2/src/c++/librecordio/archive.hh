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

#ifndef ARCHIVE_HH_
#define ARCHIVE_HH_
#include "recordio.hh"

namespace hadoop {

class Index {
public:
  virtual bool done() = 0;
  virtual void incr() = 0;
  virtual ~Index() {}
};

class IArchive {
public:
  virtual void deserialize(int8_t& t, const char* tag) = 0;
  virtual void deserialize(bool& t, const char* tag) = 0;
  virtual void deserialize(int32_t& t, const char* tag) = 0;
  virtual void deserialize(int64_t& t, const char* tag) = 0;
  virtual void deserialize(float& t, const char* tag) = 0;
  virtual void deserialize(double& t, const char* tag) = 0;
  virtual void deserialize(std::string& t, const char* tag) = 0;
  virtual void deserialize(std::string& t, size_t& len, const char* tag) = 0;
  virtual void startRecord(hadoop::Record& s, const char* tag) = 0;
  virtual void endRecord(hadoop::Record& s, const char* tag) = 0;
  virtual Index* startVector(const char* tag) = 0;
  virtual void endVector(Index* idx, const char* tag) = 0;
  virtual Index* startMap(const char* tag) = 0;
  virtual void endMap(Index* idx, const char* tag) = 0;
  virtual void deserialize(hadoop::Record& s, const char* tag) {
    s.deserialize(*this, tag);
  }
  template <typename T>
  void deserialize(std::vector<T>& v, const char* tag) {
    Index* idx = startVector(tag);
    while (!idx->done()) {
      T t;
      deserialize(t, tag);
      v.push_back(t);
      idx->incr();
    }
    endVector(idx, tag);
  }
  template <typename K, typename V>
  void deserialize(std::map<K,V>& v, const char* tag) {
    Index* idx = startMap(tag);
    while (!idx->done()) {
      K key;
      deserialize(key, tag);
      V value;
      deserialize(value, tag);
      v[key] = value;
      idx->incr();
    }
    endMap(idx, tag);
  }
  virtual ~IArchive() {}
};

class OArchive {
public:
  virtual void serialize(int8_t t, const char* tag) = 0;
  virtual void serialize(bool t, const char* tag) = 0;
  virtual void serialize(int32_t t, const char* tag) = 0;
  virtual void serialize(int64_t t, const char* tag) = 0;
  virtual void serialize(float t, const char* tag) = 0;
  virtual void serialize(double t, const char* tag) = 0;
  virtual void serialize(const std::string& t, const char* tag) = 0;
  virtual void serialize(const std::string& t, size_t len, const char* tag) = 0;
  virtual void startRecord(const hadoop::Record& s, const char* tag) = 0;
  virtual void endRecord(const hadoop::Record& s, const char* tag) = 0;
  virtual void startVector(size_t len, const char* tag) = 0;
  virtual void endVector(size_t len, const char* tag) = 0;
  virtual void startMap(size_t len, const char* tag) = 0;
  virtual void endMap(size_t len, const char* tag) = 0;
  virtual void serialize(const hadoop::Record& s, const char* tag) {
    s.serialize(*this, tag);
  }
  template <typename T>
  void serialize(const std::vector<T>& v, const char* tag) {
    startVector(v.size(), tag);
    if (v.size()>0) {
      for (size_t cur = 0; cur<v.size(); cur++) {
        serialize(v[cur], tag);
      }
    }
    endVector(v.size(), tag);
  }
  template <typename K, typename V>
  void serialize(const std::map<K,V>& v, const char* tag) {
    startMap(v.size(), tag);
    if (v.size()>0) {
      typedef typename std::map<K,V>::const_iterator CI;
      for (CI cur = v.begin(); cur!=v.end(); cur++) {
        serialize(cur->first, tag);
        serialize(cur->second, tag);
      }
    }
    endMap(v.size(), tag);
 }
  virtual ~OArchive() {}
};
}; // end namespace hadoop
#endif /*ARCHIVE_HH_*/
