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

#ifndef FILESTREAM_HH_
#define FILESTREAM_HH_

#include <stdio.h>
#include <stdint.h>
#include <string>
#include "recordio.hh"

namespace hadoop {

class FileInStream : public InStream {
public:
  FileInStream();
  bool open(const std::string& name);
  ssize_t read(void *buf, size_t buflen);
  bool skip(size_t nbytes);
  bool close();
  virtual ~FileInStream();
private:
  FILE *mFile;
};


class FileOutStream: public OutStream {
public:
  FileOutStream();
  bool open(const std::string& name, bool overwrite);
  ssize_t write(const void* buf, size_t len);
  bool advance(size_t nbytes);
  bool close();
  virtual ~FileOutStream();
private:
  FILE *mFile;
};

}; // end namespace
#endif /*FILESTREAM_HH_*/
