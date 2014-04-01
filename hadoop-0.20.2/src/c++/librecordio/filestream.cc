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

#include "filestream.hh"

using namespace hadoop;

hadoop::FileInStream::FileInStream()
{
  mFile = NULL;
}

bool hadoop::FileInStream::open(const std::string& name)
{
  mFile = fopen(name.c_str(), "rb");
  return (mFile != NULL);
}

ssize_t hadoop::FileInStream::read(void *buf, size_t len)
{
  return fread(buf, 1, len, mFile);
}

bool hadoop::FileInStream::skip(size_t nbytes)
{
  return (0==fseek(mFile, nbytes, SEEK_CUR));
}

bool hadoop::FileInStream::close()
{
  int ret = fclose(mFile);
  mFile = NULL;
  return (ret==0);
}

hadoop::FileInStream::~FileInStream()
{
  if (mFile != NULL) {
    close();
  }
}

hadoop::FileOutStream::FileOutStream()
{
  mFile = NULL;
}

bool hadoop::FileOutStream::open(const std::string& name, bool overwrite)
{
  if (!overwrite) {
    mFile = fopen(name.c_str(), "rb");
    if (mFile != NULL) {
      fclose(mFile);
      return false;
    }
  }
  mFile = fopen(name.c_str(), "wb");
  return (mFile != NULL);
}

ssize_t hadoop::FileOutStream::write(const void* buf, size_t len)
{
  return fwrite(buf, 1, len, mFile);
}

bool hadoop::FileOutStream::advance(size_t nbytes)
{
  return (0==fseek(mFile, nbytes, SEEK_CUR));
}

bool hadoop::FileOutStream::close()
{
  int ret = fclose(mFile);
  mFile = NULL;
  return (ret == 0);
}

hadoop::FileOutStream::~FileOutStream()
{
  if (mFile != NULL) {
    close();
  }
}
