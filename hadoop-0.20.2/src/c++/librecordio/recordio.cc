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

#include "recordio.hh"
#include "binarchive.hh"
#include "csvarchive.hh"
#include "xmlarchive.hh"

using namespace hadoop;

hadoop::RecordReader::RecordReader(InStream& stream, RecFormat f)
{
  switch (f) {
    case kBinary:
      mpArchive = new IBinArchive(stream);
      break;
    case kCSV:
      mpArchive = new ICsvArchive(stream);
      break;
    case kXML:
      mpArchive = new IXmlArchive(stream);
      break;
  }
}

hadoop::RecordReader::~RecordReader()
{
  delete mpArchive;
}

void hadoop::RecordReader::read(Record& record)
{
  record.deserialize(*mpArchive, (const char*) NULL);
}

hadoop::RecordWriter::RecordWriter(OutStream& stream, RecFormat f)
{
  switch (f) {
    case kBinary:
      mpArchive = new OBinArchive(stream);
      break;
    case kCSV:
      mpArchive = new OCsvArchive(stream);
      break;
    case kXML:
      mpArchive = new OXmlArchive(stream);
      break;
  }
}

hadoop::RecordWriter::~RecordWriter()
{
  delete mpArchive;
}

void hadoop::RecordWriter::write(const Record& record)
{
  record.serialize(*mpArchive, (const char*) NULL);
}

