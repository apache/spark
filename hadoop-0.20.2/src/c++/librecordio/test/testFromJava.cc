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

#include "test.hh"

int main()
{
  org::apache::hadoop::record::test::RecRecord1 r1;
  org::apache::hadoop::record::test::RecRecord1 r2;
  r1.setBoolVal(true);
  r1.setByteVal((int8_t)0x66);
  r1.setFloatVal(3.145);
  r1.setDoubleVal(1.5234);
  r1.setIntVal(4567);
  r1.setLongVal(0x5a5a5a5a5a5aLL);
  std::string& s = r1.getStringVal();
  s = "random text";
  {
    hadoop::FileInStream istream;
    istream.open("/tmp/hadooptemp.dat");
    hadoop::RecordReader reader(istream, hadoop::kBinary);
    reader.read(r2);
    if (r1 == r2) {
      printf("Binary archive test passed.\n");
    } else {
      printf("Binary archive test failed.\n");
    }
    istream.close();
  }
  {
    hadoop::FileInStream istream;
    istream.open("/tmp/hadooptemp.txt");
    hadoop::RecordReader reader(istream, hadoop::kCSV);
    reader.read(r2);
    if (r1 == r2) {
      printf("CSV archive test passed.\n");
    } else {
      printf("CSV archive test failed.\n");
    }
    istream.close();
  }
  {
    hadoop::FileInStream istream;
    istream.open("/tmp/hadooptemp.xml");
    hadoop::RecordReader reader(istream, hadoop::kXML);
    reader.read(r2);
    if (r1 == r2) {
      printf("XML archive test passed.\n");
    } else {
      printf("XML archive test failed.\n");
    }
    istream.close();
  }
  return 0;
}

