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
#include <vector>

int main()
{
  org::apache::hadoop::record::test::RecRecord1 r1;
  org::apache::hadoop::record::test::RecRecord1 r2;
  {
    hadoop::FileOutStream ostream;
    ostream.open("/tmp/hadooptmp.dat", true);
    hadoop::RecordWriter writer(ostream, hadoop::kBinary);
    r1.setBoolVal(true);
    r1.setByteVal((int8_t)0x66);
    r1.setFloatVal(3.145);
    r1.setDoubleVal(1.5234);
    r1.setIntVal(4567);
    r1.setLongVal(0x5a5a5a5a5a5aLL);
    std::string& s = r1.getStringVal();
    s = "random text";
    writer.write(r1);
    ostream.close();
    hadoop::FileInStream istream;
    istream.open("/tmp/hadooptmp.dat");
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
    hadoop::FileOutStream ostream;
    ostream.open("/tmp/hadooptmp.txt", true);
    hadoop::RecordWriter writer(ostream, hadoop::kCSV);
    r1.setBoolVal(true);
    r1.setByteVal((int8_t)0x66);
    r1.setFloatVal(3.145);
    r1.setDoubleVal(1.5234);
    r1.setIntVal(4567);
    r1.setLongVal(0x5a5a5a5a5a5aLL);
    std::string& s = r1.getStringVal();
    s = "random text";
    writer.write(r1);
    ostream.close();
    hadoop::FileInStream istream;
    istream.open("/tmp/hadooptmp.txt");
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
    hadoop::FileOutStream ostream;
    ostream.open("/tmp/hadooptmp.xml", true);
    hadoop::RecordWriter writer(ostream, hadoop::kXML);
    r1.setBoolVal(true);
    r1.setByteVal((int8_t)0x66);
    r1.setFloatVal(3.145);
    r1.setDoubleVal(1.5234);
    r1.setIntVal(4567);
    r1.setLongVal(0x5a5a5a5a5a5aLL);
    std::string& s = r1.getStringVal();
    s = "random text";
    writer.write(r1);
    ostream.close();
    hadoop::FileInStream istream;
    istream.open("/tmp/hadooptmp.xml");
    hadoop::RecordReader reader(istream, hadoop::kXML);
    reader.read(r2);
    if (r1 == r2) {
      printf("XML archive test passed.\n");
    } else {
      printf("XML archive test failed.\n");
    }
    istream.close();
  }
  
  /* 
   * Tests to check for versioning functionality
   */
  
  // basic test
  // write out a record and its type info, read it back using its typeinfo
  {
    hadoop::FileOutStream ostream, ortistream;
    ostream.open("/tmp/hadooptmp.dat", true);
    ortistream.open("/tmp/hadooprti.dat", true);
    hadoop::RecordWriter writer(ostream, hadoop::kBinary);
    hadoop::RecordWriter writerRti(ortistream, hadoop::kBinary);
    r1.setBoolVal(true);
    r1.setByteVal((int8_t)0x66);
    r1.setFloatVal(3.145);
    r1.setDoubleVal(1.5234);
    r1.setIntVal(4567);
    r1.setLongVal(0x5a5a5a5a5a5aLL);
    std::string& s = r1.getStringVal();
    s = "random text";
    writer.write(r1);
    ostream.close();
    // write out rti info
    writerRti.write(org::apache::hadoop::record::test::RecRecord1::getTypeInfo());
    ortistream.close();

    // read
    hadoop::FileInStream istream;
    istream.open("/tmp/hadooptmp.dat");
    hadoop::RecordReader reader(istream, hadoop::kBinary);
    hadoop::FileInStream irtistream;
    irtistream.open("/tmp/hadooprti.dat");
    hadoop::RecordReader readerRti(irtistream, hadoop::kBinary);
    hadoop::RecordTypeInfo rti;
    readerRti.read(rti);
    irtistream.close();
    org::apache::hadoop::record::test::RecRecord1::setTypeFilter(rti);
    reader.read(r2);
    if (r1 == r2) {
      printf("Basic versioning test passed.\n");
    } else {
      printf("Basic versioning test failed.\n");
    }
    istream.close();
  }     
  
  // versioning:write out a record and its type info, read back a similar record using the written record's typeinfo
  {
    hadoop::FileOutStream ostream, ortistream;
    ostream.open("/tmp/hadooptmp.dat", true);
    ortistream.open("/tmp/hadooprti.dat", true);
    hadoop::RecordWriter writer(ostream, hadoop::kBinary);
    hadoop::RecordWriter writerRti(ortistream, hadoop::kBinary);

    // we create an array of records to write
    std::vector<org::apache::hadoop::record::test::RecRecordOld*> recsWrite;
    int i, j, k, l;
    char buf[1000];
    for (i=0; i<5; i++) {
      org::apache::hadoop::record::test::RecRecordOld* ps1Rec = 
        new org::apache::hadoop::record::test::RecRecordOld();
      sprintf(buf, "This is record s1: %d", i);
      ps1Rec->getName().assign(buf);

      for (j=0; j<3; j++) {
        ps1Rec->getIvec().push_back((int64_t)(i+j));
      }

      for (j=0; j<2; j++) {
        std::vector<org::apache::hadoop::record::test::RecRecord0>* pVec = 
          new std::vector<org::apache::hadoop::record::test::RecRecord0>();
        for (k=0; k<3; k++) {
          org::apache::hadoop::record::test::RecRecord0 *psRec = 
            new org::apache::hadoop::record::test::RecRecord0();
          sprintf(buf, "This is record s: (%d: %d)", j, k);
          psRec->getStringVal().assign(buf);
        }
        ps1Rec->getSvec().push_back(*pVec);
      }

      sprintf(buf, "This is record s: %d", i);
      ps1Rec->getInner().getStringVal().assign(buf);

      for (l=0; l<2; l++) {
        std::vector<std::vector<std::string> >* ppVec =
          new std::vector<std::vector<std::string> >();
        for (j=0; j<2; j++) {
          std::vector< std::string >* pVec =
            new std::vector< std::string >();
          for (k=0; k<3; k++) {
            sprintf(buf, "THis is a nested string: (%d: %d: %d)", l, j, k);
            std::string* s = new std::string((const char*)buf);
            pVec->push_back(*s);
          }
        }
        ps1Rec->getStrvec().push_back(*ppVec);
      }

      ps1Rec->setI1(100+i);

      ps1Rec->getMap1()[23] = "23";
      ps1Rec->getMap1()[11] = "11";

      std::map<int32_t, int64_t>* m1 = new std::map<int32_t, int64_t>();
      std::map<int32_t, int64_t>* m2 = new std::map<int32_t, int64_t>();
      (*m1)[5] = 5;
      (*m1)[10] = 10;
      (*m2)[15] = 15;
      (*m2)[20] = 20;
      ps1Rec->getMvec1().push_back(*m1);
      ps1Rec->getMvec1().push_back(*m2);
      ps1Rec->getMvec2().push_back(*m1);

      recsWrite.push_back(ps1Rec);
    }

    // write out to file
    for (unsigned int i=0; i<recsWrite.size(); i++) {
      writer.write(*(recsWrite[i]));
    }
    ostream.close();
    // write out rti info
    writerRti.write(org::apache::hadoop::record::test::RecRecordOld::getTypeInfo());
    ortistream.close();

    // read
    hadoop::FileInStream istream;
    istream.open("/tmp/hadooptmp.dat");
    hadoop::RecordReader reader(istream, hadoop::kBinary);
    hadoop::FileInStream irtistream;
    irtistream.open("/tmp/hadooprti.dat");
    hadoop::RecordReader readerRti(irtistream, hadoop::kBinary);
    hadoop::RecordTypeInfo rti;
    readerRti.read(rti);
    irtistream.close();
    org::apache::hadoop::record::test::RecRecordNew::setTypeFilter(rti);
    
    // read records
    std::vector<org::apache::hadoop::record::test::RecRecordNew*> recsRead;
    for (unsigned int i=0; i<recsWrite.size(); i++) {
      org::apache::hadoop::record::test::RecRecordNew* ps2Rec = 
        new org::apache::hadoop::record::test::RecRecordNew();
      reader.read(*ps2Rec);
      recsRead.push_back(ps2Rec);
    }
    istream.close();

    // compare
    bool pass = true;
    for (unsigned int i=0; i<recsRead.size(); i++) {
      org::apache::hadoop::record::test::RecRecordNew* ps2In = recsRead[i];
      org::apache::hadoop::record::test::RecRecordOld* ps1Out = recsWrite[i];

      if (!ps2In->getName2().empty()) {
        printf("Error in s2: name2\n");
        pass = false;
      }

      if (!(ps2In->getInner() == ps1Out->getInner())) {
        printf("error in s2: s1 struct\n");
        pass = false;
      }

      if (0 != ps2In->getIvec().size()) {
        printf("error in s2: ivec\n");
        pass = false;
      }

      if (0 != ps2In->getSvec().size()) {
        printf("error in s2: svec\n");
        pass = false;
      }

      for (unsigned int j=0; j<ps2In->getStrvec().size(); j++) {
        ::std::vector< ::std::vector< ::std::string > >& ss2Vec = ps2In->getStrvec()[j];
        ::std::vector< ::std::vector< ::std::string > >& ss1Vec = ps1Out->getStrvec()[j];
        for (unsigned int k=0; k<ss2Vec.size(); k++) {
          ::std::vector< ::std::string >& s2Vec = ss2Vec[k];
          ::std::vector< ::std::string >& s1Vec = ss1Vec[k];
          for (unsigned int l=0; l<s2Vec.size(); l++) {
            if (s2Vec[l] != s1Vec[l]) {
              printf("Error in s2: s2Vec\n");
              pass = false;
            }
          }
        }
      }

      if (0 != ps2In->getMap1().size()) {
        printf("Error in s2: map1\n");
        pass = false;
      }

      for (unsigned int j=0; j<ps2In->getMvec2().size(); j++) {
        if (ps2In->getMvec2()[j] != ps1Out->getMvec2()[j]) {
          printf("Error in s2: mvec2\n");
          pass = false;
        }
      }
    }
  
    if (pass)   
      printf("Versioning test passed.\n");
  }     
   
  return 0;
}

