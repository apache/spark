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

#ifndef XMLARCHIVE_HH_
#define XMLARCHIVE_HH_

#include <xercesc/parsers/SAXParser.hpp>
#include <xercesc/util/PlatformUtils.hpp>
#include <xercesc/util/BinInputStream.hpp>
#include <xercesc/sax/HandlerBase.hpp>
#include <xercesc/sax/InputSource.hpp>
#include "recordio.hh"

XERCES_CPP_NAMESPACE_USE

namespace hadoop {

class Value {
private:
  std::string type;
  std::string value;
public:
  Value(const std::string& t) { type = t; }
  void addChars(const char* buf, unsigned int len) {
    value += std::string(buf, len);
  }
  const std::string& getType() const { return type; }
  const std::string& getValue() const { return value; }
};
  
class MySAXHandler : public HandlerBase {
private:
  std::vector<Value>& vlist;
  bool charsValid;
public:
  MySAXHandler(std::vector<Value>& list) : vlist(list) {charsValid = false;}
  void startElement(const XMLCh* const name, AttributeList& attr);
  void endElement(const XMLCh* const name);
  void characters(const XMLCh* const buf, unsigned int len);
};

class XmlIndex : public Index {
private:
  std::vector<Value>& vlist;
  unsigned int& vidx;
public:
  XmlIndex(std::vector<Value>& list, unsigned int& idx) : vlist(list), vidx(idx) {}
  bool done() {
   Value v = vlist[vidx];
   return (v.getType() == "/array") ? true : false;
  }
  void incr() {}
  ~XmlIndex() {} 
};

class MyBinInputStream : public BinInputStream {
private:
  InStream& stream;
  unsigned int pos;
public:
  MyBinInputStream(InStream& s) : stream(s) { pos = 0; }
  virtual unsigned int curPos() const { return pos; }
  virtual unsigned int readBytes(XMLByte* const toFill,
      const unsigned int maxToRead) {
    ssize_t nread = stream.read(toFill, maxToRead);
    if (nread < 0) {
      return 0;
    } else {
      pos += nread;
      return nread;
    }
  }
};


class MyInputSource : public InputSource {
private:
  InStream& stream;
public:
  MyInputSource(InStream& s) : stream(s) {  }
  virtual BinInputStream* makeStream() const {
    return new MyBinInputStream(stream);
  }
  virtual const XMLCh* getEncoding() const {
    return XMLString::transcode("UTF-8");
  }
  virtual ~MyInputSource() {}
};
  
class IXmlArchive : public IArchive {
private:
  std::vector<Value> vlist;
  unsigned int vidx;
  MySAXHandler *docHandler;
  SAXParser *parser;
  MyInputSource* src;
  Value next() {
    Value v = vlist[vidx];
    vidx++;
    return v;
  }
public:
  IXmlArchive(InStream& _stream) {
    vidx = 0;
    try {
      XMLPlatformUtils::Initialize();
    } catch (const XMLException& e) {
      throw new IOException("Unable to initialize XML Parser.");
    }
    parser = new SAXParser();
    docHandler = new MySAXHandler(vlist);
    parser->setDocumentHandler(docHandler);
    src = new MyInputSource(_stream);
    try {
      parser->parse(*src);
    } catch (const XMLException& e) {
      throw new IOException("Unable to parse XML stream.");
    } catch (const SAXParseException& e) {
      throw new IOException("Unable to parse XML stream.");
    }
    delete parser;
    delete docHandler;
  }
  virtual void deserialize(int8_t& t, const char* tag);
  virtual void deserialize(bool& t, const char* tag);
  virtual void deserialize(int32_t& t, const char* tag);
  virtual void deserialize(int64_t& t, const char* tag);
  virtual void deserialize(float& t, const char* tag);
  virtual void deserialize(double& t, const char* tag);
  virtual void deserialize(std::string& t, const char* tag);
  virtual void deserialize(std::string& t, size_t& len, const char* tag);
  virtual void startRecord(Record& s, const char* tag);
  virtual void endRecord(Record& s, const char* tag);
  virtual Index* startVector(const char* tag);
  virtual void endVector(Index* idx, const char* tag);
  virtual Index* startMap(const char* tag);
  virtual void endMap(Index* idx, const char* tag);
  virtual ~IXmlArchive() {
    XMLPlatformUtils::Terminate();
  }
};

class OXmlArchive : public OArchive {
private:
  OutStream& stream;
  
  std::vector<std::string> cstack;
  
  void insideRecord(const char* tag) {
    printBeginEnvelope(tag);
    cstack.push_back("record");
  }
  
  void outsideRecord(const char* tag) {
    std::string s = cstack.back();
    cstack.pop_back();
    if (s != "record") {
      throw new IOException("Error deserializing record.");
    }
    printEndEnvelope(tag);
  }
  
  void insideVector(const char* tag) {
    printBeginEnvelope(tag);
    cstack.push_back("vector");
  }
  
  void outsideVector(const char* tag) {
    std::string s = cstack.back();
    cstack.pop_back();
    if (s != "vector") {
      throw new IOException("Error deserializing vector.");
    }
    printEndEnvelope(tag);
  }
  
  void insideMap(const char* tag) {
    printBeginEnvelope(tag);
    cstack.push_back("map");
  }
  
  void outsideMap(const char* tag) {
    std::string s = cstack.back();
    cstack.pop_back();
    if (s != "map") {
      throw new IOException("Error deserializing map.");
    }
    printEndEnvelope(tag);
  }
  
  void p(const char* cstr) {
    stream.write(cstr, strlen(cstr));
  }
  
  void printBeginEnvelope(const char* tag) {
    if (cstack.size() != 0) {
      std::string s = cstack.back();
      if ("record" == s) {
        p("<member>\n");
        p("<name>");
        p(tag);
        p("</name>\n");
        p("<value>");
      } else if ("vector" == s) {
        p("<value>");
      } else if ("map" == s) {
        p("<value>");
      }
    } else {
      p("<value>");
    }
  }
  
  void printEndEnvelope(const char* tag) {
    if (cstack.size() != 0) {
      std::string s = cstack.back();
      if ("record" == s) {
        p("</value>\n");
        p("</member>\n");
      } else if ("vector" == s) {
        p("</value>\n");
      } else if ("map" == s) {
        p("</value>\n");
      }
    } else {
      p("</value>\n");
    }
  }
  
public:
  OXmlArchive(OutStream& _stream) : stream(_stream) {}
  virtual void serialize(int8_t t, const char* tag);
  virtual void serialize(bool t, const char* tag);
  virtual void serialize(int32_t t, const char* tag);
  virtual void serialize(int64_t t, const char* tag);
  virtual void serialize(float t, const char* tag);
  virtual void serialize(double t, const char* tag);
  virtual void serialize(const std::string& t, const char* tag);
  virtual void serialize(const std::string& t, size_t len, const char* tag);
  virtual void startRecord(const Record& s, const char* tag);
  virtual void endRecord(const Record& s, const char* tag);
  virtual void startVector(size_t len, const char* tag);
  virtual void endVector(size_t len, const char* tag);
  virtual void startMap(size_t len, const char* tag);
  virtual void endMap(size_t len, const char* tag);
  virtual ~OXmlArchive();
};

}
#endif /*XMLARCHIVE_HH_*/
