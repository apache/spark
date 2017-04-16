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

#include "xmlarchive.hh"
#include <stdlib.h>

using namespace hadoop;

void hadoop::MySAXHandler::startElement(const XMLCh* const name, AttributeList& attr)
{
  charsValid = false;
  char* qname = XMLString::transcode(name);
  if(std::string("boolean") == qname ||
    std::string("ex:i1") == qname ||
    std::string("i4") == qname ||
    std::string("int") == qname ||
    std::string("ex:i8") == qname ||
    std::string("ex:float") == qname ||
    std::string("double") == qname ||
    std::string("string") == qname) {
    std::string s(qname);
    Value v(s);
    vlist.push_back(v);
    charsValid = true;
  } else if(std::string("struct") == qname ||
    std::string("array") == qname) {
    std::string s(qname);
    Value v(s);
    vlist.push_back(v);
  }
  XMLString::release(&qname);
}

void hadoop::MySAXHandler::endElement(const XMLCh* const name)
{
  charsValid = false;
  char* qname = XMLString::transcode(name);
  if(std::string("struct") == qname ||
    std::string("array") == qname) {
    std::string s = "/";
    Value v(s + qname);
    vlist.push_back(v);
  }
  XMLString::release(&qname);
}

void hadoop::MySAXHandler::characters(const XMLCh* const buf, const unsigned int len)
{
  if (charsValid) {
    char *cstr = XMLString::transcode(buf);
    Value& v = vlist.back();
    v.addChars(cstr, strlen(cstr));
    XMLString::release(&cstr);
  }
}

static char hexchars[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
                          'A', 'B', 'C', 'D', 'E', 'F' };

static std::string toXMLString(std::string s)
{
  std::string r;
  size_t len = s.length();
  size_t i;
  const char* data = s.data();
  for (i=0; i<len; i++, data++) {
    char ch = *data;
    if (ch == '<') {
        r.append("&lt;");
    } else if (ch == '&') {
        r.append("&amp;");
    } else if (ch == '%') {
        r.append("%0025");
    } else if (ch < 0x20) {
        uint8_t* pb = (uint8_t*) &ch;
        char ch1 = hexchars[*pb/16];
        char ch2 = hexchars[*pb%16];
        r.push_back('%');
        r.push_back('0');
        r.push_back('0');
        r.push_back(ch1);
        r.push_back(ch2);
    } else {
        r.push_back(ch);
    }
  }
  return r;
}

static uint8_t h2b(char ch) {
  if ((ch >= '0') || (ch <= '9')) {
    return ch - '0';
  }
  if ((ch >= 'a') || (ch <= 'f')) {
    return ch - 'a' + 10;
  }
  if ((ch >= 'A') || (ch <= 'F')) {
    return ch - 'A' + 10;
  }
  return 0;
}

static std::string fromXMLString(std::string s)
{
  std::string r;
  size_t len = s.length();
  size_t i;
  uint8_t* pb = (uint8_t*) s.data();
  for (i = 0; i < len; i++) {
    uint8_t b = *pb;
    if (b == '%') {
      char *pc = (char*) (pb+1);
      // ignore the first two characters, which are always '0'
      *pc++;
      *pc++;;
      char ch1 = *pc++;
      char ch2 = *pc++;
      pb += 4;
      uint8_t cnv = h2b(ch1)*16 + h2b(ch2);
      pc = (char*) &cnv;
      r.push_back(*pc);
    } else {
      char *pc = (char*) pb;
      r.push_back(*pc);
    }
    pb++;
  }
  return r;
}

static std::string toXMLBuffer(std::string s, size_t len)
{
  std::string r;
  size_t i;
  uint8_t* data = (uint8_t*) s.data();
  for (i=0; i<len; i++, data++) {
    uint8_t b = *data;
    char ch1 = hexchars[b/16];
    char ch2 = hexchars[b%16];
    r.push_back(ch1);
    r.push_back(ch2);
  }
  return r;
}

static std::string fromXMLBuffer(std::string s, size_t& len)
{
  len = s.length();
  if (len%2 == 1) { // len is guaranteed to be even
    throw new IOException("Errror deserializing buffer.");
  }
  len = len >> 1;
  std::string t;
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
  return t;
}

void hadoop::IXmlArchive::deserialize(int8_t& t, const char* tag)
{
  Value v = next();
  if (v.getType() != "ex:i1") {
    throw new IOException("Error deserializing byte");
  }
  t = (int8_t) strtol(v.getValue().c_str(), NULL, 10);
}

void hadoop::IXmlArchive::deserialize(bool& t, const char* tag)
{
  Value v = next();
  if (v.getType() != "boolean") {
    throw new IOException("Error deserializing boolean");
  }
  t = (v.getValue() == "1");
}

void hadoop::IXmlArchive::deserialize(int32_t& t, const char* tag)
{
  Value v = next();
  if (v.getType() != "i4" && v.getType() != "int") {
    throw new IOException("Error deserializing int");
  }
  t = (int32_t) strtol(v.getValue().c_str(), NULL, 10);
}

void hadoop::IXmlArchive::deserialize(int64_t& t, const char* tag)
{
  Value v = next();
  if (v.getType() != "ex:i8") {
    throw new IOException("Error deserializing long");
  }
  t = strtoll(v.getValue().c_str(), NULL, 10);
}

void hadoop::IXmlArchive::deserialize(float& t, const char* tag)
{
  Value v = next();
  if (v.getType() != "ex:float") {
    throw new IOException("Error deserializing float");
  }
  t = strtof(v.getValue().c_str(), NULL);
}

void hadoop::IXmlArchive::deserialize(double& t, const char* tag)
{
  Value v = next();
  if (v.getType() != "double") {
    throw new IOException("Error deserializing double");
  }
  t = strtod(v.getValue().c_str(), NULL);
}

void hadoop::IXmlArchive::deserialize(std::string& t, const char* tag)
{
  Value v = next();
  if (v.getType() != "string") {
    throw new IOException("Error deserializing string");
  }
  t = fromXMLString(v.getValue());
}

void hadoop::IXmlArchive::deserialize(std::string& t, size_t& len, const char* tag)
{
  Value v = next();
  if (v.getType() != "string") {
    throw new IOException("Error deserializing buffer");
  }
  t = fromXMLBuffer(v.getValue(), len);
}

void hadoop::IXmlArchive::startRecord(Record& s, const char* tag)
{
  Value v = next();
  if (v.getType() != "struct") {
    throw new IOException("Error deserializing record");
  }
}

void hadoop::IXmlArchive::endRecord(Record& s, const char* tag)
{
  Value v = next();
  if (v.getType() != "/struct") {
    throw new IOException("Error deserializing record");
  }
}

Index* hadoop::IXmlArchive::startVector(const char* tag)
{
  Value v = next();
  if (v.getType() != "array") {
    throw new IOException("Error deserializing vector");
  }
  return new XmlIndex(vlist, vidx);
}

void hadoop::IXmlArchive::endVector(Index* idx, const char* tag)
{
  Value v = next();
  if (v.getType() != "/array") {
    throw new IOException("Error deserializing vector");
  }
  delete idx;
}

Index* hadoop::IXmlArchive::startMap(const char* tag)
{
  Value v = next();
  if (v.getType() != "array") {
    throw new IOException("Error deserializing map");
  }
  return new XmlIndex(vlist, vidx);
}

void hadoop::IXmlArchive::endMap(Index* idx, const char* tag)
{
  Value v = next();
  if (v.getType() != "/array") {
    throw new IOException("Error deserializing map");
  }
  delete idx;
}

void hadoop::OXmlArchive::serialize(int8_t t, const char* tag)
{
  printBeginEnvelope(tag);
  p("<ex:i1>");
  char sval[5];
  sprintf(sval, "%d", t);
  p(sval);
  p("</ex:i1>");
  printEndEnvelope(tag);
}

void hadoop::OXmlArchive::serialize(bool t, const char* tag)
{
  printBeginEnvelope(tag);
  p("<boolean>");
  p(t ? "1" : "0");
  p("</boolean>");
  printEndEnvelope(tag);
}

void hadoop::OXmlArchive::serialize(int32_t t, const char* tag)
{
  printBeginEnvelope(tag);
  p("<i4>");
  char sval[128];
  sprintf(sval, "%d", t);
  p(sval);
  p("</i4>");
  printEndEnvelope(tag);
}

void hadoop::OXmlArchive::serialize(int64_t t, const char* tag)
{
  printBeginEnvelope(tag);
  p("<ex:i8>");
  char sval[128];
  sprintf(sval, "%lld", t);
  p(sval);
  p("</ex:i8>");
  printEndEnvelope(tag);

}

void hadoop::OXmlArchive::serialize(float t, const char* tag)
{
  printBeginEnvelope(tag);
  p("<ex:float>");
  char sval[128];
  sprintf(sval, "%f", t);
  p(sval);
  p("</ex:float>");
  printEndEnvelope(tag);
}

void hadoop::OXmlArchive::serialize(double t, const char* tag)
{
  printBeginEnvelope(tag);
  p("<double>");
  char sval[128];
  sprintf(sval, "%lf", t);
  p(sval);
  p("</double>");
  printEndEnvelope(tag);
}

void hadoop::OXmlArchive::serialize(const std::string& t, const char* tag)
{
  printBeginEnvelope(tag);
  p("<string>");
  std::string s = toXMLString(t);
  stream.write(s.data(), s.length());
  p("</string>");
  printEndEnvelope(tag);
}

void hadoop::OXmlArchive::serialize(const std::string& t, size_t len, const char* tag)
{
  printBeginEnvelope(tag);
  p("<string>");
  std::string s = toXMLBuffer(t, len);
  stream.write(s.data(), s.length());
  p("</string>");
  printEndEnvelope(tag);
}

void hadoop::OXmlArchive::startRecord(const Record& s, const char* tag)
{
  insideRecord(tag);
  p("<struct>\n");
}

void hadoop::OXmlArchive::endRecord(const Record& s, const char* tag)
{
  p("</struct>\n");
  outsideRecord(tag);
}

void hadoop::OXmlArchive::startVector(size_t len, const char* tag)
{
  insideVector(tag);
  p("<array>\n");
}

void hadoop::OXmlArchive::endVector(size_t len, const char* tag)
{
  p("</array>\n");
  outsideVector(tag);
}

void hadoop::OXmlArchive::startMap(size_t len, const char* tag)
{
  insideMap(tag);
  p("<array>\n");
}

void hadoop::OXmlArchive::endMap(size_t len, const char* tag)
{
  p("</array>\n");
  outsideMap(tag);
}

hadoop::OXmlArchive::~OXmlArchive()
{
}
