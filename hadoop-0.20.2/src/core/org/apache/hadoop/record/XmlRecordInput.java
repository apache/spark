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

package org.apache.hadoop.record;

import java.io.InputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.parsers.SAXParser;

/**
 * XML Deserializer.
 */
public class XmlRecordInput implements RecordInput {
    
  static private class Value {
    private String type;
    private StringBuffer sb;
        
    public Value(String t) {
      type = t;
      sb = new StringBuffer();
    }
    public void addChars(char[] buf, int offset, int len) {
      sb.append(buf, offset, len);
    }
    public String getValue() { return sb.toString(); }
    public String getType() { return type; }
  }
    
  private static class XMLParser extends DefaultHandler {
    private boolean charsValid = false;
        
    private ArrayList<Value> valList;
        
    private XMLParser(ArrayList<Value> vlist) {
      valList = vlist;
    }
        
    public void startDocument() throws SAXException {}
        
    public void endDocument() throws SAXException {}
        
    public void startElement(String ns,
                             String sname,
                             String qname,
                             Attributes attrs) throws SAXException {
      charsValid = false;
      if ("boolean".equals(qname) ||
          "i4".equals(qname) ||
          "int".equals(qname) ||
          "string".equals(qname) ||
          "double".equals(qname) ||
          "ex:i1".equals(qname) ||
          "ex:i8".equals(qname) ||
          "ex:float".equals(qname)) {
        charsValid = true;
        valList.add(new Value(qname));
      } else if ("struct".equals(qname) ||
                 "array".equals(qname)) {
        valList.add(new Value(qname));
      }
    }
        
    public void endElement(String ns,
                           String sname,
                           String qname) throws SAXException {
      charsValid = false;
      if ("struct".equals(qname) ||
          "array".equals(qname)) {
        valList.add(new Value("/"+qname));
      }
    }
        
    public void characters(char buf[], int offset, int len)
      throws SAXException {
      if (charsValid) {
        Value v = valList.get(valList.size()-1);
        v.addChars(buf, offset, len);
      }
    }
        
  }
    
  private class XmlIndex implements Index {
    public boolean done() {
      Value v = valList.get(vIdx);
      if ("/array".equals(v.getType())) {
        valList.set(vIdx, null);
        vIdx++;
        return true;
      } else {
        return false;
      }
    }
    public void incr() {}
  }
    
  private ArrayList<Value> valList;
  private int vLen;
  private int vIdx;
    
  private Value next() throws IOException {
    if (vIdx < vLen) {
      Value v = valList.get(vIdx);
      valList.set(vIdx, null);
      vIdx++;
      return v;
    } else {
      throw new IOException("Error in deserialization.");
    }
  }
    
  /** Creates a new instance of XmlRecordInput */
  public XmlRecordInput(InputStream in) {
    try{
      valList = new ArrayList<Value>();
      DefaultHandler handler = new XMLParser(valList);
      SAXParserFactory factory = SAXParserFactory.newInstance();
      SAXParser parser = factory.newSAXParser();
      parser.parse(in, handler);
      vLen = valList.size();
      vIdx = 0;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
    
  public byte readByte(String tag) throws IOException {
    Value v = next();
    if (!"ex:i1".equals(v.getType())) {
      throw new IOException("Error deserializing "+tag+".");
    }
    return Byte.parseByte(v.getValue());
  }
    
  public boolean readBool(String tag) throws IOException {
    Value v = next();
    if (!"boolean".equals(v.getType())) {
      throw new IOException("Error deserializing "+tag+".");
    }
    return "1".equals(v.getValue());
  }
    
  public int readInt(String tag) throws IOException {
    Value v = next();
    if (!"i4".equals(v.getType()) &&
        !"int".equals(v.getType())) {
      throw new IOException("Error deserializing "+tag+".");
    }
    return Integer.parseInt(v.getValue());
  }
    
  public long readLong(String tag) throws IOException {
    Value v = next();
    if (!"ex:i8".equals(v.getType())) {
      throw new IOException("Error deserializing "+tag+".");
    }
    return Long.parseLong(v.getValue());
  }
    
  public float readFloat(String tag) throws IOException {
    Value v = next();
    if (!"ex:float".equals(v.getType())) {
      throw new IOException("Error deserializing "+tag+".");
    }
    return Float.parseFloat(v.getValue());
  }
    
  public double readDouble(String tag) throws IOException {
    Value v = next();
    if (!"double".equals(v.getType())) {
      throw new IOException("Error deserializing "+tag+".");
    }
    return Double.parseDouble(v.getValue());
  }
    
  public String readString(String tag) throws IOException {
    Value v = next();
    if (!"string".equals(v.getType())) {
      throw new IOException("Error deserializing "+tag+".");
    }
    return Utils.fromXMLString(v.getValue());
  }
    
  public Buffer readBuffer(String tag) throws IOException {
    Value v = next();
    if (!"string".equals(v.getType())) {
      throw new IOException("Error deserializing "+tag+".");
    }
    return Utils.fromXMLBuffer(v.getValue());
  }
    
  public void startRecord(String tag) throws IOException {
    Value v = next();
    if (!"struct".equals(v.getType())) {
      throw new IOException("Error deserializing "+tag+".");
    }
  }
    
  public void endRecord(String tag) throws IOException {
    Value v = next();
    if (!"/struct".equals(v.getType())) {
      throw new IOException("Error deserializing "+tag+".");
    }
  }
    
  public Index startVector(String tag) throws IOException {
    Value v = next();
    if (!"array".equals(v.getType())) {
      throw new IOException("Error deserializing "+tag+".");
    }
    return new XmlIndex();
  }
    
  public void endVector(String tag) throws IOException {}
    
  public Index startMap(String tag) throws IOException {
    return startVector(tag);
  }
    
  public void endMap(String tag) throws IOException { endVector(tag); }

}
