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

import java.io.IOException;
import java.util.TreeMap;
import java.util.ArrayList;
import java.io.PrintStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Stack;

/**
 * XML Serializer.
 */
public class XmlRecordOutput implements RecordOutput {

  private PrintStream stream;
    
  private int indent = 0;
    
  private Stack<String> compoundStack;
    
  private void putIndent() {
    StringBuffer sb = new StringBuffer("");
    for (int idx = 0; idx < indent; idx++) {
      sb.append("  ");
    }
    stream.print(sb.toString());
  }
    
  private void addIndent() {
    indent++;
  }
    
  private void closeIndent() {
    indent--;
  }
    
  private void printBeginEnvelope(String tag) {
    if (!compoundStack.empty()) {
      String s = compoundStack.peek();
      if ("struct".equals(s)) {
        putIndent();
        stream.print("<member>\n");
        addIndent();
        putIndent();
        stream.print("<name>"+tag+"</name>\n");
        putIndent();
        stream.print("<value>");
      } else if ("vector".equals(s)) {
        stream.print("<value>");
      } else if ("map".equals(s)) {
        stream.print("<value>");
      }
    } else {
      stream.print("<value>");
    }
  }
    
  private void printEndEnvelope(String tag) {
    if (!compoundStack.empty()) {
      String s = compoundStack.peek();
      if ("struct".equals(s)) {
        stream.print("</value>\n");
        closeIndent();
        putIndent();
        stream.print("</member>\n");
      } else if ("vector".equals(s)) {
        stream.print("</value>\n");
      } else if ("map".equals(s)) {
        stream.print("</value>\n");
      }
    } else {
      stream.print("</value>\n");
    }
  }
    
  private void insideVector(String tag) {
    printBeginEnvelope(tag);
    compoundStack.push("vector");
  }
    
  private void outsideVector(String tag) throws IOException {
    String s = compoundStack.pop();
    if (!"vector".equals(s)) {
      throw new IOException("Error serializing vector.");
    }
    printEndEnvelope(tag);
  }
    
  private void insideMap(String tag) {
    printBeginEnvelope(tag);
    compoundStack.push("map");
  }
    
  private void outsideMap(String tag) throws IOException {
    String s = compoundStack.pop();
    if (!"map".equals(s)) {
      throw new IOException("Error serializing map.");
    }
    printEndEnvelope(tag);
  }
    
  private void insideRecord(String tag) {
    printBeginEnvelope(tag);
    compoundStack.push("struct");
  }
    
  private void outsideRecord(String tag) throws IOException {
    String s = compoundStack.pop();
    if (!"struct".equals(s)) {
      throw new IOException("Error serializing record.");
    }
    printEndEnvelope(tag);
  }
    
  /** Creates a new instance of XmlRecordOutput */
  public XmlRecordOutput(OutputStream out) {
    try {
      stream = new PrintStream(out, true, "UTF-8");
      compoundStack = new Stack<String>();
    } catch (UnsupportedEncodingException ex) {
      throw new RuntimeException(ex);
    }
  }
    
  public void writeByte(byte b, String tag) throws IOException {
    printBeginEnvelope(tag);
    stream.print("<ex:i1>");
    stream.print(Byte.toString(b));
    stream.print("</ex:i1>");
    printEndEnvelope(tag);
  }
    
  public void writeBool(boolean b, String tag) throws IOException {
    printBeginEnvelope(tag);
    stream.print("<boolean>");
    stream.print(b ? "1" : "0");
    stream.print("</boolean>");
    printEndEnvelope(tag);
  }
    
  public void writeInt(int i, String tag) throws IOException {
    printBeginEnvelope(tag);
    stream.print("<i4>");
    stream.print(Integer.toString(i));
    stream.print("</i4>");
    printEndEnvelope(tag);
  }
    
  public void writeLong(long l, String tag) throws IOException {
    printBeginEnvelope(tag);
    stream.print("<ex:i8>");
    stream.print(Long.toString(l));
    stream.print("</ex:i8>");
    printEndEnvelope(tag);
  }
    
  public void writeFloat(float f, String tag) throws IOException {
    printBeginEnvelope(tag);
    stream.print("<ex:float>");
    stream.print(Float.toString(f));
    stream.print("</ex:float>");
    printEndEnvelope(tag);
  }
    
  public void writeDouble(double d, String tag) throws IOException {
    printBeginEnvelope(tag);
    stream.print("<double>");
    stream.print(Double.toString(d));
    stream.print("</double>");
    printEndEnvelope(tag);
  }
    
  public void writeString(String s, String tag) throws IOException {
    printBeginEnvelope(tag);
    stream.print("<string>");
    stream.print(Utils.toXMLString(s));
    stream.print("</string>");
    printEndEnvelope(tag);
  }
    
  public void writeBuffer(Buffer buf, String tag)
    throws IOException {
    printBeginEnvelope(tag);
    stream.print("<string>");
    stream.print(Utils.toXMLBuffer(buf));
    stream.print("</string>");
    printEndEnvelope(tag);
  }
    
  public void startRecord(Record r, String tag) throws IOException {
    insideRecord(tag);
    stream.print("<struct>\n");
    addIndent();
  }
    
  public void endRecord(Record r, String tag) throws IOException {
    closeIndent();
    putIndent();
    stream.print("</struct>");
    outsideRecord(tag);
  }
    
  public void startVector(ArrayList v, String tag) throws IOException {
    insideVector(tag);
    stream.print("<array>\n");
    addIndent();
  }
    
  public void endVector(ArrayList v, String tag) throws IOException {
    closeIndent();
    putIndent();
    stream.print("</array>");
    outsideVector(tag);
  }
    
  public void startMap(TreeMap v, String tag) throws IOException {
    insideMap(tag);
    stream.print("<array>\n");
    addIndent();
  }
    
  public void endMap(TreeMap v, String tag) throws IOException {
    closeIndent();
    putIndent();
    stream.print("</array>");
    outsideMap(tag);
  }

}
