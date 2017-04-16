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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import java.io.IOException;
import java.util.LinkedList;

/**
 * An XmlImageVisitor walks over an fsimage structure and writes out
 * an equivalent XML document that contains the fsimage's components.
 */
class XmlImageVisitor extends TextWriterImageVisitor {
  final private LinkedList<ImageElement> tagQ =
                                          new LinkedList<ImageElement>();

  public XmlImageVisitor(String filename) throws IOException {
    super(filename, false);
  }

  public XmlImageVisitor(String filename, boolean printToScreen)
       throws IOException {
    super(filename, printToScreen);
  }

  @Override
  void finish() throws IOException {
    super.finish();
  }

  @Override
  void finishAbnormally() throws IOException {
    write("\n<!-- Error processing image file.  Exiting -->\n");
    super.finishAbnormally();
  }

  @Override
  void leaveEnclosingElement() throws IOException {
    if(tagQ.size() == 0)
      throw new IOException("Tried to exit non-existent enclosing element " +
                "in FSImage file");

    ImageElement element = tagQ.pop();
    write("</" + element.toString() + ">\n");
  }

  @Override
  void start() throws IOException {
    write("<?xml version=\"1.0\" ?>\n");
  }

  @Override
  void visit(ImageElement element, String value) throws IOException {
    writeTag(element.toString(), value);
  }

  @Override
  void visitEnclosingElement(ImageElement element) throws IOException {
    write("<" + element.toString() + ">\n");
    tagQ.push(element);
  }

  @Override
  void visitEnclosingElement(ImageElement element,
      ImageElement key, String value)
       throws IOException {
    write("<" + element.toString() + " " + key + "=\"" + value +"\">\n");
    tagQ.push(element);
  }

  private void writeTag(String tag, String value) throws IOException {
    write("<" + tag + ">" + value + "</" + tag + ">\n");
  }
}
