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
package org.apache.hadoop.vaidya.util;

import java.io.IOException;
import java.io.File;
import java.io.InputStream;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.Source;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.Result;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;

import org.xml.sax.SAXParseException;
import org.xml.sax.SAXException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


/**
 * Sample Utility class to work with DOM document
 */
public class XMLUtils {

  /** Prints the specified node, then prints all of its children. */

  public static void printDOM(Node node) {

    int type = node.getNodeType();

    switch (type) {

      // print the document element
      case Node.DOCUMENT_NODE: {
        System.out.print("<?xml version=\"1.0\" ?>");
        printDOM(((Document)node).getDocumentElement());
        break;
      }

      // print element with attributes
      case Node.ELEMENT_NODE: {
      System.out.println();
        System.out.print("<");
        System.out.print(node.getNodeName());
        NamedNodeMap attrs = node.getAttributes();
        for (int i = 0; i < attrs.getLength(); i++) {
          Node attr = attrs.item(i);
          System.out.print(" " + attr.getNodeName().trim() +
                           "=\"" + attr.getNodeValue().trim() +
                           "\"");
        }
        System.out.print(">");
        NodeList children = node.getChildNodes();

        if (children != null) {
          int len = children.getLength();
          for (int i = 0; i < len; i++)
            printDOM(children.item(i));
        }
        break;
      }

      // handle entity reference nodes

      case Node.ENTITY_REFERENCE_NODE: {
        System.out.print("&");
        System.out.print(node.getNodeName().trim());
        System.out.print(";");
        break;
      }

      // print cdata sections
      case Node.CDATA_SECTION_NODE: {
        System.out.print("<![CDATA[");
        System.out.print(node.getNodeValue().trim());
        System.out.print("]]>");
        break;
      }

      // print text
      case Node.TEXT_NODE: {
      System.out.println();
        System.out.print(node.getNodeValue().trim());
        break;
      }

      // print processing instruction

    case Node.PROCESSING_INSTRUCTION_NODE: {
      System.out.print("<?");
      System.out.print(node.getNodeName().trim());
      String data = node.getNodeValue().trim(); {
        System.out.print(" ");
        System.out.print(data);
      }
        System.out.print("?>");
        break;
      }
    }

    if (type == Node.ELEMENT_NODE) {
      System.out.println();
      System.out.print("</");
      System.out.print(node.getNodeName().trim());
      System.out.print('>');
    }
  }

  /*
   * Get the value of the first (or only) element given its node name
   */
  public static String getElementValue(String elementName, Element element) throws Exception {
    String value = null;
    NodeList childNodes = element.getElementsByTagName(elementName);
    Element cn = (Element)childNodes.item(0);
    value = cn.getFirstChild().getNodeValue().trim();
    //value = childNodes.item(0).getNodeValue().trim();
    if (value == null) { 
      throw new Exception ("No element found with given name:"+elementName);
    }
    return value;
  }

  /**
   * Parse the XML file and create Document
   * @param fileName
   * @return Document
   */
  public static Document parse(InputStream fs) {
    Document document = null;
    // Initiate DocumentBuilderFactory
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

    // To get a validating parser
    factory.setValidating(false);

    // To get one that understands namespaces
    factory.setNamespaceAware(true);
    try {
      // Get DocumentBuilder
      DocumentBuilder builder = factory.newDocumentBuilder();

      // Parse and load into memory the Document
      //document = builder.parse( new File(fileName));
      document = builder.parse(fs);
      return document;
    } catch (SAXParseException spe) {
      // Error generated by the parser
      System.err.println("\n** Parsing error , line " + spe.getLineNumber()
                         + ", uri " + spe.getSystemId());
      System.err.println(" " + spe.getMessage() );
      // Use the contained exception, if any
      Exception x = spe;
      if (spe.getException() != null)
        x = spe.getException();
      x.printStackTrace();
    } catch (SAXException sxe) {
      // Error generated during parsing
      Exception x = sxe;
      if (sxe.getException() != null)
        x = sxe.getException();
      x.printStackTrace();
    } catch (ParserConfigurationException pce) {
      // Parser with specified options can't be built
      pce.printStackTrace();
    } catch (IOException ioe) {
      // I/O error
      ioe.printStackTrace();
    }
    
    return null;
  }

  /**
   * This method writes a DOM document to a file
   * @param filename
   * @param document
   */
  public static void writeXmlToFile(String filename, Document document) {
    try {
      // Prepare the DOM document for writing
      Source source = new DOMSource(document);
      
      // Prepare the output file
      File file = new File(filename);
      Result result = new StreamResult(file);

      // Write the DOM document to the file
      // Get Transformer
      Transformer xformer = TransformerFactory.newInstance().newTransformer();

      // Write to a file
      xformer.transform(source, result);

    } catch (TransformerConfigurationException e) {
      System.err.println("TransformerConfigurationException: " + e);
    } catch (TransformerException e) {
      System.err.println("TransformerException: " + e);
    }
  }

  /**
   * Count Elements in Document by Tag Name
   * @param tag
   * @param document
   * @return number elements by Tag Name
   */
  public static int countByTagName(String tag, Document document){
    NodeList list = document.getElementsByTagName(tag);
    return list.getLength();
  }
}
