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
package org.apache.hadoop.mapred;

import java.io.IOException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TestJobTrackerXmlJsp extends ClusterMapReduceTestCase {

  private static final Log LOG = LogFactory.getLog(TestJobTrackerXmlJsp.class);

  /**
   * Read the jobtracker.jspx status page and validate that the XML is well formed.
   */
  public void testXmlWellFormed() throws IOException, ParserConfigurationException, SAXException {
    MiniMRCluster cluster = getMRCluster();
    int infoPort = cluster.getJobTrackerRunner().getJobTrackerInfoPort();

    String xmlJspUrl = "http://localhost:" + infoPort + "/jobtracker.jspx";
    LOG.info("Retrieving XML from URL: " + xmlJspUrl);

    DocumentBuilder parser = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    Document doc = parser.parse(xmlJspUrl);

    // If we get here, then the document was successfully parsed by SAX and is well-formed.
    LOG.info("Document received and parsed.");

    // Make sure it has a <cluster> element as top-level.
    NodeList clusterNodes = doc.getElementsByTagName("cluster");
    assertEquals("There should be exactly 1 <cluster> element", 1, clusterNodes.getLength());
  }
}
