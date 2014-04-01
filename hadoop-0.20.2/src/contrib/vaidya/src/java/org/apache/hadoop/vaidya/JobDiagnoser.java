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
package org.apache.hadoop.vaidya;

import org.apache.hadoop.vaidya.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;


/**
 * This is a base driver class for job diagnostics. Various specialty drivers that
 * tests specific aspects of job problems e.g. PostExPerformanceDiagnoser extends the
 * this base class.
 *
 */
public class JobDiagnoser {

  /*
   * XML document containing report elements, one for each rule tested
   */
  private Document _report;

  /*
   * @report : returns report document
   */
  public Document getReport() {
    return this._report;
  }
  

  /**
   * Constructor. It initializes the report document.
   */
  public JobDiagnoser () throws Exception {
    
    /*
     * Initialize the report document, make it ready to add the child report elements 
     */
    DocumentBuilder builder = null;
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    try{
      builder = factory.newDocumentBuilder();
      this._report = builder.newDocument();
    } catch (ParserConfigurationException e) {
      e.printStackTrace();
    }
      
    // Insert Root Element
    Element root = (Element) this._report.createElement("PostExPerformanceDiagnosticReport");
    this._report.appendChild(root);
  }
  
  /*
   * Print the report document to console
   */
  public void printReport() {
    XMLUtils.printDOM(this._report);
  }
  
  /*
   * Save the report document the specified report file
   * @param reportfile : path of report file. 
   */
  public void saveReport(String filename) {
    XMLUtils.writeXmlToFile(filename, this._report);
  }
}
