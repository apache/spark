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

package org.apache.hadoop.util;

import javax.xml.transform.*;
import javax.xml.transform.stream.*;
import java.io.*;

/**
 * General xml utilities.
 *   
 */
public class XMLUtils {
  /**
   * Transform input xml given a stylesheet.
   * 
   * @param styleSheet the style-sheet
   * @param xml input xml data
   * @param out output
   * @throws TransformerConfigurationException
   * @throws TransformerException
   */
  public static void transform(
                               InputStream styleSheet, InputStream xml, Writer out
                               ) 
    throws TransformerConfigurationException, TransformerException {
    // Instantiate a TransformerFactory
    TransformerFactory tFactory = TransformerFactory.newInstance();

    // Use the TransformerFactory to process the  
    // stylesheet and generate a Transformer
    Transformer transformer = tFactory.newTransformer(
                                                      new StreamSource(styleSheet)
                                                      );

    // Use the Transformer to transform an XML Source 
    // and send the output to a Result object.
    transformer.transform(new StreamSource(xml), new StreamResult(out));
  }
}
