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

package org.apache.hadoop.record.compiler;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Container for the Hadoop Record DDL.
 * The main components of the file are filename, list of included files,
 * and records defined in that file.
 */
public class JFile {
  /** Possibly full name of the file */
  private String mName;
  /** Ordered list of included files */
  private ArrayList<JFile> mInclFiles;
  /** Ordered list of records declared in this file */
  private ArrayList<JRecord> mRecords;
    
  /** Creates a new instance of JFile
   *
   * @param name possibly full pathname to the file
   * @param inclFiles included files (as JFile)
   * @param recList List of records defined within this file
   */
  public JFile(String name, ArrayList<JFile> inclFiles,
               ArrayList<JRecord> recList) {
    mName = name;
    mInclFiles = inclFiles;
    mRecords = recList;
  }
    
  /** Strip the other pathname components and return the basename */
  String getName() {
    int idx = mName.lastIndexOf('/');
    return (idx > 0) ? mName.substring(idx) : mName; 
  }
    
  /** Generate record code in given language. Language should be all
   *  lowercase.
   */
  public int genCode(String language, String destDir, ArrayList<String> options)
    throws IOException {
    CodeGenerator gen = CodeGenerator.get(language);
    if (gen != null) {
      gen.genCode(mName, mInclFiles, mRecords, destDir, options);
    } else {
      System.err.println("Cannnot recognize language:"+language);
      return 1;
    }
    return 0;
  }
}
