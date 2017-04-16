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

import java.util.ArrayList;
import java.io.IOException;
import java.util.Iterator;

/**
 * Java Code generator front-end for Hadoop record I/O.
 */
class JavaGenerator extends CodeGenerator {
  
  JavaGenerator() {
  }
  
  /**
   * Generate Java code for records. This method is only a front-end to
   * JRecord, since one file is generated for each record.
   *
   * @param name possibly full pathname to the file
   * @param ilist included files (as JFile)
   * @param rlist List of records defined within this file
   * @param destDir output directory
   */
  void genCode(String name, ArrayList<JFile> ilist,
               ArrayList<JRecord> rlist, String destDir, ArrayList<String> options)
    throws IOException {
    for (Iterator<JRecord> iter = rlist.iterator(); iter.hasNext();) {
      JRecord rec = iter.next();
      rec.genJavaCode(destDir, options);
    }
  }
}
