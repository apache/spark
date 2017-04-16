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
package org.apache.hadoop.hdfs.server.common;

import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.FSConstants;

/**
 * The exception is thrown when external version does not match 
 * current version of the appication.
 * 
 */
public class IncorrectVersionException extends IOException {

  public IncorrectVersionException(int versionReported, String ofWhat) {
    this(versionReported, ofWhat, FSConstants.LAYOUT_VERSION);
  }
  
  public IncorrectVersionException(int versionReported,
                                   String ofWhat,
                                   int versionExpected) {
    super("Unexpected version " 
          + (ofWhat==null ? "" : "of " + ofWhat) + ". Reported: "
          + versionReported + ". Expecting = " + versionExpected + ".");
  }

}
