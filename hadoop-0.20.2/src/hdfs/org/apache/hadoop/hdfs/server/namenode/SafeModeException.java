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

package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

/**
 * This exception is thrown when the name node is in safe mode.
 * Client cannot modified namespace until the safe mode is off. 
 * 
 */
public class SafeModeException extends IOException {

  public SafeModeException(String text, FSNamesystem.SafeModeInfo mode ) {
    super(text + ". Name node is in safe mode.\n" + mode.getTurnOffTip());
  }

}
