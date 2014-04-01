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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

class JvmContext implements Writable {

  public static final Log LOG =
    LogFactory.getLog(JvmContext.class);
  
  JVMId jvmId;
  String pid;
  
  JvmContext() {
    jvmId = new JVMId();
    pid = "";
  }
  
  JvmContext(JVMId id, String pid) {
    jvmId = id;
    this.pid = pid;
  }
  
  public void readFields(DataInput in) throws IOException {
    jvmId.readFields(in);
    this.pid = Text.readString(in);
  }
  
  public void write(DataOutput out) throws IOException {
    jvmId.write(out);
    Text.writeString(out, pid);
  }
}
