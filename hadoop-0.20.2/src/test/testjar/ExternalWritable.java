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

package testjar;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * This is an example simple writable class.  This is used as a class external 
 * to the Hadoop IO classes for testing of user Writable classes.
 * 
 */
public class ExternalWritable
  implements WritableComparable {

  private String message = null;
  
  public ExternalWritable() {
    
  }
  
  public ExternalWritable(String message) {
    this.message = message;
  }
  
  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public void readFields(DataInput in)
    throws IOException {
    
    message = null;
    boolean hasMessage = in.readBoolean();
    if (hasMessage) {
      message = in.readUTF();   
    }
  }

  public void write(DataOutput out)
    throws IOException {
    
    boolean hasMessage = (message != null && message.length() > 0);
    out.writeBoolean(hasMessage);
    if (hasMessage) {
      out.writeUTF(message);
    }
  }
  
  public int compareTo(Object o) {
    
    if (!(o instanceof ExternalWritable)) {
      throw new IllegalArgumentException("Input not an ExternalWritable");
    }
    
    ExternalWritable that = (ExternalWritable)o;
    return this.message.compareTo(that.message);
  }

  public String toString() {
    return this.message;
  }
}
