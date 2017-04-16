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

package org.apache.hadoop.record;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 * Abstract class that is extended by generated classes.
 * 
 */
public abstract class Record implements WritableComparable, Cloneable {
  
  /**
   * Serialize a record with tag (ususally field name)
   * @param rout Record output destination
   * @param tag record tag (Used only in tagged serialization e.g. XML)
   */
  public abstract void serialize(RecordOutput rout, String tag)
    throws IOException;
  
  /**
   * Deserialize a record with a tag (usually field name)
   * @param rin Record input source
   * @param tag Record tag (Used only in tagged serialization e.g. XML)
   */
  public abstract void deserialize(RecordInput rin, String tag)
    throws IOException;
  
  // inheric javadoc
  public abstract int compareTo (final Object peer) throws ClassCastException;
  
  /**
   * Serialize a record without a tag
   * @param rout Record output destination
   */
  public void serialize(RecordOutput rout) throws IOException {
    this.serialize(rout, "");
  }
  
  /**
   * Deserialize a record without a tag
   * @param rin Record input source
   */
  public void deserialize(RecordInput rin) throws IOException {
    this.deserialize(rin, "");
  }
  
  // inherit javadoc
  public void write(final DataOutput out) throws java.io.IOException {
    BinaryRecordOutput bout = BinaryRecordOutput.get(out);
    this.serialize(bout);
  }
  
  // inherit javadoc
  public void readFields(final DataInput din) throws java.io.IOException {
    BinaryRecordInput rin = BinaryRecordInput.get(din);
    this.deserialize(rin);
  }

  // inherit javadoc
  public String toString() {
    try {
      ByteArrayOutputStream s = new ByteArrayOutputStream();
      CsvRecordOutput a = new CsvRecordOutput(s);
      this.serialize(a);
      return new String(s.toByteArray(), "UTF-8");
    } catch (Throwable ex) {
      throw new RuntimeException(ex);
    }
  }
}
