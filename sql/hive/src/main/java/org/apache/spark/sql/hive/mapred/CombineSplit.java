/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.mapred.InputSplit;

public class CombineSplit implements InputSplit {
  private InputSplit[] splits;
  private long totalLen;
  private String[] locations;

  public CombineSplit() {
  }

  public CombineSplit(InputSplit[] ss, long totalLen, String[] locations) {
    splits = ss;
    this.totalLen = totalLen;
    this.locations = locations;
  }

  public InputSplit getSplit(int idx) {
    return splits[idx];
  }

  public int getSplitNum() {
    return splits.length;
  }

  @Override
  public long getLength() {
    return totalLen;
  }

  @Override
  public String[] getLocations() throws IOException {
    return locations;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(totalLen);
    out.writeInt(locations.length);
    for (String location : locations) {
      out.writeUTF(location);
    }
    out.writeInt(splits.length);
    out.writeUTF(splits[0].getClass().getCanonicalName());
    for (InputSplit split : splits) {
      split.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.totalLen = in.readLong();
    this.locations = new String[in.readInt()];
    for (int i = 0; i < locations.length; i++) {
      locations[i] = in.readUTF();
    }
    splits = new InputSplit[in.readInt()];
    String className = in.readUTF();
    Class<? extends Writable> clazz = null;
    try {
      clazz = (Class<? extends Writable>) Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    for (int i = 0; i < splits.length; i++) {
      Writable value = WritableFactories.newInstance(clazz, null);
      value.readFields(in);
      splits[i] = (InputSplit) value;
    }
  }
}
