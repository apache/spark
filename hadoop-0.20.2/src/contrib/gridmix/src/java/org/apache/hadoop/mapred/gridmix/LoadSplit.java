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
package org.apache.hadoop.mapred.gridmix;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;

class LoadSplit extends CombineFileSplit {
  private int id;
  private int nSpec;
  private int maps;
  private int reduces;
  private long inputRecords;
  private long outputBytes;
  private long outputRecords;
  private long maxMemory;
  private double[] reduceBytes = new double[0];
  private double[] reduceRecords = new double[0];

  // Spec for reduces id mod this
  private long[] reduceOutputBytes = new long[0];
  private long[] reduceOutputRecords = new long[0];

  LoadSplit() {
    super();
  }

  public LoadSplit(CombineFileSplit cfsplit, int maps, int id,
      long inputBytes, long inputRecords, long outputBytes,
      long outputRecords, double[] reduceBytes, double[] reduceRecords,
      long[] reduceOutputBytes, long[] reduceOutputRecords)
      throws IOException {
    super(cfsplit);
    this.id = id;
    this.maps = maps;
    reduces = reduceBytes.length;
    this.inputRecords = inputRecords;
    this.outputBytes = outputBytes;
    this.outputRecords = outputRecords;
    this.reduceBytes = reduceBytes;
    this.reduceRecords = reduceRecords;
    nSpec = reduceOutputBytes.length;
    this.reduceOutputBytes = reduceOutputBytes;
    this.reduceOutputRecords = reduceOutputRecords;
  }

  public int getId() {
    return id;
  }
  public int getMapCount() {
    return maps;
  }
  public long getInputRecords() {
    return inputRecords;
  }
  public long[] getOutputBytes() {
    if (0 == reduces) {
      return new long[] { outputBytes };
    }
    final long[] ret = new long[reduces];
    for (int i = 0; i < reduces; ++i) {
      ret[i] = Math.round(outputBytes * reduceBytes[i]);
    }
    return ret;
  }
  public long[] getOutputRecords() {
    if (0 == reduces) {
      return new long[] { outputRecords };
    }
    final long[] ret = new long[reduces];
    for (int i = 0; i < reduces; ++i) {
      ret[i] = Math.round(outputRecords * reduceRecords[i]);
    }
    return ret;
  }
  public long getReduceBytes(int i) {
    return reduceOutputBytes[i];
  }
  public long getReduceRecords(int i) {
    return reduceOutputRecords[i];
  }
  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    WritableUtils.writeVInt(out, id);
    WritableUtils.writeVInt(out, maps);
    WritableUtils.writeVLong(out, inputRecords);
    WritableUtils.writeVLong(out, outputBytes);
    WritableUtils.writeVLong(out, outputRecords);
    WritableUtils.writeVLong(out, maxMemory);
    WritableUtils.writeVInt(out, reduces);
    for (int i = 0; i < reduces; ++i) {
      out.writeDouble(reduceBytes[i]);
      out.writeDouble(reduceRecords[i]);
    }
    WritableUtils.writeVInt(out, nSpec);
    for (int i = 0; i < nSpec; ++i) {
      WritableUtils.writeVLong(out, reduceOutputBytes[i]);
      WritableUtils.writeVLong(out, reduceOutputRecords[i]);
    }
  }
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    id = WritableUtils.readVInt(in);
    maps = WritableUtils.readVInt(in);
    inputRecords = WritableUtils.readVLong(in);
    outputBytes = WritableUtils.readVLong(in);
    outputRecords = WritableUtils.readVLong(in);
    maxMemory = WritableUtils.readVLong(in);
    reduces = WritableUtils.readVInt(in);
    if (reduceBytes.length < reduces) {
      reduceBytes = new double[reduces];
      reduceRecords = new double[reduces];
    }
    for (int i = 0; i < reduces; ++i) {
      reduceBytes[i] = in.readDouble();
      reduceRecords[i] = in.readDouble();
    }
    nSpec = WritableUtils.readVInt(in);
    if (reduceOutputBytes.length < nSpec) {
      reduceOutputBytes = new long[nSpec];
      reduceOutputRecords = new long[nSpec];
    }
    for (int i = 0; i < nSpec; ++i) {
      reduceOutputBytes[i] = WritableUtils.readVLong(in);
      reduceOutputRecords[i] = WritableUtils.readVLong(in);
    }
  }
}
