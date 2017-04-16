/* Licensed to the Apache Software Foundation (ASF) under one
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
package org.apache.hadoop.hdfs.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * This class defines a partial listing of a directory to support
 * iterative directory listing.
 */
public class DirectoryListing implements Writable {
  static {                                      // register a ctor
    WritableFactories.setFactory
      (DirectoryListing.class,
       new WritableFactory() {
         public Writable newInstance() { return new DirectoryListing(); }
       });
  }

  private HdfsFileStatus[] partialListing;
  private int remainingEntries;
  
  /**
   * default constructor
   */
  public DirectoryListing() {
  }
  
  /**
   * constructor
   * @param partialListing a partial listing of a directory
   * @param remainingEntries number of entries that are left to be listed
   */
  public DirectoryListing(HdfsFileStatus[] partialListing, 
      int remainingEntries) {
    if (partialListing == null) {
      throw new IllegalArgumentException("partial listing should not be null");
    }
    if (partialListing.length == 0 && remainingEntries != 0) {
      throw new IllegalArgumentException("Partial listing is empty but " +
          "the number of remaining entries is not zero");
    }
    this.partialListing = partialListing;
    this.remainingEntries = remainingEntries;
  }

  /**
   * Get the partial listing of file status
   * @return the partial listing of file status
   */
  public HdfsFileStatus[] getPartialListing() {
    return partialListing;
  }
  
  /**
   * Get the number of remaining entries that are left to be listed
   * @return the number of remaining entries that are left to be listed
   */
  public int getRemainingEntries() {
    return remainingEntries;
  }
  
  /**
   * Check if there are more entries that are left to be listed
   * @return true if there are more entries that are left to be listed;
   *         return false otherwise.
   */
  public boolean hasMore() {
    return remainingEntries != 0;
  }
  
  /**
   * Get the last name in this list
   * @return the last name in the list if it is not empty; otherwise return null
   */
  public byte[] getLastName() {
    if (partialListing.length == 0) {
      return null;
    }
    return partialListing[partialListing.length-1].getLocalNameInBytes();
  }

  // Writable interface
  @Override
  public void readFields(DataInput in) throws IOException {
    int numEntries = in.readInt();
    partialListing = new HdfsFileStatus[numEntries];
    for (int i=0; i<numEntries; i++) {
      partialListing[i] = new HdfsFileStatus();
      partialListing[i].readFields(in);
    }
    remainingEntries = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(partialListing.length);
    for (HdfsFileStatus fileStatus : partialListing) {
      fileStatus.write(out);
    }
    out.writeInt(remainingEntries);
  }
}
