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
package org.apache.hadoop.fs;

import org.apache.hadoop.io.*;

import java.io.*;

/*
 * A BlockLocation lists hosts, offset and length
 * of block. 
 * 
 */
public class BlockLocation implements Writable {

  static {               // register a ctor
    WritableFactories.setFactory
      (BlockLocation.class,
       new WritableFactory() {
         public Writable newInstance() { return new BlockLocation(); }
       });
  }

  private String[] hosts; //hostnames of datanodes
  private String[] names; //hostname:portNumber of datanodes
  private String[] topologyPaths; // full path name in network topology
  private long offset;  //offset of the of the block in the file
  private long length;

  /**
   * Default Constructor
   */
  public BlockLocation() {
    this(new String[0], new String[0],  0L, 0L);
  }

  /**
   * Constructor with host, name, offset and length
   */
  public BlockLocation(String[] names, String[] hosts, long offset, 
                       long length) {
    if (names == null) {
      this.names = new String[0];
    } else {
      this.names = names;
    }
    if (hosts == null) {
      this.hosts = new String[0];
    } else {
      this.hosts = hosts;
    }
    this.offset = offset;
    this.length = length;
    this.topologyPaths = new String[0];
  }

  /**
   * Constructor with host, name, network topology, offset and length
   */
  public BlockLocation(String[] names, String[] hosts, String[] topologyPaths,
                       long offset, long length) {
    this(names, hosts, offset, length);
    if (topologyPaths == null) {
      this.topologyPaths = new String[0];
    } else {
      this.topologyPaths = topologyPaths;
    }
  }

  /**
   * Get the list of hosts (hostname) hosting this block
   */
  public String[] getHosts() throws IOException {
    if ((hosts == null) || (hosts.length == 0)) {
      return new String[0];
    } else {
      return hosts;
    }
  }

  /**
   * Get the list of names (hostname:port) hosting this block
   */
  public String[] getNames() throws IOException {
    if ((names == null) || (names.length == 0)) {
      return new String[0];
    } else {
      return this.names;
    }
  }

  /**
   * Get the list of network topology paths for each of the hosts.
   * The last component of the path is the host.
   */
  public String[] getTopologyPaths() throws IOException {
    if ((topologyPaths == null) || (topologyPaths.length == 0)) {
      return new String[0];
    } else {
      return this.topologyPaths;
    }
  }
  
  /**
   * Get the start offset of file associated with this block
   */
  public long getOffset() {
    return offset;
  }
  
  /**
   * Get the length of the block
   */
  public long getLength() {
    return length;
  }
  
  /**
   * Set the start offset of file associated with this block
   */
  public void setOffset(long offset) {
    this.offset = offset;
  }

  /**
   * Set the length of block
   */
  public void setLength(long length) {
    this.length = length;
  }

  /**
   * Set the hosts hosting this block
   */
  public void setHosts(String[] hosts) throws IOException {
    if (hosts == null) {
      this.hosts = new String[0];
    } else {
      this.hosts = hosts;
    }
  }

  /**
   * Set the names (host:port) hosting this block
   */
  public void setNames(String[] names) throws IOException {
    if (names == null) {
      this.names = new String[0];
    } else {
      this.names = names;
    }
  }

  /**
   * Set the network topology paths of the hosts
   */
  public void setTopologyPaths(String[] topologyPaths) throws IOException {
    if (topologyPaths == null) {
      this.topologyPaths = new String[0];
    } else {
      this.topologyPaths = topologyPaths;
    }
  }

  /**
   * Implement write of Writable
   */
  public void write(DataOutput out) throws IOException {
    out.writeLong(offset);
    out.writeLong(length);
    out.writeInt(names.length);
    for (int i=0; i < names.length; i++) {
      Text name = new Text(names[i]);
      name.write(out);
    }
    out.writeInt(hosts.length);
    for (int i=0; i < hosts.length; i++) {
      Text host = new Text(hosts[i]);
      host.write(out);
    }
    out.writeInt(topologyPaths.length);
    for (int i=0; i < topologyPaths.length; i++) {
      Text host = new Text(topologyPaths[i]);
      host.write(out);
    }
  }
  
  /**
   * Implement readFields of Writable
   */
  public void readFields(DataInput in) throws IOException {
    this.offset = in.readLong();
    this.length = in.readLong();
    int numNames = in.readInt();
    this.names = new String[numNames];
    for (int i = 0; i < numNames; i++) {
      Text name = new Text();
      name.readFields(in);
      names[i] = name.toString();
    }
    int numHosts = in.readInt();
    for (int i = 0; i < numHosts; i++) {
      Text host = new Text();
      host.readFields(in);
      hosts[i] = host.toString();
    }
    int numTops = in.readInt();
    Text path = new Text();
    for (int i = 0; i < numTops; i++) {
      path.readFields(in);
      topologyPaths[i] = path.toString();
    }
  }
  
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(offset);
    result.append(',');
    result.append(length);
    for(String h: hosts) {
      result.append(',');
      result.append(h);
    }
    return result.toString();
  }
}
