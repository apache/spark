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

package org.apache.hadoop.hdfs.server.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.util.VersionInfo;

/**
 * NamespaceInfo is returned by the name-node in reply 
 * to a data-node handshake.
 * 
 */
public class NamespaceInfo extends StorageInfo implements Writable {
  String revision;
  String version;
  int distributedUpgradeVersion;

  public NamespaceInfo() {
    super();
  }
  
  public NamespaceInfo(int nsID, long cT, int duVersion) {
    super(FSConstants.LAYOUT_VERSION, nsID, cT); 
    version = VersionInfo.getVersion();
    revision = VersionInfo.getRevision();
    this.distributedUpgradeVersion = duVersion;
  }
  
  public String getVersion() {
    return version;
  }

  public String getRevision() {
    return revision;
  }

  public int getDistributedUpgradeVersion() {
    return distributedUpgradeVersion;
  }
  
  /////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////
  static {                                      // register a ctor
    WritableFactories.setFactory
      (NamespaceInfo.class,
       new WritableFactory() {
         public Writable newInstance() { return new NamespaceInfo(); }
       });
  }

  public void write(DataOutput out) throws IOException {
    UTF8.writeString(out, getVersion());
    UTF8.writeString(out, getRevision());
    out.writeInt(getLayoutVersion());
    out.writeInt(getNamespaceID());
    out.writeLong(getCTime());
    out.writeInt(getDistributedUpgradeVersion());
  }

  public void readFields(DataInput in) throws IOException {
    version = UTF8.readString(in);
    revision = UTF8.readString(in);
    layoutVersion = in.readInt();
    namespaceID = in.readInt();
    cTime = in.readLong();
    distributedUpgradeVersion = in.readInt();
  }
}
