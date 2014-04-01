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

import org.apache.hadoop.hdfs.server.common.UpgradeObjectCollection.UOSignature;

/**
 * Abstract upgrade object.
 * 
 * Contains default implementation of common methods of {@link Upgradeable}
 * interface.
 */
public abstract class UpgradeObject implements Upgradeable {
  protected short status;
  
  public short getUpgradeStatus() {
    return status;
  }

  public String getDescription() {
    return "Upgrade object for " + getType() + " layout version " + getVersion();
  }

  public UpgradeStatusReport getUpgradeStatusReport(boolean details) 
                                                    throws IOException {
    return new UpgradeStatusReport(getVersion(), getUpgradeStatus(), false);
  }

  public int compareTo(Upgradeable o) {
    if(this.getVersion() != o.getVersion())
      return (getVersion() > o.getVersion() ? -1 : 1);
    int res = this.getType().toString().compareTo(o.getType().toString());
    if(res != 0)
      return res;
    return getClass().getCanonicalName().compareTo(
                    o.getClass().getCanonicalName());
  }

  public boolean equals(Object o) {
    if (!(o instanceof UpgradeObject)) {
      return false;
    }
    return this.compareTo((UpgradeObject)o) == 0;
  }

  public int hashCode() {
    return new UOSignature(this).hashCode(); 
  }
}
