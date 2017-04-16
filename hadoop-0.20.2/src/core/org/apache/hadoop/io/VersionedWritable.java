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

package org.apache.hadoop.io;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

/** A base class for Writables that provides version checking.
 *
 * <p>This is useful when a class may evolve, so that instances written by the
 * old version of the class may still be processed by the new version.  To
 * handle this situation, {@link #readFields(DataInput)}
 * implementations should catch {@link VersionMismatchException}.
 */
public abstract class VersionedWritable implements Writable {

  /** Return the version number of the current implementation. */
  public abstract byte getVersion();
    
  // javadoc from Writable
  public void write(DataOutput out) throws IOException {
    out.writeByte(getVersion());                  // store version
  }

  // javadoc from Writable
  public void readFields(DataInput in) throws IOException {
    byte version = in.readByte();                 // read version
    if (version != getVersion())
      throw new VersionMismatchException(getVersion(), version);
  }

    
}
