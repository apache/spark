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

package org.apache.hadoop.fs.s3;

/**
 * Holds metadata about a block of data being stored in a {@link FileSystemStore}.
 */
public class Block {
  private long id;

  private long length;

  public Block(long id, long length) {
    this.id = id;
    this.length = length;
  }

  public long getId() {
    return id;
  }

  public long getLength() {
    return length;
  }

  @Override
  public String toString() {
    return "Block[" + id + ", " + length + "]";
  }

}
