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
package org.apache.hadoop.fs.permission;

/**
 * File system actions, e.g. read, write, etc.
 */
public enum FsAction {
  // POSIX style
  NONE("---"),
  EXECUTE("--x"),
  WRITE("-w-"),
  WRITE_EXECUTE("-wx"),
  READ("r--"),
  READ_EXECUTE("r-x"),
  READ_WRITE("rw-"),
  ALL("rwx");

  /** Retain reference to value array. */
  private final static FsAction[] vals = values();

  /** Symbolic representation */
  public final String SYMBOL;

  private FsAction(String s) {
    SYMBOL = s;
  }

  /**
   * Return true if this action implies that action.
   * @param that
   */
  public boolean implies(FsAction that) {
    if (that != null) {
      return (ordinal() & that.ordinal()) == that.ordinal();
    }
    return false;
  }

  /** AND operation. */
  public FsAction and(FsAction that) {
    return vals[ordinal() & that.ordinal()];
  }
  /** OR operation. */
  public FsAction or(FsAction that) {
    return vals[ordinal() | that.ordinal()];
  }
  /** NOT operation. */
  public FsAction not() {
    return vals[7 - ordinal()];
  }
}
