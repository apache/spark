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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/** Interface that represents the client side information for a file.
 */
public class FileStatus implements Writable, Comparable {

  private Path path;
  private long length;
  private boolean isdir;
  private short block_replication;
  private long blocksize;
  private long modification_time;
  private long access_time;
  private FsPermission permission;
  private String owner;
  private String group;
  
  public FileStatus() { this(0, false, 0, 0, 0, 0, null, null, null, null); }
  
  //We should deprecate this soon?
  public FileStatus(long length, boolean isdir, int block_replication,
                    long blocksize, long modification_time, Path path) {

    this(length, isdir, block_replication, blocksize, modification_time,
         0, null, null, null, path);
  }
  
  public FileStatus(long length, boolean isdir, int block_replication,
                    long blocksize, long modification_time, long access_time,
                    FsPermission permission, String owner, String group, 
                    Path path) {
    this.length = length;
    this.isdir = isdir;
    this.block_replication = (short)block_replication;
    this.blocksize = blocksize;
    this.modification_time = modification_time;
    this.access_time = access_time;
    this.permission = (permission == null) ? 
                      FsPermission.getDefault() : permission;
    this.owner = (owner == null) ? "" : owner;
    this.group = (group == null) ? "" : group;
    this.path = path;
  }

  /* 
   * @return the length of this file, in blocks
   */
  public long getLen() {
    return length;
  }

  /**
   * Is this a directory?
   * @return true if this is a directory
   */
  public boolean isDir() {
    return isdir;
  }

  /**
   * Get the block size of the file.
   * @return the number of bytes
   */
  public long getBlockSize() {
    return blocksize;
  }

  /**
   * Get the replication factor of a file.
   * @return the replication factor of a file.
   */
  public short getReplication() {
    return block_replication;
  }

  /**
   * Get the modification time of the file.
   * @return the modification time of file in milliseconds since January 1, 1970 UTC.
   */
  public long getModificationTime() {
    return modification_time;
  }

  /**
   * Get the access time of the file.
   * @return the access time of file in milliseconds since January 1, 1970 UTC.
   */
  public long getAccessTime() {
    return access_time;
  }

  /**
   * Get FsPermission associated with the file.
   * @return permssion. If a filesystem does not have a notion of permissions
   *         or if permissions could not be determined, then default 
   *         permissions equivalent of "rwxrwxrwx" is returned.
   */
  public FsPermission getPermission() {
    return permission;
  }
  
  /**
   * Get the owner of the file.
   * @return owner of the file. The string could be empty if there is no
   *         notion of owner of a file in a filesystem or if it could not 
   *         be determined (rare).
   */
  public String getOwner() {
    return owner;
  }
  
  /**
   * Get the group associated with the file.
   * @return group for the file. The string could be empty if there is no
   *         notion of group of a file in a filesystem or if it could not 
   *         be determined (rare).
   */
  public String getGroup() {
    return group;
  }
  
  public Path getPath() {
    return path;
  }

  /* These are provided so that these values could be loaded lazily 
   * by a filesystem (e.g. local file system).
   */
  
  /**
   * Sets permission.
   * @param permission if permission is null, default value is set
   */
  protected void setPermission(FsPermission permission) {
    this.permission = (permission == null) ? 
                      FsPermission.getDefault() : permission;
  }
  
  /**
   * Sets owner.
   * @param owner if it is null, default value is set
   */  
  protected void setOwner(String owner) {
    this.owner = (owner == null) ? "" : owner;
  }
  
  /**
   * Sets group.
   * @param group if it is null, default value is set
   */  
  protected void setGroup(String group) {
    this.group = (group == null) ? "" :  group;
  }

  //////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, getPath().toString());
    out.writeLong(length);
    out.writeBoolean(isdir);
    out.writeShort(block_replication);
    out.writeLong(blocksize);
    out.writeLong(modification_time);
    out.writeLong(access_time);
    permission.write(out);
    Text.writeString(out, owner);
    Text.writeString(out, group);
  }

  public void readFields(DataInput in) throws IOException {
    String strPath = Text.readString(in);
    this.path = new Path(strPath);
    this.length = in.readLong();
    this.isdir = in.readBoolean();
    this.block_replication = in.readShort();
    blocksize = in.readLong();
    modification_time = in.readLong();
    access_time = in.readLong();
    permission.readFields(in);
    owner = Text.readString(in);
    group = Text.readString(in);
  }

  /**
   * Compare this object to another object
   * 
   * @param   o the object to be compared.
   * @return  a negative integer, zero, or a positive integer as this object
   *   is less than, equal to, or greater than the specified object.
   * 
   * @throws ClassCastException if the specified object's is not of 
   *         type FileStatus
   */
  public int compareTo(Object o) {
    FileStatus other = (FileStatus)o;
    return this.getPath().compareTo(other.getPath());
  }
  
  /** Compare if this object is equal to another object
   * @param   o the object to be compared.
   * @return  true if two file status has the same path name; false if not.
   */
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (this == o) {
      return true;
    }
    if (!(o instanceof FileStatus)) {
      return false;
    }
    FileStatus other = (FileStatus)o;
    return this.getPath().equals(other.getPath());
  }
  
  /**
   * Returns a hash code value for the object, which is defined as
   * the hash code of the path name.
   *
   * @return  a hash code value for the path name.
   */
  public int hashCode() {
    return getPath().hashCode();
  }
}
