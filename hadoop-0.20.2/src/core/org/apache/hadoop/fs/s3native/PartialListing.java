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

package org.apache.hadoop.fs.s3native;

/**
 * <p>
 * Holds information on a directory listing for a
 * {@link NativeFileSystemStore}.
 * This includes the {@link FileMetadata files} and directories
 * (their names) contained in a directory.
 * </p>
 * <p>
 * This listing may be returned in chunks, so a <code>priorLastKey</code>
 * is provided so that the next chunk may be requested.
 * </p>
 * @see NativeFileSystemStore#list(String, int, String)
 */
class PartialListing {
  
  private final String priorLastKey;
  private final FileMetadata[] files;
  private final String[] commonPrefixes;
  
  public PartialListing(String priorLastKey, FileMetadata[] files,
      String[] commonPrefixes) {
    this.priorLastKey = priorLastKey;
    this.files = files;
    this.commonPrefixes = commonPrefixes;
  }

  public FileMetadata[] getFiles() {
    return files;
  }

  public String[] getCommonPrefixes() {
    return commonPrefixes;
  }

  public String getPriorLastKey() {
    return priorLastKey;
  }
  
}
