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

package org.apache.hadoop.eclipse.dfs;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.eclipse.ErrorMessageDialog;
import org.apache.hadoop.eclipse.server.ConfProp;
import org.apache.hadoop.eclipse.server.HadoopServer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.dialogs.MessageDialog;

/**
 * DFS Path handling for DFS
 */
public abstract class DFSPath implements DFSContent {

  protected final DFSContentProvider provider;

  protected HadoopServer location;

  private DistributedFileSystem dfs = null;

  protected final Path path;

  protected final DFSPath parent;

  /**
   * For debugging purpose
   */
  static Logger log = Logger.getLogger(DFSPath.class.getName());

  /**
   * Create a path representation for the given location in the given viewer
   * 
   * @param location
   * @param path
   * @param viewer
   */
  public DFSPath(DFSContentProvider provider, HadoopServer location)
      throws IOException {

    this.provider = provider;
    this.location = location;
    this.path = new Path("/");
    this.parent = null;
  }

  /**
   * Create a sub-path representation for the given parent path
   * 
   * @param parent
   * @param path
   */
  protected DFSPath(DFSPath parent, Path path) {
    this.provider = parent.provider;
    this.location = parent.location;
    this.dfs = parent.dfs;
    this.parent = parent;
    this.path = path;
  }

  protected void dispose() {
    // Free the DFS connection
  }

  /* @inheritDoc */
  @Override
  public String toString() {
    if (path.equals("/")) {
      return location.getConfProp(ConfProp.FS_DEFAULT_URI);

    } else {
      return this.path.getName();
    }
  }

  /**
   * Does a recursive delete of the remote directory tree at this node.
   */
  public void delete() {
    try {
      getDFS().delete(this.path, true);

    } catch (IOException e) {
      e.printStackTrace();
      MessageDialog.openWarning(null, "Delete file",
          "Unable to delete file \"" + this.path + "\"\n" + e);
    }
  }

  public DFSPath getParent() {
    return parent;
  }

  public abstract void refresh();

  /**
   * Refresh the UI element for this content
   */
  public void doRefresh() {
    provider.refresh(this);
  }

  /**
   * Copy the DfsPath to the given local directory
   * 
   * @param directory the local directory
   */
  public abstract void downloadToLocalDirectory(IProgressMonitor monitor,
      File dir);

  public Path getPath() {
    return this.path;
  }

  /**
   * Gets a connection to the DFS
   * 
   * @return a connection to the DFS
   * @throws IOException
   */
  DistributedFileSystem getDFS() throws IOException {
    if (this.dfs == null) {
      FileSystem fs = location.getDFS();
      if (!(fs instanceof DistributedFileSystem)) {
        ErrorMessageDialog.display("DFS Browser",
            "The DFS Browser cannot browse anything else "
                + "but a Distributed File System!");
        throw new IOException("DFS Browser expects a DistributedFileSystem!");
      }
      this.dfs = (DistributedFileSystem) fs;
    }
    return this.dfs;
  }

  public abstract int computeDownloadWork();

}
