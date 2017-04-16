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

import java.io.IOException;

import org.apache.hadoop.eclipse.server.HadoopServer;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.core.runtime.Status;
import org.eclipse.core.runtime.jobs.Job;

/**
 * DFS Content representation of a HDFS location
 */
public class DFSLocation implements DFSContent {

  private final DFSContentProvider provider;

  private final HadoopServer location;

  private DFSContent rootFolder = null;

  DFSLocation(DFSContentProvider provider, HadoopServer server) {
    this.provider = provider;
    this.location = server;
  }

  /* @inheritDoc */
  @Override
  public String toString() {
    return this.location.getLocationName();
  }

  /*
   * Implementation of DFSContent
   */

  /* @inheritDoc */
  public DFSContent[] getChildren() {
    if (this.rootFolder == null) {
      /*
       * DfsFolder constructor might block as it contacts the NameNode: work
       * asynchronously here or this will potentially freeze the UI
       */
      new Job("Connecting to DFS " + location) {
        @Override
        protected IStatus run(IProgressMonitor monitor) {
          try {
            rootFolder = new DFSFolder(provider, location);
            return Status.OK_STATUS;

          } catch (IOException ioe) {
            rootFolder =
                new DFSMessage("Error: " + ioe.getLocalizedMessage());
            return Status.CANCEL_STATUS;

          } finally {
            // Under all circumstances, update the UI
            provider.refresh(DFSLocation.this);
          }
        }
      }.schedule();

      return new DFSContent[] { new DFSMessage("Connecting to DFS "
          + toString()) };
    }
    return new DFSContent[] { this.rootFolder };
  }

  /* @inheritDoc */
  public boolean hasChildren() {
    return true;
  }
  
  /* @inheritDoc */
  public void refresh() {
    this.rootFolder = null;
    this.provider.refresh(this);
  }

  /*
   * Actions
   */
  
  /**
   * Refresh the location using a new connection
   */
  public void reconnect() {
    this.refresh();
  }
}
