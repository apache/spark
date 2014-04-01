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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.eclipse.server.HadoopServer;
import org.apache.hadoop.eclipse.servers.IHadoopServerListener;
import org.apache.hadoop.eclipse.servers.ServerRegistry;
import org.apache.hadoop.fs.FileSystem;

/**
 * Representation of the root element containing all DFS servers. This
 * content registers an observer on Hadoop servers so as to update itself
 * when servers are updated.
 */
public class DFSLocationsRoot implements DFSContent, IHadoopServerListener {

  /**
   * 
   */
  private final DFSContentProvider provider;

  private Map<HadoopServer, DFSLocation> map =
      new HashMap<HadoopServer, DFSLocation>();

  /**
   * Register a listeners to track DFS locations updates
   * 
   * @param provider the content provider this content is the root of
   */
  DFSLocationsRoot(DFSContentProvider provider) {
    this.provider = provider;
    ServerRegistry.getInstance().addListener(this);
    this.refresh();
  }

  /*
   * Implementation of IHadoopServerListener
   */

  /* @inheritDoc */
  public synchronized void serverChanged(final HadoopServer location,
      final int type) {

    switch (type) {
      case ServerRegistry.SERVER_STATE_CHANGED: {
        this.provider.refresh(map.get(location));
        break;
      }

      case ServerRegistry.SERVER_ADDED: {
        DFSLocation dfsLoc = new DFSLocation(provider, location);
        map.put(location, dfsLoc);
        this.provider.refresh(this);
        break;
      }

      case ServerRegistry.SERVER_REMOVED: {
        map.remove(location);
        this.provider.refresh(this);
        break;
      }
    }
  }

  /**
   * Recompute the map of Hadoop locations
   */
  private synchronized void reloadLocations() {
    map.clear();
    for (HadoopServer location : ServerRegistry.getInstance().getServers())
      map.put(location, new DFSLocation(provider, location));
  }

  /* @inheritDoc */
  @Override
  public String toString() {
    return "DFS Locations";
  }

  /*
   * Implementation of DFSContent
   */

  /* @inheritDoc */
  public synchronized DFSContent[] getChildren() {
    return this.map.values().toArray(new DFSContent[this.map.size()]);
  }

  /* @inheritDoc */
  public boolean hasChildren() {
    return (this.map.size() > 0);
  }

  /* @inheritDoc */
  public void refresh() {
    reloadLocations();
    this.provider.refresh(this);
  }

  /*
   * Actions
   */

  public void disconnect() {
    Thread closeThread = new Thread() {
      /* @inheritDoc */
      @Override
      public void run() {
        try {
          System.out.printf("Closing all opened File Systems...\n");
          FileSystem.closeAll();
          System.out.printf("File Systems closed\n");

        } catch (IOException ioe) {
          ioe.printStackTrace();
        }
      }
    };

    // Wait 5 seconds for the connections to be closed
    closeThread.start();
    try {
      closeThread.join(5000);

    } catch (InterruptedException ie) {
      // Ignore
    }
  }

}
