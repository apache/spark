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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import org.apache.hadoop.thirdparty.guava.common.annotations.VisibleForTesting;

/**
 * 
 * NameNodeResourceChecker provides a method -
 * <code>hasAvailableDiskSpace</code> - which will return true if and only if
 * the NameNode has disk space available on all volumes which are configured to
 * be checked. Volumes containing file system name/edits dirs are added by
 * default, and arbitrary extra volumes may be configured as well.
 */
public class NameNodeResourceChecker {
  private static final Log LOG = LogFactory.getLog(NameNodeResourceChecker.class.getName());

  // Space (in bytes) reserved per volume.
  private long duReserved;

  private final Configuration conf;
  private Map<String, DF> volumes;

  /**
   * Create a NameNodeResourceChecker, which will check the name dirs and edits
   * dirs set in <code>conf</code>.
   * 
   * @param conf
   * @throws IOException
   */
  public NameNodeResourceChecker(Configuration conf) throws IOException {
    this.conf = conf;
    volumes = new HashMap<String, DF>();

    duReserved = conf.getLong(DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_KEY,
        DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_DEFAULT);

    Collection<File> extraCheckedVolumes = new ArrayList<File>();
    
    for (String filePath : conf.getTrimmedStringCollection(
        DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_KEY)) {
      extraCheckedVolumes.add(new File(filePath));
    }

    addDirsToCheck(FSNamesystem.getNamespaceDirs(conf));
    addDirsToCheck(FSNamesystem.getNamespaceEditsDirs(conf));
    addDirsToCheck(extraCheckedVolumes);
  }

  /**
   * Add the passed-in directories to the list of volumes to check.
   * 
   * @param directoriesToCheck
   *          The directories whose volumes will be checked for available space.
   * @throws IOException
   */
  private void addDirsToCheck(Collection<File> directoriesToCheck)
      throws IOException {
    for (File directory : directoriesToCheck) {
      File dir = new File(directory.toURI().getPath());
      if (!dir.exists()) {
        throw new IOException("Missing directory "+dir.getAbsolutePath());
      }
      DF df = new DF(dir, conf);
      volumes.put(df.getFilesystem(), df);
    }
  }

  /**
   * Return true if disk space is available on at least one of the configured
   * volumes.
   * 
   * @return True if the configured amount of disk space is available on at
   *         least one volume, false otherwise.
   * @throws IOException
   */
  boolean hasAvailableDiskSpace()
      throws IOException {
    return getVolumesLowOnSpace().size() < volumes.size();
  }

  /**
   * Return the set of directories which are low on space.
   * @return the set of directories whose free space is below the threshold.
   * @throws IOException 
   */
  Collection<String> getVolumesLowOnSpace() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Going to check the following volumes disk space: " + volumes);
    }
    Collection<String> lowVolumes = new ArrayList<String>();
    for (DF volume : volumes.values()) {
      long availableSpace = volume.getAvailable();
      String fileSystem = volume.getFilesystem();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Space available on volume '" + fileSystem + "' is " + availableSpace);
      }
      if (availableSpace < duReserved) {
        LOG.warn("Space available on volume '" + fileSystem + "' is "
            + availableSpace +
            ", which is below the configured reserved amount " + duReserved);
        lowVolumes.add(volume.getFilesystem());
      }
    }
    return lowVolumes;
  }

  @VisibleForTesting
  void setVolumes(Map<String, DF> volumes) {
    this.volumes = volumes;
  }
}
