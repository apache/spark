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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.FSConstants;

/**
 * LeaseManager does the lease housekeeping for writing on files.   
 * This class also provides useful static methods for lease recovery.
 * 
 * Lease Recovery Algorithm
 * 1) Namenode retrieves lease information
 * 2) For each file f in the lease, consider the last block b of f
 * 2.1) Get the datanodes which contains b
 * 2.2) Assign one of the datanodes as the primary datanode p

 * 2.3) p obtains a new generation stamp form the namenode
 * 2.4) p get the block info from each datanode
 * 2.5) p computes the minimum block length
 * 2.6) p updates the datanodes, which have a valid generation stamp,
 *      with the new generation stamp and the minimum block length 
 * 2.7) p acknowledges the namenode the update results

 * 2.8) Namenode updates the BlockInfo
 * 2.9) Namenode removes f from the lease
 *      and removes the lease once all files have been removed
 * 2.10) Namenode commit changes to edit log
 */
public class LeaseManager {
  public static final Log LOG = LogFactory.getLog(LeaseManager.class);

  private final FSNamesystem fsnamesystem;

  private long softLimit = FSConstants.LEASE_SOFTLIMIT_PERIOD;
  private long hardLimit = FSConstants.LEASE_HARDLIMIT_PERIOD;

  //
  // Used for handling lock-leases
  // Mapping: leaseHolder -> Lease
  //
  private SortedMap<String, Lease> leases = new TreeMap<String, Lease>();
  // Set of: Lease
  private SortedSet<Lease> sortedLeases = new TreeSet<Lease>();

  // 
  // Map path names to leases. It is protected by the sortedLeases lock.
  // The map stores pathnames in lexicographical order.
  //
  private SortedMap<String, Lease> sortedLeasesByPath = new TreeMap<String, Lease>();

  LeaseManager(FSNamesystem fsnamesystem) {this.fsnamesystem = fsnamesystem;}

  Lease getLease(String holder) {
    return leases.get(holder);
  }
  
  SortedSet<Lease> getSortedLeases() {return sortedLeases;}

  /** @return the lease containing src */
  public Lease getLeaseByPath(String src) {return sortedLeasesByPath.get(src);}

  /** @return the number of leases currently in the system */
  public synchronized int countLease() {return sortedLeases.size();}

  /** @return the number of paths contained in all leases */
  synchronized int countPath() {
    int count = 0;
    for(Lease lease : sortedLeases) {
      count += lease.getPaths().size();
    }
    return count;
  }
  
  /**
   * Adds (or re-adds) the lease for the specified file.
   */
  synchronized Lease addLease(String holder, String src) {
    Lease lease = getLease(holder);
    if (lease == null) {
      lease = new Lease(holder);
      leases.put(holder, lease);
      sortedLeases.add(lease);
    } else {
      renewLease(lease);
    }
    sortedLeasesByPath.put(src, lease);
    lease.paths.add(src);
    return lease;
  }

  /**
   * Remove the specified lease and src.
   */
  synchronized void removeLease(Lease lease, String src) {
    sortedLeasesByPath.remove(src);
    if (!lease.removePath(src)) {
      LOG.error(src + " not found in lease.paths (=" + lease.paths + ")");
    }

    if (!lease.hasPath()) {
      leases.remove(lease.holder);
      if (!sortedLeases.remove(lease)) {
        LOG.error(lease + " not found in sortedLeases");
      }
    }
  }

  /**
   * Reassign lease for file src to the new holder.
   */
  synchronized Lease reassignLease(Lease lease, String src, String newHolder) {
    assert newHolder != null : "new lease holder is null";
    if (lease != null) {
      removeLease(lease, src);
    }
    return addLease(newHolder, src);
  }

  /**
   * Remove the lease for the specified holder and src
   */
  synchronized void removeLease(String holder, String src) {
    Lease lease = getLease(holder);
    if (lease != null) {
      removeLease(lease, src);
    }
  }

  /**
   * Finds the pathname for the specified pendingFile
   */
  synchronized String findPath(INodeFileUnderConstruction pendingFile
      ) throws IOException {
    Lease lease = getLease(pendingFile.clientName);
    if (lease != null) {
      String src = lease.findPath(pendingFile);
      if (src != null) {
        return src;
      }
    }
    throw new IOException("pendingFile (=" + pendingFile + ") not found."
        + "(lease=" + lease + ")");
  }

  /**
   * Renew the lease(s) held by the given client
   */
  synchronized void renewLease(String holder) {
    renewLease(getLease(holder));
  }
  synchronized void renewLease(Lease lease) {
    if (lease != null) {
      sortedLeases.remove(lease);
      lease.renew();
      sortedLeases.add(lease);
    }
  }

  /************************************************************
   * A Lease governs all the locks held by a single client.
   * For each client there's a corresponding lease, whose
   * timestamp is updated when the client periodically
   * checks in.  If the client dies and allows its lease to
   * expire, all the corresponding locks can be released.
   *************************************************************/
  class Lease implements Comparable<Lease> {
    private final String holder;
    private long lastUpdate;
    private final Collection<String> paths = new TreeSet<String>();
  
    /** Only LeaseManager object can create a lease */
    private Lease(String holder) {
      this.holder = holder;
      renew();
    }

    /** Get the holder of the lease */
    public String getHolder() {
      return holder;
    }

    /** Only LeaseManager object can renew a lease */
    private void renew() {
      this.lastUpdate = FSNamesystem.now();
    }

    /** @return true if the Hard Limit Timer has expired */
    public boolean expiredHardLimit() {
      return FSNamesystem.now() - lastUpdate > hardLimit;
    }

    /** @return true if the Soft Limit Timer has expired */
    public boolean expiredSoftLimit() {
      return FSNamesystem.now() - lastUpdate > softLimit;
    }

    /**
     * @return the path associated with the pendingFile and null if not found.
     */
    private String findPath(INodeFileUnderConstruction pendingFile) {
      for(String src : paths) {
        if (fsnamesystem.dir.getFileINode(src) == pendingFile) {
          return src;
        }
      }
      return null;
    }

    /** Does this lease contain any path? */
    boolean hasPath() {return !paths.isEmpty();}

    boolean removePath(String src) {
      return paths.remove(src);
    }

    /** {@inheritDoc} */
    public String toString() {
      return "[Lease.  Holder: " + holder
          + ", pendingcreates: " + paths.size() + "]";
    }
  
    /** {@inheritDoc} */
    public int compareTo(Lease o) {
      Lease l1 = this;
      Lease l2 = o;
      long lu1 = l1.lastUpdate;
      long lu2 = l2.lastUpdate;
      if (lu1 < lu2) {
        return -1;
      } else if (lu1 > lu2) {
        return 1;
      } else {
        return l1.holder.compareTo(l2.holder);
      }
    }
  
    /** {@inheritDoc} */
    public boolean equals(Object o) {
      if (!(o instanceof Lease)) {
        return false;
      }
      Lease obj = (Lease) o;
      if (lastUpdate == obj.lastUpdate &&
          holder.equals(obj.holder)) {
        return true;
      }
      return false;
    }
  
    /** {@inheritDoc} */
    public int hashCode() {
      return holder.hashCode();
    }
    
    Collection<String> getPaths() {
      return paths;
    }
    
    void replacePath(String oldpath, String newpath) {
      paths.remove(oldpath);
      paths.add(newpath);
    }
  }

  synchronized void changeLease(String src, String dst,
      String overwrite, String replaceBy) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(getClass().getSimpleName() + ".changelease: " +
               " src=" + src + ", dest=" + dst + 
               ", overwrite=" + overwrite +
               ", replaceBy=" + replaceBy);
    }

    final int len = overwrite.length();
    for(Map.Entry<String, Lease> entry : findLeaseWithPrefixPath(src, sortedLeasesByPath)) {
      final String oldpath = entry.getKey();
      final Lease lease = entry.getValue();
      //overwrite must be a prefix of oldpath
      final String newpath = replaceBy + oldpath.substring(len);
      if (LOG.isDebugEnabled()) {
        LOG.debug("changeLease: replacing " + oldpath + " with " + newpath);
      }
      lease.replacePath(oldpath, newpath);
      sortedLeasesByPath.remove(oldpath);
      sortedLeasesByPath.put(newpath, lease);
    }
  }

  synchronized void removeLeaseWithPrefixPath(String prefix) {
    for(Map.Entry<String, Lease> entry : findLeaseWithPrefixPath(prefix, sortedLeasesByPath)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(LeaseManager.class.getSimpleName()
            + ".removeLeaseWithPrefixPath: entry=" + entry);
      }
      removeLease(entry.getValue(), entry.getKey());    
    }
  }

  static private List<Map.Entry<String, Lease>> findLeaseWithPrefixPath(
      String prefix, SortedMap<String, Lease> path2lease) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(LeaseManager.class.getSimpleName() + ".findLease: prefix=" + prefix);
    }

    List<Map.Entry<String, Lease>> entries = new ArrayList<Map.Entry<String, Lease>>();
    final int srclen = prefix.length();

    for(Map.Entry<String, Lease> entry : path2lease.tailMap(prefix).entrySet()) {
      final String p = entry.getKey();
      if (!p.startsWith(prefix)) {
        return entries;
      }
      if (p.length() == srclen || p.charAt(srclen) == Path.SEPARATOR_CHAR) {
        entries.add(entry);
      }
    }
    return entries;
  }

  public void setLeasePeriod(long softLimit, long hardLimit) {
    this.softLimit = softLimit;
    this.hardLimit = hardLimit; 
  }
  
  /******************************************************
   * Monitor checks for leases that have expired,
   * and disposes of them.
   ******************************************************/
  class Monitor implements Runnable {
    final String name = getClass().getSimpleName();

    /** Check leases periodically. */
    public void run() {
      for(; fsnamesystem.isRunning(); ) {
        synchronized(fsnamesystem) {
          checkLeases();
        }

        try {
          Thread.sleep(2000);
        } catch(InterruptedException ie) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(name + " is interrupted", ie);
          }
        }
      }
    }
  }

  /** Check the leases beginning from the oldest. */
  synchronized void checkLeases() {
    for(; sortedLeases.size() > 0; ) {
      final Lease oldest = sortedLeases.first();
      if (!oldest.expiredHardLimit()) {
        return;
      }

      LOG.info("Lease " + oldest + " has expired hard limit");

      final List<String> removing = new ArrayList<String>();
      // need to create a copy of the oldest lease paths, becuase 
      // internalReleaseLease() removes paths corresponding to empty files,
      // i.e. it needs to modify the collection being iterated over
      // causing ConcurrentModificationException
      String[] leasePaths = new String[oldest.getPaths().size()];
      oldest.getPaths().toArray(leasePaths);
      for(String p : leasePaths) {
        try {
          fsnamesystem.internalReleaseLeaseOne(oldest, p);
        } catch (IOException e) {
          LOG.error("Cannot release the path "+p+" in the lease "+oldest, e);
          removing.add(p);
        }
      }

      for(String p : removing) {
        removeLease(oldest, p);
      }
    }
  }

  /** {@inheritDoc} */
  public synchronized String toString() {
    return getClass().getSimpleName() + "= {"
        + "\n leases=" + leases
        + "\n sortedLeases=" + sortedLeases
        + "\n sortedLeasesByPath=" + sortedLeasesByPath
        + "\n}";
  }
}
