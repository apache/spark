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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

class CleanupQueue {

  public static final Log LOG =
    LogFactory.getLog(CleanupQueue.class);

  private static final PathCleanupThread cleanupThread =
    new PathCleanupThread();
  private static final CleanupQueue inst = new CleanupQueue();

  public static CleanupQueue getInstance() { return inst; }

  /**
   * Create a singleton path-clean-up queue. It can be used to delete
   * paths(directories/files) in a separate thread. This constructor creates a
   * clean-up thread and also starts it as a daemon. Callers can instantiate one
   * CleanupQueue per JVM and can use it for deleting paths. Use
   * {@link CleanupQueue#addToQueue(PathDeletionContext...)} to add paths for
   * deletion.
   */
  protected CleanupQueue() { }
  
  /**
   * Contains info related to the path of the file/dir to be deleted
   */
  static class PathDeletionContext {
    final Path fullPath;// full path of file or dir
    final Configuration conf;

    public PathDeletionContext(Path fullPath, Configuration conf) {
      this.fullPath = fullPath;
      this.conf = conf;
    }
    
    protected Path getPathForCleanup() {
      return fullPath;
    }

    /**
     * Deletes the path (and its subdirectories recursively)
     * @throws IOException, InterruptedException 
     */
    protected void deletePath() throws IOException, InterruptedException {
      final Path p = getPathForCleanup();
      UserGroupInformation.getLoginUser().doAs(
          new PrivilegedExceptionAction<Object>() {
            public Object run() throws IOException {
             p.getFileSystem(conf).delete(p, true);
             return null;
            }
          });
    }

    @Override
    public String toString() {
      final Path p = getPathForCleanup();
      return (null == p) ? "undefined" : p.toString();
    }
  }

  /**
   * Adds the paths to the queue of paths to be deleted by cleanupThread.
   */
  public void addToQueue(PathDeletionContext... contexts) {
    cleanupThread.addToQueue(contexts);
  }

  // currently used by tests only
  protected boolean isQueueEmpty() {
    return (cleanupThread.queue.size() == 0);
  }

  private static class PathCleanupThread extends Thread {

    // cleanup queue which deletes files/directories of the paths queued up.
    private LinkedBlockingQueue<PathDeletionContext> queue =
      new LinkedBlockingQueue<PathDeletionContext>();

    public PathCleanupThread() {
      setName("Directory/File cleanup thread");
      setDaemon(true);
      start();
    }

    void addToQueue(PathDeletionContext[] contexts) {
      for (PathDeletionContext context : contexts) {
        try {
          queue.put(context);
        } catch(InterruptedException ie) {}
      }
    }

    public void run() {
      if (LOG.isDebugEnabled()) {
        LOG.debug(getName() + " started.");
      }
      PathDeletionContext context = null;
      while (true) {
        try {
          context = queue.take();
          context.deletePath();
          // delete the path.
          if (LOG.isDebugEnabled()) {
            LOG.debug("DELETED " + context);
          }
        } catch (InterruptedException t) {
          LOG.warn("Interrupted deletion of " + context);
          return;
        } catch (Throwable e) {
          LOG.warn("Error deleting path " + context, e);
        } 
      }
    }
  }
}
