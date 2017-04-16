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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.eclipse.Activator;
import org.apache.hadoop.eclipse.ErrorMessageDialog;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.eclipse.core.resources.IStorage;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IPath;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.PlatformObject;
import org.eclipse.core.runtime.Status;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.ui.PlatformUI;

/**
 * File handling methods for the DFS
 */
public class DFSFile extends DFSPath implements DFSContent {

  protected long length;

  protected short replication;

  /**
   * Constructor to upload a file on the distributed file system
   * 
   * @param parent
   * @param path
   * @param file
   * @param monitor
   */
  public DFSFile(DFSPath parent, Path path, File file,
      IProgressMonitor monitor) {

    super(parent, path);
    this.upload(monitor, file);
  }

  public DFSFile(DFSPath parent, Path path) {
    super(parent, path);

    try {
      FileStatus fs = getDFS().getFileStatus(path);
      this.length = fs.getLen();
      this.replication = fs.getReplication();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Download and view contents of a file
   * 
   * @return a InputStream for the file
   */
  public InputStream open() throws IOException {

    return getDFS().open(this.path);
  }

  /**
   * Download this file to the local file system. This creates a download
   * status monitor.
   * 
   * @param file
   * @throws JSchException
   * @throws IOException
   * @throws InvocationTargetException
   * @throws InterruptedException
   * 
   * @deprecated
   */
  public void downloadToLocalFile(final File file)
      throws InvocationTargetException, InterruptedException {

    PlatformUI.getWorkbench().getProgressService().busyCursorWhile(
        new IRunnableWithProgress() {
          public void run(IProgressMonitor monitor)
              throws InvocationTargetException {

            DFSFile.this.downloadToLocalFile(monitor, file);
          }
        });
  }

  /* @inheritDoc */
  @Override
  public void downloadToLocalDirectory(IProgressMonitor monitor, File dir) {

    File dfsPath = new File(this.getPath().toString());
    File destination = new File(dir, dfsPath.getName());

    if (destination.exists()) {
      boolean answer =
          MessageDialog.openQuestion(null, "Overwrite existing local file?",
              "The file you are attempting to download from the DFS "
                  + this.getPath()
                  + ", already exists in your local directory as "
                  + destination + ".\n" + "Overwrite the existing file?");
      if (!answer)
        return;
    }

    try {
      this.downloadToLocalFile(monitor, destination);

    } catch (Exception e) {
      e.printStackTrace();
      MessageDialog.openWarning(null, "Download to local file system",
          "Downloading of file \"" + this.path + "\" to local directory \""
              + dir + "\" has failed.\n" + e);
    }
  }

  /**
   * Provides a detailed string for this file
   * 
   * @return the string formatted as
   *         <tt>&lt;filename&gt; (&lt;size&gt;, r&lt;replication&gt;)</tt>
   */
  public String toDetailedString() {
    final String[] units = { "b", "Kb", "Mb", "Gb", "Tb" };
    int unit = 0;
    double l = this.length;
    while ((l >= 1024.0) && (unit < units.length)) {
      unit += 1;
      l /= 1024.0;
    }

    return String.format("%s (%.1f %s, r%d)", super.toString(), l,
        units[unit], this.replication);
  }

  /* @inheritDoc */
  @Override
  public String toString() {
    return this.path.toString();
  }

  /*
   * 
   */

  /**
   * Download the DfsFile to a local file. Use the given monitor to report
   * status of operation.
   * 
   * @param monitor the status monitor
   * @param file the local file where to put the downloaded file
   * @throws InvocationTargetException
   */
  public void downloadToLocalFile(IProgressMonitor monitor, File file)
      throws InvocationTargetException {

    final int taskSize = 1024;

    monitor.setTaskName("Download file " + this.path);

    BufferedOutputStream ostream = null;
    DataInputStream istream = null;

    try {
      istream = getDFS().open(this.path);
      ostream = new BufferedOutputStream(new FileOutputStream(file));

      int bytes;
      byte[] buffer = new byte[taskSize];

      while ((bytes = istream.read(buffer)) >= 0) {
        if (monitor.isCanceled())
          return;
        ostream.write(buffer, 0, bytes);
        monitor.worked(1);
      }

    } catch (Exception e) {
      throw new InvocationTargetException(e);

    } finally {
      // Clean all opened resources
      if (istream != null) {
        try {
          istream.close();
        } catch (IOException e) {
          e.printStackTrace();
          // nothing we can do here
        }
      }
      try {
        ostream.close();
      } catch (IOException e) {
        e.printStackTrace();
        // nothing we can do here
      }
    }
  }

  /**
   * Upload a local file to this file on the distributed file system
   * 
   * @param monitor
   * @param file
   */
  public void upload(IProgressMonitor monitor, File file) {

    final int taskSize = 1024;

    monitor.setTaskName("Upload file " + this.path);

    BufferedInputStream istream = null;
    DataOutputStream ostream = null;

    try {
      istream = new BufferedInputStream(new FileInputStream(file));
      ostream = getDFS().create(this.path);

      int bytes;
      byte[] buffer = new byte[taskSize];

      while ((bytes = istream.read(buffer)) >= 0) {
        if (monitor.isCanceled())
          return;
        ostream.write(buffer, 0, bytes);
        monitor.worked(1);
      }

    } catch (Exception e) {
      ErrorMessageDialog.display(String.format(
          "Unable to uploade file %s to %s", file, this.path), e
          .getLocalizedMessage());

    } finally {
      try {
        if (istream != null)
          istream.close();
      } catch (IOException e) {
        e.printStackTrace();
        // nothing we can do here
      }
      try {
        if (ostream != null)
          ostream.close();
      } catch (IOException e) {
        e.printStackTrace();
        // nothing we can do here
      }
    }
  }

  /* @inheritDoc */
  @Override
  public void refresh() {
    getParent().refresh();
  }

  /* @inheritDoc */
  @Override
  public int computeDownloadWork() {
    return 1 + (int) (this.length / 1024);
  }

  /**
   * Creates an adapter for the file to open it in the Editor
   * 
   * @return the IStorage
   */
  public IStorage getIStorage() {
    return new IStorageAdapter();
  }

  /**
   * IStorage adapter to open the file in the Editor
   */
  private class IStorageAdapter extends PlatformObject implements IStorage {

    /* @inheritDoc */
    public InputStream getContents() throws CoreException {
      try {
        return DFSFile.this.open();

      } catch (IOException ioe) {
        throw new CoreException(new Status(Status.ERROR,
                Activator.PLUGIN_ID, 0, "Unable to open file \""
                + DFSFile.this.path + "\"", ioe));
      }
    }

    /* @inheritDoc */
    public IPath getFullPath() {
      return new org.eclipse.core.runtime.Path(DFSFile.this.path.toString());
    }

    /* @inheritDoc */
    public String getName() {
      return DFSFile.this.path.getName();
    }

    /* @inheritDoc */
    public boolean isReadOnly() {
      return true;
    }

  }

  /*
   * Implementation of DFSContent
   */

  /* @inheritDoc */
  public DFSContent[] getChildren() {
    return null;
  }

  /* @inheritDoc */
  public boolean hasChildren() {
    return false;
  }

}
