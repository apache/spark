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

package org.apache.hadoop.eclipse.actions;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.eclipse.ImageLibrary;
import org.apache.hadoop.eclipse.dfs.DFSActions;
import org.apache.hadoop.eclipse.dfs.DFSFile;
import org.apache.hadoop.eclipse.dfs.DFSFolder;
import org.apache.hadoop.eclipse.dfs.DFSLocation;
import org.apache.hadoop.eclipse.dfs.DFSLocationsRoot;
import org.apache.hadoop.eclipse.dfs.DFSPath;
import org.eclipse.core.resources.IStorage;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.PlatformObject;
import org.eclipse.jface.action.IAction;
import org.eclipse.jface.dialogs.InputDialog;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.jface.viewers.ISelection;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.ui.IObjectActionDelegate;
import org.eclipse.ui.IPersistableElement;
import org.eclipse.ui.IStorageEditorInput;
import org.eclipse.ui.IWorkbenchPart;
import org.eclipse.ui.PartInitException;
import org.eclipse.ui.PlatformUI;

/**
 * Actual implementation of DFS actions
 */
public class DFSActionImpl implements IObjectActionDelegate {

  private ISelection selection;

  private IWorkbenchPart targetPart;

  /* @inheritDoc */
  public void setActivePart(IAction action, IWorkbenchPart targetPart) {
    this.targetPart = targetPart;
  }

  /* @inheritDoc */
  public void run(IAction action) {

    // Ignore non structured selections
    if (!(this.selection instanceof IStructuredSelection))
      return;

    // operate on the DFS asynchronously to prevent blocking the main UI
    final IStructuredSelection ss = (IStructuredSelection) selection;
    final String actionId = action.getActionDefinitionId();
    Display.getDefault().asyncExec(new Runnable() {
      public void run() {
        try {
          switch (DFSActions.getById(actionId)) {
            case DELETE: {
              delete(ss);
              break;
            }
            case OPEN: {
              open(ss);
              break;
            }
            case MKDIR: {
              mkdir(ss);
              break;
            }
            case UPLOAD_FILES: {
              uploadFilesToDFS(ss);
              break;
            }
            case UPLOAD_DIR: {
              uploadDirectoryToDFS(ss);
              break;
            }
            case REFRESH: {
              refresh(ss);
              break;
            }
            case DOWNLOAD: {
              downloadFromDFS(ss);
              break;
            }
            case RECONNECT: {
              reconnect(ss);
              break;
            }
            case DISCONNECT: {
              disconnect(ss);
              break;
            }
            default: {
              System.err.printf("Unhandled DFS Action: " + actionId);
              break;
            }
          }

        } catch (Exception e) {
          e.printStackTrace();
          MessageDialog.openError(Display.getDefault().getActiveShell(),
              "DFS Action error",
              "An error occurred while performing DFS operation: "
                  + e.getMessage());
        }
      }
    });
  }

  /**
   * Create a new sub-folder into an existing directory
   * 
   * @param selection
   */
  private void mkdir(IStructuredSelection selection) {
    List<DFSFolder> folders = filterSelection(DFSFolder.class, selection);
    if (folders.size() >= 1) {
      DFSFolder folder = folders.get(0);
      InputDialog dialog =
          new InputDialog(Display.getCurrent().getActiveShell(),
              "Create subfolder", "Enter the name of the subfolder", "",
              null);
      if (dialog.open() == InputDialog.OK)
        folder.mkdir(dialog.getValue());
    }
  }

  /**
   * Implement the import action (upload files from the current machine to
   * HDFS)
   * 
   * @param object
   * @throws SftpException
   * @throws JSchException
   * @throws InvocationTargetException
   * @throws InterruptedException
   */
  private void uploadFilesToDFS(IStructuredSelection selection)
      throws InvocationTargetException, InterruptedException {

    // Ask the user which files to upload
    FileDialog dialog =
        new FileDialog(Display.getCurrent().getActiveShell(), SWT.OPEN
            | SWT.MULTI);
    dialog.setText("Select the local files to upload");
    dialog.open();

    List<File> files = new ArrayList<File>();
    for (String fname : dialog.getFileNames())
      files.add(new File(dialog.getFilterPath() + File.separator + fname));

    // TODO enable upload command only when selection is exactly one folder
    List<DFSFolder> folders = filterSelection(DFSFolder.class, selection);
    if (folders.size() >= 1)
      uploadToDFS(folders.get(0), files);
  }

  /**
   * Implement the import action (upload directory from the current machine
   * to HDFS)
   * 
   * @param object
   * @throws SftpException
   * @throws JSchException
   * @throws InvocationTargetException
   * @throws InterruptedException
   */
  private void uploadDirectoryToDFS(IStructuredSelection selection)
      throws InvocationTargetException, InterruptedException {

    // Ask the user which local directory to upload
    DirectoryDialog dialog =
        new DirectoryDialog(Display.getCurrent().getActiveShell(), SWT.OPEN
            | SWT.MULTI);
    dialog.setText("Select the local file or directory to upload");

    String dirName = dialog.open();
    final File dir = new File(dirName);
    List<File> files = new ArrayList<File>();
    files.add(dir);

    // TODO enable upload command only when selection is exactly one folder
    final List<DFSFolder> folders =
        filterSelection(DFSFolder.class, selection);
    if (folders.size() >= 1)
      uploadToDFS(folders.get(0), files);

  }

  private void uploadToDFS(final DFSFolder folder, final List<File> files)
      throws InvocationTargetException, InterruptedException {

    PlatformUI.getWorkbench().getProgressService().busyCursorWhile(
        new IRunnableWithProgress() {
          public void run(IProgressMonitor monitor)
              throws InvocationTargetException {

            int work = 0;
            for (File file : files)
              work += computeUploadWork(file);

            monitor.beginTask("Uploading files to distributed file system",
                work);

            for (File file : files) {
              try {
                folder.upload(monitor, file);

              } catch (IOException ioe) {
                ioe.printStackTrace();
                MessageDialog.openError(null,
                    "Upload files to distributed file system",
                    "Upload failed.\n" + ioe);
              }
            }

            monitor.done();

            // Update the UI
            folder.doRefresh();
          }
        });
  }

  private void reconnect(IStructuredSelection selection) {
    for (DFSLocation location : filterSelection(DFSLocation.class, selection))
      location.reconnect();
  }

  private void disconnect(IStructuredSelection selection) {
    if (selection.size() != 1)
      return;

    Object first = selection.getFirstElement();
    if (!(first instanceof DFSLocationsRoot))
      return;

    DFSLocationsRoot root = (DFSLocationsRoot) first;
    root.disconnect();
    root.refresh();
  }

  /**
   * Implements the Download action from HDFS to the current machine
   * 
   * @param object
   * @throws SftpException
   * @throws JSchException
   * @throws InterruptedException
   * @throws InvocationTargetException
   */
  private void downloadFromDFS(IStructuredSelection selection)
      throws InvocationTargetException, InterruptedException {

    // Ask the user where to put the downloaded files
    DirectoryDialog dialog =
        new DirectoryDialog(Display.getCurrent().getActiveShell());
    dialog.setText("Copy to local directory");
    dialog.setMessage("Copy the selected files and directories from the "
        + "distributed filesystem to a local directory");
    String directory = dialog.open();

    if (directory == null)
      return;

    final File dir = new File(directory);
    if (!dir.exists())
      dir.mkdirs();

    if (!dir.isDirectory()) {
      MessageDialog.openError(null, "Download to local file system",
          "Invalid directory location: \"" + dir + "\"");
      return;
    }

    final List<DFSPath> paths = filterSelection(DFSPath.class, selection);

    PlatformUI.getWorkbench().getProgressService().busyCursorWhile(
        new IRunnableWithProgress() {
          public void run(IProgressMonitor monitor)
              throws InvocationTargetException {

            int work = 0;
            for (DFSPath path : paths)
              work += path.computeDownloadWork();

            monitor
                .beginTask("Downloading files to local file system", work);

            for (DFSPath path : paths) {
              if (monitor.isCanceled())
                return;
              try {
                path.downloadToLocalDirectory(monitor, dir);
              } catch (Exception e) {
                // nothing we want to do here
                e.printStackTrace();
              }
            }

            monitor.done();
          }
        });
  }

  /**
   * Open the selected DfsPath in the editor window
   * 
   * @param selection
   * @throws JSchException
   * @throws IOException
   * @throws PartInitException
   * @throws InvocationTargetException
   * @throws InterruptedException
   */
  private void open(IStructuredSelection selection) throws IOException,
      PartInitException, InvocationTargetException, InterruptedException {

    for (DFSFile file : filterSelection(DFSFile.class, selection)) {

      IStorageEditorInput editorInput = new DFSFileEditorInput(file);
      targetPart.getSite().getWorkbenchWindow().getActivePage().openEditor(
          editorInput, "org.eclipse.ui.DefaultTextEditor");
    }
  }

  /**
   * @param selection
   * @throws JSchException
   */
  private void refresh(IStructuredSelection selection) {
    for (DFSPath path : filterSelection(DFSPath.class, selection))
      path.refresh();

  }

  private void delete(IStructuredSelection selection) {
    List<DFSPath> list = filterSelection(DFSPath.class, selection);
    if (list.isEmpty())
      return;

    StringBuffer msg = new StringBuffer();
    msg.append("Are you sure you want to delete "
        + "the following files from the distributed file system?\n");
    for (DFSPath path : list)
      msg.append(path.getPath()).append("\n");

    if (MessageDialog.openConfirm(null, "Confirm Delete from DFS", msg
        .toString())) {

      Set<DFSPath> toRefresh = new HashSet<DFSPath>();
      for (DFSPath path : list) {
        path.delete();
        toRefresh.add(path.getParent());
      }

      for (DFSPath path : toRefresh) {
        path.refresh();
      }
    }
  }

  /* @inheritDoc */
  public void selectionChanged(IAction action, ISelection selection) {
    this.selection = selection;
  }

  /**
   * Extract the list of <T> from the structured selection
   * 
   * @param clazz the class T
   * @param selection the structured selection
   * @return the list of <T> it contains
   */
  private static <T> List<T> filterSelection(Class<T> clazz,
      IStructuredSelection selection) {
    List<T> list = new ArrayList<T>();
    for (Object obj : selection.toList()) {
      if (clazz.isAssignableFrom(obj.getClass())) {
        list.add((T) obj);
      }
    }
    return list;
  }

  private static int computeUploadWork(File file) {
    if (file.isDirectory()) {
      int contentWork = 1;
      for (File child : file.listFiles())
        contentWork += computeUploadWork(child);
      return contentWork;

    } else if (file.isFile()) {
      return 1 + (int) (file.length() / 1024);

    } else {
      return 0;
    }
  }

}

/**
 * Adapter to allow the viewing of a DfsFile in the Editor window
 */
class DFSFileEditorInput extends PlatformObject implements
    IStorageEditorInput {

  private DFSFile file;

  /**
   * Constructor
   * 
   * @param file
   */
  DFSFileEditorInput(DFSFile file) {
    this.file = file;
  }

  /* @inheritDoc */
  public String getToolTipText() {
    return file.toDetailedString();
  }

  /* @inheritDoc */
  public IPersistableElement getPersistable() {
    return null;
  }

  /* @inheritDoc */
  public String getName() {
    return file.toString();
  }

  /* @inheritDoc */
  public ImageDescriptor getImageDescriptor() {
    return ImageLibrary.get("dfs.file.editor");
  }

  /* @inheritDoc */
  public boolean exists() {
    return true;
  }

  /* @inheritDoc */
  public IStorage getStorage() throws CoreException {
    return file.getIStorage();
  }
};
