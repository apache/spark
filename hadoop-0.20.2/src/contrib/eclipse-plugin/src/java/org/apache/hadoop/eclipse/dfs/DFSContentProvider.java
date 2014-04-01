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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.eclipse.ImageLibrary;
import org.apache.hadoop.eclipse.server.HadoopServer;
import org.apache.hadoop.eclipse.servers.ServerRegistry;
import org.eclipse.jface.viewers.ILabelProvider;
import org.eclipse.jface.viewers.ILabelProviderListener;
import org.eclipse.jface.viewers.ITreeContentProvider;
import org.eclipse.jface.viewers.StructuredViewer;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Display;

/**
 * Handles viewing of DFS locations
 * <p>
 * 
 * The content handled by this provider is a tree:
 * 
 * <tt>
 * <br>DFSLocationsRoot
 * <br>\_HadoopServer
 * <br>|  \_DfsFolder
 * <br>|  |  \_DfsFile
 * <br>|  \_DfsFolder
 * <br>| ...
 * <br>\_HadoopServer...
 * </tt>
 * 
 * The code should not block here: blocking operations need to be done
 * asynchronously so as not to freeze the UI!
 */
public class DFSContentProvider implements ITreeContentProvider,
    ILabelProvider {

  /**
   * The viewer that displays this Tree content
   */
  private Viewer viewer;

  private StructuredViewer sviewer;

  private Map<HadoopServer, DFSContent> rootFolders =
      new HashMap<HadoopServer, DFSContent>();

  /**
   * Constructor: load resources (icons).
   */
  public DFSContentProvider() {
  }

  private final DFSLocationsRoot locationsRoot = new DFSLocationsRoot(this);

  /*
   * ITreeContentProvider implementation
   */

  /* @inheritDoc */
  public Object[] getChildren(Object parent) {

    if (!(parent instanceof DFSContent))
      return null;
    DFSContent content = (DFSContent) parent;
    return content.getChildren();
  }

  public Object[] test(Object parentElement) {
    if (parentElement instanceof DFSLocationsRoot) {
      return ServerRegistry.getInstance().getServers().toArray();

    } else if (parentElement instanceof HadoopServer) {
      final HadoopServer location = (HadoopServer) parentElement;
      Object root = rootFolders.get(location);
      if (root != null)
        return new Object[] { root };

      return new Object[] { "Connecting to DFS..." };

    } else if (parentElement instanceof DFSFolder) {
      DFSFolder folder = (DFSFolder) parentElement;
      return folder.getChildren();
    }

    return new Object[] { "<Unknown DFSContent>" };
  }

  /* @inheritDoc */
  public Object getParent(Object element) {

    if (element instanceof DFSPath) {
      return ((DFSPath) element).getParent();

    } else if (element instanceof HadoopServer) {
      return locationsRoot;
    }

    return null;
  }

  /* @inheritDoc */
  public boolean hasChildren(Object element) {
    if (element instanceof DFSContent) {
      DFSContent content = (DFSContent) element;
      return content.hasChildren();
    }
    return false;
  }

  /*
   * IStructureContentProvider implementation
   */

  /* @inheritDoc */
  public Object[] getElements(final Object inputElement) {
    return new Object[] { locationsRoot };
    // return ServerRegistry.getInstance().getServers().toArray();
  }

  /*
   * ILabelProvider implementation
   */

  /* @inheritDoc */
  public Image getImage(Object element) {
    if (element instanceof DFSLocationsRoot)
      return ImageLibrary.getImage("dfs.browser.root.entry");

    else if (element instanceof DFSLocation)
      return ImageLibrary.getImage("dfs.browser.location.entry");

    else if (element instanceof DFSFolder)
      return ImageLibrary.getImage("dfs.browser.folder.entry");

    else if (element instanceof DFSFile)
      return ImageLibrary.getImage("dfs.browser.file.entry");

    return null;
  }

  /* @inheritDoc */
  public String getText(Object element) {
    if (element instanceof DFSFile)
      return ((DFSFile) element).toDetailedString();

    return element.toString();
  }

  /*
   * IBaseLabelProvider implementation
   */

  /* @inheritDoc */
  public void addListener(ILabelProviderListener listener) {
  }

  /* @inheritDoc */
  public void removeListener(ILabelProviderListener listener) {
  }

  /* @inheritDoc */
  public boolean isLabelProperty(Object element, String property) {
    return false;
  }

  /*
   * IContentProvider implementation
   */

  /* @inheritDoc */
  public void dispose() {
  }

  /* @inheritDoc */
  public void inputChanged(Viewer viewer, Object oldInput, Object newInput) {
    this.viewer = viewer;
    if ((viewer != null) && (viewer instanceof StructuredViewer))
      this.sviewer = (StructuredViewer) viewer;
    else
      this.sviewer = null;
  }

  /*
   * Miscellaneous
   */

  /**
   * Ask the viewer for this content to refresh
   */
  void refresh() {
    // no display, nothing to update
    if (this.viewer == null)
      return;

    Display.getDefault().asyncExec(new Runnable() {
      public void run() {
        DFSContentProvider.this.viewer.refresh();
      }
    });
  }

  /**
   * Ask the viewer to refresh a single element
   * 
   * @param content what to refresh
   */
  void refresh(final DFSContent content) {
    if (this.sviewer != null) {
      Display.getDefault().asyncExec(new Runnable() {
        public void run() {
          DFSContentProvider.this.sviewer.refresh(content);
        }
      });

    } else {
      refresh();
    }
  }

  Viewer getViewer() {
    return this.viewer;
  }

}
