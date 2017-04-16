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

package org.apache.hadoop.eclipse.view.servers;

import java.util.Collection;

import org.apache.hadoop.eclipse.ImageLibrary;
import org.apache.hadoop.eclipse.actions.EditLocationAction;
import org.apache.hadoop.eclipse.actions.NewLocationAction;
import org.apache.hadoop.eclipse.server.HadoopJob;
import org.apache.hadoop.eclipse.server.HadoopServer;
import org.apache.hadoop.eclipse.server.IJobListener;
import org.apache.hadoop.eclipse.server.JarModule;
import org.apache.hadoop.eclipse.servers.IHadoopServerListener;
import org.apache.hadoop.eclipse.servers.ServerRegistry;
import org.eclipse.jface.action.Action;
import org.eclipse.jface.action.IAction;
import org.eclipse.jface.action.IMenuListener;
import org.eclipse.jface.action.IMenuManager;
import org.eclipse.jface.action.MenuManager;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.viewers.ILabelProviderListener;
import org.eclipse.jface.viewers.ISelection;
import org.eclipse.jface.viewers.ISelectionChangedListener;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jface.viewers.ITableLabelProvider;
import org.eclipse.jface.viewers.ITreeContentProvider;
import org.eclipse.jface.viewers.ITreeSelection;
import org.eclipse.jface.viewers.SelectionChangedEvent;
import org.eclipse.jface.viewers.TreeViewer;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeColumn;
import org.eclipse.ui.IViewSite;
import org.eclipse.ui.PartInitException;
import org.eclipse.ui.actions.ActionFactory;
import org.eclipse.ui.part.ViewPart;

/**
 * Map/Reduce locations view: displays all available Hadoop locations and the
 * Jobs running/finished on these locations
 */
public class ServerView extends ViewPart implements ITreeContentProvider,
    ITableLabelProvider, IJobListener, IHadoopServerListener {

  /**
   * Deletion action: delete a Hadoop location, kill a running job or remove
   * a finished job entry
   */
  class DeleteAction extends Action {

    DeleteAction() {
      setText("Delete");
      setImageDescriptor(ImageLibrary.get("server.view.action.delete"));
    }

    /* @inheritDoc */
    @Override
    public void run() {
      ISelection selection =
          getViewSite().getSelectionProvider().getSelection();
      if ((selection != null) && (selection instanceof IStructuredSelection)) {
        Object selItem =
            ((IStructuredSelection) selection).getFirstElement();

        if (selItem instanceof HadoopServer) {
          HadoopServer location = (HadoopServer) selItem;
          if (MessageDialog.openConfirm(Display.getDefault()
              .getActiveShell(), "Confirm delete Hadoop location",
              "Do you really want to remove the Hadoop location: "
                  + location.getLocationName())) {
            ServerRegistry.getInstance().removeServer(location);
          }

        } else if (selItem instanceof HadoopJob) {

          // kill the job
          HadoopJob job = (HadoopJob) selItem;
          if (job.isCompleted()) {
            // Job already finished, remove the entry
            job.getLocation().purgeJob(job);

          } else {
            // Job is running, kill the job?
            if (MessageDialog.openConfirm(Display.getDefault()
                .getActiveShell(), "Confirm kill running Job",
                "Do you really want to kill running Job: " + job.getJobID())) {
              job.kill();
            }
          }
        }
      }
    }
  }

  /**
   * This object is the root content for this content provider
   */
  private static final Object CONTENT_ROOT = new Object();

  private final IAction deleteAction = new DeleteAction();

  private final IAction editServerAction = new EditLocationAction(this);

  private final IAction newLocationAction = new NewLocationAction();

  private TreeViewer viewer;

  public ServerView() {
  }

  /* @inheritDoc */
  @Override
  public void init(IViewSite site) throws PartInitException {
    super.init(site);
  }

  /* @inheritDoc */
  @Override
  public void dispose() {
    ServerRegistry.getInstance().removeListener(this);
  }

  /**
   * Creates the columns for the view
   */
  @Override
  public void createPartControl(Composite parent) {
    Tree main =
        new Tree(parent, SWT.SINGLE | SWT.FULL_SELECTION | SWT.H_SCROLL
            | SWT.V_SCROLL);
    main.setHeaderVisible(true);
    main.setLinesVisible(false);
    main.setLayoutData(new GridData(GridData.FILL_BOTH));

    TreeColumn serverCol = new TreeColumn(main, SWT.SINGLE);
    serverCol.setText("Location");
    serverCol.setWidth(300);
    serverCol.setResizable(true);

    TreeColumn locationCol = new TreeColumn(main, SWT.SINGLE);
    locationCol.setText("Master node");
    locationCol.setWidth(185);
    locationCol.setResizable(true);

    TreeColumn stateCol = new TreeColumn(main, SWT.SINGLE);
    stateCol.setText("State");
    stateCol.setWidth(95);
    stateCol.setResizable(true);

    TreeColumn statusCol = new TreeColumn(main, SWT.SINGLE);
    statusCol.setText("Status");
    statusCol.setWidth(300);
    statusCol.setResizable(true);

    viewer = new TreeViewer(main);
    viewer.setContentProvider(this);
    viewer.setLabelProvider(this);
    viewer.setInput(CONTENT_ROOT); // don't care

    getViewSite().setSelectionProvider(viewer);
    
    getViewSite().getActionBars().setGlobalActionHandler(
        ActionFactory.DELETE.getId(), deleteAction);
    getViewSite().getActionBars().getToolBarManager().add(editServerAction);
    getViewSite().getActionBars().getToolBarManager().add(newLocationAction);

    createActions();
    createContextMenu();
  }

  /**
   * Actions
   */
  private void createActions() {
    /*
     * addItemAction = new Action("Add...") { public void run() { addItem(); } };
     * addItemAction.setImageDescriptor(ImageLibrary
     * .get("server.view.location.new"));
     */
    /*
     * deleteItemAction = new Action("Delete") { public void run() {
     * deleteItem(); } };
     * deleteItemAction.setImageDescriptor(getImageDescriptor("delete.gif"));
     * 
     * selectAllAction = new Action("Select All") { public void run() {
     * selectAll(); } };
     */
    // Add selection listener.
    viewer.addSelectionChangedListener(new ISelectionChangedListener() {
      public void selectionChanged(SelectionChangedEvent event) {
        updateActionEnablement();
      }
    });
  }

  private void addItem() {
    System.out.printf("ADD ITEM\n");
  }

  private void updateActionEnablement() {
    IStructuredSelection sel = (IStructuredSelection) viewer.getSelection();
    // deleteItemAction.setEnabled(sel.size() > 0);
  }

  /**
   * Contextual menu
   */
  private void createContextMenu() {
    // Create menu manager.
    MenuManager menuMgr = new MenuManager();
    menuMgr.setRemoveAllWhenShown(true);
    menuMgr.addMenuListener(new IMenuListener() {
      public void menuAboutToShow(IMenuManager mgr) {
        fillContextMenu(mgr);
      }
    });

    // Create menu.
    Menu menu = menuMgr.createContextMenu(viewer.getControl());
    viewer.getControl().setMenu(menu);

    // Register menu for extension.
    getSite().registerContextMenu(menuMgr, viewer);
  }

  private void fillContextMenu(IMenuManager mgr) {
    mgr.add(newLocationAction);
    mgr.add(editServerAction);
    mgr.add(deleteAction);
    /*
     * mgr.add(new GroupMarker(IWorkbenchActionConstants.MB_ADDITIONS));
     * mgr.add(deleteItemAction); mgr.add(new Separator());
     * mgr.add(selectAllAction);
     */
  }

  /* @inheritDoc */
  @Override
  public void setFocus() {

  }

  /*
   * IHadoopServerListener implementation
   */

  /* @inheritDoc */
  public void serverChanged(HadoopServer location, int type) {
    Display.getDefault().syncExec(new Runnable() {
      public void run() {
        ServerView.this.viewer.refresh();
      }
    });
  }

  /*
   * IStructuredContentProvider implementation
   */

  /* @inheritDoc */
  public void inputChanged(final Viewer viewer, Object oldInput,
      Object newInput) {
    if (oldInput == CONTENT_ROOT)
      ServerRegistry.getInstance().removeListener(this);
    if (newInput == CONTENT_ROOT)
      ServerRegistry.getInstance().addListener(this);
  }

  /**
   * The root elements displayed by this view are the existing Hadoop
   * locations
   */
  /* @inheritDoc */
  public Object[] getElements(Object inputElement) {
    return ServerRegistry.getInstance().getServers().toArray();
  }

  /*
   * ITreeStructuredContentProvider implementation
   */

  /**
   * Each location contains a child entry for each job it runs.
   */
  /* @inheritDoc */
  public Object[] getChildren(Object parent) {

    if (parent instanceof HadoopServer) {
      HadoopServer location = (HadoopServer) parent;
      location.addJobListener(this);
      Collection<HadoopJob> jobs = location.getJobs();
      return jobs.toArray();
    }

    return null;
  }

  /* @inheritDoc */
  public Object getParent(Object element) {
    if (element instanceof HadoopServer) {
      return CONTENT_ROOT;

    } else if (element instanceof HadoopJob) {
      return ((HadoopJob) element).getLocation();
    }

    return null;
  }

  /* @inheritDoc */
  public boolean hasChildren(Object element) {
    /* Only server entries have children */
    return (element instanceof HadoopServer);
  }

  /*
   * ITableLabelProvider implementation
   */

  /* @inheritDoc */
  public void addListener(ILabelProviderListener listener) {
    // no listeners handling
  }

  public boolean isLabelProperty(Object element, String property) {
    return false;
  }

  /* @inheritDoc */
  public void removeListener(ILabelProviderListener listener) {
    // no listener handling
  }

  /* @inheritDoc */
  public Image getColumnImage(Object element, int columnIndex) {
    if ((columnIndex == 0) && (element instanceof HadoopServer)) {
      return ImageLibrary.getImage("server.view.location.entry");

    } else if ((columnIndex == 0) && (element instanceof HadoopJob)) {
      return ImageLibrary.getImage("server.view.job.entry");
    }
    return null;
  }

  /* @inheritDoc */
  public String getColumnText(Object element, int columnIndex) {
    if (element instanceof HadoopServer) {
      HadoopServer server = (HadoopServer) element;

      switch (columnIndex) {
        case 0:
          return server.getLocationName();
        case 1:
          return server.getMasterHostName().toString();
        case 2:
          return server.getState();
        case 3:
          return "";
      }
    } else if (element instanceof HadoopJob) {
      HadoopJob job = (HadoopJob) element;

      switch (columnIndex) {
        case 0:
          return job.getJobID().toString();
        case 1:
          return "";
        case 2:
          return job.getState().toString();
        case 3:
          return job.getStatus();
      }
    } else if (element instanceof JarModule) {
      JarModule jar = (JarModule) element;

      switch (columnIndex) {
        case 0:
          return jar.toString();
        case 1:
          return "Publishing jar to server..";
        case 2:
          return "";
      }
    }

    return null;
  }

  /*
   * IJobListener (Map/Reduce Jobs listener) implementation
   */

  /* @inheritDoc */
  public void jobAdded(HadoopJob job) {
    viewer.refresh();
  }

  /* @inheritDoc */
  public void jobRemoved(HadoopJob job) {
    viewer.refresh();
  }

  /* @inheritDoc */
  public void jobChanged(HadoopJob job) {
    viewer.refresh(job);
  }

  /* @inheritDoc */
  public void publishDone(JarModule jar) {
    viewer.refresh();
  }

  /* @inheritDoc */
  public void publishStart(JarModule jar) {
    viewer.refresh();
  }

  /*
   * Miscellaneous
   */

  /**
   * Return the currently selected server (null if there is no selection or
   * if the selection is not a server)
   * 
   * @return the currently selected server entry
   */
  public HadoopServer getSelectedServer() {
    ITreeSelection selection = (ITreeSelection) viewer.getSelection();
    Object first = selection.getFirstElement();
    if (first instanceof HadoopServer) {
      return (HadoopServer) first;
    }
    return null;
  }

}
