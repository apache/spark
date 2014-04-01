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

package org.apache.hadoop.eclipse.servers;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.eclipse.Activator;
import org.apache.hadoop.eclipse.ErrorMessageDialog;
import org.apache.hadoop.eclipse.server.HadoopServer;
import org.apache.hadoop.eclipse.server.JarModule;
import org.apache.hadoop.mapred.JobConf;
import org.eclipse.core.resources.IFile;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IPath;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.Path;
import org.eclipse.debug.core.ILaunchConfigurationWorkingCopy;
import org.eclipse.jdt.launching.IJavaLaunchConfigurationConstants;
import org.eclipse.jdt.launching.IRuntimeClasspathEntry;
import org.eclipse.jdt.launching.JavaRuntime;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.jface.wizard.Wizard;
import org.eclipse.jface.wizard.WizardPage;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;

/**
 * Wizard for publishing a job to a Hadoop server.
 */

public class RunOnHadoopWizard extends Wizard {

  private MainWizardPage mainPage;

  private HadoopLocationWizard createNewPage;

  /**
   * The file resource (containing a main()) to run on the Hadoop location
   */
  private IFile resource;

  /**
   * The launch configuration to update
   */
  private ILaunchConfigurationWorkingCopy iConf;

  private IProgressMonitor progressMonitor;

  public RunOnHadoopWizard(IFile resource,
      ILaunchConfigurationWorkingCopy iConf) {
    this.resource = resource;
    this.iConf = iConf;
    setForcePreviousAndNextButtons(true);
    setNeedsProgressMonitor(true);
    setWindowTitle("Run on Hadoop");
  }

  /**
   * This wizard contains 2 pages:
   * <li> the first one lets the user choose an already existing location
   * <li> the second one allows the user to create a new location, in case it
   * does not already exist
   */
  /* @inheritDoc */
  @Override
  public void addPages() {
    addPage(this.mainPage = new MainWizardPage());
    addPage(this.createNewPage = new HadoopLocationWizard());
  }

  /**
   * Performs any actions appropriate in response to the user having pressed
   * the Finish button, or refuse if finishing now is not permitted.
   */
  /* @inheritDoc */
  @Override
  public boolean performFinish() {

    /*
     * Create a new location or get an existing one
     */
    HadoopServer location = null;
    if (mainPage.createNew.getSelection()) {
      location = createNewPage.performFinish();

    } else if (mainPage.table.getSelection().length == 1) {
      location = (HadoopServer) mainPage.table.getSelection()[0].getData();
    }

    if (location == null)
      return false;

    /*
     * Get the base directory of the plug-in for storing configurations and
     * JARs
     */
    File baseDir = Activator.getDefault().getStateLocation().toFile();

    // Package the Job into a JAR
    File jarFile = JarModule.createJarPackage(resource);
    if (jarFile == null) {
      ErrorMessageDialog.display("Run on Hadoop",
          "Unable to create or locate the JAR file for the Job");
      return false;
    }

    /*
     * Generate a temporary Hadoop configuration directory and add it to the
     * classpath of the launch configuration
     */

    File confDir;
    try {
      confDir = File.createTempFile("hadoop-conf-", "", baseDir);
      confDir.delete();
      confDir.mkdirs();
      if (!confDir.isDirectory()) {
        ErrorMessageDialog.display("Run on Hadoop",
            "Cannot create temporary directory: " + confDir);
        return false;
      }
    } catch (IOException ioe) {
      ioe.printStackTrace();
      return false;
    }

    // Prepare the Hadoop configuration
    JobConf conf = new JobConf(location.getConfiguration());
    conf.setJar(jarFile.getAbsolutePath());

    // Write it to the disk file
    try {
      // File confFile = File.createTempFile("core-site-", ".xml",
      // confDir);
      File confFile = new File(confDir, "core-site.xml");
      FileOutputStream fos = new FileOutputStream(confFile);
      conf.writeXml(fos);
      fos.close();

    } catch (IOException ioe) {
      ioe.printStackTrace();
      return false;
    }

    // Setup the Launch class path
    List<String> classPath;
    try {
      classPath =
          iConf.getAttribute(
              IJavaLaunchConfigurationConstants.ATTR_CLASSPATH,
              new ArrayList());
      IPath confIPath = new Path(confDir.getAbsolutePath());
      IRuntimeClasspathEntry cpEntry =
          JavaRuntime.newArchiveRuntimeClasspathEntry(confIPath);
      classPath.add(0, cpEntry.getMemento());
      iConf.setAttribute(IJavaLaunchConfigurationConstants.ATTR_CLASSPATH,
          classPath);

    } catch (CoreException e) {
      e.printStackTrace();
      return false;
    }

    // location.runResource(resource, progressMonitor);
    return true;
  }

  private void refreshButtons() {
    getContainer().updateButtons();
  }

  /**
   * Allows finish when an existing server is selected or when a new server
   * location is defined
   */
  /* @inheritDoc */
  @Override
  public boolean canFinish() {
    if (mainPage != null)
      return mainPage.canFinish();
    return false;
  }

  /**
   * This is the main page of the wizard. It allows the user either to choose
   * an already existing location or to indicate he wants to create a new
   * location.
   */
  public class MainWizardPage extends WizardPage {

    private Button createNew;

    private Table table;

    private Button chooseExisting;

    public MainWizardPage() {
      super("Select or define server to run on");
      setTitle("Select Hadoop location");
      setDescription("Select a Hadoop location to run on.");
    }

    /* @inheritDoc */
    @Override
    public boolean canFlipToNextPage() {
      return createNew.getSelection();
    }

    /* @inheritDoc */
    public void createControl(Composite parent) {
      Composite panel = new Composite(parent, SWT.NONE);
      panel.setLayout(new GridLayout(1, false));

      // Label
      Label label = new Label(panel, SWT.NONE);
      label.setText("Select a Hadoop Server to run on.");
      GridData gData = new GridData(GridData.FILL_BOTH);
      gData.grabExcessVerticalSpace = false;
      label.setLayoutData(gData);

      // Create location button
      createNew = new Button(panel, SWT.RADIO);
      createNew.setText("Define a new Hadoop server location");
      createNew.setLayoutData(gData);
      createNew.addSelectionListener(new SelectionListener() {
        public void widgetDefaultSelected(SelectionEvent e) {
        }

        public void widgetSelected(SelectionEvent e) {
          setPageComplete(true);
          RunOnHadoopWizard.this.refreshButtons();
        }
      });
      createNew.setSelection(true);

      // Select existing location button
      chooseExisting = new Button(panel, SWT.RADIO);
      chooseExisting
          .setText("Choose an existing server from the list below");
      chooseExisting.setLayoutData(gData);
      chooseExisting.addSelectionListener(new SelectionListener() {
        public void widgetDefaultSelected(SelectionEvent e) {
        }

        public void widgetSelected(SelectionEvent e) {
          if (chooseExisting.getSelection()
              && (table.getSelectionCount() == 0)) {
            if (table.getItems().length > 0) {
              table.setSelection(0);
            }
          }
          RunOnHadoopWizard.this.refreshButtons();
        }
      });

      // Table of existing locations
      Composite serverListPanel = new Composite(panel, SWT.FILL);
      gData = new GridData(GridData.FILL_BOTH);
      gData.horizontalSpan = 1;
      serverListPanel.setLayoutData(gData);

      FillLayout layout = new FillLayout();
      layout.marginHeight = layout.marginWidth = 12;
      serverListPanel.setLayout(layout);

      table =
          new Table(serverListPanel, SWT.BORDER | SWT.H_SCROLL
              | SWT.V_SCROLL | SWT.FULL_SELECTION);
      table.setHeaderVisible(true);
      table.setLinesVisible(true);

      TableColumn nameColumn = new TableColumn(table, SWT.LEFT);
      nameColumn.setText("Location");
      nameColumn.setWidth(450);

      TableColumn hostColumn = new TableColumn(table, SWT.LEFT);
      hostColumn.setText("Master host name");
      hostColumn.setWidth(250);

      // If the user select one entry, switch to "chooseExisting"
      table.addSelectionListener(new SelectionListener() {
        public void widgetDefaultSelected(SelectionEvent e) {
        }

        public void widgetSelected(SelectionEvent e) {
          chooseExisting.setSelection(true);
          createNew.setSelection(false);
          setPageComplete(table.getSelectionCount() == 1);
          RunOnHadoopWizard.this.refreshButtons();
        }
      });

      TableViewer viewer = new TableViewer(table);
      HadoopServerSelectionListContentProvider provider =
          new HadoopServerSelectionListContentProvider();
      viewer.setContentProvider(provider);
      viewer.setLabelProvider(provider);
      viewer.setInput(new Object());
      // don't care, get from singleton server registry

      this.setControl(panel);
    }

    /**
     * Returns whether this page state allows the Wizard to finish or not
     * 
     * @return can the wizard finish or not?
     */
    public boolean canFinish() {
      if (!isControlCreated())
        return false;

      if (this.createNew.getSelection())
        return getNextPage().isPageComplete();

      return this.chooseExisting.getSelection();
    }
  }

  /**
   * @param progressMonitor
   */
  public void setProgressMonitor(IProgressMonitor progressMonitor) {
    this.progressMonitor = progressMonitor;
  }
}
