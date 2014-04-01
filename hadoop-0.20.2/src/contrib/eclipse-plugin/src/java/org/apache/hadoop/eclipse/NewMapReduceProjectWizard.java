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

package org.apache.hadoop.eclipse;

import java.io.File;
import java.io.FilenameFilter;
import java.lang.reflect.InvocationTargetException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.eclipse.preferences.MapReducePreferencePage;
import org.apache.hadoop.eclipse.preferences.PreferenceConstants;
import org.eclipse.core.resources.IProject;
import org.eclipse.core.resources.IProjectDescription;
import org.eclipse.core.resources.ResourcesPlugin;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IConfigurationElement;
import org.eclipse.core.runtime.IExecutableExtension;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.NullProgressMonitor;
import org.eclipse.core.runtime.Path;
import org.eclipse.core.runtime.QualifiedName;
import org.eclipse.core.runtime.SubProgressMonitor;
import org.eclipse.jdt.ui.wizards.NewJavaProjectWizardPage;
import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.jface.preference.PreferenceDialog;
import org.eclipse.jface.preference.PreferenceManager;
import org.eclipse.jface.preference.PreferenceNode;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jface.wizard.IWizardPage;
import org.eclipse.jface.wizard.Wizard;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Link;
import org.eclipse.swt.widgets.Text;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.IWorkbenchWizard;
import org.eclipse.ui.PlatformUI;
import org.eclipse.ui.dialogs.WizardNewProjectCreationPage;
import org.eclipse.ui.wizards.newresource.BasicNewProjectResourceWizard;

/**
 * Wizard for creating a new MapReduce Project
 * 
 */

public class NewMapReduceProjectWizard extends Wizard implements
    IWorkbenchWizard, IExecutableExtension {
  static Logger log =
      Logger.getLogger(NewMapReduceProjectWizard.class.getName());

  private HadoopFirstPage firstPage;

  private NewJavaProjectWizardPage javaPage;

  public NewDriverWizardPage newDriverPage;

  private IConfigurationElement config;

  public NewMapReduceProjectWizard() {
    setWindowTitle("New MapReduce Project Wizard");
  }

  public void init(IWorkbench workbench, IStructuredSelection selection) {

  }

  @Override
  public boolean canFinish() {
    return firstPage.isPageComplete() && javaPage.isPageComplete()
    // && ((!firstPage.generateDriver.getSelection())
    // || newDriverPage.isPageComplete()
    ;
  }

  @Override
  public IWizardPage getNextPage(IWizardPage page) {
    // if (page == firstPage
    // && firstPage.generateDriver.getSelection()
    // )
    // {
    // return newDriverPage; // if "generate mapper" checked, second page is
    // new driver page
    // }
    // else
    // {
    IWizardPage answer = super.getNextPage(page);
    if (answer == newDriverPage) {
      return null; // dont flip to new driver page unless "generate
      // driver" is checked
    } else if (answer == javaPage) {
      return answer;
    } else {
      return answer;
    }
    // }
  }

  @Override
  public IWizardPage getPreviousPage(IWizardPage page) {
    if (page == newDriverPage) {
      return firstPage; // newDriverPage, if it appears, is the second
      // page
    } else {
      return super.getPreviousPage(page);
    }
  }

  static class HadoopFirstPage extends WizardNewProjectCreationPage
      implements SelectionListener {
    public HadoopFirstPage() {
      super("New Hadoop Project");
      setImageDescriptor(ImageLibrary.get("wizard.mapreduce.project.new"));
    }

    private Link openPreferences;

    private Button workspaceHadoop;

    private Button projectHadoop;

    private Text location;

    private Button browse;

    private String path;

    public String currentPath;

    // private Button generateDriver;

    @Override
    public void createControl(Composite parent) {
      super.createControl(parent);

      setTitle("MapReduce Project");
      setDescription("Create a MapReduce project.");

      Group group = new Group((Composite) getControl(), SWT.NONE);
      group.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
      group.setText("Hadoop MapReduce Library Installation Path");
      GridLayout layout = new GridLayout(3, true);
      layout.marginLeft =
          convertHorizontalDLUsToPixels(IDialogConstants.HORIZONTAL_MARGIN);
      layout.marginRight =
          convertHorizontalDLUsToPixels(IDialogConstants.HORIZONTAL_MARGIN);
      layout.marginTop =
          convertHorizontalDLUsToPixels(IDialogConstants.VERTICAL_MARGIN);
      layout.marginBottom =
          convertHorizontalDLUsToPixels(IDialogConstants.VERTICAL_MARGIN);
      group.setLayout(layout);

      workspaceHadoop = new Button(group, SWT.RADIO);
      GridData d =
          new GridData(GridData.BEGINNING, GridData.BEGINNING, false, false);
      d.horizontalSpan = 2;
      workspaceHadoop.setLayoutData(d);
      // workspaceHadoop.setText("Use default workbench Hadoop library
      // location");
      workspaceHadoop.setSelection(true);

      updateHadoopDirLabelFromPreferences();

      openPreferences = new Link(group, SWT.NONE);
      openPreferences
          .setText("<a>Configure Hadoop install directory...</a>");
      openPreferences.setLayoutData(new GridData(GridData.END,
          GridData.CENTER, false, false));
      openPreferences.addSelectionListener(this);

      projectHadoop = new Button(group, SWT.RADIO);
      projectHadoop.setLayoutData(new GridData(GridData.BEGINNING,
          GridData.CENTER, false, false));
      projectHadoop.setText("Specify Hadoop library location");

      location = new Text(group, SWT.SINGLE | SWT.BORDER);
      location.setText("");
      d = new GridData(GridData.END, GridData.CENTER, true, false);
      d.horizontalSpan = 1;
      d.widthHint = 250;
      d.grabExcessHorizontalSpace = true;
      location.setLayoutData(d);
      location.setEnabled(false);

      browse = new Button(group, SWT.NONE);
      browse.setText("Browse...");
      browse.setLayoutData(new GridData(GridData.BEGINNING, GridData.CENTER,
          false, false));
      browse.setEnabled(false);
      browse.addSelectionListener(this);

      projectHadoop.addSelectionListener(this);
      workspaceHadoop.addSelectionListener(this);

      // generateDriver = new Button((Composite) getControl(), SWT.CHECK);
      // generateDriver.setText("Generate a MapReduce driver");
      // generateDriver.addListener(SWT.Selection, new Listener()
      // {
      // public void handleEvent(Event event) {
      // getContainer().updateButtons(); }
      // });
    }

    @Override
    public boolean isPageComplete() {
      boolean validHadoop = validateHadoopLocation();

      if (!validHadoop && isCurrentPage()) {
        setErrorMessage("Invalid Hadoop Runtime specified; please click 'Configure Hadoop install directory' or fill in library location input field");
      } else {
        setErrorMessage(null);
      }

      return super.isPageComplete() && validHadoop;
    }

    private boolean validateHadoopLocation() {
      FilenameFilter gotHadoopJar = new FilenameFilter() {
        public boolean accept(File dir, String name) {
          return (name.startsWith("hadoop") && name.endsWith(".jar")
              && (name.indexOf("test") == -1) && (name.indexOf("examples") == -1));
        }
      };

      if (workspaceHadoop.getSelection()) {
        this.currentPath = path;
        return new Path(path).toFile().exists()
            && (new Path(path).toFile().list(gotHadoopJar).length > 0);
      } else {
        this.currentPath = location.getText();
        File file = new Path(location.getText()).toFile();
        return file.exists()
            && (new Path(location.getText()).toFile().list(gotHadoopJar).length > 0);
      }
    }

    private void updateHadoopDirLabelFromPreferences() {
      path =
          Activator.getDefault().getPreferenceStore().getString(
              PreferenceConstants.P_PATH);

      if ((path != null) && (path.length() > 0)) {
        workspaceHadoop.setText("Use default Hadoop");
      } else {
        workspaceHadoop.setText("Use default Hadoop (currently not set)");
      }
    }

    public void widgetDefaultSelected(SelectionEvent e) {
    }

    public void widgetSelected(SelectionEvent e) {
      if (e.getSource() == openPreferences) {
        PreferenceManager manager = new PreferenceManager();
        manager.addToRoot(new PreferenceNode(
            "Hadoop Installation Directory", new MapReducePreferencePage()));
        PreferenceDialog dialog =
            new PreferenceDialog(this.getShell(), manager);
        dialog.create();
        dialog.setMessage("Select Hadoop Installation Directory");
        dialog.setBlockOnOpen(true);
        dialog.open();

        updateHadoopDirLabelFromPreferences();
      } else if (e.getSource() == browse) {
        DirectoryDialog dialog = new DirectoryDialog(this.getShell());
        dialog
            .setMessage("Select a hadoop installation, containing hadoop-X-core.jar");
        dialog.setText("Select Hadoop Installation Directory");
        String directory = dialog.open();

        if (directory != null) {
          location.setText(directory);

          if (!validateHadoopLocation()) {
            setErrorMessage("No Hadoop jar found in specified directory");
          } else {
            setErrorMessage(null);
          }
        }
      } else if (projectHadoop.getSelection()) {
        location.setEnabled(true);
        browse.setEnabled(true);
      } else {
        location.setEnabled(false);
        browse.setEnabled(false);
      }

      getContainer().updateButtons();
    }
  }

  @Override
  public void addPages() {
    /*
     * firstPage = new HadoopFirstPage(); addPage(firstPage ); addPage( new
     * JavaProjectWizardSecondPage(firstPage) );
     */

    firstPage = new HadoopFirstPage();
    javaPage =
        new NewJavaProjectWizardPage(ResourcesPlugin.getWorkspace()
            .getRoot(), firstPage);
    // newDriverPage = new NewDriverWizardPage(false);
    // newDriverPage.setPageComplete(false); // ensure finish button
    // initially disabled
    addPage(firstPage);
    addPage(javaPage);

    // addPage(newDriverPage);
  }

  @Override
  public boolean performFinish() {
    try {
      PlatformUI.getWorkbench().getProgressService().runInUI(
          this.getContainer(), new IRunnableWithProgress() {
            public void run(IProgressMonitor monitor) {
              try {
                monitor.beginTask("Create Hadoop Project", 300);

                javaPage.getRunnable().run(
                    new SubProgressMonitor(monitor, 100));

                // if( firstPage.generateDriver.getSelection())
                // {
                // newDriverPage.setPackageFragmentRoot(javaPage.getNewJavaProject().getAllPackageFragmentRoots()[0],
                // false);
                // newDriverPage.getRunnable().run(new
                // SubProgressMonitor(monitor,100));
                // }

                IProject project =
                    javaPage.getNewJavaProject().getResource().getProject();
                IProjectDescription description = project.getDescription();
                String[] existingNatures = description.getNatureIds();
                String[] natures = new String[existingNatures.length + 1];
                for (int i = 0; i < existingNatures.length; i++) {
                  natures[i + 1] = existingNatures[i];
                }

                natures[0] = MapReduceNature.ID;
                description.setNatureIds(natures);

                project.setPersistentProperty(new QualifiedName(
                    Activator.PLUGIN_ID, "hadoop.runtime.path"),
                    firstPage.currentPath);
                project.setDescription(description,
                    new NullProgressMonitor());

                String[] natureIds = project.getDescription().getNatureIds();
                for (int i = 0; i < natureIds.length; i++) {
                  log.fine("Nature id # " + i + " > " + natureIds[i]);
                }

                monitor.worked(100);
                monitor.done();

                BasicNewProjectResourceWizard.updatePerspective(config);
              } catch (CoreException e) {
                // TODO Auto-generated catch block
                log.log(Level.SEVERE, "CoreException thrown.", e);
              } catch (InvocationTargetException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
              } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
              }
            }
          }, null);
    } catch (InvocationTargetException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return true;
  }

  public void setInitializationData(IConfigurationElement config,
      String propertyName, Object data) throws CoreException {
    this.config = config;
  }
}
