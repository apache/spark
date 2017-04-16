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

package org.apache.hadoop.eclipse.launch;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hadoop.eclipse.servers.RunOnHadoopWizard;
import org.eclipse.core.resources.IFile;
import org.eclipse.core.resources.IResource;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.debug.core.ILaunchConfiguration;
import org.eclipse.debug.core.ILaunchConfigurationType;
import org.eclipse.debug.core.ILaunchConfigurationWorkingCopy;
import org.eclipse.jdt.core.IJavaProject;
import org.eclipse.jdt.core.IType;
import org.eclipse.jdt.core.JavaCore;
import org.eclipse.jdt.debug.ui.launchConfigurations.JavaApplicationLaunchShortcut;
import org.eclipse.jdt.launching.IJavaLaunchConfigurationConstants;
import org.eclipse.jdt.launching.IRuntimeClasspathEntry;
import org.eclipse.jdt.launching.JavaRuntime;
import org.eclipse.jface.wizard.IWizard;
import org.eclipse.jface.wizard.WizardDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

/**
 * Add a shortcut "Run on Hadoop" to the Run menu
 */

public class HadoopApplicationLaunchShortcut extends
    JavaApplicationLaunchShortcut {

  static Logger log =
      Logger.getLogger(HadoopApplicationLaunchShortcut.class.getName());

  // private ActionDelegate delegate = new RunOnHadoopActionDelegate();

  public HadoopApplicationLaunchShortcut() {
  }

  /* @inheritDoc */
  @Override
  protected ILaunchConfiguration findLaunchConfiguration(IType type,
      ILaunchConfigurationType configType) {

    // Find an existing or create a launch configuration (Standard way)
    ILaunchConfiguration iConf =
        super.findLaunchConfiguration(type, configType);
    if (iConf == null) iConf = super.createConfiguration(type);
    ILaunchConfigurationWorkingCopy iConfWC;
    try {
      /*
       * Tune the default launch configuration: setup run-time classpath
       * manually
       */
      iConfWC = iConf.getWorkingCopy();

      iConfWC.setAttribute(
          IJavaLaunchConfigurationConstants.ATTR_DEFAULT_CLASSPATH, false);

      List<String> classPath = new ArrayList<String>();
      IResource resource = type.getResource();
      IJavaProject project =
          (IJavaProject) resource.getProject().getNature(JavaCore.NATURE_ID);
      IRuntimeClasspathEntry cpEntry =
          JavaRuntime.newDefaultProjectClasspathEntry(project);
      classPath.add(0, cpEntry.getMemento());

      iConfWC.setAttribute(IJavaLaunchConfigurationConstants.ATTR_CLASSPATH,
          classPath);

    } catch (CoreException e) {
      e.printStackTrace();
      // FIXME Error dialog
      return null;
    }

    /*
     * Update the selected configuration with a specific Hadoop location
     * target
     */
    IResource resource = type.getResource();
    if (!(resource instanceof IFile))
      return null;
    RunOnHadoopWizard wizard =
        new RunOnHadoopWizard((IFile) resource, iConfWC);
    WizardDialog dialog =
        new WizardDialog(Display.getDefault().getActiveShell(), wizard);

    dialog.create();
    dialog.setBlockOnOpen(true);
    if (dialog.open() != WizardDialog.OK)
      return null;

    try {
      iConfWC.doSave();

    } catch (CoreException e) {
      e.printStackTrace();
      // FIXME Error dialog
      return null;
    }

    return iConfWC;
  }

  /**
   * Was used to run the RunOnHadoopWizard inside and provide it a
   * ProgressMonitor
   */
  static class Dialog extends WizardDialog {
    public Dialog(Shell parentShell, IWizard newWizard) {
      super(parentShell, newWizard);
    }

    @Override
    public void create() {
      super.create();

      ((RunOnHadoopWizard) getWizard())
          .setProgressMonitor(getProgressMonitor());
    }
  }
}
