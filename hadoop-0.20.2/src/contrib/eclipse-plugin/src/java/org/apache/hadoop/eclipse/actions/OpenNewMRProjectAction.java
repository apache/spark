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

import org.apache.hadoop.eclipse.NewMapReduceProjectWizard;
import org.eclipse.jface.action.Action;
import org.eclipse.jface.viewers.StructuredSelection;
import org.eclipse.jface.window.Window;
import org.eclipse.jface.wizard.WizardDialog;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.PlatformUI;

/**
 * Action to open a new Map/Reduce project.
 */

public class OpenNewMRProjectAction extends Action {

  @Override
  public void run() {
    IWorkbench workbench = PlatformUI.getWorkbench();
    Shell shell = workbench.getActiveWorkbenchWindow().getShell();
    NewMapReduceProjectWizard wizard = new NewMapReduceProjectWizard();
    wizard.init(workbench, new StructuredSelection());
    WizardDialog dialog = new WizardDialog(shell, wizard);
    dialog.create();
    dialog.open();
    // did the wizard succeed?
    notifyResult(dialog.getReturnCode() == Window.OK);
  }
}
