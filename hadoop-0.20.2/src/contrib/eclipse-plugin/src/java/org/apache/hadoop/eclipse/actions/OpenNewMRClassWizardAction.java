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

import java.util.logging.Logger;

import org.apache.hadoop.eclipse.NewDriverWizard;
import org.apache.hadoop.eclipse.NewMapperWizard;
import org.apache.hadoop.eclipse.NewReducerWizard;
import org.eclipse.jface.action.Action;
import org.eclipse.jface.viewers.StructuredSelection;
import org.eclipse.jface.window.Window;
import org.eclipse.jface.wizard.WizardDialog;
import org.eclipse.ui.INewWizard;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.PlatformUI;
import org.eclipse.ui.cheatsheets.ICheatSheetAction;
import org.eclipse.ui.cheatsheets.ICheatSheetManager;


/**
 * Action to open a new MapReduce Class.
 */

public class OpenNewMRClassWizardAction extends Action implements
    ICheatSheetAction {

  static Logger log = Logger.getLogger(OpenNewMRClassWizardAction.class
      .getName());

  public void run(String[] params, ICheatSheetManager manager) {

    if ((params != null) && (params.length > 0)) {
      IWorkbench workbench = PlatformUI.getWorkbench();
      INewWizard wizard = getWizard(params[0]);
      wizard.init(workbench, new StructuredSelection());
      WizardDialog dialog = new WizardDialog(PlatformUI.getWorkbench()
          .getActiveWorkbenchWindow().getShell(), wizard);
      dialog.create();
      dialog.open();

      // did the wizard succeed ?
      notifyResult(dialog.getReturnCode() == Window.OK);
    }
  }

  private INewWizard getWizard(String typeName) {
    if (typeName.equals("Mapper")) {
      return new NewMapperWizard();
    } else if (typeName.equals("Reducer")) {
      return new NewReducerWizard();
    } else if (typeName.equals("Driver")) {
      return new NewDriverWizard();
    } else {
      log.severe("Invalid Wizard requested");
      return null;
    }
  }

}
