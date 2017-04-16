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

import org.eclipse.core.resources.IFile;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jdt.core.IJavaElement;
import org.eclipse.jdt.internal.ui.wizards.NewElementWizard;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.ui.INewWizard;
import org.eclipse.ui.IWorkbench;

/**
 * Wizard for creating a new Driver class (a class that runs a MapReduce job).
 * 
 */

public class NewDriverWizard extends NewElementWizard implements INewWizard,
    IRunnableWithProgress {
  private NewDriverWizardPage page;

  /*
   * @Override public boolean performFinish() { }
   */
  public void run(IProgressMonitor monitor) {
    try {
      page.createType(monitor);
    } catch (CoreException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public NewDriverWizard() {
    setWindowTitle("New MapReduce Driver");
  }

  @Override
  public void init(IWorkbench workbench, IStructuredSelection selection) {
    super.init(workbench, selection);

    page = new NewDriverWizardPage();
    addPage(page);
    page.setSelection(selection);
  }

  @Override
  /**
   * Performs any actions appropriate in response to the user having pressed the
   * Finish button, or refuse if finishing now is not permitted.
   */
  public boolean performFinish() {
    if (super.performFinish()) {
      if (getCreatedElement() != null) {
        selectAndReveal(page.getModifiedResource());
        openResource((IFile) page.getModifiedResource());
      }

      return true;
    } else {
      return false;
    }
  }

  @Override
  /**
   * 
   */
  protected void finishPage(IProgressMonitor monitor)
      throws InterruptedException, CoreException {
    this.run(monitor);
  }

  @Override
  public IJavaElement getCreatedElement() {
    return page.getCreatedType().getPrimaryElement();
  }
}
