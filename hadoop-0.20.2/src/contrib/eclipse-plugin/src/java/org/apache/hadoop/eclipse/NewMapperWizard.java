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

import java.io.IOException;
import java.util.Arrays;

import org.eclipse.core.resources.IFile;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.core.runtime.Path;
import org.eclipse.jdt.core.IJavaElement;
import org.eclipse.jdt.core.IType;
import org.eclipse.jdt.internal.ui.wizards.NewElementWizard;
import org.eclipse.jdt.ui.wizards.NewTypeWizardPage;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.ui.INewWizard;
import org.eclipse.ui.IWorkbench;

/**
 * Wizard for creating a new Mapper class (a class that runs the Map portion
 * of a MapReduce job). The class is pre-filled with a template.
 * 
 */

public class NewMapperWizard extends NewElementWizard implements INewWizard,
    IRunnableWithProgress {
  private Page page;

  public NewMapperWizard() {
    setWindowTitle("New Mapper");
  }

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

  @Override
  public void init(IWorkbench workbench, IStructuredSelection selection) {
    super.init(workbench, selection);

    page = new Page();
    addPage(page);
    page.setSelection(selection);
  }

  public static class Page extends NewTypeWizardPage {
    private Button isCreateMapMethod;

    public Page() {
      super(true, "Mapper");

      setTitle("Mapper");
      setDescription("Create a new Mapper implementation.");
      setImageDescriptor(ImageLibrary.get("wizard.mapper.new"));
    }

    public void setSelection(IStructuredSelection selection) {
      initContainerPage(getInitialJavaElement(selection));
      initTypePage(getInitialJavaElement(selection));
    }

    @Override
    public void createType(IProgressMonitor monitor) throws CoreException,
        InterruptedException {
      super.createType(monitor);
    }

    @Override
    protected void createTypeMembers(IType newType, ImportsManager imports,
        IProgressMonitor monitor) throws CoreException {
      super.createTypeMembers(newType, imports, monitor);
      imports.addImport("java.io.IOException");
      imports.addImport("org.apache.hadoop.io.WritableComparable");
      imports.addImport("org.apache.hadoop.io.Writable");
      imports.addImport("org.apache.hadoop.mapred.OutputCollector");
      imports.addImport("org.apache.hadoop.mapred.Reporter");
      newType
          .createMethod(
              "public void map(WritableComparable key, Writable values, OutputCollector output, Reporter reporter) throws IOException \n{\n}\n",
              null, false, monitor);
    }

    public void createControl(Composite parent) {
      // super.createControl(parent);

      initializeDialogUnits(parent);
      Composite composite = new Composite(parent, SWT.NONE);
      GridLayout layout = new GridLayout();
      layout.numColumns = 4;
      composite.setLayout(layout);

      createContainerControls(composite, 4);
      createPackageControls(composite, 4);
      createSeparator(composite, 4);
      createTypeNameControls(composite, 4);
      createSuperClassControls(composite, 4);
      createSuperInterfacesControls(composite, 4);
      // createSeparator(composite, 4);

      setControl(composite);

      setSuperClass("org.apache.hadoop.mapred.MapReduceBase", true);
      setSuperInterfaces(Arrays
          .asList(new String[] { "org.apache.hadoop.mapred.Mapper" }), true);

      setFocus();
      validate();
    }

    @Override
    protected void handleFieldChanged(String fieldName) {
      super.handleFieldChanged(fieldName);

      validate();
    }

    private void validate() {
      updateStatus(new IStatus[] { fContainerStatus, fPackageStatus,
          fTypeNameStatus, fSuperClassStatus, fSuperInterfacesStatus });
    }
  }

  @Override
  public boolean performFinish() {
    if (super.performFinish()) {
      if (getCreatedElement() != null) {
        openResource((IFile) page.getModifiedResource());
        selectAndReveal(page.getModifiedResource());
      }

      return true;
    } else {
      return false;
    }
  }

  @Override
  protected void finishPage(IProgressMonitor monitor)
      throws InterruptedException, CoreException {
    this.run(monitor);
  }

  @Override
  public IJavaElement getCreatedElement() {
    return page.getCreatedType().getPrimaryElement();
  }

}
