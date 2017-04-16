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

package org.apache.hadoop.eclipse.server;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.ui.IEditorInput;
import org.eclipse.ui.IEditorPart;
import org.eclipse.ui.IEditorSite;
import org.eclipse.ui.IPropertyListener;
import org.eclipse.ui.IWorkbenchPartSite;
import org.eclipse.ui.PartInitException;

public class HadoopPathPage implements IEditorPart {

  public IEditorInput getEditorInput() {
    // TODO Auto-generated method stub
    return null;
  }

  public IEditorSite getEditorSite() {
    // TODO Auto-generated method stub
    return null;
  }

  public void init(IEditorSite site, IEditorInput input)
      throws PartInitException {
    // TODO Auto-generated method stub

  }

  public void addPropertyListener(IPropertyListener listener) {
    // TODO Auto-generated method stub

  }

  public void createPartControl(Composite parent) {
    // TODO Auto-generated method stub

  }

  public void dispose() {
    // TODO Auto-generated method stub

  }

  public IWorkbenchPartSite getSite() {
    // TODO Auto-generated method stub
    return null;
  }

  public String getTitle() {
    // TODO Auto-generated method stub
    return null;
  }

  public Image getTitleImage() {
    // TODO Auto-generated method stub
    return null;
  }

  public String getTitleToolTip() {
    // TODO Auto-generated method stub
    return null;
  }

  public void removePropertyListener(IPropertyListener listener) {
    // TODO Auto-generated method stub

  }

  public void setFocus() {
    // TODO Auto-generated method stub

  }

  public Object getAdapter(Class adapter) {
    // TODO Auto-generated method stub
    return null;
  }

  public void doSave(IProgressMonitor monitor) {
    // TODO Auto-generated method stub

  }

  public void doSaveAs() {
    // TODO Auto-generated method stub

  }

  public boolean isDirty() {
    // TODO Auto-generated method stub
    return false;
  }

  public boolean isSaveAsAllowed() {
    // TODO Auto-generated method stub
    return false;
  }

  public boolean isSaveOnCloseNeeded() {
    // TODO Auto-generated method stub
    return false;
  }

}
