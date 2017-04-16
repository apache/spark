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

import org.apache.hadoop.eclipse.ImageLibrary;
import org.apache.hadoop.eclipse.servers.HadoopLocationWizard;
import org.eclipse.jface.action.Action;
import org.eclipse.jface.wizard.Wizard;
import org.eclipse.jface.wizard.WizardDialog;


/**
 * Action corresponding to creating a new MapReduce Server.
 */

public class NewLocationAction extends Action {
  public NewLocationAction() {
    setText("New Hadoop location...");
    setImageDescriptor(ImageLibrary.get("server.view.action.location.new"));
  }

  @Override
  public void run() {
    WizardDialog dialog = new WizardDialog(null, new Wizard() {
      private HadoopLocationWizard page = new HadoopLocationWizard();

      @Override
      public void addPages() {
        super.addPages();
        setWindowTitle("New Hadoop location...");
        addPage(page);
      }

      @Override
      public boolean performFinish() {
        page.performFinish();
        return true;
      }

    });

    dialog.create();
    dialog.setBlockOnOpen(true);
    dialog.open();

    super.run();
  }
}
