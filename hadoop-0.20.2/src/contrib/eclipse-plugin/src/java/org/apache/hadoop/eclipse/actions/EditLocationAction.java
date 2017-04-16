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
import org.apache.hadoop.eclipse.server.HadoopServer;
import org.apache.hadoop.eclipse.servers.HadoopLocationWizard;
import org.apache.hadoop.eclipse.view.servers.ServerView;
import org.eclipse.jface.action.Action;
import org.eclipse.jface.wizard.Wizard;
import org.eclipse.jface.wizard.WizardDialog;

/**
 * Editing server properties action
 */
public class EditLocationAction extends Action {

  private ServerView serverView;

  public EditLocationAction(ServerView serverView) {
    this.serverView = serverView;

    setText("Edit Hadoop location...");
    setImageDescriptor(ImageLibrary.get("server.view.action.location.edit"));
  }

  @Override
  public void run() {

    final HadoopServer server = serverView.getSelectedServer();
    if (server == null)
      return;

    WizardDialog dialog = new WizardDialog(null, new Wizard() {
      private HadoopLocationWizard page = new HadoopLocationWizard(server);

      @Override
      public void addPages() {
        super.addPages();
        setWindowTitle("Edit Hadoop location...");
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
